# Deployment (CI/CD to production)

```
pull request ──► ci: lint · secret scan · bundle validation (dev+prod, strict) · unit · e2e
push to main ──► cd: ci again ──► production approval ──► AWS ingestion (SAM)
                                                     └──► Databricks job (Asset Bundle)
```

* Databricks is deployed with `databricks bundle deploy -t prod` by a service principal over
  OAuth M2M. The production job runs as that principal, from a folder only it can write. Each
  deploy builds a wheel with a unique version (`<release>+<run>.g<sha>`, e.g. `1.0.0+57.g1a2b3c4`),
  so serverless never picks up a stale wheel, and the version still names the release it came from.
* AWS is deployed with `sam deploy` through GitHub OIDC, using a short-lived role that only the
  `production` environment of this repository can assume. No AWS keys are stored in GitHub.
* Both deploy jobs stay off until the repository variables `DEPLOY_DATABRICKS` and `DEPLOY_AWS`
  are set, so CI stays green while the setup below is incomplete.
* Host, bucket, e-mail and ids are GitHub environment secrets, which are masked in the public
  Actions logs. None of them is in the repository.
* A deploy never starts a run. `bundle deploy` only creates or updates the job definition; runs
  start from the schedule, from *Run now*, or from a manual `cd` run with `run_job` checked.

## How the pieces connect

```
   GitHub (code)                       AWS (data)                          Databricks (compute)
 ┌──────────────┐  CD pushes code   ┌──────────────────────────┐        ┌─────────────────────────────┐
 │ this repo    │──────────────────────────────────────────────────────►│ job + wheel (Python package)│
 │              │  CD deploys       │ Lambda downloads registers│        │ runs on serverless compute  │
 │              │──────────────────►│   ▼                       │        │   │ reads/writes through     │
 └──────────────┘                   │ S3 bucket                 │◄───────┼───┘ UC volumes             │
                                    │  .../lakehouse/landing    │  IAM role (storage credential)       │
                                    │  .../lakehouse/managed    │        └─────────────────────────────┘
                                    │ website bucket .../data   │◄─── publish task writes site JSON
                                    └──────────────────────────┘
```

**How the code reaches Databricks.** Databricks never pulls from GitHub; the CD workflow pushes.
`databricks bundle deploy` builds the Python package in `src/` into a wheel, uploads it with the job
definition to the workspace (`/Workspace/Users/<service principal>/.bundle/...`), and creates or
updates the job through the Jobs API. When the schedule fires, serverless compute starts, installs
the wheel, and calls `registry-lakehouse bronze --country FR ...` for each task.

**How Databricks finds the bucket.** The bucket name is never in the code or the repo. It lives only
in Unity Catalog, set up once with `infra/databricks/setup.sql`:

1. an **IAM role** in your AWS account allows reading and writing the bucket,
2. a **storage credential** in Databricks holds that role's ARN,
3. an **external location** says "Databricks may use this credential for `s3://<bucket>/<prefix>`",
4. a **volume** (`registry.files.landing`) gives that S3 prefix a Databricks path:
   `/Volumes/registry/files/landing`,
5. the job receives `--landing-root /Volumes/registry/files/landing` (bundle default), and the code
   reads files there as if they were local.

**Buckets.** The landing prefix can live in an existing raw-data bucket; the job only needs the
`.../lakehouse/` prefix. Keep one bucket per purpose (raw data, website). Separate dev and prod
buckets become worth it once more than one person deploys. Write the website data into the bucket
that serves the site, under the project page's prefix, so page and data share an origin and no
CORS rule is needed. Keep the Databricks workspace in the same AWS region as the buckets to avoid
cross-region transfer charges.

## One-time setup

### 1. AWS: let GitHub deploy the ingestion stack

No new bucket is needed (see above). This creates only the IAM role GitHub Actions uses.

```bash
aws cloudformation deploy --template-file infra/aws/github-deploy-role.yaml \
  --stack-name eu-registry-github-deploy --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides LandingBucket=<landing-bucket>
# add CreateOidcProvider=false if the account already trusts token.actions.githubusercontent.com
aws cloudformation describe-stacks --stack-name eu-registry-github-deploy \
  --query "Stacks[0].Outputs[?OutputKey=='DeployRoleArn'].OutputValue" --output text
```

If you run it with a non-admin IAM user, that user needs a temporary inline policy for the
`cloudformation:*ChangeSet*` / `DescribeStacks` actions on this stack, plus create/get/update/delete on
the `eu-registry-github-deploy` role and the GitHub OIDC provider. Remove the policy afterwards: the
power to edit a role includes the power to change who may assume it. If a first attempt failed,
the stack may sit in `REVIEW_IN_PROGRESS`; re-running `deploy` recovers it.
If CD then fails with *Not authorized to perform sts:AssumeRoleWithWebIdentity*, the token's `sub`
claim differs from `GitHubSubjectPrefix`: repositories with immutable OIDC subjects send
`repo:<owner>@<id>/<repo>@<id>`, others send `repo:<owner>/<repo>`.

### 2. Databricks: service principal and Unity Catalog

1. **Service principal:** Settings → Identity and access → Service principals → Add. Name it
   `registry-lakehouse-deployer`. Under *Secrets*, generate an OAuth secret, and note the
   **application id** and the **secret**.
2. **Entitlements:** give it *Workspace access*, and allow it to use serverless compute (default in
   most workspaces).
3. **Storage credential:** Catalog → External data → Credentials → Create → *AWS IAM role*, named
   `registry_s3`. Follow the trust-policy instructions shown, and give the role read/write on the
   landing bucket and on the website-data bucket/prefix.
4. **Catalog, volumes and grants:** run `infra/databricks/setup.sql` after filling in the placeholders.
   If a `CREATE EXTERNAL LOCATION` fails with `LOCATION_OVERLAP`, an existing external location
   (for example one created for other workloads) already covers that S3 path. Skip that statement and keep
   the rest: Unity Catalog uses the existing location, whose credential then needs read/write on those prefixes.
   For the website prefix, give that credential's IAM role an inline policy limited to the `data/`
   prefix (GetBucketLocation on the bucket; ListBucket with an `s3:prefix` condition; Get/Put/DeleteObject
   on `<prefix>/*`). An existing location marked *read-only* must be switched to read/write.
   Grants on the `system` catalog (step 5) need a **metastore admin** (account console → Catalog →
   metastore). Without them only the `cost_report` task is skipped, and it logs a warning.
5. **Secret scope** (with your own CLI login):
   ```bash
   databricks secrets create-scope registry-lakehouse
   databricks secrets put-secret registry-lakehouse pseudonym-key   # long random string, never change it
   databricks secrets put-secret registry-lakehouse alert-webhook   # optional Slack/Teams webhook URL
   databricks secrets put-acl registry-lakehouse <service-principal-application-id> READ
   ```

### 3. GitHub: environment, secrets, switches

Repository → Settings → Environments → **New environment `production`**:

* *Deployment branches and tags*: branch `main` and tags `v*` (a rollback deploys a release tag).
  *Required reviewers*: yourself (each deploy waits for your click).
* **Environment secrets:**

| Secret | Value |
|---|---|
| `DATABRICKS_HOST` | `https://<workspace>.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | service principal application id |
| `DATABRICKS_CLIENT_SECRET` | its OAuth secret |
| `ALERT_EMAIL` | where failure / slow-run / Lambda alarms go |
| `AWS_DEPLOY_ROLE_ARN` | output of step 1 |
| `LANDING_BUCKET` | landing bucket name |

* **Environment variable:** `AWS_REGION` (e.g. `eu-west-1`). Optional: `DATABRICKS_CATALOG` (default `registry`).

Repository → Settings → Secrets and variables → Actions → **Variables** (repository level):
`DEPLOY_DATABRICKS=true` and `DEPLOY_AWS=true` once steps 1-2 are done.

Also recommended: Settings → Code security → enable **secret scanning and push protection**, and
Settings → Branches → protect `main` (require the `ci` check and a PR).

### 4. After the first AWS deploy

* **Geocoding worker:** nothing to do. It runs daily at 06:00 UTC and processes request files that
  have no results yet. (An S3 event notification would conflict with Databricks *file events*,
  which already own an S3 → SQS rule over the lakehouse prefix.)
* **Confirm the SNS e-mail subscription** you receive.
* **First snapshots:** wait for the monthly schedules, or invoke the downloader once per register
  from the Lambda console (the test events are the JSON files in `infra/requests/` and
  `infra/requests/reference/`). The newest `landing/<COUNTRY>/ingest_date=YYYY-MM-DD/` folder wins.
  Undated files directly under `landing/<COUNTRY>/` are only a fallback when no dated snapshot
  exists, and can be archived once the downloader has landed one.

### 5. First production run

Actions → `cd` → *Run workflow* with **run_job** checked, or Databricks → Jobs →
`registry-lakehouse-prod` → *Run now*. After 3-5 runs, calibrate the cost model (`docs/cost.md`).

## Website

The `publish` task writes `cities.geojson`, `fishing.geojson`, `industry_by_country.json`,
`pipeline.json`, `insights.json` and `meta.json` into the `registry.files.site_data` volume, which is an S3 prefix
you control. Serve `site/index.html` with a `data/` folder pointing at that prefix (same bucket
behind CloudFront, or a sync into your website repo). If the JSON is served from another origin,
allow it with an S3 CORS rule for your site's domain.

## Releases

`main` is always deployable; a release marks a version worth returning to. The version lives in
`src/registry_lakehouse/__init__.py` and the notes in `CHANGELOG.md`.

1. On a branch, set the version (`python tools/set_version.py 1.1.0`) and move the
   *Unreleased* notes in `CHANGELOG.md` under a `## [1.1.0] - <date>` heading. Merge it.
2. Tag the merge commit on `main` and push the tag:
   ```bash
   git switch main && git pull
   git tag -a v1.1.0 -m "v1.1.0" && git push origin v1.1.0
   ```
3. The `release` workflow runs the full CI, checks that the tag is on `main`, matches
   `__version__` and has changelog notes, then publishes a GitHub release with the wheel,
   `SHA256SUMS` and those notes.

A release deploys nothing by itself: the same commit was already deployed when it merged.

## Rollback

* **Code, to a release:** Actions → `cd` → *Run workflow* → *Use workflow from* the tag, e.g.
  `v1.0.0` (or `gh workflow run cd.yml --ref v1.0.0`). CD runs the tests and redeploys the job and
  the Lambdas exactly as tagged. The wheel is rebuilt from the tag, stamped `1.0.0+<run>.g<sha>`;
  compare its source with the release's wheel if you need to be sure. The next merge to `main`
  deploys `main` again, so fix forward from there.
* **Code, one change:** revert the commit on `main`, and CD redeploys without it.
* **Data:** `RESTORE TABLE registry.silver.company TO VERSION AS OF <n>` (see `docs/operations.md`).
