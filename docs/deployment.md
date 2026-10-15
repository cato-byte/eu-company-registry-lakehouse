# Deployment (CI/CD to production)

```
pull request ──► ci: lint · secret scan · bundle validation (dev+prod, strict) · unit · e2e
push to main ──► cd: ci again ──► production approval ──► AWS ingestion (SAM)
                                                     └──► Databricks job (Asset Bundle)
```

* **Databricks** is deployed with `databricks bundle deploy -t prod`, as a **service principal**
  over OAuth M2M. The production job runs as that principal, from a folder only it can write.
  Each deploy builds a wheel with a unique version (`0.2.<run>+g<sha>`), so serverless never
  reuses a stale wheel.
* **AWS** is deployed with `sam deploy`, through **GitHub OIDC**: a short-lived role that only the
  `production` environment of this repository can assume. No AWS keys are stored in GitHub.
* Both deploy jobs are **off** until you switch them on (repository variables `DEPLOY_DATABRICKS` and
  `DEPLOY_AWS`), so the pipeline stays green while the setup below is incomplete.
* Nothing identifying is in the repository. Host, bucket, e-mail and ids are GitHub **secrets**,
  which are masked in the (public) Actions logs.

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

**How the code reaches Databricks.** Databricks never pulls from GitHub. The CD workflow pushes:
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

**Which bucket?** Keep the existing raw-data bucket that already holds `company_records/lakehouse/landing/`.
One bucket per purpose (raw data, website, query results) is the usual standard. Separate buckets
per environment (dev/prod) are common in companies, but not needed for a one-person project.
Publish the website data into the bucket that already serves your website, under the project page's
prefix, so the page and its data share one origin (no CORS setup). Put the Databricks workspace in
the **same AWS region** as the buckets, to avoid cross-region transfer costs.

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
   (e.g. one created for earlier notebooks) already covers that S3 path. Skip that statement and keep
   the rest: Unity Catalog uses the existing location, whose credential then needs read/write on those prefixes.
5. **Secret scope** (with your own CLI login):
   ```bash
   databricks secrets create-scope registry-lakehouse
   databricks secrets put-secret registry-lakehouse pseudonym-key   # long random string, never change it
   databricks secrets put-secret registry-lakehouse alert-webhook   # optional Slack/Teams webhook URL
   databricks secrets put-acl registry-lakehouse <service-principal-application-id> READ
   ```

### 3. GitHub: environment, secrets, switches

Repository → Settings → Environments → **New environment `production`**:

* *Deployment branches*: `main` only. *Required reviewers*: yourself (each deploy waits for your click).
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

* **Geocoding worker trigger:** S3 bucket → Properties → Event notifications → *All object create
  events*, prefix `company_records/lakehouse/landing/_GEOCODE/requests/`, suffix `.json`, destination
  the `GeocodeWorkerFunction` Lambda (its name is in the stack outputs).
* **Confirm the SNS e-mail subscription** you receive.
* **Fresh snapshots:** the old files sit directly under `landing/<COUNTRY>/`, and Norway's is still the
  raw JSON array. Either wait for the schedules, or start each download once from the Lambda console
  (test events = the JSON files in `infra/requests/` and `infra/requests/reference/`). Then move the
  old undated files away (e.g. to `landing_archive/`), so only dated `ingest_date=` snapshots remain.
* **Old Delta folders** from the notebook era (`lakehouse/bronze`, `staging`, `silver_vault`, `gold`)
  are no longer used and can be deleted to save storage.

### 5. First production run

Actions → `cd` → *Run workflow* with **run_job** checked, or Databricks → Jobs →
`registry-lakehouse-prod` → *Run now*. After 3-5 runs, calibrate the cost model (`docs/cost.md`).

## Website

The `publish` task writes `cities.geojson`, `fishing.geojson`, `industry_by_country.json`,
`pipeline.json` and `meta.json` into the `registry.files.site_data` volume, which is an S3 prefix
you control. Serve `site/index.html` with a `data/` folder pointing at that prefix (same bucket
behind CloudFront, or a sync into your website repo). If the JSON is served from another origin,
allow it with an S3 CORS rule for your site's domain.

## Rollback

* **Code:** revert the commit on `main`, and CD redeploys the previous version.
* **Data:** `RESTORE TABLE registry.silver.company TO VERSION AS OF <n>` (see `docs/operations.md`).
