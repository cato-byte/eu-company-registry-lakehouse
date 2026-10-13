-- One-time Unity Catalog setup for production. Run in a Databricks SQL editor as a workspace /
-- metastore admin, after replacing every <placeholder>. See docs/deployment.md.
--
-- Prerequisite (UI): Catalog Explorer > External data > Credentials > Create credential
--   type "AWS IAM role", name `registry_s3`. Databricks shows the trust policy to put on the role.
--   The role needs s3:GetObject/PutObject/DeleteObject/ListBucket on the landing bucket and on the
--   bucket/prefix that serves the website data.

-- 1. Where the raw files and the website data live
--    <landing-bucket>: the existing raw-data bucket. <site-bucket>/<site-data-prefix>: the bucket that serves
--    the website, e.g. prefix en/projects/eu-company-registry/data, next to the project page.
CREATE EXTERNAL LOCATION IF NOT EXISTS registry_landing
  URL 's3://<landing-bucket>/company_records/lakehouse'
  WITH (STORAGE CREDENTIAL `registry_s3`)
  COMMENT 'Landing zone written by the ingestion Lambdas';

CREATE EXTERNAL LOCATION IF NOT EXISTS registry_site_data
  URL 's3://<site-bucket>/<site-data-prefix>'
  WITH (STORAGE CREDENTIAL `registry_s3`)
  COMMENT 'Static JSON served by the website';

-- 2. Catalog. Managed tables (bronze/silver/gold/ops) are stored under the landing bucket.
--    Omit MANAGED LOCATION if the metastore already has default storage.
CREATE CATALOG IF NOT EXISTS registry
  MANAGED LOCATION 's3://<landing-bucket>/company_records/lakehouse/managed'
  COMMENT 'EU company registry lakehouse';

CREATE SCHEMA IF NOT EXISTS registry.files COMMENT 'Volumes for raw files and published site data';

-- 3. Volumes the job reads and writes (the bundle defaults point here)
CREATE EXTERNAL VOLUME IF NOT EXISTS registry.files.landing
  LOCATION 's3://<landing-bucket>/company_records/lakehouse/landing';
CREATE EXTERNAL VOLUME IF NOT EXISTS registry.files.site_data
  LOCATION 's3://<site-bucket>/<site-data-prefix>';

-- 4. Least privilege for the deploying / running service principal (use its application id)
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG registry TO `<service-principal-application-id>`;
GRANT USE SCHEMA, READ VOLUME, WRITE VOLUME ON SCHEMA registry.files TO `<service-principal-application-id>`;

-- 5. Cost report task: read-only access to billing system tables
GRANT USE CATALOG ON CATALOG system TO `<service-principal-application-id>`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.billing TO `<service-principal-application-id>`;

-- 6. Optional: let yourself browse everything the job creates
GRANT USE CATALOG, USE SCHEMA, SELECT, READ VOLUME ON CATALOG registry TO `<your-email>`;
