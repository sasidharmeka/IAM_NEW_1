# IAM analytics pipeline — manual and low cost

This project publishes the included synthetic IAM events, converts them to
Parquet, runs 22 PySpark detections in one Managed Service for Apache Spark
(Dataproc Serverless) batch, and writes the current findings to BigQuery.

There is no Composer, Airflow, Cloud Run, Workflows, Scheduler, Docker image,
always-on VM, or automatic cloud deployment. The only cloud entry point is a
manual GitHub Actions workflow.

## Do I need to edit the code?

No. For the included data and workflow, do not put project IDs, bucket names,
keys, or service-account emails in the source code. Configure the selected
GitHub Environment as described below, then choose an operation in Actions.

The only normal reason to edit code is to change detections or add a new source
schema. The sample CSV files can be replaced with data that has the same column
names.

## What runs

```text
Manual GitHub Actions run
  ├─ validate-only: lint + tests; no GCP authentication
  └─ cloud run
       ├─ producer.py -> one Pub/Sub topic
       ├─ consumer.py -> gs://BUCKET/source/SOURCE/*.parquet
       └─ full-pipeline only -> one TTL-bounded Spark batch -> BigQuery
```

The producer validates all six CSV files before publishing anything. The
consumer acknowledges messages only after every Parquet upload succeeds. The
Spark job deduplicates source IDs, computes all detections in one batch, and
overwrites the current result tables so retries do not append duplicates.

## Fastest GitHub setup

### 1. Create or choose one GCP service account

For a personal demo, the simplest setup uses one service account for GitHub and
the Spark runtime. The commands below are a one-time setup, not part of each
pipeline run. Replace only `YOUR_PROJECT_ID`.

```bash
export PROJECT_ID="YOUR_PROJECT_ID"
export SA_NAME="iam-pipeline"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud config set project "${PROJECT_ID}"
gcloud iam service-accounts create "${SA_NAME}" \
  --display-name="IAM pipeline demo"

for ROLE in \
  roles/serviceusage.serviceUsageAdmin \
  roles/storage.admin \
  roles/pubsub.editor \
  roles/bigquery.admin \
  roles/dataproc.editor \
  roles/dataproc.worker
do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${ROLE}"
done

gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/iam.serviceAccountUser"

gcloud iam service-accounts keys create iam-pipeline-key.json \
  --iam-account="${SA_EMAIL}"
```

These roles are deliberately simple for a standalone demo project. For a
shared or production project, split the GitHub deploy identity from the Spark
runtime identity and reduce permissions. If your organization blocks service
account keys, use Workload Identity Federation instead.

Never commit `iam-pipeline-key.json`. Copy its entire JSON content into the
GitHub secret in the next step, verify authentication, and remove the local
copy.

### 2. Create a GitHub Environment

Open **Repository → Settings → Environments**, create `dev`, and add these
Environment variables:

| GitHub Environment variable | Required for | Value |
| --- | --- | --- |
| `GCP_PROJECT_ID` | ingestion/full | GCP project ID, not the project name |
| `BUCKET_NAME` | ingestion/full | Globally unique bucket name, without `gs://` |
| `BQ_DATASET` | full only | For example `iam_data_dev`; letters, numbers, underscores |
| `DATAPROC_SERVICE_ACCOUNT` | full only | Service-account email, such as `iam-pipeline@PROJECT_ID.iam.gserviceaccount.com` |
| `GCP_REGION` | optional | Defaults to `us-central1` |

For the fastest authentication path, add one Environment secret:

| GitHub Environment secret | Value |
| --- | --- |
| `GCP_SA_KEY` | The entire contents of `iam-pipeline-key.json` |

Leave `WIF_PROVIDER` and `DEPLOY_SERVICE_ACCOUNT` unset when using
`GCP_SA_KEY`.

For keyless authentication, do the opposite: omit `GCP_SA_KEY` and add these
Environment variables after configuring a GitHub Workload Identity provider:

| GitHub Environment variable | Value |
| --- | --- |
| `WIF_PROVIDER` | Full provider resource name |
| `DEPLOY_SERVICE_ACCOUNT` | Service account that GitHub may impersonate |

You can later create a separate `prod` Environment with different values. No
code changes are required.

The bucket, Dataproc batch, and BigQuery dataset all use `GCP_REGION`. Keeping
them in one region avoids an indirect BigQuery-load location mismatch.

### 3. Run it manually

Open **Actions → IAM Pipeline (Manual, Low Cost) → Run workflow**. Select the
Environment and one operation:

| Operation | Cloud behavior | Cost posture |
| --- | --- | --- |
| `validate-only` | Runs compile, lint, and tests; never authenticates to GCP | No GCP usage |
| `ingestion-only` | Creates/reuses Pub/Sub and Storage, then writes Parquet | Tiny sample-data usage; no Spark |
| `full-pipeline` | Runs ingestion, then exactly one Spark batch and BigQuery writes | Paid serverless compute while the batch runs |

Start with `validate-only`. Then use `ingestion-only`. Run `full-pipeline` only
when both succeed.

## Cost guardrails

- Nothing runs on pushes, pull requests, or a schedule.
- `validate-only` cannot reach the cloud job because it has no GCP credentials.
- One Environment can have only one pipeline run at a time.
- A full run submits exactly one Spark batch using runtime 2.2.
- Spark dynamic allocation is capped at two executors for this small dataset.
- Dataproc receives `--ttl=30m`; GitHub stops waiting after 45 minutes.
- Pub/Sub unacknowledged-message retention is one day.
- A newly created bucket receives a 30-day object-deletion lifecycle rule.
- Soft delete is disabled only on a newly created dedicated bucket, preventing
  deleted demo objects from continuing to incur storage charges.
- An existing bucket is reused without changing its lifecycle, because it may
  contain unrelated data. Add a lifecycle manually if it is dedicated to this
  project.
- Topics, subscriptions, buckets, datasets, and result tables remain after a
  run, but none of them is an always-on compute service.

These controls bound runtime, not currency. Cloud rates, minimums, retries, and
other resources in the same billing account can still affect the bill. Create a
small Google Cloud budget and alerts before the first full run. Budget alerts
warn you; they do not automatically stop services.

## Mac setup and local validation

GitHub runs do not require local Python or Java. Install these only if you want
to develop or test on your Mac.

```bash
brew install python@3.12 openjdk@17
export PATH="$(brew --prefix openjdk@17)/bin:${PATH}"
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"

python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
python -m compileall -q .
ruff check .
pytest -q
```

Apple Silicon and Intel Macs are both supported because `brew --prefix`
resolves the correct Homebrew path.

To run the Spark transformations locally without writing to BigQuery, first
provide local Parquet files under one directory per source, then use:

```bash
spark-submit spark_job/spark_transformation_job.py \
  --env=dev \
  --input_root=file:///absolute/path/to/source \
  --bq_project=local-project \
  --bq_dataset=iam_data_dev \
  --run_id=local-001 \
  --dry_run
```

The GitHub path is easier for the first end-to-end cloud run because it handles
resource creation, authentication, ingestion, packaging, and Spark submission.

## Repository map

```text
.github/workflows/run-pipeline.yaml  only deployment workflow
config/gcs-lifecycle.json            cleanup rule for a newly created bucket
data/                                six synthetic IAM CSV sources
pipeline_config.py                   source names, files, IDs, required columns
producer.py                          validates and publishes CSV rows
consumer.py                          pulls, deduplicates, writes Parquet, then acks
ingest.py                            bounded producer + consumer entry point
spark_job/schemas.py                 explicit Spark schemas
spark_job/detections.py              22 IAM detections
spark_job/spark_transformation_job.py consolidated Spark/BigQuery job
tests/                               ingestion, workflow, and Spark validation
```

## BigQuery outputs

The full run replaces these tables:

```text
impossible_login_anomalies           mfa_bypass_events
mfa_bypass_events_all_apps           impossible_travel_events
privilege_escalation_without_request ad_volume_spikes
rejected_but_executed                high_risk_sequence
excessive_data_transferred           role_drift
stale_access                         multi_app_single_session
mfa_drift                            suspicious_approver_behavior
ad_group_churn_rate                  orphan_event_detection
orphan_user_detection               cleanest_users
shadow_access_events                 risk_scores
identity_timelines                   suspicious_time_warp
```

Each result includes a detection name, deterministic finding ID, event date,
pipeline run ID, and detection timestamp.

## If an old Composer environment still exists

Removing Composer code from this repository does not delete an already-created
Composer environment. List it first:

```bash
gcloud composer environments list --project YOUR_PROJECT_ID
```

Only after confirming that it is not shared and that nothing must be preserved,
delete the exact environment explicitly:

```bash
gcloud composer environments delete ENVIRONMENT_NAME \
  --location=REGION \
  --project=YOUR_PROJECT_ID
```

That deletion is destructive and is intentionally not automated here.
# IAM-new
