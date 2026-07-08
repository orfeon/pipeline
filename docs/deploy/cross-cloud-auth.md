# Cross-Cloud Authentication Setup (GCP ⇔ AWS)

How to run pipelines on one cloud while reading/writing the other cloud's resources, without
distributing long-lived keys. Design background: [developer/cloud-auth.md](../developer/cloud-auth.md).

| Runs on | Accesses | Mechanism | Section |
|---|---|---|---|
| EMR / Managed Service for Apache Flink (AWS) | BigQuery, GCS, Spanner, ... | GCP Workload Identity Federation | [§1](#1-running-on-aws-accessing-gcp-resources) |
| Dataflow (GCP) | S3, Secrets Manager | AWS STS `AssumeRoleWithWebIdentity` | [§2](#2-running-on-gcp-dataflow-accessing-aws-resources) |

---

## 1. Running on AWS, accessing GCP resources

The worker's AWS IAM role is exchanged for GCP credentials via
[Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation) (WIF).
No service-account key files are involved: the WIF credential config is a plain JSON file with
role/audience references and **no key material**, so it can be committed or bundled in the jar.

### 1.1 One-time GCP setup

```sh
# workload identity pool + AWS provider
gcloud iam workload-identity-pools create pipeline-pool \
  --location=global --display-name="Pipeline AWS pool"
gcloud iam workload-identity-pools providers create-aws pipeline-aws \
  --location=global --workload-identity-pool=pipeline-pool \
  --account-id={AWS_ACCOUNT_ID}

# service account the federated identity impersonates
gcloud iam service-accounts create pipeline-worker
gcloud iam service-accounts add-iam-policy-binding \
  pipeline-worker@{PROJECT}.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/{PROJECT_NUMBER}/locations/global/workloadIdentityPools/pipeline-pool/attribute.aws_role/arn:aws:sts::{AWS_ACCOUNT_ID}:assumed-role/{EMR_OR_MSF_ROLE}"
# grant the service account the data-access roles the pipeline needs (BigQuery, GCS, ...)

# generate the credential config file (no secrets inside)
gcloud iam workload-identity-pools create-cred-config \
  projects/{PROJECT_NUMBER}/locations/global/workloadIdentityPools/pipeline-pool/providers/pipeline-aws \
  --service-account=pipeline-worker@{PROJECT}.iam.gserviceaccount.com \
  --aws --output-file=gcp-credentials.json
```

Note: signed-JWT minting (`utils.gcp.signJwt`, Drive impersonation) requires the
`--service-account` impersonation form shown above; direct resource access without impersonation
cannot sign. Google Workspace modules that need domain-wide delegation are not supported via WIF.

### 1.2 Point the pipeline at the credential config

The resolution order is: `options.gcp.credentials` → `MERCARI_PIPELINE_GCP_CREDENTIALS`
env var / system property → standard ADC (`GOOGLE_APPLICATION_CREDENTIALS`).

Also set `options.gcp.project` (or the `GOOGLE_CLOUD_PROJECT` env var) — off Google Cloud there
is no metadata server to infer it from.

**EMR (Spark)** — ship the file and use standard ADC on executors:

```sh
mvn clean package -DskipTests -Pspark
spark-submit \
  --files gcp-credentials.json \
  --conf spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=gcp-credentials.json \
  --conf spark.yarn.appMasterEnv.GOOGLE_APPLICATION_CREDENTIALS=gcp-credentials.json \
  --class com.mercari.solution.MPipeline pipeline-bundled.jar \
  --config=s3://my-bucket/config.yaml --runner=SparkRunner
```

**EMR (Flink on YARN)** — same idea with `-yt` (ship files) and
`containerized.taskmanager.env.GOOGLE_APPLICATION_CREDENTIALS`.

**Managed Service for Apache Flink** — MSF does not allow arbitrary environment variables;
bundle the file into the application jar (e.g. under `src/main/resources/`) and reference it
from the config instead:

```yaml
options:
  gcp:
    project: my-gcp-project
    credentials: classpath:gcp-credentials.json
```

The config itself can be loaded from `s3://...` (authenticated by the worker's IAM role).

### 1.3 Secrets

`jdbc`/`postgres`/`tidb` credentials and the `select` Hash key accept AWS Secrets Manager
references natively — no GCP required: a full secret ARN or `aws-sm://{name}`
(region from `options.aws.region`).

---

## 2. Running on GCP (Dataflow), accessing AWS resources

The Dataflow worker service account's OIDC ID token (Google is a standard OIDC issuer) is
exchanged for temporary AWS credentials via STS `AssumeRoleWithWebIdentity`. No AWS keys.

### 2.1 One-time AWS setup

1. Create an IAM **OIDC identity provider** for `https://accounts.google.com` with your chosen
   audience (e.g. `mercari-pipeline`).
2. Create the IAM role the pipeline will assume, with a trust policy like:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Federated": "arn:aws:iam::{AWS_ACCOUNT_ID}:oidc-provider/accounts.google.com" },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "accounts.google.com:aud": "mercari-pipeline",
        "accounts.google.com:sub": "{SERVICE_ACCOUNT_UNIQUE_ID}"
      }
    }
  }]
}
```

(`sub` is the worker service account's numeric unique ID — `gcloud iam service-accounts
describe ... --format='value(uniqueId)'`.)

3. Grant the role the S3 / Secrets Manager permissions the pipeline needs.

### 2.2 Pipeline options

```yaml
options:
  aws:
    region: ap-northeast-1
    credentials:
      type: gcpFederation
      roleArn: arn:aws:iam::{AWS_ACCOUNT_ID}:role/pipeline-role
      audience: mercari-pipeline
```

This applies to `s3://` access (storage/files modules, schema/query/config file loading) and
AWS Secrets Manager resolution. See [config/options/aws.md](../config/options/aws.md) for all
parameters.

---

## 3. HashiCorp Vault

`vault://v1/{kv-path}#{field}` secret references are configured via environment variables
(or system properties): `VAULT_ADDR` (required), `VAULT_NAMESPACE`, and `VAULT_AUTH`:

| `VAULT_AUTH` | Additional settings | Notes |
|---|---|---|
| `token` | `VAULT_TOKEN` | inferred when `VAULT_TOKEN` is set |
| `gcp` | `VAULT_AUTH_SERVICE_ACCOUNT`, `VAULT_ROLE` | Vault gcp auth backend (GCP-signed JWT); inferred when `VAULT_AUTH_SERVICE_ACCOUNT` is set |
| `aws-iam` | `VAULT_ROLE`, optional `VAULT_AWS_IAM_SERVER_ID` | Vault aws auth backend (iam method), signs with the AWS default credentials chain |

---

## 4. Caveats

- Cross-cloud data paths incur **egress cost and latency** on every read/write — colocate the
  heavy side of the pipeline with the data where possible.
- The pipeline config file itself, when loaded from the other cloud's storage, authenticates
  via the ambient default chain (worker role / env), not via `options.*` inside that config.
- Google Workspace modules (`drive`, sheets) may require key material that federation cannot
  replace (domain-wide delegation); treat them as unsupported off GCP.
- Custom AWS providers reach only the s3 filesystem and Secrets Manager today; a future
  Kinesis/SQS module would need per-IO wiring (developer/cloud-auth.md §8).
