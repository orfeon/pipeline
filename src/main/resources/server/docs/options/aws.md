# AWS Options

Options related to AWS. These settings are applied to Beam's
[AwsOptions](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/aws2/options/AwsOptions.html),
which is used by the `s3://` filesystem (storage/files modules) and AWS IOs.

## AWS options

| parameter   | type        | description                                                                                          |
|-------------|-------------|--------------------------------------------------------------------------------------------------------|
| region      | String      | AWS region, e.g. `ap-northeast-1`.                                                                   |
| endpoint    | String      | Service endpoint override, e.g. `http://localhost:4566` for a LocalStack emulator.                   |
| credentials | Credentials | How AWS credentials are obtained. If omitted, the AWS default credentials provider chain is used (environment variables, profile, EC2/ECS/EKS role) — the recommended setup when running on AWS with an IAM role attached to the workers. |

## Credentials options

| parameter       | type    | description                                                                                                                          |
|-----------------|---------|----------------------------------------------------------------------------------------------------------------------------------------|
| type            | String  | One of `default`, `static`, `assumeRole`, `gcpFederation`. If omitted, inferred: `assumeRole` when `roleArn` is set, `static` when `accessKey` is set, otherwise `default` (`gcpFederation` is never inferred). |
| accessKey       | String  | (`static`) Access key ID. Prefer IAM roles or `assumeRole`; static keys are a fallback only.                                         |
| secretKey       | String  | (`static`) Secret access key.                                                                                                        |
| sessionToken    | String  | (`static`, optional) Session token for temporary credentials.                                                                        |
| roleArn         | String  | (`assumeRole`, `gcpFederation`) ARN of the IAM role to assume via STS.                                                               |
| roleSessionName | String  | (`assumeRole`, `gcpFederation`, optional) Session name. Defaults to `mercari-pipeline`.                                              |
| externalId      | String  | (`assumeRole`, optional) External ID required by the role's trust policy.                                                            |
| durationSeconds | Integer | (`assumeRole`, `gcpFederation`, optional) Session duration.                                                                          |
| audience        | String  | (`gcpFederation`, optional) OIDC audience of the GCP ID token; must match an audience configured on the AWS IAM OIDC identity provider for `accounts.google.com`. Defaults to `roleArn`. |

### `gcpFederation` — access AWS from pipelines running on Google Cloud, without AWS keys

The worker's GCP identity (e.g. the Dataflow worker service account) mints an OIDC ID token,
which STS `AssumeRoleWithWebIdentity` exchanges for temporary credentials of `roleArn`.
Setup on the AWS side: create an IAM OIDC identity provider for `accounts.google.com` with your
chosen `audience`, and let the role's trust policy allow `sts:AssumeRoleWithWebIdentity` for that
provider (optionally conditioned on `accounts.google.com:sub` = the service account's unique ID).
Currently applied to `s3://` access (the storage/files modules).

## Example

```yaml
options:
  aws:
    region: ap-northeast-1
    credentials:
      roleArn: arn:aws:iam::123456789012:role/pipeline-role
```

```yaml
# Workers run with an IAM role (EMR / Managed Service for Apache Flink): region only.
options:
  aws:
    region: ap-northeast-1
```

```yaml
# Running on Dataflow, reading/writing s3:// without AWS keys (GCP identity federation).
options:
  aws:
    region: ap-northeast-1
    credentials:
      type: gcpFederation
      roleArn: arn:aws:iam::123456789012:role/pipeline-role
      audience: mercari-pipeline
```
