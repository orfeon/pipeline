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
| type            | String  | One of `default`, `static`, `assumeRole`. If omitted, inferred: `assumeRole` when `roleArn` is set, `static` when `accessKey` is set, otherwise `default`. |
| accessKey       | String  | (`static`) Access key ID. Prefer IAM roles or `assumeRole`; static keys are a fallback only.                                         |
| secretKey       | String  | (`static`) Secret access key.                                                                                                        |
| sessionToken    | String  | (`static`, optional) Session token for temporary credentials.                                                                        |
| roleArn         | String  | (`assumeRole`) ARN of the IAM role to assume via STS.                                                                                |
| roleSessionName | String  | (`assumeRole`, optional) Session name. Defaults to `mercari-pipeline`.                                                               |
| externalId      | String  | (`assumeRole`, optional) External ID required by the role's trust policy.                                                            |
| durationSeconds | Integer | (`assumeRole`, optional) Session duration.                                                                                           |

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
