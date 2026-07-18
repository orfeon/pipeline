# Cross-Cloud Authentication (Design Document)

Status: **Accepted — Phase 0 done (AWS options wiring: `options.aws` region/endpoint/credentials
mapped onto Beam `AwsOptions` with `default`/`static`/`assumeRole` providers; user docs at
`docs/config/options/aws.md`),
Phase 1 done (`GcpCredentialsCache` central provider with `options.gcp.credentials` /
`MERCARI_PIPELINE_GCP_CREDENTIALS` sources, `MCredentialFactory` for Beam GCP IOs, metadata-server
guard `IAMUtil.isOnGcp()`, explicit-project error in `OptionUtil.getDefaultProject`; all direct
`getApplicationDefault()` call sites migrated except the tasks module slated for removal
(the `vertexai.gemini` transform has since been deleted); deploy guides remain scheduled for
Phase 5),
Phase 2 done (`credentials.type: gcpFederation`: `GcpFederatedAwsCredentialsProvider` — GCP ID
token → STS `AssumeRoleWithWebIdentity` — delivered to workers via `S3Options.s3ClientFactoryClass`
= `GcpFederatedS3ClientFactory` + string `FederationOptions` (see the §5.2 amendment on why the
Jackson-module approach was replaced); `S3Util.storage(PipelineOptions)` unifies schema-sampling
and runtime credentials, `StorageSource.s3` static keys deprecated with warning; the STS-exchange
LocalStack IT stays scheduled for Phase 5),
Phase 3 done (`SecretProvider`/`SecretProviders` with GCP SM / AWS SM / Vault backends,
`software.amazon.awssdk:secretsmanager` dependency added, jdbc/postgres/tidb/jdbc-sink/Hash call
sites migrated, `utils.secrets.get()` template function; Vault auth stays GCP-only until Phase 5,
Secrets Manager LocalStack IT scheduled with Phase 5),
Phase 4 done (`ResourceUtil` unified loader; `Config.load` and single-shot read sites across
config/queries/schema files/descriptors/models/microbatch checkpoints migrated — configs,
schemas and SQL now load from `s3://` as well as `gs://` and local paths; GCS-only
streaming/listing loaders intentionally kept on `StorageUtil`, see §6.2 notes),
Phase 5 done (Vault auth methods `VAULT_AUTH: gcp | aws-iam | token` with external tokens never
revoked and the gcp login-endpoint URL bug fixed; deploy guide at
`docs/deploy/cross-cloud-auth.md`; `AwsAuthIT` (LocalStack: s3 filesystem wiring, Secrets
Manager, STS federated exchange) and `VaultIT` (token auth) — **all phases complete**)**
Scope: how pipelines obtain GCP and AWS credentials so that a pipeline can run on either cloud and
access sources/sinks/secrets on the other one — Dataflow (GCP) reading/writing AWS resources, and
Flink/Spark on AWS (EMR, EMR on EKS, Amazon Managed Service for Apache Flink) reading/writing GCP
resources. Covers credentials, secret resolution, and config/schema file loading.

Out of scope (tracked separately, not blocked by this design):
- The Server's launch backends (Dataflow Flex Template / Dataproc Serverless) and its
  IAP-header caller identification (`X-Goog-Authenticated-User-Email`) — the Server remains
  GCP-hosted for now.
- Trimming GCP libraries out of AWS builds (`pom.xml` declares all GCP deps at compile scope
  regardless of runner profile). Auth abstraction does not require it; revisit if image size or
  CVE surface becomes a concern.
- Clouds other than GCP and AWS. Interfaces introduced here should not actively block a third
  provider, but we do not design for one (YAGNI).
- `vertexai.gemini` transform's token-refresh logic — resolved: the module has been deleted.

This document is the single source of truth for the cross-cloud auth work. Each implementation PR
should reference the relevant section rather than restating the design.

---

## 1. Problems in the current design

1. **No central credential provider.** ~15 classes independently call
   `GoogleCredentials.getApplicationDefault()` (e.g. `StorageUtil`, `PubSubUtil`, `FirestoreUtil`,
   `BigQueryUtil`, `DriveUtil`, `SecretManagerUtil`, `GcpFunctions`). The closest thing to a hub is
   `IAMUtil.getAccessToken()` (used by `ParameterManagerUtil`, `ArtifactRegistryUtil`,
   `DataprocUtil`). There is no injection point to substitute how credentials are obtained.
2. **AWS options are dead code.** `Options.setOptions` wires `GCPOptions` unconditionally but never
   calls `AWSOptions.setOptions`; the `AWSOptions` credentials provider was a stub returning `null`.
   Runtime S3 IO silently depends on whatever ambient credentials the AWS default provider chain
   finds.
3. **Split-brain S3 auth.** `StorageSource` uses per-module static keys (`s3.accessKey/secretKey`)
   only for construction-time schema sampling via `S3Util`, while the actual data IO goes through
   Beam's S3 filesystem using `AwsOptions` — two different credential sources for one module.
4. **Cloud-neutral modules transitively require GCP auth.** `jdbc`/`postgres`/`tidb` resolve
   passwords through GCP Secret Manager (`SecretManagerUtil.isSecretName` → `getSecret`); ~18
   modules load schema/config files through `StorageUtil` (GCS-only); FreeMarker template functions
   `gcp.secret()`/`gcp.accessToken()` are globally available; `Config.load` dispatches `gs://`,
   Parameter Manager, and `ar://` schemes that all need GCP auth at launch time.
5. **GCE metadata assumptions.** `IAMUtil` calls `metadata.google.internal` endpoints and
   `ServiceOptions.getDefaultProjectId()` is used for default project resolution
   (`OptionUtil.getDefaultProject`, `GcpFunctions.project`) — both hang or fail off-GCP.
6. **Vault is GCP-locked.** `VaultClient` only implements the Vault GCP auth backend (login with a
   GCP-signed JWT) and fails at construction without GCP credentials.

## 2. Design principles

### P1 — Don't distribute keys; federate identities

Static keys (AWS access keys, GCP service-account key JSON) are supported only as a fallback. The
primary mechanism in both directions is **workload identity federation**:

- **AWS → GCP**: GCP Workload Identity Federation natively trusts AWS IAM roles. An
  `external_account` credential config (a JSON file containing role/audience references, **no key
  material** — safe to commit or embed in configs) lets the google-auth library exchange the
  worker's AWS role identity for GCP credentials. Existing ADC call sites work unmodified once the
  config is visible to them.
- **GCP → AWS**: AWS STS accepts Google as a standard OIDC issuer. A GCP service-account ID token
  passed to `AssumeRoleWithWebIdentity` yields temporary AWS credentials — no AWS key material on
  Dataflow workers.

### P2 — Abstract the consuming services, not the credential

A unified `Credential` interface across clouds is meaningless (an OAuth2 access token and a SigV4
signing key are not interchangeable). Both SDKs already resolve credentials from the environment
(ADC; AWS default provider chain). What we abstract instead are the **functions that consume
auth**: secret resolution, object-storage/file access, and config loading.

### P3 — Lazy and guarded GCP access

A pipeline that uses no GCP-backed feature must run on AWS with zero GCP credentials present.
Nothing may touch ADC or the GCE metadata server unless a GCP feature is actually exercised.

### P4 — Explicit configuration off-cloud

Metadata-derived defaults (project ID, region) only exist on the home cloud. When running
off-cloud, required identifiers must come from config, and missing ones must fail **at assembly
time** with a precise message, not at runtime with an SDK stack trace.

## 3. Authentication matrix

| Runs on \ accesses | GCP resources | AWS resources |
|---|---|---|
| **Dataflow (GCP)** | ADC (native, unchanged) | STS `AssumeRoleWithWebIdentity` with a GCP SA ID token (§5) |
| **EMR / MSF (AWS)** | GCP WIF `external_account` config resolved through ADC / central provider (§4) | IAM role via default provider chain (native) |

## 4. Direction A — running on AWS, accessing GCP resources

### 4.1 Central GCP credential provider (`GcpCredentialsCache`)

A single class replaces the scattered `getApplicationDefault()` calls:

```java
public class GcpCredentialsCache {
    // Resolution order:
    //   1. explicit options.gcp.credentials (inline JSON, classpath:, gs://, s3://, or file path)
    //   2. MERCARI_PIPELINE_GCP_CREDENTIALS env var / system property (same source syntax;
    //      for workers whose environment the user controls, e.g. EMR executors)
    //   3. ADC — which itself honors GOOGLE_APPLICATION_CREDENTIALS, then the metadata server
    public static GoogleCredentials credentials();
    public static AccessToken accessToken();   // absorbs IAMUtil.getAccessToken()
}
```

`classpath:` and inline-JSON support is what makes **Amazon Managed Service for Apache Flink**
viable: MSF does not allow arbitrary environment variables, so the WIF config must be bundled in
the application jar (or embedded in the pipeline config — it contains no secrets, see P1) and
loaded programmatically via `GoogleCredentials.fromStream`.

Beam's GCP IOs (BigQueryIO, SpannerIO, …) obtain credentials through
`GcpOptions.getGcpCredential` → `CredentialFactory`. We provide a custom `CredentialFactory`
(set via `GcpOptions.setCredentialFactoryClass`) backed by the same resolution logic, so module
code and Beam IO share one credential source.

### 4.2 Metadata-server guard

`IAMUtil`'s `metadata.google.internal` calls get an "is this GCP?" guard (env heuristics +
one cached reachability probe with a short timeout). Off GCP, metadata paths are skipped
immediately instead of hanging.

### 4.3 Explicit project resolution

When GCP modules are used, `options.gcp.project` becomes required if
`ServiceOptions.getDefaultProjectId()` returns null (i.e. off-GCP without `GOOGLE_CLOUD_PROJECT`).
Validated at assembly time.

### 4.4 Worker distribution of the WIF config

- **EMR (Spark)**: `spark-submit --files` + `GOOGLE_APPLICATION_CREDENTIALS` on executors.
- **EMR (Flink on YARN)**: ship files + env, same idea.
- **MSF**: jar-bundled `classpath:` config via `options.gcp.credentials` (§4.1).

These become deploy-guide documentation (`docs/deploy/`), not code.

## 5. Direction B — running on GCP (Dataflow), accessing AWS resources

### 5.1 `AWSOptions` wiring (Phase 0)

`Options.setOptions` calls `AWSOptions.setOptions` unconditionally (null-guarded like
`GCPOptions`). The options block grows a credentials section:

```yaml
options:
  aws:
    region: ap-northeast-1
    endpoint: http://localhost:4566        # optional; emulators (LocalStack)
    credentials:
      type: default | static | assumeRole  # optional; inferred from fields when omitted
      accessKey: "..."                     # static (fallback only; prefer roles)
      secretKey: "..."
      sessionToken: "..."                  # static, optional
      roleArn: arn:aws:iam::123456789012:role/pipeline-role   # assumeRole
      roleSessionName: mercari-pipeline    # assumeRole, optional
      externalId: "..."                    # assumeRole, optional
      durationSeconds: 3600                # assumeRole, optional
```

This is mapped onto Beam's `AwsOptions` (`awsRegion`, `endpoint`, `awsCredentialsProvider`), which
both Beam's S3 filesystem and any aws2 IO consume. When no credentials block is given, Beam's
default (`DefaultCredentialsProvider`, i.e. env/profile/role chain) applies — the correct behavior
for EMR/MSF where the worker's IAM role is the identity.

**Serialization constraint (verified against Beam 2.74 `AwsModule`):** Beam serializes
`AwsOptions.awsCredentialsProvider` to workers via Jackson, and supports only a fixed set —
`DefaultCredentialsProvider`, `StaticCredentialsProvider`, `Profile/EnvironmentVariable/
SystemProperty/Container/AnonymousCredentialsProvider`, `StsAssumeRoleCredentialsProvider`,
`StsAssumeRoleWithWebIdentityCredentialsProvider`. Anything else throws
`Unsupported AWS credentials provider type`. Phase 0 therefore only emits supported types.

### 5.2 `GcpFederatedAwsCredentialsProvider` (Phase 2)

A serializable `AwsCredentialsProvider` for Dataflow→AWS:

1. On the worker, mint a GCP ID token from the worker credentials
   (`GcpCredentialsCache.credentials()` as `IdTokenProvider` — compute-engine and
   service-account credentials qualify) with the configured audience.
2. Exchange it via STS `AssumeRoleWithWebIdentity` (an **unsigned** call — the STS client uses
   `AnonymousCredentialsProvider`) for the configured `roleArn`.
3. Cache and refresh before expiry (120s margin).

**How it reaches workers.** The original idea — a custom Jackson module so Beam's options
ObjectMapper could round-trip the provider inside `AwsOptions` — does not work deterministically:
`AwsModule`'s (de)serializer is attached to the `AwsCredentialsProvider` interface via a mixin,
throws on unknown `@type`s, and a competing mixin registration wins or loses purely on module
registration order. The provider must therefore **never be set into `AwsOptions`** (Dataflow
serializes all set options at submission). Instead:

- the federation parameters ride as plain string options
  (`GcpFederatedAwsCredentialsProvider.FederationOptions`), and
- `S3Options.s3ClientFactoryClass` — Beam's official extension point, serialized as a class
  name — is set to `GcpFederatedS3ClientFactory`, which applies Beam's default builder wiring
  and swaps in the federated provider on the worker.

Config surface: `credentials.type: gcpFederation` + `roleArn` (+ optional `audience` defaulting
to the role ARN, `roleSessionName`, `durationSeconds`). Coverage: Beam's `s3://` filesystem and
`S3Util` — the entire AWS surface used by modules today. A future aws2 IO (Kinesis/SQS/...)
would need per-IO wiring via its `ClientConfiguration`; noted in §8.

### 5.3 `S3Util` unification

`S3Util.storage(accessKey, secretKey, region)` and `StorageSource.S3Parameters` static keys are
deprecated. Construction-time schema sampling builds its client through the same
`S3Options.s3ClientFactoryClass` chain as the runtime IO (`S3Util.storage(PipelineOptions)`),
removing the split-brain of §1.3 and covering `gcpFederation` automatically. `S3Parameters`
keeps working for one deprecation cycle (warning at assembly).

## 6. Common layers

### 6.1 `SecretProvider`

```java
public interface SecretProvider {
    boolean matches(String reference);
    String resolve(String reference);
}
```

Implementations, dispatched by reference syntax:

| Provider | Reference syntax | Notes |
|---|---|---|
| GCP Secret Manager | `projects/{p}/secrets/{s}/versions/{v}` | existing pattern, backward compatible |
| AWS Secrets Manager | `arn:aws:secretsmanager:...` (region from the ARN) or `aws-sm://name` (region from `options.aws.region`) | new; adds `software.amazon.awssdk:secretsmanager` dependency |
| HashiCorp Vault | `vault://v1/{kv-path}#{field}` | wraps existing `VaultClient`; connection from `VAULT_ADDR`/`VAULT_NAMESPACE`/`VAULT_ROLE`/`VAULT_AUTH_SERVICE_ACCOUNT` env vars (GCP auth only until Phase 5) |

The registry (`SecretProviders`) is configured with the pipeline options by `Options.setOptions`,
so launcher-side AWS resolution honors `options.aws` (region/endpoint/credentials incl.
`gcpFederation`). Call sites migrated: `JdbcSource`, `JdbcSink`, `PostgresSource`, `TiDBSource`
(user/password), `util/pipeline/select/Hash` (HMAC key). A cloud-neutral
`utils.secrets.get("...")` template function is added; `utils.gcp.secret` stays as a deprecated
GCP-only alias.

### 6.2 Unified file loading over Beam `FileSystems`

`Config.load` and the `StorageUtil.readString/readBytes` call sites (schema files, proto
descriptors, queries — ~18 modules) move to a loader built on `FileSystems.open()`
(`util/domain/file/ResourceUtil`: read/write/exists over `gs://`, `s3://`, and local paths).
Beam already abstracts the schemes including credentials (via §4/§5 wiring), so both work in
both directions without new abstraction. GCP-only schemes (`ar://`, Parameter Manager resources,
Pub/Sub subscription) remain explicit branches in `Config.load`.

Implementation notes:
- Beam's `FileSystems` statically registers only the local filesystem; `gs`/`s3` handlers appear
  with `setDefaultPipelineOptions`. `Options.setOptions` registers the fully-wired options, but
  `Config.load` runs earlier — `ResourceUtil` therefore registers default options once as a
  fallback when a scheme is missing, and never clobbers an existing registration. Config loading
  from `s3://` thus authenticates via the ambient default chain (env/role), not `options.aws`
  (which lives inside the config being loaded).
- Sites that stay on `StorageUtil` (GCS-only by design, not gated by `gs://`-vs-`s3://`):
  directory-listing/streaming loaders (`MHuggingFaceTokenizer`, `TokenAnalyzer` dictionaries,
  `OnnxModel`/`OnnxGenModel` client-based reads, `PDFExtractTransform` runtime reads) and GCS
  write/list/copy operations in sinks.

### 6.3 Template functions

Add cloud-neutral `utils.secrets.get()` (backed by 6.1). `gcp.*` namespace stays for
compatibility; `gcp.project()`/`gcp.account()` gain the §4.2 guard so merely *registering* the
functions never triggers GCP access off-cloud.

### 6.4 `VaultClient` auth methods

`VAULT_AUTH: gcp | aws-iam | token` (env var / system property; previously hard-coded to the GCP
backend). `aws-iam` logs into Vault's aws auth backend with a SigV4-signed STS
`GetCallerIdentity` request (AWS default credentials chain; optional
`VAULT_AWS_IAM_SERVER_ID` header); `token` accepts a pre-issued token from `VAULT_TOKEN` —
externally supplied tokens are never revoked by the client (login tokens still are). When
`VAULT_AUTH` is unset it is inferred from `VAULT_TOKEN` / `VAULT_AUTH_SERVICE_ACCOUNT`.

## 7. Phases

| Phase | Deliverable | Outcome |
|---|---|---|
| **0** | Wire `AWSOptions.setOptions` into `Options.setOptions`; implement `default`/`static`/`assumeRole` provider mapping + `endpoint`; docs + tests. (`sts` is already on the classpath via beam aws2; no pom change.) | S3 pipelines on EMR/MSF authenticate via the worker's IAM role, configured — not accidental |
| **1** | `GcpCredentialsCache` + metadata guard + custom `CredentialFactory` + explicit-project validation | **Direction A works**: EMR/MSF → BigQuery/GCS/Spanner via WIF |
| **2** | `GcpFederatedAwsCredentialsProvider` + Jackson module; `S3Util`/`S3Parameters` unification & deprecation | **Direction B works**: Dataflow → S3 etc. without AWS keys |
| **3** | `SecretProvider` (GCP SM / AWS SM / Vault) + `secret()` template function; add `secretsmanager` dependency | jdbc/postgres/tidb usable with either cloud's secret store |
| **4** | `FileSystems`-based unified loader for `Config.load` / `StorageUtil` call sites | configs & schema files loadable from `gs://` and `s3://` everywhere |
| **5** | Vault auth methods; deploy guides (EMR / MSF / WIF setup); LocalStack ITs consolidated | docs + test hardening |

Phases 1 and 2 are independent; order may be swapped by demand. Each phase is a separate PR
referencing this document.

## 8. Known limitations

- **Google Workspace modules (`drive`, sheets) off-GCP**: domain-wide delegation scenarios can
  require SA key material that WIF cannot replace; impersonation-based paths work only with
  `service_account_impersonation` configured in the WIF config. Documented as "requires extra
  setup or unsupported on AWS".
- **`IAMUtil.signJwt`** off-GCP requires the WIF config to impersonate a service account (the
  federated identity itself cannot sign).
- Cross-cloud data paths incur egress cost and latency; the deploy guides must say so.
- Beam `AwsModule` limits which provider types survive options serialization (§5.1) and cannot
  be extended deterministically; custom providers must flow through a factory-class option
  (§5.2), which today covers the s3 filesystem only. Adding an aws2 IO module (Kinesis/SQS/...)
  later requires wiring the federated provider into that IO's `ClientConfiguration`.

## 9. Testing

- Unit: options mapping (`AWSOptionsTest`) — region/endpoint/provider type per config shape,
  including a Jackson round-trip through discovered modules to catch "Unsupported provider" early.
- IT (Testcontainers, `*IT.java`, skipped by default, `-DskipITs=false`): `AwsAuthIT`
  (LocalStack — s3:// read/write through the options-wired filesystem, Secrets Manager
  resolution by ARN and `aws-sm://`, and the STS `AssumeRoleWithWebIdentity` exchange with a
  stubbed ID token) and `VaultIT` (dev-mode Vault — `vault://` resolution with token auth,
  non-revocation of external tokens). GCP WIF cannot be emulated locally; Direction A gets
  coverage via unit tests around resolution order plus the setup guide
  (`docs/deploy/cross-cloud-auth.md`).
