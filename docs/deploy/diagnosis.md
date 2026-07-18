# Automated Dataflow failure diagnosis

The API server can diagnose failed Dataflow jobs automatically: a Cloud Logging **Log Router
sink** forwards job-failure log entries to **Pub/Sub**, whose **push subscription** calls the
server's webhook. The server collects the job's error information (Dataflow job messages +
worker error logs), lets the bundled agent analyze it against the framework's own source code,
and delivers a verdict to the configured destinations (Slack / generic webhook / a structured
Cloud Logging record).

```
Dataflow job fails
  → Cloud Logging (job-message "Workflow failed...")
  → Log Router sink → Pub/Sub topic → push subscription
  → POST https://<server>/webhook/events/dataflow
  → diagnose (job messages + worker logs + source code) → notify
```

## 1. Create the Pub/Sub topic and the Log Router sink

```bash
PROJECT=<your-project>
TOPIC=dataflow-failures

gcloud pubsub topics create ${TOPIC} --project ${PROJECT}

gcloud logging sinks create dataflow-failure-sink \
  pubsub.googleapis.com/projects/${PROJECT}/topics/${TOPIC} \
  --project ${PROJECT} \
  --log-filter='resource.type="dataflow_step" AND log_id("dataflow.googleapis.com/job-message") AND severity>=ERROR AND textPayload:"Workflow failed"'

# Allow the sink's writer identity to publish to the topic
WRITER=$(gcloud logging sinks describe dataflow-failure-sink --project ${PROJECT} --format='value(writerIdentity)')
gcloud pubsub topics add-iam-policy-binding ${TOPIC} --project ${PROJECT} \
  --member="${WRITER}" --role="roles/pubsub.publisher"
```

For streaming jobs you may also want to match `"Workflow failed"`-less error patterns; any
entry that carries `resource.labels.job_id` works — the server deduplicates per job (6h TTL),
so a broader filter only costs extra deliveries, not extra diagnoses.

## 2. Create the push subscription

```bash
SERVER_URL=https://<your-server-host>

gcloud pubsub subscriptions create dataflow-failure-push \
  --project ${PROJECT} \
  --topic ${TOPIC} \
  --push-endpoint="${SERVER_URL}/webhook/events/dataflow" \
  --push-auth-service-account=<invoker-sa>@${PROJECT}.iam.gserviceaccount.com \
  --ack-deadline=30
```

Authentication options (use at least one):

- **OIDC push auth** (`--push-auth-service-account`, recommended) with an ingress that
  validates the token (e.g. Cloud Run / IAP).
- **Shared secret**: set env `MERCARI_PIPELINE_WEBHOOK_TOKEN` on the server and append
  `?token=<secret>` to the push endpoint URL (or send the `X-Mercari-Pipeline-Token` header).
  Without the env var set, the endpoint performs no authentication of its own.

The endpoint answers `204` on acceptance (including duplicates and non-job events) and `429`
when its diagnosis queue is full, which makes Pub/Sub redeliver later. Diagnosis runs
asynchronously on a single background worker.

## 3. Configure the server

| Env var | Meaning |
|---|---|
| `MERCARI_PIPELINE_DATAFLOW_PROJECT` / `MERCARI_PIPELINE_DATAFLOW_REGION` | Defaults for job lookup (the event's own project/region take precedence) |
| `MERCARI_PIPELINE_WEBHOOK_TOKEN` | Optional shared secret for the webhook endpoint |
| `MERCARI_PIPELINE_DIAGNOSIS_SLACK_WEBHOOK` | Slack incoming-webhook URL to post the verdict to |
| `MERCARI_PIPELINE_DIAGNOSIS_WEBHOOK` | Generic endpoint that receives the full diagnosis record as JSON |
| `MERCARI_PIPELINE_DIAGNOSIS_LOG_NAME` | Cloud Logging log name for the structured diagnosis history (default `mercari-pipeline-diagnosis`) |
| `MERCARI_PIPELINE_AGENT_MODEL` / `MERCARI_PIPELINE_AGENT_LOCATION` | Gemini model used by the agents (default `gemini-3.1-flash-lite` / `global`) |

The server's service account needs, in addition to its launch permissions:

- `roles/dataflow.viewer` — read job state and job messages
- `roles/logging.viewer` — read worker error logs
- `roles/logging.logWriter` — write the diagnosis history record
- read access to wherever configs referenced by job parameters live (e.g. GCS), if configs
  are not passed inline

## 4. Diagnosis history

Every diagnosis is written as a `jsonPayload` record to the configured log name:

```
logName="projects/<project>/logs/mercari-pipeline-diagnosis"
jsonPayload.jobId, jsonPayload.diagnosis.causeCategory, jsonPayload.diagnosis.summary, ...
```

Query it from Logs Explorer (or via the agent) to spot recurring failures, e.g.:

```
logName="projects/<project>/logs/mercari-pipeline-diagnosis"
jsonPayload.diagnosis.causeCategory="framework_bug"
```

Notes:

- Deduplication is in-memory (per server instance). With multiple replicas or across restarts,
  a failure may occasionally be diagnosed and notified more than once.
- Diagnosed verdicts are advisory: the agent never mutates jobs or configs on its own.
