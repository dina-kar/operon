# Dapr trigger edge

Build `operon-trigger-edge:dev` from the repository root with
`docker build -f deploy/dapr/edge/Dockerfile -t operon-trigger-edge:dev .`,
then set the image to one available in your cluster and apply this directory
with `kubectl apply -k deploy/dapr` after installing Dapr, TiKV, and Kafka.
The `operon-stream` Deployment is a one-replica integration topology using
`operon dev`, with an image built using `--features tikv,stream-grpc,durable-tikv`,
and a persistent volume.
Set the Operon image and PD service name for your cluster. Create the
`workflow-triggers` stream before sending events. Do not scale this Deployment:
the dev process has a fixed node ID. A production multi-replica layout needs
`operon cluster` peers and object storage configuration.
The adapter is Rust and depends on `dapr` with default features disabled;
the default-enabled Dapr Workflow SDK feature is therefore absent. The
webhook endpoint is `POST /events/webhook` on the edge Service. It answers
401 unless the `operon-trigger-edge-webhook` Secret (key `token`) is set and
the caller sends `Authorization: Bearer <token>`. The pub/sub routes
(`/events/kafka`, `/events/agent`) accept only the pod's own Dapr sidecar,
over loopback; add a NetworkPolicy if other workloads must not reach the
edge Service at all. The sidecar also forwards service invocations over
loopback, so each app gets its own Configuration with a deny-by-default
`accessControl` policy: nothing may invoke `operon-trigger-edge`, and only
`operon-trigger-edge` may invoke `operon-stream`. The guard refuses to start
unless the Configuration denies invocation by default. These policies rely
on Dapr mTLS identities (see below). Use Dapr 1.15 or later: the Configuration denies every
Workflow API version (stable, beta and alpha), and the guard checks all of
them. Dapr's
HTTP binding is output-only, so inbound webhooks enter this app's HTTP
listener; its call to `operon-stream` uses Dapr's gRPC proxy.

Kafka and AI-agent publishers publish through Dapr pub/sub; Dapr wraps each
message as a CloudEvent and delivers it to the edge as
`application/cloudevents+json`. Webhook publishers send a CloudEvent in
binary mode (`ce-*` headers) or structured mode. The edge passes the event to
`StreamService.ProduceCloudEvents` unchanged: a structured event goes through
as the JSON it arrived as, a binary-mode event as a protobuf event. Nothing
is renamed or rewritten, and a request that is not a CloudEvent is refused
(`DROP` for pub/sub, 415 for the webhook). The stream stores each event in the
CloudEvents Kafka binding's layout (`ce_*` headers) and deduplicates by
`source` + `id` (design 02, section 7.4): a redelivered event, or a `Produce`
that timed out after appending, is answered `duplicate` and appended once.
Dapr pub/sub acknowledges (`SUCCESS`) after the append or a duplicate; a
failed call or an event another request is still appending answers `RETRY`;
an invalid event answers `DROP`. Publishers keep `id` stable across their own
retries (Dapr's `cloudevent.id` publish metadata, or a CloudEvent they build
themselves).

The trigger stream consumer keys each workflow invocation by the event's
idempotency key, SHA-256(`source`, 0x00, `id`), or by the record's `ce_source`
and `ce_id` headers. The stream's deduplication window (one hour by default,
`--cloudevents-dedup-window`, at most 24 hours) bounds how long a redelivery
is recognized; the consumer's Resonate idempotency check covers anything
older.

The deployment's init container reads the live Dapr Configuration and
Components via Kubernetes API. It fails startup if the Workflow APIs are not
denied or a Workflow component is present. The app repeats this check on
startup. The edge Service exposes only the app's HTTP port; it does not expose
the sidecar's API externally.

Dapr service-to-service mTLS is controlled by the installed Dapr control
plane's `daprsystem` configuration. It must be enabled in that installation;
the application Configuration cannot turn it on for a single sidecar.

The Operon stream service must run with a Dapr sidecar carrying app ID
`operon-stream`, `dapr.io/app-protocol: grpc`, and its gRPC listener as
`dapr.io/app-port`. The stream gRPC listener has no authentication, so
Operon serves it on loopback only (`--stream-grpc-listen 127.0.0.1:8091`);
the sidecar in the same pod reaches it there. The Resonate service and each workflow worker likewise
need their own sidecars and the `operon-no-workflow` configuration (or,
for a service other apps invoke, its own Configuration naming its callers). Those
deployments depend on the separate TiKV-backed Resonate server and worker
entrypoints and are not defined by this trigger edge package.
