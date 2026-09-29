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
webhook endpoint is `POST /events/webhook` on the edge Service. Dapr's
HTTP binding is output-only, so inbound webhooks enter this app's HTTP
listener; its call to `operon-stream` uses Dapr's gRPC proxy.

Kafka and AI-agent publishers send JSON `{"event_id":"publisher-stable-id",
"payload":{...}}` to their respective topics. Dapr wraps this as a
CloudEvent. Webhook publishers send the same JSON body. The edge normalizes
these into records on `workflow-triggers` and includes a stable SHA-256 ID
derived from the source and publisher event ID. Dapr pub/sub acknowledges only
after `StreamService.Produce` succeeds; gRPC failures request redelivery.

The trigger stream consumer must use `resonate-invocation-id` as its durable
workflow invocation key. Stream production currently has no native atomic
deduplication, so duplicate deliveries can append multiple records; one
workflow execution requires the consumer's Resonate idempotency check. Keep
the same publisher `event_id` on retries.

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
need their own sidecars and the `operon-no-workflow` configuration. Those
deployments depend on the separate TiKV-backed Resonate server and worker
entrypoints and are not defined by this trigger edge package.
