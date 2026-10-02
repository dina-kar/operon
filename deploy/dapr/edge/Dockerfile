# Build from the repository root. The adapter compiles the same stream proto
# as Loams, and the Dapr SDK is built without its default Workflow feature.
FROM rust:1.97-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY deploy/dapr/edge/Cargo.toml deploy/dapr/edge/Cargo.lock deploy/dapr/edge/build.rs /src/deploy/dapr/edge/
COPY deploy/dapr/edge/src /src/deploy/dapr/edge/src
COPY crates/loams-stream-grpc/proto /src/crates/loams-stream-grpc/proto
RUN cargo build --release --locked --manifest-path deploy/dapr/edge/Cargo.toml

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/deploy/dapr/edge/target/release/loams-trigger-edge /usr/local/bin/loams-trigger-edge
USER 65532:65532
ENTRYPOINT ["loams-trigger-edge"]
