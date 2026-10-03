# The conformance corpus

Design §44 §10.4, SDK1 Task 4: the fixtures every SDK's suite replays, so
thirteen clients can be compared against the same bytes.

```
index.json          what the corpus covers, one row per case
recorded/*.json     one case each: the request that produced it and the exact
                    status, headers and body the server answered with
```

**Recorded, not written.** Every case was captured from a real `loams dev` by
`sdks/conformance/record-fixtures.mjs` and is replayed by
`sdks/conformance/fixture-server.mjs`. A hand-written expectation would only
prove that the SDK agrees with whoever wrote it; a recording proves it agrees
with the server.

```
loams dev --listen 127.0.0.1:8080 &
LOAMS_TEST_ENDPOINT=http://127.0.0.1:8080 node sdks/conformance/record-fixtures.mjs
git diff -- sdks/fixtures          # re-recording is only meaningful if nothing changes
```

`sdks/conformance/run.sh` does the same round trip and fails on a diff.

## What the corpus covers, and why the encoding matters

Three RPCs, each in four encodings, plus the streaming refusal:

| RPC | What it is |
|---|---|
| `InstanceService/GetInstance` | a successful call, no auth — the first thing any client makes |
| `InstanceService/WhoAmI` | a structured-reason error (`not_implemented`: this build has no auth) |
| `LiveService/Query` | the unavailable-service path on a unary RPC (`feature_not_in_variant`) |
| `LiveService/Watch` | the same refusal on a server stream |

Each unary RPC is recorded four times, because **the response body is not
interchangeable between encodings**: `application/json` (proto3 JSON, what
`curl` sends), `application/proto`, `application/grpc-web+proto` and
`application/grpc-web+json`. An SDK's transport picks one and the corpus cannot
know which, so it carries all four and the fixture server keys on the exact
content type. Handing a gRPC-Web client a JSON body shows up as a parse error
inside somebody's SDK, which is the worst place for a fixture mismatch to
surface.

Two more distinctions the corpus keeps:

- **HTTP status is not the error.** Connect answers 501 with a JSON body;
  gRPC-Web answers 200 and puts the code in the trailers. A client that reads
  only status codes passes the first and silently fails the second.
- **The refusal on a stream is not an HTTP status either.** It arrives inside
  the Connect streaming envelope, so a streaming client that reports it has to
  read the envelope.

## What it does not cover yet

`loams.collection.v1.ListCollections` and the rest of the API1 surface, so:
pagination, idempotency-keyed mutations, the consistency token, and a stream
that actually carries data. Those arrive with the RPCs (API1 Tasks 2–4) and are
recorded the same way. Until then each SDK's suite pins the SDK's own half of
those behaviours against a stub, and says so — see
`docs/sdk/runtime-contract.md`, where every clause names the test that pins it
and what it does not pin.
