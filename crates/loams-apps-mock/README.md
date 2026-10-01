# loams-apps-mock

A stateful mock of the Loams app protos (design [§37](../../docs/design/37-desktop-and-mobile-apps.md) §12, plan [AP0](../../docs/plans/2026-10-01-ap0-app-protos.md) Task 5): `loams.instance.v1`, `loams.devices.v1`, `loams.approvals.v1`, `loams.operations.v1` and `loams.notifications.v1`, over Connect (binary and JSON), gRPC and gRPC-Web on one loopback listener. The console, Loams Desktop and the phone apps (`ostrium-labs/loams-mobile`) develop and test against it until the server side (AP4) exists.

```bash
cargo run -p loams-apps-mock                        # http://127.0.0.1:8084
cargo run -p loams-apps-mock -- --heartbeat-secs 2  # faster heartbeats while debugging
```

```bash
# Connect JSON with curl: no auth needed for GetInstance.
curl -s -H 'content-type: application/json' -d '{}' \
  http://127.0.0.1:8084/loams.instance.v1.InstanceService/GetInstance
# Everything else takes a fake bearer token.
curl -s -H 'content-type: application/json' -H 'authorization: Bearer mock-access-usr_omar' \
  -d '{}' http://127.0.0.1:8084/loams.approvals.v1.ApprovalService/ListApprovals
```

## Fake credentials

Nothing here is a secret, and the mock refuses a non-loopback `--listen` (D111).

| Token | Meaning |
|---|---|
| `Bearer mock-access-<principal>` | A session authenticated now (decides `STEP_UP_SESSION` approvals) |
| `Bearer mock-stale-<principal>` | A session authenticated 10 minutes ago (gets `step_up_required`) |

Seed principals: `usr_dana` (a developer), `usr_omar` (an approver), `agt_claude` (an agent acting for Dana). The seed has a pending destructive approval in the protected `env_prod` (requested by the agent, so Dana cannot approve it), a pending low-risk one in `env_dev`, an expired one, a running import, an inbox notification and Omar's paired phone.

## What is real and what is stubbed

| Area | State |
|---|---|
| `GetInstance`, `WhoAmI` (actor chain for agents) | Real, from the seed |
| Approvals: list, get, decide | Real: revisions, idempotency keys, the rules in `src/acceptance.rs` (expiry, already decided, stale revision, reason required, requester cannot approve, step-up) |
| `WatchApprovals` | Real: snapshot, upserts and removes, heartbeats, resume from a cursor, `snapshot_reset` for an unknown or evicted cursor |
| Decision proofs (compact JWS) | **Stub**: a request with a proof is refused with `not_implemented`, never trusted. Verification against seed device keys is the next AP0 step |
| Devices: `CreatePairing` (QR payload v1), list, rename, revoke | Real, in memory |
| Push targets, notification preferences, test notifications | **Stub** (`unimplemented`, reason `not_implemented`) |
| Operations: get, list | Real, from the seed; `WatchOperations` is a snapshot then heartbeats; `CancelOperation` is a stub |
| Notifications: list, mark read | Real; `WatchNotifications` is a snapshot then heartbeats |
| YAML scenarios (AP0 Ruling 9) | **Not yet**: the seed is fixed |
| The pairing grant and the RFC 8693 exchange | Not here: they belong to the unified auth plan |

CORS allows the console's Vite dev server (`http://localhost:5173`) and the Tauri webview origins, for development builds only.
