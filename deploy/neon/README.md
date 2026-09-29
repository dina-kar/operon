# Neon on RustFS (development)

The stack from the Neon + WeSQL spike ([§23](../../docs/design/23-neon-and-wesql.md)). It runs
Neon's storage (storage broker, one pageserver, one safekeeper) on a RustFS bucket and starts
Postgres computes against an explicit tenant and timeline. Loam does not link Neon; in the
design, Loam plays the part of the control plane that the commands below play by hand.

It is derived from `neondatabase/neon` `docker-compose/` (Apache-2.0). `compute/config.json` is
Neon's compute spec with one safekeeper; `compute/start.sh` replaces Neon's `compute.sh`, because
the `compute-node` image has no `curl`, `jq` or `nc`, and tenants and timelines are created by
the caller.

Not a production layout: no storage controller, no Neon proxy, no TLS, default credentials,
`fsync = off` in the compute spec, and the safekeeper keeps WAL in its container filesystem.
Images are `latest`, which is the last public build (2025-08-26, digest
`sha256:ead56a7b…` for `neon`); pin by digest before relying on it.

Resources in the spike: about 5 GB (`neon`) plus 1.3 GB (`compute-node-v16`) of images; storage
services under 0.5 GB of RAM together.

## Run

With Docker, or Podman plus `DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock`:

```sh
docker compose up -d rustfs create-bucket storage_broker pageserver safekeeper1

# Create a tenant and its first timeline through the pageserver API.
export TENANT_ID=$(openssl rand -hex 16) TIMELINE_ID=$(openssl rand -hex 16)
curl -X PUT -H 'Content-Type: application/json' \
  -d '{"mode":"AttachedSingle","generation":1,"tenant_conf":{}}' \
  localhost:9898/v1/tenant/$TENANT_ID/location_config
curl -X POST -H 'Content-Type: application/json' \
  -d "{\"new_timeline_id\":\"$TIMELINE_ID\",\"pg_version\":16}" \
  localhost:9898/v1/tenant/$TENANT_ID/timeline/

docker compose up -d compute1
PGPASSWORD=cloud_admin psql -h 127.0.0.1 -p 55433 -U cloud_admin postgres
```

## Branch

A branch is a new timeline whose ancestor is an existing one. It takes tens of milliseconds and
copies no data:

```sh
export BRANCH_TIMELINE_ID=$(openssl rand -hex 16)
curl -X POST -H 'Content-Type: application/json' \
  -d "{\"new_timeline_id\":\"$BRANCH_TIMELINE_ID\",\"ancestor_timeline_id\":\"$TIMELINE_ID\"}" \
  localhost:9898/v1/tenant/$TENANT_ID/timeline/
docker compose --profile branch up -d compute2
PGPASSWORD=cloud_admin psql -h 127.0.0.1 -p 55434 -U cloud_admin postgres
```

Add `"ancestor_start_lsn": "<lsn>"` to branch from a point in the past.

## Stop

```sh
docker compose --profile branch down -v
```
