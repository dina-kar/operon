# WeSQL on RustFS (development)

The stack from the Neon + WeSQL spike ([§23](../../docs/design/23-neon-and-wesql.md)): one
WeSQL server (apecloud, MySQL 8.0.35 with the SmartEngine storage engine on object storage,
**GPL-2.0-only**) on a RustFS bucket. Loams runs it as a separate, unmodified service and never
links it.

Two settings matter on RustFS:

- WeSQL's S3 client (the AWS C++ SDK) always uses virtual-hosted-style URLs
  (`http://wesql.rustfs:9000/…`). RustFS therefore needs `RUSTFS_SERVER_DOMAINS`, and the
  network needs the alias `wesql.rustfs`. Without them, initialization retries for about five
  minutes and fails with `curlCode: 6, Couldn't resolve host name`.
- `WESQL_OBJECTSTORE_PROVIDER=minio` with `WESQL_OBJECTSTORE_ENDPOINT=http://rustfs:9000`.

Keep the `wesql-data` volume. In the spike, commits made after the last object-store snapshot
were lost when the container was replaced without its volume, although their binlog slices were
in the bucket (§23 §9.2).

Known limits found in the spike: every table is SmartEngine (`ENGINE=InnoDB` is rewritten), and
SmartEngine refuses foreign keys (`ERROR 1235: SE currently doesn't support foreign key
constraints`), so Forgejo's migrations fail. The newest image is `8.0.35-0.1.0_beta5.40`
(2025-01-21).

## Run

With Docker, or Podman plus `DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock`:

```sh
docker compose up -d
# Ready after about 75 s on first start; the server accepts connections a few seconds before
# it can write ("Consensus Not Leader"), so retry the first write.
mysql -h 127.0.0.1 -P 13306 -uroot -ploams-dev
docker compose down
```

To delete the data as well (destructive: removes the `wesql-data` and `rustfs-data` volumes):

```sh
docker compose down -v
```
