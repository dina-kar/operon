# Architecture overview

Object storage is the source of truth, in open formats. A stateless gateway and router speak the native, Qdrant, Elasticsearch and Flight SQL protocols; the log is the spine; TiKV (or an embedded Raft metastore) holds metadata and transactions.

- Diagram and summary: [README](https://github.com/ostrium-labs/loams/blob/main/README.md#architecture)
- Full design: [docs/design](https://github.com/ostrium-labs/loams/blob/main/docs/design/README.md) (start with the pitch and the architecture)
- Decisions: [decision log](https://github.com/ostrium-labs/loams/blob/main/docs/design/13-decision-log.md)
- What is open and what is not: [open-core boundary](https://github.com/ostrium-labs/loams/blob/main/docs/open-core.md)
