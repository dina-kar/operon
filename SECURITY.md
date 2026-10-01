# Security Policy

## Supported versions

Loams has no stable release yet. Security fixes land on the `main` branch only. Once releases start, this section will list the supported release lines.

## Reporting a vulnerability

**Please do not report security vulnerabilities in public issues, pull requests or discussions.**

Report them privately through GitHub's private vulnerability reporting:

1. Open the repository's **Security** tab and choose **Report a vulnerability** ([direct link](../../security/advisories/new)).
2. Fill in the advisory form. Only you and the maintainers can see it.

If you cannot use GitHub, email **security@ostriumlabs.com**. Please don't put vulnerability details in a public channel while you wait for a reply.

Please include:

- the affected component (for example a gateway, the log, the metastore, the query engine, Loams Live or Loams Durable) and the version or commit;
- steps to reproduce, or a proof of concept;
- your assessment of the impact, for example data exposure across namespaces, an authentication bypass, or loss of acknowledged writes.

## What to expect

- We acknowledge a report within **3 business days**.
- We keep you updated while we investigate, and agree on a disclosure timeline with you. The default is to publish an advisory once a fix is on `main`, within **90 days** of the report.
- We credit reporters in the advisory unless you ask us not to.

## Scope

Of particular interest:

- isolation between tenants and namespaces;
- authentication and authorization in any protocol gateway (native, Flight SQL, Qdrant, Elasticsearch, Postgres, Resonate);
- handling of object-storage and TiKV credentials;
- server-side request forgery, for example through durable task delivery targets;
- any path that could lose or corrupt acknowledged data.

Out of scope: findings that need a compromised host or bucket, denial of service through unbounded requests on a loopback-bound, unauthenticated development server (the default for `loams dev`), and issues in third-party dependencies that are already public (please report those upstream; we track advisories with `cargo-deny`).
