# Security Policy

## Supported versions

Operon has no stable release yet. Security fixes are applied to the `main` branch.

## Reporting a vulnerability

**Do not open a public issue for security vulnerabilities.**

Report privately through GitHub's **"Report a vulnerability"** button (Security Advisories) on the repository, or contact the maintainers listed in [MAINTAINERS.md](MAINTAINERS.md).

Please include:

- affected component (gateway, log, meta, query, worker) and version or commit;
- steps to reproduce, or a proof of concept;
- impact assessment (for example data exposure across namespaces, auth bypass, durability loss).

We aim to acknowledge reports within 3 business days and to agree on a disclosure timeline with the reporter.

## Scope notes

Of particular interest: tenant (namespace) isolation, authentication and authorization in any protocol gateway, credential handling for object storage, and any path that could lose or corrupt acknowledged data.
