# cordis patches

`@loams/cordis` pins `cordis@4.0.0-rc.10` (MIT, © Shigma). Every fix we need before upstream ships it is a `pnpm patch` logged here (AP1a Ruling 1): the upstream issue or PR, why, and when to remove it. Vendor into `web/vendor/cordis` only when a patch lives longer than 30 days or upstream is silent for 90 (Q427).

| # | Patch | Upstream | Why | Remove when |
|---|---|---|---|---|
| — | none yet | | | |

The harness's vendored cordis (`@deepseek-ai/cordis` 4.0.1, 18 logged modifications) is a reference for fixes we may need (re-entrant fiber disposal, transactional config reload). A fix is ported as our own patch with a reference, never by copying the vendored tree (§37 §3.3).
