# Publish your project to the Loams Cloud Marketplace

Status: **draft developer guide**. The marketplace is being built; the listing and install flow are not live yet, and this guide describes the process partners should plan for. Where a detail is not decided, it says so. Nothing here is an announcement of a partnership or listing.

The Loams Cloud marketplace lets Loams Cloud users install third-party software, such as an OSS project or a vendor's product, into their own Loams environment. Listings install **from the same open package a self-hoster uses**, so what you publish is also what anyone can run themselves.

## Who can list

- **OSS projects**: any project under an OSI-approved license, listed by its maintainers or with their consent.
- **Vendors**: companies shipping a product, open or commercial, that runs on Loams. A commercial product still installs from an open package format; the product's license is yours.

You must be able to show that you control the project or have the owner's permission. Listing doesn't imply endorsement by Loams, and you must follow the [trademark policy](../../TRADEMARKS.md).

## The package

A listing is one **package**: the same manifest-described package a self-hoster installs without the marketplace. A package is a versioned, signed release of your project, plus a manifest.

The manifest schema is **not yet published**; it lands, versioned, with the marketplace. Plan for the manifest to say at least:

| Field group | What it declares |
|---|---|
| Identity | Name, version, license (SPDX), source repository, publisher |
| Contents | What to install: container images by digest, Helm chart or deploy template, functions or jobs |
| Requirements | The Loams version range and the Loams capabilities it uses (streams, collections, jobs, secrets, ...) |
| Permissions | The **consent scopes** it requests (see below) |
| Configuration | The inputs a user is asked for, with defaults and which are secrets |
| Support | Documentation, issue tracker and contact |

Build on the [deploy-target convention](../ecosystem/deploy-buttons.md): a package that works with a "Deploy to" template is most of the way to a marketplace package.

## Security and review requirements

Every listing must meet these before it is published, and on every update.

1. **Integrity principle.** The package does what its manifest says, and the manifest is complete. Hidden network calls, undeclared data access or runtime behavior that differs from the manifest are grounds for removal.
2. **Least-privilege consent scopes.** Request only the scopes you need. The install screen shown to the user is **generated from your manifest**, so what the user consents to is exactly what the package can do. A scope you don't declare is a scope you don't have.
3. **Signed releases.** Release artifacts are signed. The project signs its own with SignPath ([issue #253](https://github.com/ostrium-labs/loams/issues/253)); partners sign with their own key or SignPath's free OSS program, and publish the verification key. Unsigned artifacts are rejected.
4. **SBOM.** Each release ships a software bill of materials (SPDX or CycloneDX) listing all dependencies, and images are pinned by digest.
5. **Vulnerability handling.** A security contact, and fixes for known high-severity issues within a security-fix window (TBD until the review policy is published).
6. **No billing hooks inside packages.** A package must not contain metering, billing or license-enforcement calls to Loams Cloud's commercial systems, and must not call private platform APIs. Packages use only the open, documented Loams hooks and APIs. Any commercial arrangement is handled outside the package.
7. **Tenant isolation.** The package runs inside the user's tenant and does not reach other tenants' data.
8. **License clarity.** Your license and those of your dependencies are declared and compatible with redistribution.

## Submission flow

1. **Propose.** Open a Discussion in Ecosystem & Partners, then a listing request (an issue or form; the template ships with the marketplace). Say what the project is and what it needs from Loams.
2. **Upstream what Loams needs.** If your package needs a change in Loams, follow [ECOSYSTEM.md](../../ECOSYSTEM.md).
3. **Submit the package** and its listing assets.
4. **Review.** Maintainers and the marketplace reviewers check the requirements above against the manifest and the artifacts.
5. **Test install.** The package is installed in a clean test environment, run, upgraded and uninstalled. Failures come back with a report.
6. **Publish.** The listing goes live. Updates repeat steps 3 to 5 in a shorter form.

Expect a few weeks at first; the target will be published once the review process is running.

## Listing assets

- **Copy:** a name, a one-line summary, a description (what it does, who it is for, what it installs) and a link to docs.
- **Logo:** SVG preferred, square, legible on light and dark backgrounds.
- **Screenshots:** two to five at 16:9, accurate to the current version, with no personal data.
- **Links:** source repository, documentation, issue tracker, security contact, license.

## Support and update obligations

- Publish a **support channel** and respond to install problems reported through it.
- Keep the package compatible with **supported Loams versions**; mark a release with the range it supports. Listings that stay broken after a Loams release may be hidden until fixed.
- Ship **security fixes** as new signed releases and tell us.
- Keep listing information current. Abandoned listings are marked deprecated, then removed, after notice.
- Provide a documented **uninstall** that leaves the user's data intact in their bucket.

## Revenue

Revenue arrangements for paid listings, including any revenue share, are **to be announced**. Free and open-source listings carry no fee.

## Questions

Start in [Discussions](https://github.com/ostrium-labs/loams/discussions) (Ecosystem & Partners).
