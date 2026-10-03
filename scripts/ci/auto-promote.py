#!/usr/bin/env python3
"""Promote a first-time contributor to the committers team after a merged PR.

The decision logic lives here so it can be exercised offline against a mock
GitHub API; the workflow only supplies a credential and the event payload.
Every step is idempotent: the common case (the author is already a committer)
exits before any write, and the membership endpoints are PUTs.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_API_BASE = os.environ.get("GITHUB_API_URL", "https://api.github.com")
BOT_SUFFIX = "[bot]"

# Statuses that answer "no" rather than "the call failed".
NOT_FOUND = 404
FORBIDDEN = 403


class GitHub:
    """Minimal GitHub REST client.

    `token` may be empty, which allows only unauthenticated reads. Every call
    returns `(status, body)` instead of raising, so the caller decides what an
    error means; a status of 0 means the request never reached GitHub.
    """

    def __init__(self, api_base: str, token: str = "") -> None:
        self.api_base = api_base.rstrip("/")
        self.token = token
        self.calls: list[tuple[str, str]] = []

    def request(self, method: str, path: str, payload: dict | None = None):
        self.calls.append((method, path))
        url = f"{self.api_base}{path}"
        data = json.dumps(payload).encode() if payload is not None else None
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "loams-auto-promote",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as response:
                body = response.read()
                return response.status, json.loads(body) if body else None
        except urllib.error.HTTPError as error:
            body = error.read()
            try:
                return error.code, json.loads(body) if body else None
            except json.JSONDecodeError:
                return error.code, None
        except urllib.error.URLError as error:
            return 0, {"error": str(error.reason)}


def is_bot(login: str, user_type: str) -> bool:
    """Bots are never contributors: they are not people to seat on a team."""
    return user_type == "Bot" or login.endswith(BOT_SUFFIX)


def promote(
    github: GitHub,
    org: str,
    team: str,
    login: str,
    user_type: str,
    dry_run: bool,
) -> dict:
    """Decide and act on one contributor. `result["action"]` is one of
    skipped-bot, skipped-no-credential, skipped-member, failed, deferred,
    promoted."""
    result: dict = {"login": login, "org": org, "team": team, "dry_run": dry_run}

    if is_bot(login, user_type):
        result["action"] = "skipped-bot"
        return result

    if not github.token:
        # Without an org-members credential we cannot even read team
        # membership, so we must not guess whether a promotion is needed.
        result["action"] = "skipped-no-credential"
        return result

    status, membership = github.request("GET", f"/orgs/{org}/teams/{team}/memberships/{login}")
    if status == 200 and isinstance(membership, dict) and membership.get("state") == "active":
        result["action"] = "skipped-member"
        return result
    if status == FORBIDDEN:
        result["action"] = "failed"
        result["error"] = f"the credential cannot read {team!r} team membership (HTTP 403)"
        return result
    if status not in (200, NOT_FOUND):
        result["action"] = "failed"
        result["error"] = f"team membership lookup returned HTTP {status}"
        return result

    status, _ = github.request("GET", f"/orgs/{org}/memberships/{login}")
    if status == 200:
        result["already_org_member"] = True
    elif status == NOT_FOUND:
        result["already_org_member"] = False
    else:
        result["action"] = "failed"
        result["error"] = f"organisation membership lookup returned HTTP {status}"
        return result

    if dry_run:
        result["action"] = "promoted"
        result["would_do"] = ([] if result["already_org_member"]
                              else [f"invite {login} to {org}"]) + [f"add {login} to {org}/{team}"]
        return result

    if not result["already_org_member"]:
        status, _ = github.request(
            "PUT", f"/orgs/{org}/memberships/{login}", {"role": "member", "state": "active"}
        )
        if status not in (200, 201):
            result["action"] = "deferred"
            result["error"] = f"inviting {login} to {org} returned HTTP {status}"
            return result
        result["invited"] = True

    status, body = github.request("PUT", f"/orgs/{org}/teams/{team}/memberships/{login}",
                                  {"role": "member"})
    if status not in (200, 201):
        result["action"] = "deferred"
        result["error"] = f"adding {login} to {team} returned HTTP {status}"
        result["body"] = body
        return result
    result["added"] = True
    result["action"] = "promoted"
    return result


def welcome_body(login: str, org: str, team: str, invited: bool) -> str:
    """The comment posted on the merged PR: the audit record for the
    promotion, and the place the reversal instructions live."""
    what = (
        f"You have been invited to the `{org}` organisation and added to the `{team}` team."
        if invited
        else f"You were already in the `{org}` organisation and have been added to the `{team}` team."
    )
    return (
        f"@{login}, welcome aboard. Your first merged PR to `loams` is in, so the contributor "
        f"promotion has run. {what}\n\n"
        f"`{team}` may review and pull requests into `dev`; merging into `main` stays with the "
        f"maintainers. [GOVERNANCE.md](https://github.com/{org}/loams/blob/dev/GOVERNANCE.md) "
        f"describes the role and [CONTRIBUTING.md](https://github.com/{org}/loams/blob/dev/"
        f"CONTRIBUTING.md) describes how a change gets reviewed.\n\n"
        f"If you would rather stay an outside contributor, say so and a maintainer will remove you "
        f"from `{team}`. Nothing else about your access changes."
    )


def deferred_body(login: str, org: str, team: str, reason: str) -> str:
    """Posted when a write was refused: the contributor still deserves the hand-off."""
    return (
        f"@{login}, welcome aboard. Your first merged PR to `loams` is in, so you are eligible for "
        f"the `{team}` team. The automatic promotion did not complete ({reason}).\n\n"
        f"A maintainer can finish it by hand: add `@{login}` to "
        f"[{org}/{team}](https://github.com/orgs/{org}/teams/{team}), and to the organisation itself "
        f"if they are not already a member. No repository permission has changed."
    )


def fallback_body(login: str, org: str, team: str) -> str:
    """Posted when no org-members credential is configured: the promotion is by
    hand, so the comment has to say exactly what is missing and who can do it."""
    return (
        f"@{login}, welcome aboard. Your first merged PR to `loams` is in, so you are eligible for "
        f"the `{team}` team.\n\n"
        f"This repository has no organisation-members credential configured, so the promotion is by "
        f"hand for now. A maintainer needs to add `@{login}` to "
        f"[{org}/{team}](https://github.com/orgs/{org}/teams/{team}), and to the organisation if "
        f"they are not already a member.\n\n"
        f"To automate it, add the `ORG_MEMBERS_TOKEN` (or `ORG_APP_ID` plus `ORG_APP_PRIVATE_KEY`) "
        f"organisation secret described in "
        f"[docs/plans/2026-10-03-ops-auto-promote.md](https://github.com/{org}/loams/blob/dev/docs/"
        f"plans/2026-10-03-ops-auto-promote.md)."
    )


def comment(github: GitHub, repo: str, number: int, body: str, dry_run: bool) -> bool:
    if dry_run:
        print("--- dry run: would comment on PR #%d ---" % number)
        print(body)
        return True
    status, _ = github.request("POST", f"/repos/{repo}/issues/{number}/comments", {"body": body})
    if status not in (200, 201):
        print(f"warning: comment on PR #{number} returned HTTP {status}", file=sys.stderr)
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--org", default="ostrium-labs")
    parser.add_argument("--team", default="committers")
    parser.add_argument("--repo", default="ostrium-labs/loams")
    parser.add_argument("--login", required=True)
    parser.add_argument("--user-type", default="User")
    parser.add_argument("--pr", type=int, default=0, help="merged PR number to comment on")
    parser.add_argument("--token", default="", help="org-members credential; empty disables writes")
    parser.add_argument("--comment-token", default="",
                        help="credential for the PR comment; defaults to --token. The org-members "
                             "credential usually has no repository scope, so this is normally the "
                             "job's own GITHUB_TOKEN.")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    github = GitHub(args.api_base, args.token)
    # Two credentials on purpose: organisation membership and a pull-request
    # comment need different scopes, and the org-members credential is not
    # expected to carry repository rights.
    commenter = GitHub(args.api_base, args.comment_token or args.token)
    result = promote(github, args.org, args.team, args.login, args.user_type, args.dry_run)
    action = result["action"]

    if action == "promoted":
        if args.pr:
            body = welcome_body(args.login, args.org, args.team, result.get("invited", False))
            comment(commenter, args.repo, args.pr, body, args.dry_run)
    elif action == "deferred":
        if args.pr:
            comment(commenter, args.repo, args.pr,
                    deferred_body(args.login, args.org, args.team, result.get("error", "unknown")),
                    args.dry_run)
        print(f"deferred: {result.get('error')}", file=sys.stderr)
    elif action == "skipped-no-credential":
        if args.pr:
            comment(commenter, args.repo, args.pr, fallback_body(args.login, args.org, args.team),
                    args.dry_run)
    elif action == "failed":
        print(f"failed: {result.get('error')}", file=sys.stderr)
        print(json.dumps(result, indent=2), file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
