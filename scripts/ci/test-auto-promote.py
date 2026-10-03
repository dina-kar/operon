#!/usr/bin/env python3
"""Exercise auto-promote's decision logic against a mock GitHub API.

Every test runs the real `auto-promote.py` over HTTP against a local server, so
the assertions cover the wire calls (method, path, body) and not just the
return value.
"""
from __future__ import annotations

import json
import subprocess
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SCRIPT = Path(__file__).with_name("auto-promote.py")

MEMBER = {"state": "active", "role": "member"}


class MockGitHub(BaseHTTPRequestHandler):
    """Answers only the endpoints auto-promote uses. `state` is per test:
    `in_team` / `in_org` decide the read answers, and `fail_writes` makes every
    PUT and the comment POST return 403 so the hand-off path can be tested."""

    state: dict = {}

    def log_message(self, *args):  # noqa: D102 - silence the default stderr spam
        pass

    def _route(self):
        self.state.setdefault("calls", []).append(
            {"method": self.command, "path": self.path,
             "auth": self.headers.get("Authorization")})
        if self.state.get("fail_writes") and self.command == "PUT" and self.path.startswith("/orgs/"):
            return 403, {"message": "forbidden"}
        if self.command == "GET" and "/teams/" in self.path and "/memberships/" in self.path:
            return (200, MEMBER) if self.state.get("in_team") else (404, {"message": "Not Found"})
        if self.command == "GET" and "/memberships/" in self.path:
            return (200, MEMBER) if self.state.get("in_org") else (404, {"message": "Not Found"})
        if self.command == "PUT" and "/teams/" in self.path:
            return 200, dict(MEMBER, team={"slug": "committers"})
        if self.command == "PUT":
            return 201, dict(MEMBER, state="active")
        if self.command == "POST":
            length = int(self.headers.get("Content-Length") or 0)
            MockGitHub.state.setdefault("comments", []).append(json.loads(self.rfile.read(length)))
            return 201, {"id": 1}
        return 404, {"message": "no mock route"}

    def _handle(self):
        status, payload = self._route()
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_PUT = do_POST = _handle


class AutoPromoteTests(unittest.TestCase):
    def setUp(self):
        MockGitHub.state = {"in_team": False, "in_org": False, "comments": [], "calls": []}
        self.base = self.start_mock(MockGitHub)

    def start_mock(self, handler):
        """Cleanups run last-in-first-out, so the server must be shut down
        before its thread is joined or `join` blocks on a live loop."""
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}"

    def run_script(self, *extra, login="new-contributor", token="org-token", user_type="User"):
        argv = [sys.executable, str(SCRIPT), "--api-base", self.base, "--login", login,
                "--pr", "42"]
        if token is not None:
            argv += ["--token", token]
        if user_type:
            argv += ["--user-type", user_type]
        proc = subprocess.run(argv + list(extra), capture_output=True, text=True)
        return proc

    @staticmethod
    def actions(state):
        return [(call["method"], call["path"]) for call in state.get("calls", [])]

    @staticmethod
    def writes(state):
        """PUTs and the comment POST: the calls that change something."""
        return [(method, path) for method, path in
                AutoPromoteTests.actions(state) if method in ("PUT", "POST")]

    def test_new_contributor_is_invited_added_and_welcomed(self):
        MockGitHub.state["in_org"] = False
        proc = self.run_script()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        result = json.loads(proc.stdout)
        self.assertEqual(result["action"], "promoted")
        self.assertTrue(result["invited"])
        self.assertIn(("PUT", "/orgs/ostrium-labs/memberships/new-contributor"),
                      self.actions(MockGitHub.state))
        self.assertIn(("PUT", "/orgs/ostrium-labs/teams/committers/memberships/new-contributor"),
                      self.actions(MockGitHub.state))
        comments = MockGitHub.state["comments"]
        self.assertEqual(len(comments), 1)
        self.assertIn("invited to the `ostrium-labs` organisation", comments[0]["body"])
        self.assertIn("would rather stay an outside contributor", comments[0]["body"])

    def test_existing_org_member_is_added_without_an_invite(self):
        MockGitHub.state["in_org"] = True
        proc = self.run_script()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        result = json.loads(proc.stdout)
        self.assertEqual(result["action"], "promoted")
        self.assertNotIn("invited", result)
        self.assertNotIn(("PUT", "/orgs/ostrium-labs/memberships/new-contributor"),
                         self.actions(MockGitHub.state))
        self.assertIn("already in the `ostrium-labs` organisation",
                      MockGitHub.state["comments"][0]["body"])

    def test_second_merged_pr_is_a_no_op(self):
        """The idempotency rule: an author already in the team is skipped, and
        no org membership, team or comment call is made."""
        MockGitHub.state["in_team"] = True
        MockGitHub.state["in_org"] = True
        proc = self.run_script()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["action"], "skipped-member")
        # Exactly one read, the team lookup; nothing else is touched.
        self.assertEqual(self.actions(MockGitHub.state),
                         [("GET", "/orgs/ostrium-labs/teams/committers/memberships/new-contributor")])
        self.assertEqual(MockGitHub.state["comments"], [])

    def test_bot_author_is_skipped_without_any_call(self):
        for login, user_type in (("dependabot[bot]", "Bot"), ("ci-runner", "Bot"),
                                 ("some-action[bot]", "User")):
            with self.subTest(login=login, user_type=user_type):
                MockGitHub.state = {"in_team": False, "in_org": False, "comments": [], "calls": []}
                proc = self.run_script(login=login, user_type=user_type)
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(json.loads(proc.stdout)["action"], "skipped-bot")
                self.assertEqual(self.actions(MockGitHub.state), [])
                self.assertEqual(MockGitHub.state["comments"], [])

    def test_without_a_credential_it_defers_to_a_maintainer(self):
        proc = self.run_script(token=None)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["action"], "skipped-no-credential")
        # No membership read at all, and the only write is the hand-off comment.
        self.assertEqual(self.actions(MockGitHub.state),
                         [("POST", "/repos/ostrium-labs/loams/issues/42/comments")])
        body = MockGitHub.state["comments"][0]["body"]
        self.assertIn("promotion is by hand", body)
        self.assertIn("ORG_MEMBERS_TOKEN", body)

    def test_dry_run_reports_the_plan_without_touching_anything(self):
        proc = self.run_script("--dry-run")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        result = json.loads(proc.stdout[proc.stdout.index("{"):])
        self.assertEqual(result["action"], "promoted")
        self.assertEqual(result["would_do"],
                         ["invite new-contributor to ostrium-labs",
                          "add new-contributor to ostrium-labs/committers"])
        self.assertEqual(self.writes(MockGitHub.state), [])
        self.assertEqual(MockGitHub.state["comments"], [])

    def test_refused_write_degrades_to_a_hand_off_comment(self):
        """A credential without organisation-members rights must not look like a
        success: the PR gets the manual-promotion comment instead."""
        MockGitHub.state["fail_writes"] = True
        proc = self.run_script()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["action"], "deferred")
        body = MockGitHub.state["comments"][0]["body"]
        self.assertIn("did not complete", body)
        self.assertIn("No repository permission has changed", body)

    def test_org_members_and_comment_credentials_are_separate(self):
        """The membership writes and the PR comment use different tokens: the
        org-members credential has no repository scope, and GITHUB_TOKEN has no
        organisation-members scope."""
        proc = self.run_script("--comment-token", "repo-token")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        by_auth = {}
        for call in MockGitHub.state["calls"]:
            by_auth.setdefault(call["auth"], []).append((call["method"], call["path"]))
        self.assertEqual(sorted(by_auth), ["Bearer org-token", "Bearer repo-token"])
        self.assertEqual(by_auth["Bearer repo-token"],
                         [("POST", "/repos/ostrium-labs/loams/issues/42/comments")])

    def test_unreadable_team_membership_fails_loudly(self):
        """Without a Members: read scope the team lookup is 403, not 404. That
        is a misconfiguration, not "not a member", and must not promote."""
        class Forbidden(MockGitHub):
            def _route(self):
                if self.command == "GET" and "/teams/" in self.path:
                    return 403, {"message": "Resource not accessible by integration"}
                return MockGitHub._route(self)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Forbidden)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "--api-base", f"http://127.0.0.1:{server.server_port}",
             "--login", "new-contributor", "--pr", "42", "--token", "bad-token"],
            capture_output=True, text=True)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("cannot read", proc.stderr)
        self.assertNotIn("promoted", proc.stdout)


if __name__ == "__main__":
    unittest.main()
