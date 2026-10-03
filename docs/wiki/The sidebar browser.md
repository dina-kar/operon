# The sidebar browser

Loams Desktop can dock one of your apps — Zulip, Plane or Forgejo — in a panel
beside your work, instead of sending you to a separate browser window.

## What it does

- **It remembers you.** Sign in once and the session lasts: each app in each
  environment gets its own saved profile on disk, so a development instance and
  a production one never share a login, and Zulip's session never appears in
  Forgejo's.
- **It stays inside your deployment.** The panel can only reach the app you
  opened and your identity provider. A link in a chat message cannot turn it
  into a general-purpose browser, and nothing it does can reach the Loams
  credentials your desktop is holding.
- **It works the same on Linux, macOS and Windows.** There is one integration,
  not three.

## What it is not

**The sidebar is not a full browser, and it is not trying to be one.** It draws
what the page would look like and forwards your clicks and keystrokes. That means
some things a browser window gives you are missing:

- **Scrolling has no momentum.** The wheel works; the flick-to-coast does not.
- **There is no text caret or selection.** You can click into a field and type,
  but you cannot drag-select across it.
- **Animation is coarser than a browser window's.** Pages that animate heavily
  update less often than 60 times a second, because each update is a fresh
  render of the page.
- **A page built around a specific browser's quirks may not look exactly right.**
  The engine here is not Chromium. It renders ordinary server-rendered web apps
  accurately, and the apps Loams embeds are exactly that. A page that depends on
  a Chromium-only behaviour, or that draws itself with WebGL or `<canvas>`, is
  the kind of thing that would show it.

## When to use "Open in browser" instead

The sidebar is for **reading and light interaction** — a Zulip thread, a Plane
issue, a Forgejo diff. For anything heavier, use the panel's **Open in browser**
action. It opens the same URL in your normal browser, where you already have
your own sessions, your extensions, your bookmarks and your password manager.
Both routes stay available; nothing was taken away.

If you would rather not install a separate browser engine at all, **Open in
browser** works on its own and needs no extra setup. The sidebar is opt-in per
app.

## Sign-in

You are not asked to sign in inside the panel. Loams performs the sign-in for
you and hands the panel only a short-lived session for that one app — the same
way it authenticates everything else. That is also why passkeys work here: they
are checked by Loams, not by the panel's rendering engine.

## Privacy and what is stored

Each app profile stores that app's own session cookie on your machine, in
Loams's application data directory, readable only by your user account. It never
stores your Loams credentials — that is a design rule with a test attached to
it, not an intention.

To sign out of an app completely, close the panel and remove that app's
profile directory; the next open starts from the app's login page.