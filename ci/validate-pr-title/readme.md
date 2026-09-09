This folder holds the validation rules applied to GitHub pull request titles, and
the job that reports on them.

- `validate.js` — the rules themselves: `type(subType): description`.
- `check.js` — reads the title from the workflow event, posts the `PR title` commit
  status, and leaves a comment explaining a rejection. The comment is updated in
  place while the title stays wrong and deleted once it is fixed.
- Tests run with node and need no dependencies: `node ./validate.test.js` and
  `node ./check.test.js`.

Run by [.github/workflows/pr_title.yml](../../.github/workflows/pr_title.yml) on
pull requests and on merge groups. It authenticates with the workflow's own
`GITHUB_TOKEN`, so it needs no bot account and no personal access token.

The tests live in a second workflow,
[pr_title_rules.yml](../../.github/workflows/pr_title_rules.yml), rather than
alongside the job that posts the status. Run as a step of that job, a failing or
flaky test stops check.js from running at all, the required status is never
posted, and the pull request sits behind a check that is merely missing. A path
filter runs them only when this folder or either workflow changes, which also
keeps a second near-identical row off every unrelated pull request.

## Fork pull requests get no status

A pull request from a fork gets a read-only token, so the job that posts the
status cannot run and is skipped. No status is posted, and while this context is
required, a fork cannot satisfy it without a bypass. Danger behaved the same way
and carried the same `if:` guard, so this is inherited rather than new, but it is
worth knowing before relying on the required check: at the time of writing, 46 of
100 open pull requests come from forks and none of them carries a status.

Fixing it means posting the verdict from a workflow that has a write token on a
fork pull request, which is `pull_request_target`. That is safe here only because
the title comes from the event payload and the checker is checked out from the
base branch, so no code from the fork ever runs — a property any change to this
folder has to preserve.

## The status context and the ruleset

The status context is `PR title`, and the `master` ruleset requires that exact
string. The two are a pair: a required context nothing posts never reports, and
every pull request sits behind it, so `CONTEXT` in check.js cannot be renamed on
its own. check.test.js spells the string out rather than comparing against
`CONTEXT`, so a rename fails the suite instead of quietly agreeing with itself.

Code and ruleset cannot change in the same instant, so a rename needs an order
that leaves nothing blocked:

1. Make check.js post both the old and the new context, and merge that.
2. Add the new context to the ruleset, then remove the old one. Both are being
   posted, so no pull request is ever waiting on a context nobody writes.
3. Drop the old context from check.js.

Going straight from one name to the other blocks every pull request between the
two edits, in whichever order they are made: the ruleset either requires a context
that is no longer posted, or one that is not posted yet.

The `Danger` to `PR title` rename was done as a straight swap rather than by that
recipe, with the ruleset edited by hand as the change merged. That was safe only
because the window is the gap between the merge and the ruleset edit, and someone
was watching it. Anything less deliberate should use the three steps above.

This context read `Danger` until the ruleset was moved off that name. That was
inherited from the Danger JS job this replaced; check.js still recognises the
comments Danger left behind, which is a separate thing from the status name.
