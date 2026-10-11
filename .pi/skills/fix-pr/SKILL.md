---
name: fix-pr
description: Fix QuestDB PR review claims one at a time - validate each claim, fix real user-facing problems at the root cause with red tests, and commit each fix; then review the fix commits with review-pr and repeat until no Critical or Moderate findings remain. Adjacent findings become GitHub issues. Use when the user runs /skill:fix-pr or explicitly asks to fix review claims.
allowed-tools: bash read edit write subagent ctx_execute ctx_execute_file
metadata:
  argument-hint: "[--include-adjacent] [--no-issues] [--review-level=N] [--max-review-rounds=N] <review claims>"
---

# Fix PR review claims

Handle the claims one at a time, in order:

1. Validate the claim. State the problem and the user impact in one line each.
2. Skip it if it is false, already fixed, or its impact is negligible.
3. Otherwise propose a root-cause fix, and have an independent agent try to
   break it before any code changes.
4. Write simple red tests, fix, run the related tests, commit.

When the queue is empty, review the fix commits with `review-pr`, fix its
Critical and Moderate findings the same way, and repeat until none remain.
File Adjacent findings as GitHub issues.

`CLAUDE.md` governs code, tests and commit messages.

## Arguments

`$ARGUMENTS` is the text after `/skill:fix-pr`. If it contains no claims, ask
for them.

- `--include-adjacent`: fix Adjacent items instead of filing them.
- `--no-issues`: list Adjacent items instead of filing them.
- `--review-level=N`: `review-pr` level for the final review; default `1`.
- `--max-review-rounds=N`: default `3`.

## Keep the main session small

The main session only coordinates. Fresh helper agents analyze and fix each
claim, write their details to files, and reply in a few lines. Read only those
replies - never `analysis.md` or `fix.md`, unless a reply is unclear or the
user asks. A small main session is cheap to resume after waiting on a helper;
a large one is not.

Launch helpers one at a time as ordinary foreground `subagent` calls (never
`async: true`) with `context: "fresh"` and the task templates at the end of
this file. While the main session waits inside a tool call, pi keeps its
prompt cache warm; a session that ends its turn and is woken later by a
background helper loses the cache and pays to rebuild it.

| Helper | Agent | Extra parameters |
|---|---|---|
| Analysis | `worker` | `timeoutMs: 3600000`, `acceptance: { level: "none", reason: "fix-pr checks the result" }` |
| Check | `reviewer` | none |
| Fix | `worker` | `timeoutMs: 5400000`, `acceptance: { level: "none", reason: "fix-pr checks the commit" }` |

## Setup

1. Record the repo root, the branch, `START=$(git rev-parse HEAD)`, and the PR
   base: `git merge-base HEAD origin/<base>`, where `<base>` comes from
   `gh pr view --json baseRefName`, else from
   `git symbolic-ref --short refs/remotes/origin/HEAD`.
2. If tracked files have uncommitted changes, list them and ask whether to
   continue. Fix commits never include them.
3. Create a state directory outside the repo: `mktemp -d /tmp/fix-pr.XXXXXX`.
4. Parse the claims. From a `review-pr` report, queue Critical and Moderate
   items, list Minor items as not fixed (cosmetic) unless the user asks, and
   send Adjacent items to the issue list unless `--include-adjacent`. Keep
   each claim's text verbatim and give it a short ID (the review's number,
   or C1, C2, ...).
5. Show the queue in a few lines and start. Don't wait for approval.

## Per claim

Save `git status --porcelain` before each helper. Afterwards it must be
unchanged (a commit doesn't change it); if it isn't, stop and show the
difference.

1. **Analyze.** Launch the analysis helper. Show the user its PROBLEM and
   IMPACT lines.
   - `SKIP`: record the reason and go to the next claim.
   - `OTHER` lines: a bug in code this PR added or changed becomes a new claim
     at the end of the queue; a pre-existing one goes to the issue list.
2. **Check.** Launch the check helper. It didn't write the proposal and can't
   edit anything; its only job is to find what the fix would break.
   - `OK`: go to the fix.
   - `BREAKS`: run the analysis again with the objection. Each claim gets two
     proposals; if the second is rejected too, mark the claim blocked and
     move on.
3. **Fix.** Launch the fix helper.
   - `FIXED <sha>`: check with `git show --stat <sha>` that it changed only
     the files named on the FIX line, plus tests. Record it.
   - `STOPPED`: if the plan was wrong, treat it as a rejected proposal and go
     back to step 1 with the reason. If the cause is outside the plan (for
     example, a file with the user's uncommitted changes), mark the claim
     blocked. If the build or test environment is broken, stop the run and
     report.

## Final review

When the queue is empty:

1. Read `.pi/skills/review-pr/SKILL.md` and follow it with
   `--range=<START>..HEAD --level=<review-level>`. In later rounds, review
   only the new commits: `--range=<previous round's HEAD>..HEAD`.
2. Queue its Critical and Moderate findings as new claims and run them through
   the per-claim steps. List Minor findings. Send Adjacent findings to the
   issue list; invoking this skill is the user's request to file them.
3. If a finding concerns a claim this run already fixed or skipped, don't loop
   on it: list it for the user with both views.
4. Repeat until a review finds no Critical or Moderate items, or
   `--max-review-rounds` is reached.

## Issue list

Adjacent means pre-existing on the PR base and not made worse by this PR.
Unless `--no-issues`, file each one:

- Repository: the one that owns the affected file. Run
  `gh repo view --json nameWithOwner -q .nameWithOwner` in that file's
  directory.
- Search first:
  `gh issue list --repo <repo> --state all --search "<key words>" --limit 5`.
  If an issue already covers it, record that issue instead of filing.
- `gh issue create --repo <repo> --title "<one-line problem>" --body-file <file>`.
  The body covers impact, location (file:line), symptom, how it's reached,
  suggested fix, and where it was found (PR or branch).
- Never auto-file security-sensitive bugs (authentication or access-control
  bypass, data exposed across users, memory corruption). List them for the
  user instead.

## Rules

- One local commit per fixed claim. Never push, amend, rebase, reset, stash,
  clean, switch branches or create worktrees.
- Stage only the files a fix changed; never `git add -A` or `git commit -a`.
- `java-questdb-client/` is a separate repo: commit inside it first, as
  `CLAUDE.md` says.
- Don't call a failure pre-existing or flaky without proof.
- If a helper fails to launch or crashes, retry once; then stop and report.

## Final summary

| ID | Problem | Impact | Outcome |
|---|---|---|---|

Outcome is `fixed <sha>`, `skipped: <reason>` or `blocked: <reason>`. Then one
line each: review rounds and what they found; issues filed or matched (links);
items left for the user (blocked, security-sensitive, Minor, disagreements);
and `git log --oneline <START>..HEAD`. State that nothing was pushed.

## Task templates

Fill in the `<placeholders>`; keep tasks this short.

### Analysis

```text
Analyze one PR review claim in <repo>. Follow CLAUDE.md.
Don't change existing files or commit. For probes, create new files and
delete them when done.

Claim <ID>: <verbatim claim>
PR base: <sha>. Earlier fixes in this run: <ID: one line each, or none>.
[Revision only: your previous proposal is in <state>/<ID>/analysis.md. It
was rejected: <objection>. Change the fix, or show with file:line why the
objection is wrong.]

1. Check the claim against the current code; run something if reading
   isn't conclusive.
2. Problem: one plain line.
3. Impact on users: one line - who, how badly, how often.
4. FIX if users get wrong results, errors, crashes, hangs, data loss, or a
   meaningful slowdown or memory cost, or if the claim is a missing or
   ineffective test for behavior this PR adds or changes. Otherwise SKIP
   (false, already fixed, or negligible impact).
5. For FIX: find the root cause - the first place the code goes wrong, not
   where the symptom shows. Propose the smallest fix there; no guard, catch
   or special case at the symptom unless that layer owns the check. List
   what else uses the code you'd change (callers, overrides, sibling
   implementations) and why each keeps working.
6. For FIX: describe simple red tests that assert the correct result, or
   say why none is feasible. A few statements, not a harness.
7. Other bugs you noticed: say whether each is in code this PR added or
   changed (compare with the PR base) or pre-existing. For a pre-existing
   one, write an issue body to <state>/<ID>/issue-<slug>.md.

Write <state>/<ID>/analysis.md, starting with the verbatim claim. Reply
with only these lines:
DECISION: FIX | SKIP
PROBLEM: <one line>
IMPACT: <one line>
FIX: <one line, naming the files> | REASON: <one line, for SKIP>
TESTS: <one line>
OTHER: none | one line per bug: PR or PRE-EXISTING [security], file:line, problem, issue body path
```

### Check

```text
Independent check of a proposed fix in <repo>. You didn't write it. Don't
edit anything.

Read <state>/<ID>/analysis.md: the claim, the proposed fix, and what else
uses the code it changes. Then read the code and try to break the proposal:
- What works today that the fix would break? Consider other callers and
  inputs, NULL, empty and boundary values, error paths, cleanup, and large
  data.
- Does the same bug stay reachable through a path the fix misses?
- Does it patch the symptom instead of the cause?

Report only problems you can show in the code, with file:line. Style
preferences and "could test more" don't count.
Reply with only: OK, or BREAKS followed by at most five lines, one per
problem.
```

### Fix

```text
Fix one confirmed claim in <repo>. Follow CLAUDE.md.
The plan is <state>/<ID>/analysis.md; an independent check approved it.
Earlier fixes in this run: <ID: one line each, or none>.
Run `git status --porcelain` first and never edit a file it lists.

1. Write the red tests from the plan and run them. They must fail, for the
   claimed reason; if they pass, stop.
   For a missing-test claim, write the test, then show it can fail by
   briefly breaking the code it protects; restore that file with
   `git checkout -- <file>`.
2. Fix the root cause as planned.
3. Run the red tests and the tests for the code that uses what you changed
   (listed in the plan). All must pass. One Maven command at a time.
4. If the plan turns out wrong (another approach or unplanned files are
   needed), stop: restore the files you modified with `git checkout --`,
   delete the files you created, and say why.
5. Commit only your files (`git add <paths>`). Commit message per CLAUDE.md:
   title of at most 50 characters, no type prefix; body with the problem,
   root cause and fix.

Never push, amend, rebase, reset, stash or clean.
Write <state>/<ID>/fix.md. Reply with only these lines:
RESULT: FIXED <sha> | STOPPED: <one line why>
TESTS: <red tests failing before and passing after; related tests run>
FILES: <changed files>
OTHER: none | one line per bug, as in the analysis (write issue bodies the same way)
```
