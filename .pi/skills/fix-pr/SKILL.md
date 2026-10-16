---
name: fix-pr
description: Develop features and fix bugs or review findings from a list, one item at a time, with exhaustive tests enforced while developing - design the change and its test plan against CLAUDE.md "Testing" and have both challenged independently, build test-first, run the item's tests with coverage (ci/diff_coverage.py) and add tests until every changed line, branch and condition runs, have the tests reviewed independently and close the gaps; commit. When the list is done, run review-pr once as a backstop and treat what it finds the same way. Adjacent findings outside this PR's code become GitHub issues. Use when the user runs /skill:fix-pr or asks to develop or fix items with it.
allowed-tools: bash read edit write subagent ctx_execute ctx_execute_file
metadata:
  argument-hint: "[--include-adjacent] [--no-issues] [--review-level=N] [--max-review-rounds=N] <items: features, bugs or review findings>"
---

# Develop and fix with exhaustive tests

Handle the items one at a time, in order. An item is a feature to build, a bug
to fix, or a finding from a review.

1. Understand the item. Validate a bug or finding; pin down the behavior of a
   feature. State the problem or goal and the user impact in one line each.
2. Skip it if it is false, already done, or its impact is negligible. Ask the
   user when a feature's behavior is ambiguous.
3. Design the best root-cause change - alternatives weighed; no regressions,
   slowdowns, races or reinvented code - and the test plan that proves it,
   dimension by dimension, per CLAUDE.md "Testing". An independent agent tries
   to break both before any code changes. Only a flaw in the design sends the
   plan back; the tests it finds missing join the plan, and other bugs it
   finds become separate items or are reported.
4. Write the failing tests first and build the change. Run the item's tests
   with coverage (`ci/diff_coverage.py`), read the lines, branches and
   conditions that never ran, and add tests until everything runs, measuring
   only the new tests with `--append`. Commit.
5. An independent agent reviews the tests against the plan and the rules; if
   it finds gaps, a fresh agent writes the missing tests. Commit.

Tests are written while developing; the review at the end is the last resort,
not the place where test cases get discovered. When the queue is empty, run
`review-pr` once over all commits. Treat its Critical and Moderate findings,
and Adjacent ones in code this PR added or changed, as new items: each is a
test that should have existed. File the other Adjacent findings as GitHub
issues.

CLAUDE.md governs code, tests and commit messages. Its "Testing" section is the
standard every item meets.

## Arguments

`$ARGUMENTS` is the text after `/skill:fix-pr`. If it contains no items, ask
for them.

- `--include-adjacent`: fix Adjacent review findings instead of filing them.
- `--no-issues`: list Adjacent items instead of filing them.
- `--review-level=N`: `review-pr` level for the backstop review; default `1`.
- `--max-review-rounds=N`: default `2`.

## Keep the main session small

The main session only coordinates. Fresh helper agents analyze, build and test
each item, write their details to files, and reply in a few lines. Read only
those replies - never the files in the state directory, unless a reply is
unclear or the user asks. A small main session is cheap to resume after
waiting on a helper; a large one is not.

Launch helpers one at a time as ordinary foreground `subagent` calls (never
`async: true`) with `context: "fresh"` and the task templates at the end of
this file. While the main session waits inside a tool call, pi keeps its
prompt cache warm; a session that ends its turn and is woken later by a
background helper loses the cache and pays to rebuild it.

| Helper | Agent | Extra parameters |
|---|---|---|
| Analysis | `worker` | `timeoutMs: 3600000`, `acceptance: { level: "none", reason: "fix-pr checks the result" }` |
| Check | `reviewer` | none |
| Build | `worker` | `timeoutMs: 7200000`, `acceptance: { level: "none", reason: "fix-pr checks the commit" }` |
| Test review | `reviewer` | none |
| Test gaps | `worker` | `timeoutMs: 5400000`, `acceptance: { level: "none", reason: "fix-pr checks the commit" }` |

## Setup

1. Record the repo root, the branch, `START=$(git rev-parse HEAD)`, and the PR
   base: `git merge-base HEAD origin/<base>`, where `<base>` comes from
   `gh pr view --json baseRefName`, else from
   `git symbolic-ref --short refs/remotes/origin/HEAD`.
2. If tracked files have uncommitted changes, list them and ask whether to
   continue. Commits never include them.
3. Create a state directory outside the repo: `mktemp -d /tmp/fix-pr.XXXXXX`.
4. Parse the items. Each entry of a list of features or bugs is one item. From
   a `review-pr` report, queue Critical and Moderate findings and Adjacent
   ones in code this PR added or changed (compare with the PR base), list
   Minor ones as not fixed (cosmetic) unless the user asks, and send the
   other Adjacent ones to the issue list unless `--include-adjacent`. Keep
   each item's text verbatim and give it a short ID (the user's or the
   review's number, or I1, I2, ...).
5. Show the queue in a few lines and start. Don't wait for approval.

## Per item

Save `git status --porcelain` before each helper. Afterwards it must be
unchanged (a commit doesn't change it); if it isn't, stop and show the
difference. Record `ITEM_START=$(git rev-parse HEAD)` before the build.

1. **Analyze.** Launch the analysis helper. Show the user its PROBLEM and
   IMPACT lines, and for a change its CHANGE, WHY and TESTS lines.
   - `SKIP`: record the reason and go to the next item.
   - `ASK`: show the QUESTIONS, wait for the user's answers, and run the
     analysis again with them.
   - `OTHER` lines: `PR` and `RELATED` bugs become new items at the end of
     the queue, since CLAUDE.md bundles related fixes into one PR;
     `PRE-EXISTING` ones go to the issue list. Skip a bug already queued or
     listed.
2. **Check.** Launch the check helper. It didn't write the plan and can't edit
   anything; its only job is to find what the change would break, a better
   change, or a hole in the test plan. It labels each problem `REDESIGN` (the
   design is wrong for this item), `ADD` (a gap in the test plan) or
   `ADJACENT` (a separate bug, including the same kind of bug in other code).
   Only `REDESIGN` sends the plan back. A `REDESIGN` line that only asks for
   more tests or a citation counts as `ADD`; one about a separate defect
   elsewhere, even of the same kind, counts as `ADJACENT`.
   - `ADJACENT` lines, whatever else the reply says: handle them like OTHER
     lines in step 1, but list a `PRE-EXISTING` one for the user instead of
     filing it; the check can't write an issue body.
   - `OK`, or no `REDESIGN` line: replace `<state>/<ID>/amendments.md` with
     the `ADD` lines, verbatim (an empty file if there are none), and go to
     the build. The amendments are part of the plan from here on.
   - `REDESIGN`: run the analysis again with the `REDESIGN` and `ADD` lines.
     Each item gets two proposals; the second check is told what sent the
     first one back. If the second proposal draws a `REDESIGN` line too, mark
     the item blocked and move on.
3. **Build.** Launch the build helper.
   - `BUILT <sha>`: check with `git show --stat <sha>` that it changed only the
     files named on the CHANGE line, plus tests. Record its COVERAGE and
     EXISTING lines.
   - `STOPPED`: if the plan was wrong, treat it as a rejected proposal and go
     back to step 1 with the reason. If the cause is outside the plan (for
     example, a file with the user's uncommitted changes), mark the item
     blocked. If the build or test environment is broken, stop the run and
     report.
4. **Test review.** The test-review helper has no shell, so write its inputs:
   `git show <sha> > <state>/<ID>/build.diff`; the build helper has already
   written `<state>/<ID>/coverage.json`. Launch it.
   - `OK`: the item is done.
   - `GAPS`: launch the test-gaps helper with those lines.
5. **Test gaps.** On `ADDED <sha>`, check with `git show --stat <sha>` that it
   changed only tests, and record it with its COVERAGE line. On `NONE`, record
   why; if the build or test environment is broken, stop the run. Uncovered
   lines either helper left go to the final summary as open. Handle OTHER
   lines as in step 1. Don't review the tests again for this item; the
   backstop review covers the rest.

## Backstop review

When the queue is empty:

1. Read `.pi/skills/review-pr/SKILL.md` and follow it with
   `--range=<START>..HEAD --level=<review-level>`. In a later round, review
   only the new commits: `--range=<previous round's HEAD>..HEAD`. Its Step 2.6
   measures the coverage of the whole range at HEAD, in a scratch worktree.
   That catches what the per-item runs miss, since each measured one item's
   files at its own commit: coverage a later item broke, lines a helper left
   open, files an item didn't measure. It grades never-run code by what it
   would break if it were wrong.
2. Queue its Critical and Moderate findings, and Adjacent ones in code this
   PR added or changed (compare with the PR base, not START), as new items
   and run them through the per-item steps. Each one is a test the standard
   should have produced: its test plan names the dimension of CLAUDE.md
   "Testing" that missed it. List Minor findings. List the never-run lines
   the review judged low-exposure as open coverage gaps. Send the other
   Adjacent findings to the issue list; invoking this skill is the user's
   request to file them.
3. If a finding concerns an item this run already built or skipped, don't loop
   on it: list it for the user with both views.
4. Repeat until a review finds no Critical or Moderate items, or
   `--max-review-rounds` is reached.

## Issue list

Adjacent means pre-existing on the PR base and not made worse by this PR.
Unless `--no-issues`, file each one:

- Repository: the one that owns the affected file. Run
  `gh repo view --json nameWithOwner -q .nameWithOwner` in that file's
  directory. A crash or hang that hostile input triggers (`[crash]` on a
  helper's line) goes to `questdb/questdb-enterprise` instead, with
  `--label crash`; never describe it anywhere public (CLAUDE.md "Failure
  paths and misuse"). When `gh repo view questdb/questdb-enterprise` fails,
  as it does for OSS contributors, show its issue body in the final summary
  instead of filing it.
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

- One local commit per item for the change and its tests, plus one if the
  test-gaps step added tests. Never push, amend, rebase, reset, stash, clean,
  switch branches or create worktrees; the one exception is the scratch
  worktree review-pr creates and removes in the backstop review.
- Stage only the files a helper changed; never `git add -A` or `git commit -a`.
- Never weaken an existing test to make a change pass (CLAUDE.md "Test
  integrity"); a changed expectation needs its reason in the commit message.
- `java-questdb-client/` is a separate repo: commit inside it first, as
  CLAUDE.md says.
- Don't call a failure pre-existing or flaky without proof.
- If a helper fails to launch or crashes, retry once; then stop and report.

## Final summary

| ID | Item | Impact | Outcome | Coverage |
|---|---|---|---|---|

Outcome is `built <sha>` (plus `tests <sha>` when the test-gaps step added
tests), `skipped: <reason>` or `blocked: <reason>`. Coverage is the final
changed lines and branches covered, e.g. `42/42 L, 18/18 B`.

Then one line each: review rounds, what they found and the range's measured
coverage; issues filed or matched (links); items left for the user (blocked,
security-sensitive, unfiled `ADJACENT` bugs, Minor, disagreements, open
coverage gaps); and
`git log --oneline <START>..HEAD`. State that nothing was pushed. Then print
in full each crash issue the Issue list could not file.

End with the PR body bullets, per CLAUDE.md "Git & PR Conventions": the
EFFECT of each built item that isn't `none`, and any benchmark an EFFECT line
names, whose JMH results go in the body. No test plan.

## Task templates

Fill in the `<placeholders>`; keep tasks this short.

### Analysis

```text
Analyze one item in <repo>. Follow CLAUDE.md, including "Testing".
Don't change existing files or commit. For probes, create new files; when
done, copy those that reproduce the problem to <state>/<ID>/probes/ for the
build, and delete them from the repo.

Item <ID> (<feature | bug | review finding>): <verbatim item>
PR base: <sha>. Earlier items in this run: <ID: one line each, or none>.
[Revision only: your previous proposal is in <state>/<ID>/analysis.md, with
test-plan amendments in <state>/<ID>/amendments.md if that file exists. Its
design was sent back: <REDESIGN lines, or the build's STOPPED reason>.
Change the design, or show with file:line why an objection is wrong. Fold
these test-plan gaps and the amendments into the new plan where they still
apply: <ADD lines, or none>.]
[Answers from the user: <answers>]

1. Understand the item.
   - Bug or finding: check it against the current code; run something if
     reading isn't conclusive.
   - Feature: pin down the behavior - syntax, results, NULL handling, errors
     with their message and position, and how it combines with the rest of
     SQL. Take decisions from the item, existing QuestDB behavior, the SQL
     standard or PostgreSQL. A decision none of them settles is a question.
2. Problem or goal: one plain line.
3. Impact on users: one line - who, how badly, how often.
4. Decide: BUILD a feature, FIX a bug or finding, SKIP (false, already done,
   or negligible impact), or ASK when the feature needs a decision. A finding
   that a test is missing or ineffective for behavior this PR adds or changes
   is a FIX.
5. For FIX: find the root cause - the first place the code goes wrong, not
   where the symptom shows - and the assumption that fails there. Use
   `git log` and `git blame` to learn why the code is written that way; the
   change must keep whatever that protects. No guard, catch or special case at
   the symptom unless that layer owns the check.
6. For BUILD and FIX: find the best change, not the first one that works.
   Weigh the real alternatives, such as another layer, and choose the one that:
   - closes every path to the item's problem, not just the one the item
     names; the same kind of bug in other code is a separate bug (step 9);
   - keeps every user of the changed code working: list the callers,
     overrides and sibling implementations, and why each still works;
   - adds no avoidable cost on hot paths: say whether the changed code runs
     per row, per page frame, per query or once, and what the change adds;
   - adds no race: name the threads that share the changed state, if any,
     and why ordering, visibility and object lifetime stay safe;
   - reuses what exists: search for a helper, an established pattern or an
     earlier fix of the same kind of bug, and build on it.
   Between equally good changes, choose the smaller.
7. For BUILD and FIX: write the test plan. Go through the 14 dimensions of
   CLAUDE.md "What exhaustive means". For each that applies, list the tests:
   name, input -> expected result, and where the expected result comes from
   (never from running the code under test). For each that doesn't apply,
   give the reason. Name the fuzz harness to extend and the shapes to pin, the
   oracle (reference query, sqllogictest file or model), the existing test
   classes that exercise the code you change, and, for a speed change, the
   deterministic performance assertion and the benchmark.
8. For BUILD and FIX: write the PR body bullet, per CLAUDE.md "Git & PR
   Conventions": the net effect against the PR base, what users gain or no
   longer suffer, in at most 15 words, not what the code does. For a change
   made for speed or on a data path, name the JMH benchmark whose results the
   PR reports. The bullet is none when users see no change against the PR
   base, such as a fix for a bug only this PR's own commits have, or a
   missing test.
9. Other bugs you noticed, including the same kind of bug in other code. Mark
   each PR if it is in code this PR added or changed (compare with the PR
   base), RELATED if it is pre-existing but the item's own trigger - the same
   input, operation or fault - reaches it, else PRE-EXISTING. PR and RELATED
   bugs become items of their own in this run, so keep them out of this plan
   unless this change can't work without them. For a PRE-EXISTING one, write
   an issue body to <state>/<ID>/issue-<slug>.md. Add [security] when a bug
   is security-sensitive and [crash] when hostile input can crash or hang the
   server through it.

Write <state>/<ID>/analysis.md, starting with the verbatim item; include the
alternatives you rejected and why, and the test plan as a table: dimension,
applies, tests or reason. Reply with only these lines:
DECISION: BUILD | FIX | SKIP | ASK
PROBLEM: <one line>
IMPACT: <one line>
EFFECT: <the PR body bullet; the JMH benchmark to report, if any> | none
CHANGE: <one line, naming the files> | REASON: <one line, for SKIP> | QUESTIONS: <for ASK, one line each>
WHY: <one line: why this change beats the alternatives>
TESTS: <one line: the dimensions covered and the ones not applicable>
OTHER: none | one line per bug: PR, RELATED or PRE-EXISTING [security] [crash], file:line, problem, issue body path (PRE-EXISTING only)
```

### Check

```text
Independent check of a proposed change and its test plan in <repo>. You
didn't write them. Don't edit anything.

Read <state>/<ID>/analysis.md: the item, the proposed change, the rejected
alternatives, what else uses the code it changes, and the test plan. Then read
the code and try to break the proposal:
- What works today that the change would break? Consider other callers and
  inputs, NULL, empty and boundary values, error paths, cleanup, object
  reuse (toTop, clear, pools), and large data.
- Does the item's problem stay reachable through a path the change misses?
- Does it patch the symptom instead of the cause?
- Does it add avoidable cost on a hot path: allocation, copying, or a call
  or branch per row or page frame?
- Can it race with another thread: unsafe publication, check-then-act, use
  after close?
- Does it reimplement something the codebase already has?
- Is a rejected or overlooked alternative better?
Then hold the test plan to CLAUDE.md "Testing":
- a dimension that applies but is marked not applicable or covered thinly;
- an expected result that comes from running the code under test;
- a fuzz harness that covers the area but is not extended, or a missing
  oracle where one exists.
[Second proposal only: the first was sent back for: <REDESIGN lines, or the
build's STOPPED reason>. Repeat as a REDESIGN line any of it this proposal
leaves unresolved.]

Report only problems you can show in the code or the plan, with file:line or
the dimension and the missing case. Style preferences don't count; neither
does a wrong statement in the analysis that changes nothing about the change
or its tests. Label each problem:
- REDESIGN: built as planned, the change would be wrong for this item: it
  breaks what works today, leaves the item's problem reachable, patches the
  symptom, adds avoidable hot-path cost or a race, reimplements existing
  code, or is clearly worse than an alternative. Show how.
- ADD: a gap in the test plan: a missing test, a dimension skipped for a
  wrong reason, or an expected result without an independent source. Name
  the dimension, the case, input -> expected, and its source. A problem that
  more tests or a citation would solve is ADD, never REDESIGN.
- ADJACENT: a separate bug the change neither causes nor must fix to solve
  the item, including the same kind of bug in other code. Mark it PR (in
  code this PR added or changed), RELATED (pre-existing, but the item's own
  trigger reaches it) or PRE-EXISTING, [security] when it is
  security-sensitive, and [crash] when hostile input can crash or hang the
  server through it.
Reply with only: OK, or at most eight lines, one per problem, each starting
with its label, REDESIGN lines first.
```

### Build

```text
Build one item in <repo>. Follow CLAUDE.md, including "Testing".
The plan is <state>/<ID>/analysis.md plus the test-plan amendments in
<state>/<ID>/amendments.md; an independent check approved it. Probes that
reproduced the problem, if any, are in <state>/<ID>/probes/.
Earlier items in this run: <ID: one line each, or none>.
Run `git status --porcelain` first and never edit a file it lists.

1. Write the planned tests, amendments included, and run them. They must
   fail: for a bug, for the reason in the plan; for a feature, because the
   behavior doesn't exist yet. Leave out an amendment that turns out wrong or
   depends on another item, and say why on TESTS.
   A test that passes before the change proves nothing; rework it. Each test
   asserts the exact result - rows, value, error message and position, state -
   and stays plain, as CLAUDE.md "Writing tests" requires (no reflection).
   For a missing-test item, write the test, then show it can fail by briefly
   breaking the code it protects; restore that file with
   `git checkout -- <file>`.
2. Build the change as planned. When it changes Rust code that Java calls,
   run the Java tests with `-P build-rust-library` so they load the changed
   library, and run the Rust checks CLAUDE.md requires.
3. Run the planned tests and the existing tests of the code you changed
   (listed in the plan) through
   `python3 ci/incremental_compile.py -- test -Dtest=<classes>`. All must
   pass. One Maven command at a time. A test that fails for a reason outside
   this item stays out of the commit and goes on OTHER.
4. Measure coverage before committing:
   python3 ci/diff_coverage.py --base HEAD --tests <the item's test classes>
     --paths <the production files you changed> --json <state>/<ID>/coverage.json
   Read the lines, branches and conditions that never ran and add tests that
   reach them, each asserting what the line does. Measure only the new tests
   by adding `--append --tests <new test classes>` to the same command; after
   changing production code, run the full command again. Repeat until every
   changed line and branch runs, or prove a line unreachable and guard it with
   an assertion.
5. Don't weaken an existing test (CLAUDE.md "Test integrity"). If an existing
   expectation was wrong, change it and say why on EXISTING and in the commit
   message.
6. If the plan turns out wrong (another approach or unplanned files are
   needed), stop: restore the files you modified with `git checkout --`,
   delete the files you created, and say why.
7. Commit only your files (`git add <paths>`). Commit message per CLAUDE.md:
   title of at most 50 characters, no type prefix; body with the problem or
   goal, the root cause or design, and the tests.

Never push, amend, rebase, reset, stash or clean.
Write <state>/<ID>/build.md. Reply with only these lines:
RESULT: BUILT <sha> | STOPPED: <one line why>
TESTS: <tests that failed first and pass now; other new tests; existing tests run>
COVERAGE: <changed lines x/y, branches x/y; each uncovered line with its reason, or none>
EXISTING: none | <existing tests changed: file, what and why>
FILES: <changed files>
OTHER: none | one line per bug, as in the analysis (write issue bodies the same way)
```

### Test review

```text
Independent review of the tests for one item in <repo>. You didn't write
them. Don't edit anything.

Item <ID>. The plan and its test plan are in <state>/<ID>/analysis.md, with
amendments in <state>/<ID>/amendments.md; the diff of commit <sha> is
<state>/<ID>/build.diff, and the coverage report is
<state>/<ID>/coverage.json. Read them and the code the diff changes, then
check the tests against CLAUDE.md "Testing":
- every dimension that applies has tests, and every skipped one a sound
  reason;
- each test asserts the exact result of the code it covers, not merely that
  it ran, so breaking that code would fail the test;
- expected results come from an independent source, not from the code's
  output;
- every changed line, branch and condition runs, or the exception is
  justified;
- where a fuzz harness covers the area, its generator learned the feature
  and a test pins the new shapes;
- where an oracle applies, a reference query, sqllogictest file or model
  checks the results;
- the commit doesn't weaken an existing test;
- the tests follow "Writing tests": plain, reusing helpers, no reflection.
Ask only for tests whose absence leaves changed behavior unprotected; not
more cases of a covered kind, and nothing for unchanged code.
Reply with only: OK, or GAPS followed by one line per missing or weak test,
most important first:
<dimension>: <case>: <input> -> <expected>; protects <file:line>
```

### Test gaps

```text
Add missing tests for one item in <repo>. Follow CLAUDE.md, including
"Testing".
Item <ID>: commit <sha>, item start <ITEM_START>, plan <state>/<ID>/analysis.md
and <state>/<ID>/amendments.md.
An independent review found these gaps:
<GAPS lines>
Run `git status --porcelain` first and never edit a file it lists.

1. Write one simple test per gap, next to the item's tests and reusing their
   setup; no reflection. Each asserts the exact result of the code it
   protects. Skip a gap that can't be tested reliably and say why.
2. Run them through `python3 ci/incremental_compile.py -- test -Dtest=<classes>`.
   If one fails, work out the correct result from the plan and how similar
   code behaves - never copy the actual output into the expectation.
   A wrong test: correct it. Wrong code: don't touch it; leave the test out
   and report the bug on OTHER.
3. Measure the new tests' coverage on top of the build's run:
   python3 ci/diff_coverage.py --base <ITEM_START> --append --tests <new test classes>
     --paths <the item's production files> --json <state>/<ID>/coverage-final.json
   If it reports no previous run, run all of the item's test classes without
   `--append`. Close any gap it shows.
4. Commit only the tests (`git add <paths>`). Commit message per CLAUDE.md,
   naming the item they cover.

Never push, amend, rebase, reset, stash or clean. One Maven command at a
time.
Write <state>/<ID>/tests.md. Reply with only these lines:
RESULT: ADDED <sha> | NONE: <one line why>
COVERAGE: <changed lines x/y, branches x/y; exceptions>
TESTS: <tests added, each with the gap it closes; gaps skipped and why>
OTHER: none | one line per bug, as in the analysis (write issue bodies the same way)
```
