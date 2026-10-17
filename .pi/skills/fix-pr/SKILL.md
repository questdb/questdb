---
name: fix-pr
description: Develop features and fix bugs or review findings from a list, one item at a time, with exhaustive tests enforced while developing - design the change and its test plan against CLAUDE.md "Testing" and have both challenged independently, build test-first, run the item's tests with coverage (ci/diff_coverage.py) and add tests until every changed line, branch and condition runs, have the tests reviewed independently and close the gaps; commit. When the queue is empty, run review-pr at level 3 over the whole PR, as before approval, and treat what it finds the same way until a review comes back clean. Bugs found on the way that the PR base already has become GitHub issues, not work for the run. Use when the user runs /skill:fix-pr or asks to develop or fix items with it.
allowed-tools: bash read edit write subagent ctx_execute ctx_execute_file
metadata:
  argument-hint: "[--include-adjacent] [--no-issues] [--review-level=N] [--max-review-rounds=N] <items: features, bugs or review findings>"
---

# Develop and fix with exhaustive tests

Handle the items one at a time, in order. An item is a feature to build, a bug
to fix, or a finding from a review.

1. Understand the item. Validate a bug or finding; pin down the behavior of a
   feature. State the problem or goal and the user impact in one line each.
   An item whose symptoms have more than one root cause is split, one item
   per cause.
2. Skip it if it is false, already done, or its impact is negligible. Ask the
   user when a feature's behavior is ambiguous.
3. Design the best root-cause change - argued correct for every input it
   reaches, alternatives weighed; no regressions, slowdowns, races or
   reinvented code - and the test plan that proves it, dimension by
   dimension, per CLAUDE.md "Testing". When no change can be argued correct,
   the item is blocked, not guessed at. An independent agent tries to break
   both before any code changes. Only a flaw in the design sends the
   plan back; the tests it finds missing join the plan, and other bugs it
   finds become separate items or are reported.
4. Write the failing tests first and build the change as designed; a design
   that turns out wrong goes back to step 3. Run the item's tests with
   coverage (`ci/diff_coverage.py`), read the lines, branches and conditions
   that never ran, and add tests until everything runs, measuring only the
   new tests with `--append`. Commit.
5. An independent agent reviews the tests against the plan and the rules; if
   it finds gaps, a fresh agent writes the missing tests. Commit.

Tests are written while developing; the review at the end is the last resort,
not the place where test cases get discovered. When the queue is empty, a
helper runs `review-pr` at level 3 over the whole PR - the review the user
runs before approving it. Its Critical and Moderate findings, and Adjacent
ones this PR introduced, become new items: each is a test that should have
existed. The other Adjacent findings become GitHub issues. The review runs
again after those items, until it finds nothing to fix or
`--max-review-rounds` is reached.

The run's scope is what the PR sets out to do plus what it breaks, as in
review-pr's "Keep the blast radius of the PR small": the items given, the
review's findings, and bugs this PR caused, this run's commits included;
`--include-adjacent` adds the review's Adjacent findings. Any other bug the
PR base already has becomes an issue, even when an item's trigger reaches
it: each fix in old code turns up more old bugs, so a run that chases them
doesn't end. CLAUDE.md's "Bundle related fixes into one PR" says where a fix
goes, not which bugs to fix. A bug in this run's own commits sends the item
that made it back to design.

CLAUDE.md governs code, tests and commit messages. Its "Testing" section is the
standard every item meets.

## Arguments

`$ARGUMENTS` is the text after `/skill:fix-pr`. If it contains no items, ask
for them.

- `--include-adjacent`: fix Adjacent review findings instead of filing them;
  file one that ends blocked.
- `--no-issues`: list Adjacent items instead of filing them.
- `--review-level=N`: `review-pr` level of each backstop review; default `3`,
  the level of the review before approval.
- `--max-review-rounds=N`: default `2`. A level-3 round takes hours.

## Keep the main session small

The main session only coordinates. Fresh helper agents analyze, build and test
each item and review the PR, write their details to files, and reply in a few
lines. Read only those replies - never the files in the state directory, unless
a reply is unclear or the user asks. A small main session is cheap to resume
after waiting on a helper; a large one is not.

Launch helpers one at a time as ordinary foreground `subagent` calls (never
`async: true`) with `context: "fresh"`, `model: "<HELPER_MODEL>"` (Setup) and
the task templates at the end of this file. While the main session waits
inside a tool call, pi keeps its prompt cache warm; a session that ends its
turn and is woken later by a background helper loses the cache and pays to
rebuild it.

Every agent of the run thinks at max. The builtin `worker` and `reviewer`
default to high, and a launch overrides that only through the `:max` suffix of
its model; `review-pr-runner` and the review's `review-prober` agents set max
in their definitions. The timeouts leave room for it.

| Helper | Agent | Extra parameters |
|---|---|---|
| Analysis | `worker` | `timeoutMs: 7200000`, `acceptance: { level: "none", reason: "fix-pr checks the result" }` |
| Check | `reviewer` | `timeoutMs: 3600000` |
| Build | `worker` | `timeoutMs: 10800000`, `acceptance: { level: "none", reason: "fix-pr checks the commit" }` |
| Test review | `reviewer` | `timeoutMs: 3600000` |
| Test gaps | `worker` | `timeoutMs: 5400000`, `acceptance: { level: "none", reason: "fix-pr checks the commit" }` |
| Review | `review-pr-runner` | `timeoutMs: 28800000`, `acceptance: { level: "none", reason: "fix-pr reads the report" }` |

The review helper is the only one that launches agents of its own: the
review's discovery agents and falsifiers. Its definition,
`.pi/agents/review-pr-runner.md`, allows that. If it fails twice, run the
review in the main session instead: read `.pi/skills/review-pr/SKILL.md` and
follow it with the helper's arguments.

## Setup

1. Record the repo root, the branch, `START=$(git rev-parse HEAD)`, the PR
   number if `gh pr view --json number` finds one, and the PR base:
   `git merge-base HEAD origin/<base>`, where `<base>` comes from
   `gh pr view --json baseRefName`, else from
   `git symbolic-ref --short refs/remotes/origin/HEAD`. Record the helper
   model too: `HELPER_MODEL="$PI_PROVIDER/$PI_MODEL:max"`, this session's
   model at max thinking.
2. If tracked files have uncommitted changes, list them and ask whether to
   continue. Commits never include them. If this session doesn't think at
   max (`$PI_REASONING_LEVEL`), say so and ask too.
3. Create a state directory outside the repo: `mktemp -d /tmp/fix-pr.XXXXXX`.
4. Parse the items. Each entry of a list of features or bugs is one item. From
   a `review-pr` report, queue Critical and Moderate findings and Adjacent
   ones this PR introduced (their Origin, or compare with the PR base), list
   Minor ones as not fixed (cosmetic) unless the user asks, and send the
   other Adjacent ones to the issue list unless `--include-adjacent`. Keep
   each item's text verbatim and give it a short ID (the user's or the
   review's number, or I1, I2, ...). Save the items, verbatim, to
   `<state>/items.md`.
5. Show the queue in a few lines and start. Don't wait for approval.

## Per item

Save `git status --porcelain` before each helper. Afterwards it must be
unchanged (a commit doesn't change it); if it isn't, stop and show the
difference. Record `ITEM_START=$(git rev-parse HEAD)` before the build.

1. **Analyze.** Launch the analysis helper. Show the user its PROBLEM and
   IMPACT lines, and for a change its CHANGE, WHY and TESTS lines.
   - `SKIP`: record the reason and go to the next item.
   - `BLOCK`: no change within the item can be argued correct. Mark the item
     blocked with the REASON and go to the next item.
   - `ASK`: show the QUESTIONS, wait for the user's answers, and run the
     analysis again with them.
   - `SPLIT`: replace the item with its ITEMS lines, at the front of the
     queue in their order, with the IDs <ID>a, <ID>b, ...; each names
     `<state>/<ID>/analysis.md` as context.
   - `OTHER` lines: a `PR` bug becomes a new item at the end of the queue,
     unless it names an item of this run, which it reopens (step 6); a
     `PRE-EXISTING` one goes to the issue list. Skip a bug already queued or
     listed.
2. **Check.** Launch the check helper. It didn't write the plan and can't edit
   anything; its only job is to find what the change would break, a better
   change, or a hole in the test plan. It labels each problem `REDESIGN` (the
   design is wrong for this item), `ADD` (a test the plan lacks that the
   change would pass) or `ADJACENT` (a separate bug, including the same kind
   of bug in other code). Only `REDESIGN` sends the plan back. A `REDESIGN`
   line that only asks for more tests or a citation counts as `ADD`. An `ADD`
   line that expects the planned change to fail its test counts as
   `REDESIGN`, or as `ADJACENT` when it shows a separate bug the PR base has
   too: a failing test is a flaw, not a gap. A line about a separate defect
   elsewhere, even of the same kind, counts as `ADJACENT`. An `ADJACENT`
   line that names the item under check counts as `REDESIGN`: its own
   commits are part of its design.
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
     EXISTING lines, and handle OTHER lines as in step 1.
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
6. **Reopen.** A `PR` bug on a helper's OTHER or ADJACENT line that names an
   item of this run - its commits caused the bug or contain it - reopens
   that item instead of becoming a new one: the item's design missed
   something, and a patch on top would repeat the miss. Move the item to the
   front of the queue and run it again from step 1 as a revision, with the
   bug as what sent the design back; its build commits on top of the earlier
   ones. An item is reopened once: if the reopened proposal is sent back, or
   a bug is tied to the item after it is rebuilt, mark it blocked. A
   reopened item that ends blocked lists reverting its commits as one option
   for the user.

## Backstop review

When the queue is empty, review the whole PR as the user does before
approving it: every commit since the PR base, including those from before
START, not only this run's. Number the rounds from 1.

1. With a PR, write `<state>/pr-body.md` without reading it: in round 1 the
   published body (`gh pr view <PR> --json body -q .body`), later the
   previous round's `<state>/review<N-1>/pr-body.md`, followed by the EFFECT
   bullet of each item built since, one `- ` line each. The review checks the
   body the PR will get, not the published one.
2. Launch the review helper for round N. It follows review-pr with
   `<PR> --range=<PR base>..<HEAD sha> --level=<review-level>
   --body=<state>/pr-body.md --prior=<files>`; without a PR, drop `<PR>` and
   `--body`. The prior files are the `report.md` of every earlier round, and
   `<state>/items.md` when the items came from a review: the review re-tests
   their findings at HEAD. Its Step 2.6 measures the coverage of the whole PR
   at HEAD, in a scratch worktree, and grades never-run code by what it would
   break if it were wrong.
3. Queue each FINDING line as a new item with the ID `R<N>-<ID>` and run it
   through the per-item steps. Its verbatim text is the file
   `<state>/review<N>/<ID>.md`: name that file in the analysis task instead of
   quoting it. Each one is a test the standard should have produced: its test
   plan names the dimension of CLAUDE.md "Testing" that missed it. Send the
   ISSUE lines to the issue list, with their bodies in
   `<state>/review<N>/issue-<slug>.md`, or queue them under
   `--include-adjacent`; invoking this skill is the user's request to file
   them.
4. If a finding concerns an item this run already built or skipped, don't loop
   on it: list it for the user with both views.
5. Repeat until a round queues no item, or `--max-review-rounds` rounds have
   run. Build what the last round queued anyway: no review sees those items,
   and the final summary says so.

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

- One local commit per build of an item for the change and its tests, plus
  one if the test-gaps step added tests. Never push, amend, rebase, reset,
  stash, clean, switch branches or create worktrees; the one exception is
  the scratch worktree review-pr creates and removes in the backstop review.
- Stage only the files a helper changed; never `git add -A` or `git commit -a`.
- A helper may ask the main session a question mid-task
  (`contact_supervisor`). Answer only with facts from its task, the state
  directory or the user. Never approve a change to the design - new logic,
  another approach - since the main session hasn't read the code: tell the
  helper to stop and give the reason in its reply (`STOPPED`, or `ASK` or
  `BLOCK` from the analysis), and handle that reply as usual. Only the
  analysis and the check decide a design.
- Never weaken an existing test to make a change pass (CLAUDE.md "Test
  integrity"); a changed expectation needs its reason in the commit message.
- `java-questdb-client/` is a separate repo: commit inside it first, as
  CLAUDE.md says.
- Don't call a failure pre-existing or flaky without proof.
- If a helper fails to launch, retry once; then stop and report. If it
  times out or ends without its reply lines and the repository is
  unchanged, launch a fresh one once - for the analysis, as a continuation
  of what the first one wrote. Don't resume the old session: it has to
  rebuild its whole context in one turn. If the fresh one fails too, mark
  the item blocked with what it left and go to the next item; if two items
  in a row end this way, stop and report. The review helper is the
  exception: after it fails twice, the main session runs the review itself
  ("Keep the main session small").

## Final summary

| ID | Item | Impact | Outcome | Coverage |
|---|---|---|---|---|

Outcome is `built <sha>` (plus `tests <sha>` when the test-gaps step added
tests), `skipped: <reason>` or `blocked: <reason>`; add `not reviewed` to an
item built after the last review round. A reopened item shows each run, as
in `built <sha>; reopened: blocked: <reason>`. Coverage is the final changed
lines and branches covered, e.g. `42/42 L, 18/18 B`.

Then one line each: review rounds, each with its verdict, what it found and
the PR's measured coverage, and the path of the last round's report, which
holds its Minor findings and the never-run lines it judged low-exposure;
issues filed or matched (links); items left for the user (blocked,
security-sensitive, unfiled `ADJACENT` bugs, disagreements, items not
reviewed); and `git log --oneline <START>..HEAD`. State that nothing was
pushed. Then print in full each crash issue the Issue list could not file.

End with the PR body, per CLAUDE.md "Git & PR Conventions". With a PR, print
the last round's `<state>/review<N>/pr-body.md` plus the EFFECT bullet of each
item built after that round, and the title and labels from that round's report
when it changed them. Without a PR, print the EFFECT bullet of each built item.
Skip EFFECT lines that are `none`, and add any benchmark an EFFECT line names,
whose JMH results go in the body. No test plan.

## Task templates

Fill in the `<placeholders>`; keep tasks this short.

### Analysis

```text
Analyze one item in <repo>. Follow CLAUDE.md, including "Testing".
Don't change existing files or commit. For probes, create new files; when
done, copy those that reproduce the problem to <state>/<ID>/probes/ for the
build, and delete them from the repo. Probes run the code as it is, at HEAD
or the PR base. Don't build or run changed copies of production code: a
design is argued here and tried by the build, after the check.
Write <state>/<ID>/analysis.md as you go, adding each step's result as you
reach it, so that a timeout loses nothing.

Item <ID> (<feature | bug | review finding | bug found in this run>): <verbatim item>
PR base: <sha>. Items of this run so far: <one line per item: ID, commits,
summary; or none>.
[Revision only: your previous proposal is in <state>/<ID>/analysis.md, with
test-plan amendments in <state>/<ID>/amendments.md if that file exists. Its
design was sent back: <REDESIGN lines, the build's STOPPED reason, or the bug
that reopened the item>. Find the assumption in its argument that this
breaks, correct it, and derive the design again from there; a change that
only answers the example is not a revision. Or show with file:line why an
objection is wrong. Fold these test-plan gaps and the amendments into the
new plan where they still apply: <ADD lines, or none>.]
[Answers from the user: <answers>]
[Continuation only: an earlier helper ran out of time on this item. Continue
from what it wrote in <state>/<ID>/; don't repeat its probes.]

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
   or negligible impact), ASK when the feature needs a decision, BLOCK when
   no change within the item can be argued correct (step 6), or SPLIT when
   the item has more than one root cause (step 5). A finding
   that a test is missing or ineffective for behavior this PR adds or changes
   is a FIX. A bug found in this run that the PR base already has is a SKIP;
   report it on OTHER as PRE-EXISTING (step 9).
5. For FIX: find the root cause - the first place the code goes wrong, not
   where the symptom shows - and the assumption that fails there. Use
   `git log` and `git blame` to learn why the code is written that way; the
   change must keep whatever that protects. No guard, catch or special case at
   the symptom unless that layer owns the check. If the item's symptoms come
   from more than one root cause, decide SPLIT: name one sub-item per root
   cause, in the order to fix them, and stop there.
6. For BUILD and FIX: find the best change, not the first one that works.
   Weigh the real alternatives, such as another layer, and choose the one that:
   - is correct for every input it reaches, by the argument below;
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
   Argue the change correct; a few sentences do for a small change. State
   what the changed code must guarantee for the result to be right, and show
   that every input the change newly lets through, and every path it adds or
   skips, keeps each guarantee. A guard protects all the code after it, not
   only the case it was written for: before relaxing one, find what each
   later step assumes about the inputs it kept out.
   Before adding an early return, list what the code it skips ensured.
   Where the change can't tell which way an input goes, as with an unknown
   type, it takes the safe side. Don't pick between variants by trying them:
   probes show how the current code behaves, and the design follows from the
   argument. If no change within the item can be argued correct, decide
   BLOCK and say what a sound fix would take.
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
9. Other bugs your work ran into, such as the same kind of bug in other
   code; don't go looking for more. Mark each one PR if this PR caused it,
   this run's commits included: some trigger gets a worse result than on
   the PR base, or the bug is in behavior this PR adds. Add the ID of the
   item of this run whose commits caused it or contain it, if any; a bug in
   the commits of the item you analyze belongs in this plan, not here. Mark
   it PRE-EXISTING when the PR base gives the same result - run the trigger
   there unless the base code settles it - even when this item's trigger
   reaches it. PR bugs become items of this run or reopen the item named,
   so keep them out of this plan unless the item can't be done without
   them. PRE-EXISTING bugs become issues, not work for this run: fix one in
   this plan only when the item can't be done without it; otherwise leave
   its input out of this plan's tests, with a comment naming the bug, and
   write an issue body to <state>/<ID>/issue-<slug>.md. Add [security] when
   a bug is security-sensitive and [crash] when hostile input can crash or
   hang the server through it.

analysis.md starts with the verbatim item and holds the argument that the
change is correct, the alternatives you rejected and why, and the test plan
as a table: dimension, applies, tests or reason. Reply with only these
lines:
DECISION: BUILD | FIX | SKIP | ASK | BLOCK | SPLIT
PROBLEM: <one line>
IMPACT: <one line>
EFFECT: <the PR body bullet; the JMH benchmark to report, if any> | none
CHANGE: <one line, naming the files> | REASON: <one line, for SKIP or BLOCK> | QUESTIONS: <for ASK, one line each> | ITEMS: <for SPLIT, one line per sub-item: its root cause and the tests or symptoms it covers>
WHY: <one line: why this change beats the alternatives>
TESTS: <one line: the dimensions covered and the ones not applicable>
OTHER: none | one line per bug: PR, PR <item ID> or PRE-EXISTING [security] [crash], file:line, problem, issue body path (PRE-EXISTING only)
```

### Check

```text
Independent check of a proposed change and its test plan in <repo>. You
didn't write them. Don't edit anything.

Read <state>/<ID>/analysis.md: the item, the proposed change and the
argument that it is correct, the rejected alternatives, what else uses the
code it changes, and the test plan. Items of this run so far: <one line per
item: ID, commits, summary; or none>. Then read the code and try to break
the proposal:
- Does the argument hold? Find an input the change newly lets through, or
  a path it adds or skips, that breaks a guarantee the result depends on,
  whether or not the argument names it.
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
- Does it change more than the item needs, such as fixing a separate
  pre-existing bug the item can do without?
Then hold the test plan to CLAUDE.md "Testing":
- a dimension that applies but is marked not applicable or covered thinly;
- an expected result that comes from running the code under test;
- a fuzz harness that covers the area but is not extended, or a missing
  oracle where one exists.
[Revision only: the previous proposal was sent back for: <REDESIGN lines,
the build's STOPPED reason, or the bug that reopened the item>. Repeat as a
REDESIGN line any of it this proposal leaves unresolved, including the same
flaw reached through another input or clause.]

Report only problems you can show in the code or the plan, with file:line or
the dimension and the missing case. Style preferences don't count; neither
does a wrong statement in the analysis that changes nothing about the change
or its tests. Label each problem:
- REDESIGN: built as planned, the change would be wrong for this item: it
  breaks what works today, leaves the item's problem reachable, patches the
  symptom, adds avoidable hot-path cost or a race, reimplements existing
  code, changes more than the item needs, or is clearly worse than an
  alternative. Show how.
- ADD: a gap in the test plan that the planned change would pass: a missing
  test, a dimension skipped for a wrong reason, or an expected result
  without an independent source. Name the dimension, the case, input ->
  expected, and its source. A problem that more tests or a citation would
  solve is ADD, never REDESIGN. A test the planned change would fail is a
  flaw, not a gap: REDESIGN, unless it shows a separate bug the PR base has
  too, which is ADJACENT.
- ADJACENT: a separate bug the change neither causes nor must fix to solve
  the item, including the same kind of bug in other code. Mark it PR when
  this PR caused it, this run's commits included (some trigger gets a worse
  result than on the PR base, or it is in behavior this PR adds), with the
  ID of the item of this run whose commits caused it or contain it, if any;
  else PRE-EXISTING. Add [security] when it is security-sensitive, and
  [crash] when hostile input can crash or hang the server through it.
Reply with only: OK, or at most eight lines, one per problem, each starting
with its label, REDESIGN lines first.
```

### Build

```text
Build one item in <repo>. Follow CLAUDE.md, including "Testing".
The plan is <state>/<ID>/analysis.md plus the test-plan amendments in
<state>/<ID>/amendments.md; an independent check approved it. Probes that
reproduced the problem, if any, are in <state>/<ID>/probes/.
Items of this run so far: <one line per item: ID, commits, summary; or none>.
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
6. Build only the design the plan describes. Stop when it turns out wrong:
   another approach or unplanned files are needed; a planned, amended or
   existing test that is right fails because of the change; or the change
   needs logic the plan doesn't describe - a guard, branch, early return or
   fallback. Don't design that logic yourself and don't ask the supervisor
   to approve it: the analysis and the check decide a design. Restore the
   files you modified with `git checkout --`, delete the files you created,
   and say why on STOPPED.
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
and <state>/<ID>/amendments.md. Items of this run so far: <one line per item:
ID, commits, summary; or none>.
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

### Review

```text
Review round <N> of a fix-pr run in <repo>: the review the user runs before
approving the PR. Read .pi/skills/review-pr/SKILL.md and follow it with these
arguments:
<PR> --range=<PR base>..<HEAD sha> --level=<review-level>
  --body=<state>/pr-body.md --prior=<comma-separated prior files>
[Without a PR: --range=<PR base>..<HEAD sha> --level=<review-level>
  --prior=<comma-separated prior files>]
You are the review's parent: launch its agents yourself. Write your files to
<state>/review<N>/.

When the review is done, write there:
- report.md: the Step 4 report;
- <ID>.md (C1, M1, A1, ... as numbered in the report): each Critical and
  Moderate finding, and each Adjacent finding this PR introduced (its
  Origin), verbatim;
- issue-<slug>.md: the draft of each other Adjacent finding, ready to file;
- pr-body.md: the body with Step 2's replacements applied (with a PR only).
Reply with only these lines:
VERDICT: approve | approve with comments | request changes | needs discussion
FINDING: <ID> <Critical | Moderate | Adjacent> <one line>, one line each | none
ISSUE: <slug> [security] [crash] <one line>, one line each | none
MINOR: <count>
METADATA: OK | changed: <title, body, labels> | no PR
COVERAGE: <changed lines x/y, branches x/y of the whole PR>
```
