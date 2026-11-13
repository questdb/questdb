---
name: review-pr
description: Review a GitHub pull request, a local Git range, a PR's local commits, or the checked-out branch with its unpushed and uncommitted changes (--local) against QuestDB coding standards. Use when asked to review a PR, given a PR number or URL, given --range=<base>..<head>, given both, or given --local, optionally with a review level 0..3. Reports only the problems it can prove, in plain language, and rates whether each one should block the merge.
allowed-tools: bash read subagent
metadata:
  argument-hint: "[PR number or URL] [--range=<base>..<head> | --local] [--level=0..3] [--block-at=critical|high|medium|low] [--body=<file>] [--prior=<file>,...]"
---

# Review a QuestDB pull request

**Version 5.0.** Each PR that changes this file raises it.

**Usage:** `/skill:review-pr [PR number or URL] [--range=<base>..<head> | --local] [--level=0..3] [--block-at=critical|high|medium|low] [--body=<file>] [--prior=<file>,...]`

pi appends the text after `/skill:review-pr` as a `User:` message; treat it as
`$ARGUMENTS`. It names what to review:

- **A PR number or URL:** the PR as pushed to GitHub. Local changes are ignored.
- **`--range=<base>..<head>`:** a local Git range. Leave out the head
  (`--range=<base>..`) to review the working tree, uncommitted changes included.
- **A PR and `--range` together:** the PR's local commits, which may not be
  pushed yet, as `fix-pr` reviews them. The PR supplies the title, body, labels
  and comments; the range supplies the code.
- **`--local`:** the checked-out branch as it is on disk: its commits, pushed or
  not, its uncommitted changes and its new files, with the branch's PR when it
  has one. `--local` and `--range` both name the code to review; when both are
  given, ask which one the user means.

If no target is given, ask for one. The options:

- `--level=N` (also `-lN`, or a bare digit 0-3): how much of the review runs;
  default 0 ("Review levels").
- `--block-at=<tier>`: the lowest risk that blocks the PR; default `high`. A
  finding rated lower ships with the PR, and a private issue tracks it (Step 8).
- `--body=<file>`: check this file as the PR body instead of the published one,
  such as a draft that covers commits not pushed yet (Step 2).
- `--prior=<file>,...`: reports or finding lists from earlier reviews of this
  change. Step 4f checks whether their findings still hold.

Remove the options before passing the PR number or URL to `gh`.

This skill runs in pi. Use `bash` for read-only `gh`, `git`, `rg`, `fd` and
`find` commands, `read` for files, and `subagent(...)` for the review's agents.
Don't edit files or push. Build and run tests only in the scratch worktree of
Step 5, never in the user's checkout.

## How to review

You are a senior QuestDB engineer reviewing a change before it merges. Users
trust QuestDB with their data: a bug can lose or corrupt it, and a fix takes a
release to reach them. Find the problems that would hurt users, and prove each
one. Be critical and direct. Don't be nice, and don't try to look thorough by
reporting a lot.

- **Report only what you can prove.** Each finding costs the author a round of
  work, and each weak one makes them trust the rest of the report less. Block
  only on problems with a real consequence for users, and report the rest at
  the severity they deserve. Approving a correct change is the normal outcome,
  and a review with no findings is a good review when the change is right.
- **Severity is not risk.** Severity is what a user who hits a problem loses.
  Risk adds how likely users are to hit it. A real bug that few users will meet
  ships with a private issue instead of holding up the PR (Step 8). Never doubt
  a bug because it is rare: prove it, and let the risk rating decide whether it
  blocks.
- **Read beyond the diff.** Read the code around each change, its callers and
  its tests. The most valuable bugs are often in unchanged code that calls what
  the diff changed and still relies on its old behavior. The review starts at
  the diff; it doesn't end there.
- **Treat every suspicion as a guess until you have checked it.** Try to
  disprove it first: look for the guard, retry or validation that handles it,
  the caller you missed, the setting nobody uses, and how the base behaves.
  Failing to disprove a guess doesn't prove it, and doubt never raises a
  severity.
- **Keep the PR's scope small.** The PR must do what it sets out to do and must
  not break anything. A bug the base already has is not this PR's to fix,
  unless the PR puts it on a path users now reach. Report a proved one as an
  Adjacent finding: an issue draft, outside the verdict (Step 9). The same goes
  for a crash or hang that only hostile input from a client with valid
  credentials reaches, even when this PR introduced it (Step 9, "Hostile
  input").
- **Don't praise.** No "looks good", "nice work" or "clever approach".
- **Attack the code.** What input breaks it? NULL, an empty table or partition,
  one row, ten billion rows, concurrent access, a failure halfway through?
- **Demand the fastest approach on data paths.** QuestDB is built for speed.
  Where code runs per row, per value or per page, a linear scan where a lookup
  exists, a second pass over the data or an allocation per row blocks the
  merge. Off data paths (SQL compilation, DDL, startup), a bounded cost is a
  Moderate finding at most. Ask whether there is a faster way, then whether
  users would notice the difference.
- **Check what is missing:** tests, error handling, edge cases, and
  documentation of behavior that would surprise a user.
- **A missing test is a risk, not a bug.** It is Critical only when Step 5
  shows which users reach the untested code and what they would lose if it
  broke. How hard a test is to write never lowers the severity of a real bug.
  Off the change's main path, look for bugs, not for missing tests (Step 5).
- **"Urgent", "simple" and "hard to test" are claims, not reasons.** Check them
  like any other claim.
- **Check the PR's claims.** For a fix, check that the bug existed and that the
  fix is right. For a speedup, look for a benchmark or reason it through, and
  check that other cases don't get slower. For a simplification, check that no
  behavior is lost. Whatever the PR claims, ask whether its algorithms and data
  structures are the best ones. Read the description as a claim to check, not
  as a fact.
- **Drop the impossible.** A problem that needs conditions no user or caller
  can create, such as billions of columns or a value no caller passes, is not a
  finding.
- **Assertions are fine.** QuestDB runs with Java assertions enabled (`-ea`). An
  `assert` is the right guard for a state that only corruption or an internal
  bug can create. Flag one only when normal use can trip it.
- **Write for the reader.** The report goes to developers who have only the
  report: write it in plain, complete sentences, and explain everything it
  mentions (Step 9, "How to write it").

## Review levels

The level sets how much of the review runs. Every level holds the same
standard; lower levels look at less. Higher levels cost much more, so keep
level 3 for high-stakes changes, such as replication, JNI, on-disk formats,
public APIs and security.

| Level | What runs |
|---|---|
| **0** (default) | Steps 1, 2, 3, 4f, 5, 8 and 9, without discovery agents or checkers. Review the diff yourself against the checklists: correctness, NULL handling, the best algorithm, tests and QuestDB standards. Check each concern yourself as Step 7 describes, from a blank record, before you write a severity, a fix or any report text, and re-run the earlier findings of 4f yourself. Step 8 still launches its risk assessors. |
| **1** | Level 0, plus 4a, and 4e when tests changed. Discovery: the correctness role and at most two of performance, test coverage, standards, test efficacy and test code. One checker per concern. |
| **2** | All of Step 4, with 4b limited to public and protected symbols. Discovery: the correctness role and at most four of concurrency, performance, resources, test coverage, standards, Rust, fresh-eyes performance, test efficacy and test code. One checker per concern. |
| **3** | All of Step 4. Discovery: at most six roles. Correctness always; callers when changed symbols have callers outside the diff; a role for an area only when the diff touches that area; the test roles only for changed tests or a fix; fresh eyes only when another independent pass is worth it. One checker per concern. |

Depth comes from evidence, such as a traced path and a run that shows the bug,
not from the number of agents.

Open the review with one line that names the target, the level, the model and
this skill's version (under the title), such as "Reviewing PR #1234 at level 2
with <provider>/<model>, thinking <level>, review-pr <version>". With `--local`,
write "Reviewing local changes of PR #1234 ...", or "of branch `<branch>`"
without a PR. When the level was not given, add that level 3 is the full
review. Take the model from pi, never from memory: models misstate their own
name and version.

```bash
printf '%s/%s, thinking %s\n' "$PI_PROVIDER" "$PI_MODEL" "$PI_REASONING_LEVEL"
```

If it prints no model, write `model unknown`.

## The review's agents

You lead the review: you choose the roles, keep private notes, decide which
concerns become findings, rate their severity and risk, and write the report.
Agents return concerns or evidence, never findings. Each task is
self-contained and read-only. Launch every agent with fresh context
(`context: "fresh"`) as a foreground call (`async: false`); a parallel launch
still runs them at the same time. Write large inputs, such as the diff and the
maps, to a temporary file and name it in each task instead of pasting them.

- **Discovery (Step 6):** pi's builtin `reviewer`, one task per role. It reads
  code and runs nothing: it has no shell, thinks at high and keeps its default
  timeout of 30 minutes. It gets the diff, the submodule decisions of Step 3,
  the change map of Step 4, the coverage map of Step 5, its role and the reply
  rules of Step 6. The two fresh-eyes roles get less (see their entries).
- **Checkers (Step 7):** `review-prober` (`.pi/agents/review-prober.md`), one per
  concern: a read-only reviewer with a shell, which runs the probes the check
  needs. Give each `timeoutMs: 3600000` and, in its task, a budget of about 30
  minutes: a probe that needs longer reports what it has and marks the rest
  unknown. A checker gets only the concern, stated neutrally, the revisions,
  the relevant files and the raw artifacts. Never send it the discovery agent's
  reasoning, a proposed severity or fix, other agents' opinions, or a claim
  that the concern was verified.
- **Risk assessors (Step 8):** `risk-assessor` (`.pi/agents/risk-assessor.md`),
  one per finding it rates. It gets what a checker gets, plus the trigger that
  was run, its results at the head and at the base, and the PR body.

## Step 1: Gather the context

Every mode ends this step with `$BASE` set: the commit the change is measured
against. Steps 3 and 7 need it; without it, the review can't tell what the PR
changed.

### A PR

Set `$PR` to the PR number or URL from `$ARGUMENTS`, then fetch the metadata,
the diff, the comments and the base:

```bash
PR='<PR number or URL from $ARGUMENTS, options removed>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr view "$PR" --comments
# Replies in review threads on the diff, which --comments leaves out:
PR_API=$(gh pr view "$PR" --json url --jq .url | sed -E 's#https://github.com/([^/]+/[^/]+)/pull/#repos/\1/pulls/#')
gh api "$PR_API/comments" --paginate --jq '.[] | "\(.user.login) on \(.path):\(.line // .original_line): \(.body)"'
BASE=$(gh pr view "$PR" --json baseRefOid --jq .baseRefOid)
```

### A PR with a local range

When both a PR and `--range=<base>..<head>` are given, run the commands above
except `gh pr diff` and the `BASE` line, then take `$BASE` and the diff from Git
as for a local range, below. `<head>` is the reviewed revision wherever a step
names the PR's head, such as `REV` in Step 5. Step 2 runs, on the `--body` file
when one is given.

### A local range (`--range`)

With only `--range=<base>..<head>`, there is no PR, so no title, description or
labels. Take the diff from Git:

```bash
BASE='<base from --range>'
HEAD='<head from --range, or empty for the working tree>'
git diff "$BASE"${HEAD:+"...$HEAD"} --stat
git diff "$BASE"${HEAD:+"...$HEAD"}
git diff "$BASE"${HEAD:+"...$HEAD"} --name-only
```

With the head empty, the diff includes uncommitted changes, the usual case when
reviewing a `fix-pr` result before it is pushed. `git diff` doesn't show
untracked files: list them with `git status --porcelain` and read those that
belong to the change, above all new tests, or Step 5 misses them.

Without a PR, skip Step 2 and say so in the report. Every other step runs as
usual. In particular, the review still follows callers outside the changed
files: limiting it to those files would skip the search for breakage in
unchanged code, which finds the most valuable bugs.

### Local changes (`--local`)

`--local` reviews the checked-out branch as it is on disk. This step records
that code as a snapshot commit and reviews it as a range, so every later step
runs as it does for `--range`.

**Find the PR.** Without a PR number, use the branch's PR when its `state` is
`OPEN`. Review without a PR, and say why, only when HEAD is detached, the PR
found is not `OPEN`, or `gh` reports `no pull requests found for branch`. Any
other `gh` failure, such as a rate limit, a network error or a missing login,
says nothing about the PR: stop, show the error and ask whether to review
without the PR. Such a review skips Step 2 and the PR's earlier review threads
(Step 4f), so it may raise again what the author already declined, and it takes
`$BASE` from the default branch instead of the PR's base. A PR number given
with `--local` must be the branch's PR: when the command below shows another PR
or none, stop and ask the user to check that PR out (`gh pr checkout <number>`)
or to leave the number out; when `gh` fails otherwise, stop and ask as above.

```bash
cd "$(git rev-parse --show-toplevel)"
git branch --show-current
gh pr view --json number,state,baseRefName,headRefOid   # the branch's PR
```

**Set `$BASE`** to the commit where the branch leaves its base branch: the PR's
`baseRefName`, or the default branch without a PR, as `fix-pr` does.

```bash
BASE_REF='<baseRefName of the PR>'
# Without a PR: BASE_REF=$(git symbolic-ref --short refs/remotes/origin/HEAD | sed 's|^origin/||')
git fetch --quiet origin "$BASE_REF" || echo "fetch failed: using the local origin/$BASE_REF"
BASE=$(git merge-base HEAD "origin/$BASE_REF")
```

**Choose the new files.** List the untracked files and pick those that belong
to the change, such as new sources, tests and resources. Leave out the rest,
such as scratch notes, logs, dumps and data directories, and list them in the
report as not reviewed.

```bash
git --no-optional-locks status --porcelain | sed -n 's/^?? //p'
```

**Take the snapshot** with one command, as written: the subshell stops at the
first error, and every `git` command in it uses a temporary index. When it
fails, fix what it reports, such as a mistyped path, and run it again. The
snapshot writes only to Git's object store: no branch or ref moves, and the
user's index and working tree stay as they are. It holds each file as it is on
disk, staged or not, and the commit each submodule has checked out, but not
changes left uncommitted inside a submodule: list such a submodule in the
report as not reviewed beyond its commit.

```bash
REV=$(
  set -eu
  cd "$(git rev-parse --show-toplevel)"
  IDX=$(mktemp /tmp/review-pr-index-XXXXXX)
  trap 'rm -f "$IDX"' EXIT
  cp "$(git rev-parse --git-path index)" "$IDX"
  export GIT_INDEX_FILE="$IDX"
  git add -u
  git add -- <the untracked paths that belong to the change>   # drop this line when none do
  TREE=$(git write-tree)
  git -c user.name=review-pr -c user.email=review-pr@localhost \
    commit-tree -p HEAD -m 'review-pr --local snapshot' "$TREE"
)
echo "REV=${REV:?the snapshot failed}"
git diff --quiet "$BASE" "$REV" && echo "NOTHING TO REVIEW"
```

On `NOTHING TO REVIEW`, say so and stop. Otherwise continue as
`--range=$BASE..$REV`: as "A PR with a local range" when there is a PR, else as
"A local range". `$REV` is the reviewed revision wherever a step names the
head: Step 3 lists submodule pointer moves in `$BASE...$REV`, Step 5 builds its
worktree at `$REV`, and Steps 7 and 8 give `$REV` as the head. The review reads
files in the working tree, so tell the user that it covers the snapshot, and
ask them to leave the working tree alone until the review ends.

The report names as its head the snapshot `$REV`, the branch and its HEAD
commit, and with a PR, the PR's head on GitHub (`headRefOid`). When the branch
doesn't contain that head (`git merge-base --is-ancestor <headRefOid> HEAD`
fails), say so: the branch lacks commits pushed to the PR, or was rebased.
`approve-pr` and `reject-pr` look for the snapshot in the report, and post a
review of local changes only when the PR's head on GitHub has the same tree.

## Step 2: Title and description

Skip this step without a PR (`--range` or `--local` alone), and say so in the
report.

Check the title, body and labels against CLAUDE.md "Git & PR Conventions". With
`--body=<file>`, check that file as the body. Read the title as a QuestDB user
scanning the release notes, who never sees the code, and the body as a reviewer
who hasn't opened the diff yet.

- **Type.** A change users can see (query results, SQL, configuration, error
  messages, wire or on-disk format, anything breaking) takes `feat`, `fix` or
  `perf`. Flag a technical type (`chore`, `ci`, `test`, `build`, `refactor`,
  `style`) on such a change, and say what users see.
- **User-facing title** (`feat`, `fix`, `perf`, `docs`). Would that user
  understand it and see what they gain? Flag class and method names, internal
  mechanisms and jargon, and a title that describes the code change instead of
  its effect. The description repeats the verb.
- **Technical title.** Technical terms are fine, but the title alone must make
  the reason for the change obvious. Flag a title that only says what changed.
- **Body.** Short bullets, one per effect against the base branch, at most 15
  words each, with `Fixes #NNN` on the bullet for that issue, then the approach:
  one to three sentences per fix or feature. Flag:
  - a bullet that describes a code change instead of its effect;
  - an effect of the diff that no bullet states, above all a regression or a
    breaking change;
  - a bullet or an approach that the diff doesn't support;
  - a fix or feature without an approach;
  - a slowdown or tradeoff without its measured size;
  - a change made for speed, or one on a data path, without its JMH table;
  - a commit hash (a tandem PR's head and submodule pointers included), or a
    count of tests, files, lines or commits. Ask for its removal without
    checking it against the code: such details go stale, and the review
    records the revisions;
  - anything else: a test plan, coverage, fuzz seeds, investigation output
    (probe results, "fails without the fix" evidence, lists of covered shapes
    or configurations, "Not changed" lists), a walkthrough of the files, design
    narrative beyond the approach, selling. Never ask the author to add any of
    this.
- **Deferred findings** (Step 8) are not effects for this check: they are known
  bugs the PR ships with, tracked in private issues. Never ask for one in the
  body, and never name one in this part of the report.
- **Labels** match the title (`perf(sql)` gets SQL and Performance).
- Several related fixes in one PR are fine; don't ask for a split.

Leave out a problem someone already declined on the PR (Step 4f). For every
other problem, write the replacement, ready to paste: the title, or the whole
body. Finish the body check once you know what the diff does.

## Step 3: Submodule pointers

Runs at every level. A moved submodule pointer is not necessarily a change this
PR makes. Before reviewing anything inside a submodule, find out whether its
new commit is already on the submodule's default branch. Skipping this is how a
review blames a PR for months of upstream work that has already shipped.

List the pointer moves, then check each new commit:

```bash
# With --local, use $REV (Step 1) in place of HEAD: it holds uncommitted pointer moves.
git diff "$BASE...HEAD" --submodule=short | grep -E '^(diff --git|[+-]Subproject commit)'

cd <submodule path>
git fetch origin --quiet
DEF=$(git symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's|^origin/||')   # e.g. main, master
git merge-base --is-ancestor <new-sha> "origin/$DEF" && echo UPSTREAM || echo NOT-UPSTREAM
git branch -r --contains <new-sha>
```

Each move is one of:

- **Already upstream:** the new commit is on the submodule's default branch.
  That work has already landed upstream; this PR only moves the pointer to pick
  it up. Don't review it, don't blame this PR for its behavior, and build no
  findings from it. The one possible finding is code in this diff that calls
  the new submodule code wrongly; report it at that call, in this diff.
- **Not upstream:** the new commit exists only on a feature or PR branch. The
  submodule's changes are part of this change: review them.
- **Unknown:** the branch can't be found (no network, a shallow clone, a
  missing remote). Treat it as not upstream, and say in the report that the
  scope was chosen without this check.

Check nested submodules on their own: an OSS pointer that is not upstream says
nothing about the client pointer inside it, which is often already upstream in
the same PR. The report says, in a sentence, what each moved pointer means for
the review (Step 9).

## Step 4: Map the change

Before discovery, map what the change touches. Search with `rg` and `fd`; never
reason about callers from memory. Discovery agents get this map.

### 4a What changed

For each added or changed function, method, trait, struct field, SQL function
or operator, and public constant, write:

- **Symbol:** its fully qualified name.
- **Before:** signature, return type, errors thrown or returned, panics,
  mutation (`&self` or `&mut self`, `final` or not), ordering and idempotency,
  allocation, thread safety.
- **After:** the same.
- **Difference:** what changed in behavior, in one line.

"Refactored", "cleaned up", "improved" and "simplified" are not differences:
state the behavior that changed, or "no behavior change", and only after
checking.

### 4b Callers

For each changed symbol that is public, protected, package-private or exported
(`pub`, `pub(crate)`), search the whole repository with `rg` for every caller,
implementation, override and reference outside the diff, and list them by
file. In Java, also look for subclasses that override it, interfaces that
declare it, reflection (`getMethod`, `getDeclaredField`, `Class.forName`), SQL
function and operator registrations (`function_list.txt`, `OperatorRegistry`)
and service loader entries. In Rust, also look for trait impls, macros, JNI
exports and their Java callers, and `extern "C"` boundaries.

Never say "this is only used here" without the search that shows it.

### 4c Contracts

For each changed symbol, write one line per item, before and after:

- the inputs it throws or panics on;
- the errors it returns, and which callers pass them on;
- iteration order, sort stability and where NULLs sort;
- idempotency and re-entrancy;
- the locks it takes, in which order, and which it still holds when it
  returns;
- allocation, on a data path or at compile time;
- thread rules: `Send` and `Sync`, which threads may call it, threads attached
  to the JVM for JNI;
- whether `null` and the NULL sentinels (`Numbers.LONG_NULL`,
  `Numbers.INT_NULL`, NaN) are still told apart;
- cleanup: drop and cancellation in Rust, `finally` and `close()` in Java;
- SQL: the clauses it can now appear in that it couldn't before (WHERE, GROUP
  BY, JOIN ON, ORDER BY, window frames, partition filters, materialized view
  definitions).

### 4d Where else the change shows

End the map with the places the change is visible from that the diff doesn't
touch: the callers from 4b, grouped by where they run, such as data paths, SQL
compilation, async code, JNI, replication, materialized views and parallel
workers. Discovery must look at every entry: this list is where breakage in
unchanged code hides.

### 4e Tests and test helpers

Only when the PR adds or changes test code. Search with `rg` and `fd`:

- **Helpers the tests could reuse:** base test classes, shared setup, helper
  methods, fixtures and assertion utilities near the changed tests (`extends
  Abstract.*Test`, `*TestUtils`, `assertMemoryLeak`, `assertQuery`, protected
  helpers in base classes). A finding that a test rebuilds helper X needs X on
  this list.
- **Changed shared test code:** list the users of a changed base test class,
  helper or fixture, as in 4b. A change there can break every test that uses
  it.
- **What each test exercises:** for each new or changed test, the production
  symbols from 4a it reaches.

### 4f Earlier findings

Runs at every level. Collect the findings of earlier reviews of this change:
those in the PR's comments, reviews and review threads, and those in every
`--prior` file. Read the replies to them.

**An item someone pushed back on is settled.** A reply that declines it (won't
fix, by design, accepted as is, out of scope, left for a follow-up) or disputes
it with a reason settles it. Never re-test a settled item, and never raise it
again anywhere in the report, the title and description and Adjacent findings
included, even when it still reproduces and you disagree with the reply. The
same goes for a concern that restates it: the same problem in the same place,
however it is worded or found. Only a problem with another symptom, or one that
a later commit made worse than what the reply answered, is new. List the
settled items in the map, each with a link to its reply.

List every other earlier finding in the map: its trigger, the symptom and size
it claimed, and the commit that claims to fix it, if any. A claimed fix needs
checking like any claim: re-run each trigger at the reviewed revision and at
the base. At level 1 and above, each goes to Step 7 as a concern, and a checker
re-runs it; at level 0, re-run it yourself. A finding that still reproduces is
a concern like any other. One that no longer reproduces needs no line in the
report; if you mention it, say in full what the problem was and how you know
that it is fixed.

## Step 5: Test coverage

Runs at every level, for every PR that changes production code, and above all
for one without test changes: its test scrutiny happens here. At level 0, take
the behavior changes from the diff; at level 1 and above, from 4a.

### Build the map

One row per behavior change: each changed symbol whose behavior changed, split
further into each new or changed branch, error path, and NULL or boundary case.
For each row, record:

- **Change:** the symbol and the behavior, branch or path.
- **Test:** the test class and method that exercises it, found by searching the
  test tree with `rg` or `fd` for the symbol, the SQL function or operator name,
  the error message or the configuration key. Record the search: "existing
  tests probably cover it" doesn't count.
- **What fails:** the assertion that fails if this behavior breaks, and why.
  "The test calls the method" is not enough.
- **Who reaches it:** the supported operation, setting or event that runs the
  changed code, and who does it. "Public" or "user-visible" doesn't say who.
- **What they would see:** a plausible way the code could break, and what those
  users would then see. Tell real harm apart from cosmetic output, routine
  noise or inconvenience for developers.
- **Cases:** normal input, errors, NULL, boundaries, concurrency and cleanup, as
  they apply, each covered, not covered, or not applicable with a reason. A
  change that the "Crash tests" checklist names also has a crash case, which
  that checklist decides.
- **Decision:** covered, Critical gap, Moderate gap, accepted, uncommon path, or
  no behavior change (below), and why. For an untested row, also record the
  change's risk (how complex it is, how many callers it has, shared state and
  concurrency, the tests and safeguards around it), the least invasive test
  that would cover it, and what would make that test costly or fragile: setup,
  test-only hooks in production code, global state or async log capture,
  timing, platform dependence, and the alternatives you looked for. "Hard to
  test" alone is not evidence.

### Measure it

A search shows which tests might reach a change, not which lines they run. Once
the Test column is filled, run `ci/diff_coverage.py` (CLAUDE.md "Measuring
coverage locally") at the reviewed revision, unless the change touches no
production Java or Rust code. Build and run only in a scratch worktree, never
in the user's checkout, which may hold other work and its own build. Keep dots
out of the worktree's path: `Os.loadLib` cuts a library path at its first dot,
and tests can't start under such a path.

```bash
REPO=$(git rev-parse --show-toplevel)
TMP=$(mktemp -d /tmp/review-pr-XXXXXX)
WT="$TMP/src"
# REV is the reviewed revision:
#   PR:           git fetch origin "pull/<number>/head" "<baseRefName>"; REV=<headRefOid>
#   range:        REV=<head> (also with a PR)
#   --local:      REV=<the snapshot of Step 1; it already holds the change's untracked files>
#   working tree: REV=$(git stash create); REV=${REV:-HEAD}
#                 (a snapshot of the tracked changes; once the worktree exists,
#                 copy the change's untracked files from Step 1 into it)
git worktree add --detach "$WT" "$REV"
cd "$WT"
python3 "$REPO/ci/diff_coverage.py" --base "$(git merge-base "$BASE" "$REV")" \
  --tests '<test classes the change adds or changes, plus those the map names>' \
  --ignore-test-failures --json "$TMP/coverage.json"
```

The script runs from `$REPO` because the reviewed revision may predate it. If
the change adds no test and the map names none, skip the run: no test runs any
changed line. If the script can't run for another reason (exit code 2, such as
a missing client jar or Rust toolchain), install nothing: keep the map from the
searches, and say in the report that coverage was not measured, and why. A
build error, or a test that fails at the reviewed revision, is a concern for
Step 7. Remove the worktree when the review ends:
`git worktree remove --force "$WT"`.

### Update the map

- Each changed line, branch or condition that never ran belongs to an untested
  row; add a row where the map has none.
- A row marked covered whose lines never ran is untested: its test doesn't
  reach the change.
- **Judge code that never ran by what it controls, not by its size.** It may be
  wrong today, not only after a later change. Assume it doesn't do what it says
  (the guard lets the bad value through, the cleanup keeps the resource, the
  conversion returns a wrong number) and work out what a user would get:
  - **what it controls:** rows returned or written, data on disk, which path
    runs, a release or a rollback, a permission check, or only message text
    and logging;
  - **loud or silent:** an exception gets noticed; a wrong value, a lost row or
    a swallowed error doesn't;
  - **how users get there, and how often:** routine SQL, a rare setting, only a
    fault, or only hostile input;
  - **how far the damage spreads:** one query, the table on disk, replicas and
    backups, or the whole process (a Rust panic aborts the JVM);
  - **what would catch the mistake downstream:** an assertion, a validation, a
    checksum.

  Silent wrong or lost data, corruption, a security failure, or a crash or
  hang on a reachable path makes a Critical gap when it meets the bar below; a
  loud, bounded failure makes a Moderate gap. A failure that the hostile-input
  rule covers (Step 9) is no gap in this PR: list its lines as making no
  finding, and when a probe shows the failure, write an Adjacent finding. Error
  and cleanup paths are the code that most often never runs, and a mistake
  there tends to hide the failure or leave state half-written, so rarity alone
  never makes them harmless: disks fill and connections drop in production.
  One hunk can hold both grades: in a guard that rejects an oversized `RANGE`,
  a wrong count in the error message is cosmetic, but a guard that lets the
  range through can make `(int) steps + 1` wrap, so the join silently uses too
  few offsets.
- When the possible damage is Critical, reach the code with a probe test in the
  scratch worktree before deciding, with the expected result worked out
  independently. If the code already does the wrong thing, that is a bug,
  proved by the probe, not a coverage gap. Either way, the finding carries the
  probe as the test to add, except on an uncommon path, where a passing probe
  makes no finding (below).
- Code that never ran is never hidden. The report's Tests section lists the
  lines that make no finding, each with its reason (Step 9). Only a line proved
  unreachable and guarded by an assertion is left out.

### Uncommon paths need a bug, not a test

A missing test is a gap only on the change's main path: the behavior the PR
adds or fixes, reached the way its users usually reach it, with default
settings and the SQL, types, ingestion and operations they commonly use. That
includes the trigger of a fixed bug, and the main path's own error and cleanup
paths, since disks fill and connections drop in production. Every other way to
reach the changed code is an uncommon path: a non-default setting or table
type, such as a non-WAL table; a rarely used operation or its edge cases, such
as ATTACH or DETACH PARTITION; a less common route into the change, such as ILP
over TCP into a non-WAL table; an unusual combination of features.

Probe an uncommon path when the change may behave differently there. A probe
that fails shows a bug, which is a concern like any other. Otherwise the row
makes no finding, and the report asks for no test. CLAUDE.md "Testing" asks
authors for such tests while they develop; a review doesn't raise their
absence.

### Decide each untested row

Decide on the evidence, not on the kind of change:

- **Critical gap:** only when the changed path is supported and reachable,
  named users reach it, a plausible break would cost them something Critical
  (lost or corrupt data, a security failure, an outage or hang, a compatibility
  break, a leak without bound, or similar harm to operators), nothing in place
  already contains that risk, and the row passes Step 7. "Bug fix", "public
  API", "user-visible", "concurrency" or "security" never makes a gap Critical
  by itself. A Critical gap blocks unless Step 8 rates its risk below
  `--block-at`.
- **Moderate gap:** real but bounded exposure: most bug fixes without a
  regression test, internal and error paths with a distinct but not Critical
  consequence, or real uncertainty that falls short of the Critical bar.
- **Accepted:** low-risk, local or mechanical behavior where the least invasive
  meaningful test would cost more, or break more easily, than the code it
  tests, and safeguards keep the remaining risk small. For example, a fixed
  log level, whose stable test would need invasive capture of the global async
  log. Keep the reason private unless it matters for the verdict.
- **Uncommon path:** see above. Not a gap.
- **No behavior change:** checked to change no behavior, such as a pure rename,
  dead-code removal, or a change to comments, docs or CI only.

A bug-fix label or zero test changes starts this analysis; neither decides the
severity or the verdict. Urgency excuses neither a real bug nor a Critical gap.
Discovery agents and the report work from this map. Publish only the gaps that
pass Step 7; keep the other rows private unless the user asks. At level 0, rows
may cover a whole symbol to save effort, but new error paths and NULL or
boundary cases still get rows of their own.

## Step 6: Find concerns

Run discovery as fresh builtin `reviewer` agents through `subagent`, one task
per role. Pick only the roles whose area the change really touches, within the
level's limit. More agents are not more evidence; skip the roles that don't
apply.

Each agent gets the diff, the change map from Step 4 (with the earlier
findings and settled items of 4f), the coverage map from Step 5 with the path
of its coverage report, its role, and the reply rules below. Write the diff and
the maps to a shared file and name it in each task. The two fresh-eyes roles
get less (see their entries).

### Reply rules for every discovery agent

Put these in every task:

- You suggest concerns; you don't decide findings. Return each concern as one
  specific claim that a test or a search could disprove. Don't assign a
  severity, propose a fix or write persuasive titles, and don't call anything
  "verified", "proved" or "confirmed". Where a role or checklist mentions
  findings or severity, it says what to look at, not what to conclude.
- For each concern, give:
  - **Cause:** the changed lines it blames, or the unchanged caller and the
    behavior of the changed code it relies on.
  - **Trigger:** the user operation, setting, data, client version or event
    that creates every condition it needs. If you find none, write
    `trigger: unknown`; don't invent one. When only hostile input triggers it
    (Step 9, "Hostile input"), say so, say where you looked for a legitimate
    trigger, and say whether the input needs valid credentials.
  - **Path and symptom:** how the trigger reaches the code, and what the user
    would see.
  - **Evidence:** the code that shows it, with file and line.
  - **Against it:** the strongest evidence against it: an unsupported or
    experimental feature, a format nothing writes, a missing caller, a retry,
    guard, lock or validation, recovery downstream, or the same or better
    behavior on the base.
  - **Unknowns:** discovery runs nothing, so mark what happens at the head and
    at the base `unknown` for Step 7 to run, along with anything else you
    didn't check.
- A claim with "never", "only", "exactly one", "no retry" or a similar absolute
  needs a search of every caller or event source, not one traced path.
- A claim whose only consequence is to support another claim is evidence for
  that claim, not a concern of its own: if the other claim falls, so does
  this one.
- A bug the base already has, or hardening where the PR keeps the base's
  behavior for the same trigger or improves it, is not this PR's problem. Mark
  it pre-existing, and never propose it as a change to this PR.
- Two agents that repeat the same reasoning make one concern, not a
  confirmation. Only independent kinds of evidence support each other, and even
  they don't skip Step 7.
- Never return a concern that restates a settled item (Step 4f). A problem with
  another symptom, or one a later commit made worse than the reply answered, is
  new: say how it differs.
- Returning nothing is fine, and better than returning a guess.

### Roles

Pick from this list; never launch all of it. A role applies the checklists its
entry names (see "Checklists").

- **Correctness:** "Correctness". Check the new behavior at every caller from
  4b. When the diff touches QWP ingestion, role changes or failover, a
  `java-questdb-client` pointer that brings client or ingestion changes, or
  tests that drive a producer through a failover, also apply
  `.pi/skills/review-pr/qwp-store-and-forward.md`.
- **Concurrency:** "Concurrency". Check each caller from 4b against the new
  lock order and thread rules from 4c.
- **Performance:** "Performance", with the cost of each concern stated as that
  checklist asks. Also return every slowdown or tradeoff the PR body states,
  with the size the body gives, for a checker to measure at realistic scale at
  the head and at the base; one stated without a size, or bigger than stated,
  is a concern like any other. Check whether code that is now reachable from a
  new place (4d) lands on a hot path that makes an acceptable cost expensive.
- **Resources:** "Resources" and "Per-query memory limits". Follow each caller
  from 4b that creates, owns or hands over a changed type, and check its
  cleanup on every path.
- **Test coverage:** "Tests", "Test assertions" and "Crash tests". Work from the
  Step 5 map: re-check each test it names and what fails (read the assertion;
  don't trust the map), and look for behavior changes the map missed. Then
  pick the three to five riskiest changed lines on the main path that ran
  (comparisons at boundaries, error handling, null checks) and ask, for each:
  which test fails if this line is wrong, with its condition inverted, its
  bound off by one or its null check dropped? Where no test would, add an
  untested row, without deciding its severity; Step 5 decides, and only rows
  that pass Step 7 are published.
- **Standards:** "Code standards", "Logging" and "TODO comments".
- **Rust** (only when the diff has `.rs` files): "Rust".
- **Callers:** walk every caller from 4b. Read the calling function and its
  callers two levels up, and answer:
  - Does this caller pass input the new behavior handles wrongly?
  - Does it rely on a contract from 4c that the change broke?
  - Does it run somewhere the new behavior fails even with valid input: a WHERE
    clause, async code, a JNI thread, under a lock, an error path, a hot loop,
    a parallel worker, replication, a materialized view refresh?
  - For SQL functions and operators: does the symbol now compile in clauses it
    didn't before (WHERE on an indexed column, JOIN ON, a GROUP BY key, ORDER
    BY, a window frame, a materialized view), and does it work there end to
    end?
  - Do all overrides of a changed Java method, and all impls of a changed Rust
    trait, keep the new contract?
  - Do all Java callers of a changed JNI signature pass the right types and
    lifetimes?

  Return one verdict per caller: safe, concern, or not enough evidence. Pick
  this role whenever changed symbols have callers outside the diff; a small
  change to a widely used symbol usually needs it.
- **Fresh eyes:** runs apart from the other roles, to escape their checklists.
  It gets only the diff and the names of the changed files: no map, no
  contracts, no checklists. Its only instruction: "Find a few ways this code
  could be wrong that a test could show, and try to disprove each before you
  return it." It reads and searches the repository as it likes, and replies by
  the rules above. A concern that only this role finds is not stronger for
  being unique, and agreement with another role is no confirmation unless it
  brings another kind of evidence. Pick it only when another independent pass
  is worth it.
- **Fresh-eyes performance:** runs apart from the performance role. It gets the
  diff and the full files the diff touches, but not the performance checklist,
  the maps or other agents' output. For each added or changed function, it
  reads the whole implementation and asks: what is the fastest way to do this,
  and does the code match it? It works from the code, not from a checklist:
  how often each piece of data is read, and in what order; passes that could
  merge or go away; lookups that could be O(1); allocations a reused buffer
  would avoid; branches that arithmetic could replace; scalar loops over column
  data that could be vectorized; structure in the input (sorted, partitioned,
  bounded) that the code ignores; work done always that is needed only
  sometimes; collections built only to be iterated once. It reads the callers
  to learn real input sizes: a scan once at startup is not a scan per row. Each
  concern states what the code does now and its complexity, the faster
  approach and its complexity, and why it matters (how often it runs, how much
  data, whether it's on a hot path). It leaves zero-GC and style to the other
  roles. Pick it only when the diff changes loops, algorithms, data
  structures, allocation or a likely hot path.
- **Test efficacy** (only for changed tests or a fix): "Test efficacy".
- **Test code** (only for changed tests): "Test code" and "SQL in tests".

Collect the replies in your private notes. Split each compound story into
single claims, merge duplicates (the same claim with the same evidence), and
record which claims depend on which. Don't write report text, severities or
fixes yet: a concern is not a finding.

## Step 7: Check each concern

Each concern goes through the same stages: a guess, an attempt to disprove it,
proof, and only then a finding. A concern that lacks any proof, holds an
unresolved contradiction, fails to reproduce, has no real trigger, or depends
on a claim that was dropped is dropped. There is no middle ground for a
behavior claim: "could not disprove" never means proved, and an unproved claim
is never kept at a lower severity.

At levels 1-3, launch one fresh `review-prober` per concern, as "The review's
agents" describes. It gets only the concern, stated neutrally; the repository,
the base and head revisions (commit SHAs, or a hash of the captured diff for
an uncommitted working tree) and the relevant files; and the raw evidence and
artifact paths. At level 0, check each concern yourself the same way, starting
from a blank record, before you write anything about it.

The checker first builds the strongest case against the concern: nothing
creates the state it needs; the setup it needs isn't supported; the versions or
formats it needs can't occur together; a caller or event source was missed; a
retry, guard, lock, validation or later correction handles it; or the base
behaves the same or better. Only a concern that survives gets proof.

### What a finding needs

A behavior concern becomes a finding only when each of these is backed by
evidence:

- **Cause:** the changed lines, or the unchanged caller plus the contract this
  PR changed.
- **Trigger:** a supported operation, setting, client, version or event that
  creates every condition. Code that would mishandle an input doesn't prove
  that anything produces that input.
- **Path:** the whole way from the trigger to the symptom, through callers,
  event sources, retries, guards and locks.
- **Head:** the trigger, run at the reviewed revision, and what it produced.
- **Base:** the same trigger, run at `$BASE`, and what it produced; or proof
  that the base can't run the trigger at all, because the behavior is new.
- **Symptom:** what a user can observe. A claim that only supports another
  claim is not a finding.
- **Disproof tried:** the strongest attempt to disprove it, and why it fails.
- **Artifact:** the command or test, its output, the environment and settings,
  and the revision. Claims about races, ordering, retries, restarts, file
  system state, compatibility and on-disk formats always need a run: reading
  the source can't prove them.

A **static finding**, such as a compile error, a broken coding rule on a
changed line or an unterminated LOG chain, needs only the source that proves
it: trigger, head, base and run don't apply. A coverage gap can be proved by
searches that find no test, but its trigger, path, the users who reach it, what
a break would cost them, and their impact still need evidence (Step 5). Never
mark the trigger, the path or the impact as not applicable when the claim
depends on how the code behaves when it runs.

Some claims need more:

- A compatibility claim about a format, version or state needs something that
  actually writes that state in a supported setup. A constant comparison or a
  reader's guard writes nothing.
- A claim with "never", "only", "exactly one", "no retry" or a similar absolute
  needs a search of every case plus a run. One traced path proves only that
  path.
- A concurrency or ordering claim needs the interleaving forced or observed;
  prose about timing is no evidence.
- A claim that a test misses a regression needs the test run at the head and
  with the production change reverted.
- When a claim is dropped, drop every claim that depends on it; don't keep its
  supporting claims as Moderate findings.

When a check can't be run, note why in your private notes and drop the
concern. Never fall back from a failed or impossible run to confident prose.

### Checks by kind

Once the evidence above is in:

1. Read the source at the cited lines yourself; don't rely on an agent's
   account of it.
2. Follow the whole path: callers, class hierarchies, runtime types. A method
   called on a base-class reference may run a subclass's override
   (`PartitionDescriptor.clear()` against
   `OwnedMemoryPartitionDescriptor.clear()`).
3. **JNI and FFI:** read both the Java caller and the Rust function, and check
   ownership, error propagation and cleanup on both sides.
4. **Leaks:** follow every allocation to its free or close on every path:
   normal, error and `finally`. Check overridden `close()` and `clear()`
   methods. Before claiming a leak between an allocation and the code that
   registers its cleanup, check that the code in between can throw.
5. **Rust panics:** check that the panic can be reached; an earlier guard or
   return may prevent it. Through JNI, check that some Java caller can pass the
   input that panics; when every caller validates it first, drop the concern.
6. **Numeric overflow:** check that it happens at realistic scale. QuestDB
   handles billions to a few trillion rows, thousands of tables and thousands
   of columns, not billions of columns. Drop an overflow that needs more.
7. **Performance:** check the analysis (the complexity, the hot or cold path)
   and state the cost as "Performance" asks; a claim without a cost is not
   proved. Don't drop a correct finding because today's tables are small:
   data grows. A worse algorithm than necessary is always worth reporting;
   its cost decides whether it blocks.
8. **Callers:** re-read the caller and its callers two levels up, and confirm
   that production code reaches the broken behavior. Concerns about callers are
   valuable and easy to overstate.
9. **Tests:** re-read the assertion in its context and confirm it can fail for
   the claimed regression. For "passes without the fix", use a scratch worktree
   made as in Step 5 (never the user's checkout): run the test at the head,
   revert the production change (`git checkout <base> -- <files>`), and run it
   again. The finding needs the test passing at the head and failing without
   the fix. If you can't build or run the test, drop the concern and note why.
   The same goes for every runtime claim that would be Critical: run its
   trigger and attach the output, or drop it.
10. **Coverage gaps:** drop a gap on an uncommon path (Step 5). For the rest,
    check the recorded search and what fails, then try to disprove the risk:
    indirect assertions, guards, types and compile-time checks, restricted
    inputs, validation downstream, operational controls. A Critical gap needs
    evidence of the supported path, the users who reach it, a plausible break
    and what it costs them: it may be hypothetical about whether the code is
    wrong today, never about who reaches it or what it costs. Reject a bare
    "simple", "urgent", "hard to test" or "covered indirectly": evidence that
    a test costs too much names the test, why it is invasive or unstable, and
    why cheaper stable tests don't work.
11. **Test code:** confirm that flagged reflection has a real non-reflective
    alternative; some QuestDB internals need reflection in tests. Confirm a
    rebuilt helper by finding the helper and checking that its signature fits
    the test.
12. **A swallowed exception that causes wrong results, a leak or corrupt
    state:** a `catch` block shows defensive code, not that anything throws.
    Name all three of:
    - the exception type and the statement that throws it: quote the line;
    - that the cited catch clause catches that type. `catch (SqlException |
      CairoException)` doesn't catch `OutOfMemoryError`,
      `IllegalArgumentException` or `NullPointerException`, and an exception
      that escapes makes the query fail loudly, the opposite of the claim;
    - that the throwing statement runs with the arguments the caller really
      passes: constants, capacity reserved in advance and early returns often
      rule it out.

    If any of the three is missing, drop the concern; don't relabel it as
    hardening. Also look for the version without an exception: a `void`
    method that frees or drops its argument on an early return (`if (x) {
    free(arg); return; }`) breaks the same rule, is usually far easier to
    reach, and isn't fixed by reordering the code around the call. Report
    that path instead, or as well.
13. **Chains:** a claim of several steps ("A publishes early, B throws, C
    swallows the error, D reads stale data, the rows are wrong") is only as
    strong as its weakest step. Find that step, usually "this state can
    really occur", and try to disprove it first; support for each step alone
    doesn't prove the chain. When the weakest step is about runtime behavior
    ("the plan contains factory X", "the guard doesn't fire", "this branch
    runs", or anything about races, ordering, retries, restarts or file
    state), reading code doesn't settle it: the checker must run it, at the
    cited revision. An agent's prose is not an artifact, and agreement between
    agents proves nothing.
14. **The fix:** work it out only once the finding stands, then check it: every
    variable it uses is in scope and not null, ownership changes cause no
    double free or leak, and it closes every path that reaches the bug. A
    plausible fix is never evidence that a bug is real.
15. **Impact against the base.** Answer, in order:
    - **Who:** every user, users of a named feature, a specific query, DDL or
      ingestion shape, or operators only. "Any user in principle" is not an
      answer. When no supported user or operator can create the trigger, drop
      a behavior concern; don't keep it as Moderate. An attacker is not such a
      user for a failure that the hostile-input rule covers; that concern
      becomes an Adjacent finding.
    - **What changes for them:** the difference from the base for the same
      trigger, as run at both revisions. Only a fully static finding may
      compare the source instead.
    - **How much and how often:** per row, per query, per restart, once. Reuse
      the cost from check 7.
    - **What catches it first:** in the code, a later validation, a retry, a
      checksum, a caller that discards the value, a guard elsewhere in the PR;
      in the process, a team procedure, a merge or release step, a CI check or
      a deployment step that corrects the state before it reaches anyone.
      Name it, or write "none found" and where you looked.
    - **The result:** worse than the base, the same, or better. Only "worse"
      becomes a finding.

    For a coverage gap, answer the same questions from the evidence of Step 5:
    a missing test lowers confidence, but it doesn't create impact. A Critical
    gap without evidence of real impact, or of what a test would cost, is not
    Critical: make it Moderate, accept it with the evidence, or drop it. Judge
    a static code-quality finding from the changed lines.

### Outcomes

Record each concern in your private notes as one of:

- **Finding in the changed code:** every proof is in, and the bug is in the
  diff.
- **Finding in unchanged code:** every proof is in, and unchanged code breaks
  because of a contract this PR changed.
- **Dropped, pre-existing:** the base behaves the same or worse, and the PR
  opens no new path to it.
- **Dropped, hostile input only:** every proof is in, and the hostile-input rule
  (Step 9) covers the failure, whether or not this PR introduced it.
- **Dropped, false:** evidence disproves it.
- **Dropped, unproved:** a trigger, path, run, artifact or dependency is
  missing.
- **Dropped, settled:** it restates a settled item (Step 4f).

A pre-existing or hostile-input concern whose trigger, path and run are all
proved leaves your notes as an Adjacent finding (Step 9). False and unproved
concerns never leave your notes.

Prove a list of similar problems item by item: never check a few and publish
the rest. Each item you report needs its own trigger and evidence.

Checkers may run in parallel; each gets only its own neutral claim and raw
evidence. Check every returned proof yourself before you write the report.

## Step 8: Rate the risk

A real bug that few users will ever meet should not hold up the PR that
carries it. This step decides which findings block the PR and which ship as
Deferred, with a private issue to track them.

Give each finding its severity first (Step 9, "Severity"). Then rate every
Critical finding, coverage gaps included, except those that block by rule: a
compile error, a test that fails at the reviewed revision, a broken rule of
"Test assertions", an unterminated LOG chain, and a broken rule of
`qwp-store-and-forward.md`. Moderate and Minor findings never block, so they
need no rating, and Adjacent findings are not findings against this PR.

Launch one `risk-assessor` per finding, all at once, as foreground calls with
`timeoutMs: 3600000`, at every level. Like a checker, it gets a neutral
account and works out who is affected by itself: the finding's claim, the
trigger that was run with its artifact paths, the results at the head and at
`$BASE`, the relevant files, the PR body and the revisions. Never send the
severity, the Impact line, the discovery reasoning, the fix or other agents'
opinions. Name an output file in the review's temporary directory.

Check each reply before you use it:

- the author's reply it tests is the strongest the code allows, and the code or
  the artifacts show whether it holds;
- the impact is the consequence the artifacts show, not a worse one that might
  follow;
- each condition of the trigger has a tier and evidence;
- the trigger is rated no more common than its rarest condition;
- the risk is the one the table in `.pi/agents/risk-assessor.md` gives for that
  impact and likelihood.

Correct a rating by the evidence, in either direction, and look up the risk
again. When a reply lacks evidence for a condition, or the artifacts show a
more common trigger or a worse consequence than it rates, use the more common
likelihood or the worse impact. When it rates a consequence or a trigger that
neither the artifacts nor the code show, use the milder impact or the rarer
likelihood that they do show. A finding whose assessor fails twice blocks.

A finding whose author's reply holds is no bug of this PR: drop it, or keep
what remains at the severity it earns, such as an avoidable cost that is no
regression, as a Minor note. Of the rest, a finding rated at or above
`--block-at` (default `high`) is Blocking, and one rated below it is Deferred.

## Checklists

Review the diff against these. Each discovery role applies the ones its entry
names; at level 0, you apply every one that fits the change.

### Correctness

- NULL: tell the NULL sentinels (`Numbers.INT_NULL`, `Numbers.LONG_NULL`, NaN)
  apart from real values, and NULL meaning "not set yet" apart from a NULL
  value.
- Edge cases and error paths.
- `SqlException` positions point at the offending character, not at the start
  of the expression.
- Logic errors, off-by-one, wrong bounds, operator precedence.
- New reach: the SQL clauses, async contexts, error paths, parallel workers and
  lock-held states each changed symbol can now appear in; check that it works
  in each.

### Concurrency

- Races: shared mutable state without synchronization, a missing `volatile`,
  unsafe publication.
- Lock order that can deadlock.
- Thread-safety of structures that threads share.
- Changed code now called from a thread or context (4d) where its old
  assumptions don't hold.

### Performance

QuestDB is built for speed. On a data path, where work happens per row, per
value, per page or per partition, or a path users wait for or repeat, the bar
is the best known algorithm, and missing it blocks. Off data paths (SQL
compilation, DDL, startup, metadata changes), the bar is the same, but a cost
with a fixed bound is Moderate.

- **Complexity:** state the complexity of each new or changed loop, and ask
  whether a better algorithm exists: a linear scan where a hash lookup or a
  direct index would do, a sort where a linear method exists, nested loops
  where one pass or one sort would do.
- **Passes:** data traversed more than once (parsed, then validated, then
  transformed; collected into a list, then iterated) should take one pass,
  unless a pass needs the complete output of the previous one.
- **Repeated work:** values recomputed on each call that could be computed
  once; repeated lookups of the same key; re-parsing data already parsed.
- **Data structures:** the right structure for the access pattern: a hash set
  rather than a search through a list, a heap rather than a sorted array for
  insert and extract-min, an indexed array rather than a linked traversal.
- **Copies and conversions:** data copied where it could be used in place;
  conversions such as String to `CharSequence` or `byte[]` to
  `DirectByteCharSequence` where the original form works.
- **No garbage on data paths:** no `java.util` collections where
  `io.questdb.std` has one; no String creation or concatenation (use
  `StringSink` or another `CharSink`); no capturing lambdas, which allocate on
  every call (non-capturing ones are cached); no autoboxing, which hides in
  generic methods, `java.util` collections and wrapper return types. One
  allocation per row is a finding.
- **Vectorization:** column data processed one value at a time where QuestDB's
  native SIMD code could process it in bulk; an existing optimized routine
  that the code ignores.
- **SQL compilation:** allocation while compiling SQL is fine. A compile cost
  that grows with the input (O(n²) in columns or terms, re-parsing on every
  execution, work per row of a cursor) is Critical; a fixed cost per
  compilation (one more small allocation, a scan over the columns, a few
  hundred nanoseconds) is Moderate.

**State the cost of every performance finding:** what it multiplies by (rows
scanned, values converted, pages read, partitions opened), or the fixed bound
that caps it (the column count, the configuration keys, once per compilation,
once at startup), and whether users wait for it. A finding without that can't
be rated and is dropped. A cost that grows with the data on a data path is
Critical: an extra page read per scanned row, a second pass over a partition,
O(n²) in the partition count, a change of algorithm class on a query path,
anything that measurably moves query latency, ingestion throughput, or disk or
network volume. A bounded cost off the data path is Moderate, even when a
better algorithm plainly exists: name that algorithm and the bound that makes
the cost acceptable.

### Code standards

Changed lines follow CLAUDE.md "Coding guidelines". The rules reviews most
often need to check:

- class members grouped by kind (static, then instance) and visibility;
- `is` and `has` names for booleans;
- Java 17 features (enhanced switch, text blocks, pattern variables in
  `instanceof`), except in `java-questdb-client`, which targets Java 11;
- `ObjList` rather than object arrays;
- ASCII only in log messages;
- no third-party Java dependencies;
- no code smells: long or deeply nested methods, unclear intent, dead code.

### Logging

- Every LOG chain ends with `.$()` or `.I$()`. A chain without it is never
  logged normally: the next log call on the same thread reports it as
  abandoned, and in tests that call throws. CI's `find_unterminated_logs.py`
  fails the build on it, so it blocks by rule.
- `.put()` in a chain returns a plain sink, which has no `.$()`, so the chain
  can't be ended.
- Code that runs inside a chain, such as a `toString()` passed to `$()`, must
  not log: the inner log call abandons the outer chain.

### Resources

- Resources are closed on every path, error paths above all.
- try-with-resources where it applies.
- Native memory is freed exactly once.

### Per-query memory limits

Apply when the diff adds or changes a native allocation, a factory or cursor
that owns growing native buffers, or a pooled memory class (`Map`,
`RecordChain`, `RecordArray`, sort and tree chains, `GroupByAllocator`, join-key
maps).

A per-query `MemoryTracker` caps the native memory that one query, one
materialized view refresh or one batch of WAL apply may allocate.
`SqlExecutionContext.getMemoryTracker()` holds it; the tracker overloads of
`Unsafe.malloc`, `realloc` and `free`, `getNativeAllocator(tag, tracker)` and
Rust's `QdbAllocator` charge it. A `null` tracker charges only the global
counter.

- **Charge the tracker** for memory that grows with the data or with the
  number of keys and has no fixed cap: hash maps, sort and record chains, join
  maps, group-by state, LATEST BY lists, set-operation maps, `ORDER BY ...
  LIMIT` buffers, window partition maps and RANGE frames, SAMPLE BY FILL,
  parquet decode buffers. Such an allocation that passes `null` is a coverage
  concern: record the query that grows it and decide it in Step 5. It is
  Critical only when that query shows real impact.
- **Don't charge it** for memory with a fixed cap, or that lives as long as the
  process or the session: page frames, JIT buffers, `string_agg`, fixed-size
  heaps, ROWS frames, table reader and writer columns, symbol tables,
  connection buffers, mapped pages. Charging these is a finding too: it adds
  two atomic updates to every malloc and free, and tracker-aware pooled classes
  free their memory when the cursor closes instead of keeping it for the next
  query, so the charge gives up pooling for nothing.
- **The same tracker allocates and frees.** Memory charged to a tracker and
  freed with `null`, or the reverse, unbalances the counter and trips the
  `recordPerQueryMemAlloc` assertion. Check every free and close path, error
  paths and `toTop()`, `clear()` and cursor reuse included.
- **Nested SQL uses the outer tracker.** Subqueries, the inner SELECT of a
  materialized view refresh and SQL run during WAL apply use the tracker
  already on the context. A new site that acquires a tracker without checking
  for an outer one counts the memory twice.
- **Tests:** a newly charged allocation needs a `*MemoryTrackerTest` showing
  that going over the limit throws the per-query out-of-memory error, that a
  run under the limit succeeds, and that a loop of `getCursor()` and close
  stays balanced. A missing test, or a missing test that a factory charges the
  tracker, is an untested row for Step 5.

### Store-and-forward (QWP)

When the diff touches QWP ingestion (connection upgrade and role checks,
in-place demotion or switch, connection handling on a role change), replication
failover, a `java-questdb-client` pointer that brings client or ingestion
changes, or tests that drive a producer through a failover or switch, read
`.pi/skills/review-pr/qwp-store-and-forward.md` and apply it. A change that
breaks one of its rules blocks by rule.

### Rust

- Code that can panic at runtime: `unwrap()`, `expect()`, indexing without a
  bounds check, `panic!`, `unreachable!`, `todo!`, integer overflow in release
  builds, `slice::from_raw_parts` with bad input. A panic in Rust that Java
  calls through JNI aborts the whole JVM.
- CLAUDE.md bans `unwrap()` and `expect()` on data from files or other external
  input: such a call on a changed line is a finding on that line. Elsewhere,
  report a panic only when some input reaches it (Step 7, check 5). An
  `expect()` or `debug_assert!` on a path proved unreachable is what CLAUDE.md
  asks for.
- Fallible operations return `Result` or `Option` and propagate errors with
  `?`.

### SQL in tests

- Keywords in uppercase.
- `expr::TYPE` casts rather than `CAST()`.
- `_` separators in numbers of five or more digits (`1_000_000`).
- Multiline strings for long statements and expected results.
- No DELETE: QuestDB has none. Suggest DROP PARTITION or a soft-delete column.
- `execute()` for DDL, and one INSERT for many rows.

### Test assertions

These block by rule: report a broken one under Blocking, whatever its user
impact. Apply them to test lines the diff adds or changes; a line it only moves
or re-indents is not a finding.

- **Query results go through the `assertQuery` builder** (CLAUDE.md "Writing
  tests"): `assertQuery(sql).returns(expected)` for data, `.assertsPlan(...)`
  for plans, or `.withPlan(...)` to check the plan along with the data. The old
  `assertSql(...)`, `TestUtils.assertSql(...)` and `assertPlanNoLeakCheck(...)`
  helpers are gone; a new test that rebuilds one, printing a cursor and
  comparing the text, is a finding. `TestServerMain.assertSql(sql, expected)`,
  a wrapper for a running server, is fine, and so is a test that compares what
  a client receives over a wire protocol.
- **`.returnsOnce(...)` on a query whose output is stable is a finding.** It
  reads the cursor once and skips what `.returns(...)` checks: a second pass,
  `calculateSize()`, variable-length columns and the factory's properties.
  Those checks catch real bugs: a cursor that doesn't reset in `toTop()`, a
  `size()` that differs between passes, random access that returns wrong
  values. `returnsOnce` is right only when the output really differs between
  two reads of unchanged data: an unseeded `rnd_*`, `now()`, `sysdate()`,
  `systimestamp()`, or a row order nothing guarantees. The author must say why
  the output is unstable; "it was simpler" is no reason.
- **An `assertMemoryLeak(...)` whose only statement is one `assertQuery(...)`
  is a finding.** The builder checks for leaks itself, and the wrapper usually
  forces a `.noLeakCheck()` on the builder, which turns that check off. Drop
  the wrapper and the `.noLeakCheck()`. The wrapper is right when it holds
  several statements that share one leak check, such as DDL, inserts and
  several assertions.

### Test efficacy

Show that each test exercises the change and could fail if the change broke:

- Assertions that can't fail: `assertTrue(true)`, `assertEquals(x, x)`, a
  literal compared with itself, a value the test itself just set, a test with
  no assertion and no expected exception.
- Tests that pass with or without the production change: follow the data from
  the changed code to the assertion.
- Only the normal path tested: no assertion on the error, exception or NULL
  path that the change added.
- Broken concurrency tests: races in the test itself, missing latches or
  barriers, an `AssertionError` thrown on another thread and lost instead of
  failing the test, `Thread.sleep` used to order threads.
- Native memory allocated in setup that leaks when the test fails; a missing
  `assertMemoryLeak()`.
- For a fix: the new test must fail without the fix. Name the production line
  each test depends on, and what its assertion does with that line reverted. A
  test that still passes without the fix is not a regression test.

Each concern names the assertion, and why it can't fail or what it misses.

### Test code

Review the tests as code, as strictly as production code:

- Reflection (`setAccessible(true)`, `getDeclaredField`, `getDeclaredMethod`,
  `Field.set`, `Class.forName`) where a public API, a test helper or a
  constructor reaches the same state. Name the alternative.
- Copied setup or assertions where a helper from 4e already does the job, and
  repeated blocks that should be one helper or a parameterized test. Name the
  helper.
- Javadoc on test methods that runs to paragraphs, repeats the test name, or is
  stacked; a precise test name and at most a one-line comment are enough.
- Leftovers: dead or commented-out code, copy-paste mistakes (a `testFoo` that
  tests bar), `System.out.println`, `@Ignore` without a ticket, and numbers of
  five or more digits without `_` separators.
- Zero-GC rules and `io.questdb.std` over `java.util` don't apply to tests:
  don't flag `java.util` collections or allocation there. Member grouping,
  `is`/`has` names and the SQL style rules do apply.

### Tests

- Weigh each missing test by its impact (Step 5). Missing tests never block by
  themselves, and don't demand a brittle or invasive test whose cost
  outweighs a small remaining risk.
- **Execution modes:** where the changed code can behave differently in WAL
  and non-WAL tables, out-of-order and in-order writes, JIT-compiled and
  interpreted filters, parallel and serial execution, and partitioned and
  non-partitioned tables, ask for tests in the modes users usually run the
  change in. Other modes, such as non-WAL tables, are uncommon paths (Step 5):
  probe them where the change may misbehave, and ask for no test. Discovery
  agents can't run probes: they return the probe as a concern, and Step 7
  runs it.
- **Fuzz tests:** for a change to a parser, an encoder or decoder, an ingestion
  protocol, or out-of-order and WAL merging, search for fuzz tests of the
  changed area (`rg -l Fuzz`). One that exists but was neither extended nor run
  against the change is an untested row for Step 5.
- **Other places:** each place from 4d on the main path needs a test that
  reaches the changed code from there; record a missing one as an untested
  row. An uncommon place gets a probe where the change may misbehave, never a
  request for a test.
- Error paths, NULL inputs, columns and results, boundaries (an empty table or
  partition, one row, maximum values, empty strings), concurrent access to
  shared state, and `assertMemoryLeak()` for anything that allocates native
  memory.
- A fix has a test that reproduces the bug and fails without the fix.
- Tests assert behavior and the right values, not implementation details.

### Crash tests

Apply when the diff adds a write operation (DDL, a WAL event type, an
ingestion path), a storage layout or a setting that changes the on-disk
format, a background job (such as compaction, squash, convert or purge), or a
format change in `_txn`, `_meta` or `_cv`.

The PR should add the new operation or setting to the crash tests. Decide with
`rg` over the test tree, not from memory, and cite the test you find. Either
kind counts:

- **The fuzz library:** a `Fuzz*Operation` in `io.questdb.test.fuzz` that
  `FuzzTransactionGenerator` draws, with a probability above zero in
  `RandomizedAdaptiveCrashFuzzTest.configureFuzz()`. It suits what a fuzz
  transaction can express: DDL, a WAL event, a partition operation, a table
  setting. `configureFuzz()` sets every probability itself, so an operation
  that other fuzz tests draw may still be at 0 there.
- **A crash test** in `io.questdb.test.cairo.crash` that runs the new code on
  `CrashFaultFilesFacade`, crashes it at each durability operation and checks
  what recovery leaves, as `RebaseWalPublishDurabilityCrashTest` does for
  REBASE WAL. `AbstractAdaptiveCrashSweepTest.forEachAdaptiveCrashPoint` sweeps
  every crash point of a workload. It suits a background job, an ingestion path
  or a format change. A setting counts when such a test runs with it on.

When the PR adds neither, add an untested row in Step 5. It is a Moderate gap
at most: a question of coverage, not of durability. On Linux, every adaptive
save point flushes the whole file system with `syncfs`, so new code has no
flush of its own to forget; what goes untested is recovery from a crash
partway through it. A probe showing that such a crash recovers to a wrong state
is a bug, rated as usual. This holds only for the adaptive mode. In SYNC mode,
each write path flushes its own files, so a SYNC path that skips a flush it
needs is a durability bug, a concern like any other. Even in the adaptive mode,
the WAL commit path flushes its own files (`WalWriter.syncIfRequired0` makes
each WAL file durable before the commit is sequenced), so a new WAL file that
it doesn't flush can lose acknowledged data on a power loss before the next
save point: also a durability bug.

### Permissions

Apply when the PR adds an ALTER operation or another statement that changes
table state. Two `rg` searches decide it; run them instead of reasoning.

- **A state-changing statement calls `SecurityContext.authorize*()`** on its
  execution path. Cite the call: `AlterOperation.authorize()` for ALTER, the
  operation or compiler class for the rest. A missing call is a proved finding:
  the search that finds the new operation and the search that finds no
  `authorize*` call for it are the evidence. A state change that no security
  context can refuse is a privilege bypass, which is Critical.
- **A new `authorize*()` method is implemented wherever it is abstract:** in
  `AbstractAllowAllSecurityContext` and `AbstractReadOnlySecurityContext`,
  which the allow-all, read-only and deny-all contexts extend, and in test
  `SecurityContext` classes the compiler doesn't reach. A permissive `default`
  body instead makes every implementation that doesn't override it grant the
  permission, including the enterprise ones this checkout can't see. The
  interface uses `default` on purpose in places, so ask for the reason; a
  missing reason is the finding, not the `default` itself.
- **Enterprise wiring can't be checked from this repository:** registration in
  `Permission.java`, GRANT and REVOKE parsing in `PermissionParser`, the
  `EntSecurityContextBase` and `AdminSecurityContext` implementations, and
  `deniedOnReplica()` on replicas. When the PR adds an `authorize*()` method,
  note them once as an enterprise follow-up. They are not findings and don't
  affect the verdict.

### TODO comments

For each `TODO`, `FIXME`, `HACK`, `XXX` or `WORKAROUND` the diff adds (not one
it only moves): is it unfinished work that should block the merge, or a known
limitation that is fine to ship? Flag one that looks like a deferred bug or an
unfinished implementation, and check that an issue it names exists.

## Step 9: Write the report

The report is read by the PR's author and its other reviewers: QuestDB
developers who know the code base, but who see only this report. They haven't
read this skill, your notes, the earlier reviews or the PR thread.

### What goes in

- Only findings: concerns that passed Step 7. Leave out the concerns you
  dropped, how you disproved them, and how many agents ran or concerns they
  raised. Finish checking before you write: never report a guess and withdraw
  it later.
- Adjacent findings: bugs the base already has, and failures that only hostile
  input reaches. They are issue drafts, not findings against this PR (below).
- A report with no findings is a good report when the change is right.
- Keep it to what a reader can act on in one sitting. If a normal-sized PR
  yields more than about seven findings, check each one again, and remove
  duplicates, findings that depend on others, ones this PR didn't cause (they
  move to Adjacent findings) and text of little value. Depth shows in the
  evidence, not in the length.

### How to write it

Write as a senior colleague would explain a problem at the author's desk:
plainly, in full sentences, about the code and what it does to users.

- **Make each finding stand on its own.** The first time you mention a test, a
  setting, a value or a mechanism, say in a few words what it is or does. Name
  real things, such as a test method, a file and line, a setting or a SQL
  statement, and describe them; don't invent short names for them ("the pin",
  "the window", "the gate") or reuse a term that only the PR thread or an
  earlier review defined.
- **Write full sentences.** Keep the articles and verbs; don't compress
  sentences into notes. When a finding runs long, cut whole points rather than
  words. Explain a problem in a paragraph; keep bullets for lists of separate
  items, such as the results of several runs.
- **Say what each number counts,** with its unit: "39 crash points", "154
  seconds", "two page reads per row instead of one".
- **Lead with what happens to the user,** then the evidence, then the fix. Say
  why it matters in the user's terms.
- **Use the words of the code, of SQL and of QuestDB's documentation.** Don't
  describe how the review ran: leave out its steps, agents, checkers, roles,
  maps, notes and private labels, and the review level outside the opening
  line. Earlier reviews on the PR may use review jargon, such as "admitted",
  "falsified", "falsifier", "candidate", "population", "net-negative",
  "in-diff", "out-of-diff", "UPSTREAM-SYNC", "exempt", "gate" or
  "distribution"; never copy it. Say what happened instead: "unchanged code
  in `Foo.bar()` breaks", "rerun at `abc1234`, the test fails".
- **Leave out sections that would be empty,** and don't list the rules the
  change follows ("no `returnsOnce`", "members in order"). The verdict says
  when there are no findings.
- **Use short sentences in active voice** (CLAUDE.md), one idea per sentence.

For example, don't write:

> M1 is fixed: the pin's self-check now tries a count below the pin. An
> independent falsifier re-tested it at `abc1234`.

Write:

> The problem the earlier review raised as M1 is fixed. Each crash sweep
> checks that a commit performs exactly as many durability operations
> (flushes and the like) as the test expects on each platform, so that
> removing a flush makes the test fail. The test of that check used to try
> only a count above the expected one, so a check that caught only increases
> would have passed. At `abc1234` it also tries a lower count, and it fails
> when the check is changed to `>` or to `<`.

And don't write "`questdb` submodule: UPSTREAM-SYNC, out of scope. No in-diff
or out-of-diff breakage; no coverage gaps admitted." Write: "The PR moves the
`questdb` submodule to `99088c665d`, which is already on OSS master, so its
changes are not part of this review."

### Severity

Severity is what a user loses, not the checklist a finding came from. Rate a
finding by the worst consequence a user can see on a path they can reach. Don't
rate up to be safe: an inflated Critical costs the author as much as a real
one, and teaches them to skim. Severity is not the verdict either: Step 8
weighs each Critical finding by how likely users are to hit it.

A user is someone who runs queries, ingests data or operates a deployment, not
a QuestDB developer, a CI job or the release process. A problem that only
affects the team, such as a slower build, a broken local setup or an awkward
merge, is never Critical; it is Moderate at most, and usually Minor.

**Critical:** you can finish the sentence "Because of this, the user sees ___"
with one of:

- **wrong or missing data:** wrong query results, silent truncation, lost or
  duplicated rows, corrupt data on disk, a replica that diverges, wrong
  materialized view content;
- **a crash, hang or outage:** a panic, deadlock, livelock, endless loop, out of
  memory, running out of file descriptors, threads or connections, or a leak
  that grows each time an operation repeats (when only hostile input reaches
  it, see "Hostile input");
- **a security failure:** a privilege bypass, a permission not enforced,
  credentials or another tenant's data exposed;
- **a broken failure:** an operation that fails silently or with the wrong
  error, an error message the user can't act on, an exception swallowed so
  that failure looks like success, a fault lost or unlogged so an incident
  can't be diagnosed;
- **a compatibility break:** the on-disk format, the wire protocol, a public,
  SQL or JNI API, or the meaning of a setting changed so that existing
  clients, existing data or a rolling upgrade break;
- **a slowdown users feel** ("Performance");
- **a Critical coverage gap** (Step 5): supported code that named users reach,
  where a plausible break would cause one of the above, nothing in place
  contains it, and a stable test is worth writing.

Each needs a trigger the user can run: "the user does X and sees Y". For a
coverage gap: "the user does X; if this code broke by doing Y, the user would
see Z". "Could return wrong results" is not evidence. Drop a behavior concern
that has no supported trigger and none of these consequences; don't keep it as
Moderate. A broken rule, a maintainability problem or a coverage gap that the
changed source proves can still be Moderate or Minor.

**Configurations that disagree.** When the same statement is accepted under
one configuration and rejected under another (a setting changes the plan a
guard sees, or a check runs in only some execution modes), the inconsistency
is a finding of its own, rated by what the inconsistency itself costs. The
worst case reachable through the more permissive configuration is a separate
concern, which needs its own trigger, symptom and comparison with the base.

**Moderate:** a real problem this PR causes, with bounded impact or impact only
on developers: a broken coding rule on a changed line, a test proved weak,
missing coverage of an internal path on the main path, wrong documentation, a
bounded cost off the data path. A runtime theory that nothing reaches,
hardening of unchanged behavior, or a claim that only supports another is not
Moderate: drop it.

**Minor:** cosmetics on changed lines: member order, naming, formatting,
comment wording, import order.

Don't inflate and don't deflate. Filing a real bug that users see as Moderate
is a review failure; so is filing a small, fixed compile-time cost as
Critical. When two ratings are defensible, pick the one the evidence supports.

### Hostile input

A failure that only hostile input from a client with valid credentials reaches
is hard to trigger. When such a failure costs only availability, it is not a
finding against this PR, even when this PR introduced it: it becomes an
Adjacent finding, an issue draft held to the same evidence as a finding.
Hardening against attackers is separate work, and this keeps the PR's scope
finite.

- **Hostile input** is input that nothing legitimate produces: forged lengths,
  counts or offsets; a file crafted to break its own format; protocol messages
  in an order no driver sends; a size or nesting depth that no person or tool
  would produce, chosen to exhaust a limit.
- **Legitimate input** includes mistakes and accidents: typos, wrong types,
  NULL, empty and boundary values, unusual but valid SQL (large generated
  statements included), an expensive query, anything a released client or
  driver sends, malformed lines from a hand-written ILP client, invalid UTF-8
  from a real export, a dropped connection, a full disk, a slow peer. A failure
  that these reach is rated as usual.
- **Without credentials, a trigger is easy.** A failure that a client without
  valid credentials can trigger, on a server that requires them, is rated as
  usual: connection and TLS setup, protocol handshakes, the authentication
  exchange, request parsing before the credentials check, and endpoints that
  can't require credentials. Prove it with a probe that sends the input
  without valid credentials.
- **Rate by the easiest trigger.** Before applying this rule, look for
  legitimate input that reaches the same code, and check whether the input
  gets there before authentication. Say in the draft where you looked.
- **Only availability** means a controlled failure that a restart clears: a
  panic, abort or crash, a hang or deadlock, out of memory, running out of file
  descriptors, threads or connections. These are rated as usual even when only
  hostile input reaches them:
  - an out-of-bounds read or write, such as a segfault in native code or
    through `Unsafe`: slightly different input can leak or overwrite memory
    without a crash;
  - corrupt, lost or wrong data, a replica that diverges, a server that crashes
    again on restart;
  - a permission bypass or exposed data.
- **The PR's own claim.** When the PR says it fixes or hardens against a class
  of input, a failure on that class tests the claim and is rated as usual.
- A changed line that breaks a written rule, such as CLAUDE.md's ban on
  `unwrap()` on external input in Rust, is still a finding on that line.

### Not findings

Drop these before they reach the report:

- **How the PR lands.** The review covers what the code does, not the merge:
  submodule pointer positions, merge order between repositories, branches,
  labels, and anything true only while the PR is open. A PR that moves a
  submodule pointer pins the commit it was built with; that is no defect.
- **Anything true of every PR of this shape.** Ask: would this appear on every
  PR like this one? Then it describes the workflow, not this change.
- **Decisions the project made.** When the project's tooling or conventions
  allow something on purpose, such as a CI check that passes by design, a
  documented exception or a choice the PR body names, overriding it needs
  evidence that the decision is wrong, not that it is permissive. A slowdown or
  tradeoff the PR body states counts as a decision only with its measured size,
  and gets measured again (the performance role, Step 6).
- **Submodule code that is already upstream** (Step 3). This PR didn't write it
  and can't be asked to fix it. A break found there belongs in an issue against
  that repository. The one exception is code in this diff that calls it
  wrongly, reported at that call.
- **Settled items** (Step 4f), and concerns that restate them. A probe showing
  that one still reproduces doesn't reopen it.

### Layout

In this order, leaving out any section that has nothing in it:

1. **Opening:** the line from "Review levels", then the revisions reviewed: the
   head, `$BASE`, and the new commit of each submodule pointer the PR moves,
   such as a tandem PR's head. PR bodies leave commit hashes out (CLAUDE.md), so
   the review records them. With `--local`, give the head as Step 1 describes,
   and list what was not reviewed: untracked files left out, and submodules
   with uncommitted changes. Then one sentence for each moved pointer, saying
   whether its changes are part of this review (Step 3): without it, the reader
   can't tell what the review covered.
2. **Verdict:** "**Verdict: approve.**", or another verdict below, followed by
   one to three sentences: what the change does, whether it does it correctly,
   and what matters most in the report. Add a sentence for each regression or
   tradeoff the PR makes on purpose; Deferred findings stay in their own
   section. At levels 0 and 1, say that callers outside the diff were not
   searched exhaustively, so the report doesn't read as a full check.
3. **Blocking**, **Deferred**, **Moderate**, **Minor** and **Adjacent
   findings**, as described below.
4. **Title and description** (Step 2).
5. **Tests.**

#### Blocking

Findings that hold up this PR: the Critical findings that Step 8 rates at or
above `--block-at`, and those that block by rule. Order them by risk, then by
impact, and number them B1, B2, ... Write each one from its completed evidence,
after the check, never from a first impression:

```markdown
**B1. <What goes wrong, in one plain sentence of about 12 words.>**

<A short paragraph that a reader new to the PR can follow: what the user does,
what they see, and how that differs from the base for the same trigger. Explain
every test, setting or mechanism it mentions.>

- **Impact:** who is affected, how much and how often, compared with the base.
- **Evidence:** the run that shows it: the command or test, its output and the
  revision; or, for a static finding, the source that proves it.
- **Risk:** the tier, with its impact and likelihood, and the rarest condition
  of the trigger with the evidence for its rating; or "blocks by rule", and
  which rule.
- **Strongest objection:** the best case the author could make that this is
  fine, and the evidence that answers it.
- **Where:** the file and lines. Say whether the bug is in the changed code or
  in unchanged code that the change breaks; for the latter, name the caller and
  what it relied on.
- **Why:** the path through the code from the trigger to the symptom, as short
  as it can be while still showing that the bug is real.
- **Fix:** what to change in this PR.
```

The result at the base is required. If the base gives the same or a worse
result, the PR didn't cause the problem: drop it, or move a proved one to
Adjacent findings. For new behavior, say that the base can't run the trigger,
and why. A base that rejects the trigger is not a worse result than a wrong
answer. For a performance finding, give its cost: what it multiplies by. For a
missing test, give the plausible break, what already guards against it, the
test to add, and why a cheaper test won't do.

#### Deferred

Critical findings that Step 8 rates below `--block-at`: real bugs this PR
causes that few enough users will meet for the PR to ship with them. They never
affect the verdict, and they are never dropped: each becomes a ready-to-file
issue for `questdb/questdb-enterprise`. Keep them out of everything public, the
PR body and comments included (CLAUDE.md "Git & PR Conventions"). Number them
D1, D2, ..., and give each:

- **Problem:** one plain sentence of about 12 words; it becomes the issue title.
- **Risk:** the tier, impact and likelihood, and each condition of the trigger
  with its tier and evidence.
- **Where:** the file and lines.
- **Trigger and symptom:** the trigger that was run, what it returns at the
  reviewed revision and at the base, and what a user sees.
- **Easiest trigger:** the most common way to hit it, when the risk assessor
  found an easier one.
- **Fix:** one or two sentences.
- **Found in:** the PR or range, and the reviewed revision.

Offer to file them; don't file anything unless asked. `approve-pr` files them
when it approves the PR, and `fix-pr` during its run, with the labels `bug` and
`deferred` (`deferred` alone for a missing test). When `gh repo view
questdb/questdb-enterprise` fails, as it does for OSS contributors, keep the
drafts in the console report and file nothing.

#### Moderate

Problems worth fixing that don't block. Number them M1, M2, ..., each with a
plain title sentence, a short paragraph as for Blocking, and the **Impact**,
**Evidence** and **Fix** lines. A guess about runtime behavior doesn't belong
here; it belongs nowhere. For example:

```markdown
**M1. ATTACH PARTITION checks Parquet column types with a nested loop.**

For each table column, the new type check in
`TableWriter.attachValidateParquetColumnTypes()` searches the whole column
list of the partition's Parquet metadata again, so its work grows with the
number of table columns times the number of Parquet columns. With 1,000
columns, that is about a million comparisons, roughly a millisecond, per
ATTACH. It runs once per ATTACH and never on a query, so users won't notice
it, but a lookup by column ID does the same work in linear time.

- **Impact:** ATTACH of a Parquet partition is slower on tables with
  thousands of columns; queries are not affected.
- **Evidence:** `TableWriter.java:6151-6164` calls `findParquetColumnIndex()`
  (`:8870-8878`) once per table column, and that method loops over every
  Parquet column.
- **Fix:** build a map from column ID to Parquet column index once, as
  `ParquetMetaFileReader.getColumnIndexById()` does, and look each column up
  in it.
```

#### Minor

Cosmetics on changed lines, one bullet each: the problem and the fix in a
sentence or two, with the file and line.

#### Adjacent findings

Bugs the base already has, found in code this review visited (the changed
files, their callers, the places the change shows), that this PR doesn't
introduce, break or worsen; and failures the hostile-input rule covers,
whether or not this PR introduced them. Open the section with a sentence
that says so, and that they don't affect the verdict. They are not findings
against this PR: they never appear under Blocking, Deferred, Moderate or
Minor, and are never proposed as changes to this PR. Reporting them keeps the
work that found them from going to waste.

Each comes from a concern dropped as pre-existing or as hostile input only
whose trigger, path and run were proved; disproved and unproved concerns never
appear here. Number them A1, A2, ..., and give each, ready to file without
more investigation:

- **Problem:** one plain sentence of about 12 words; it becomes the issue title.
- **Impact:** who is affected and how much, or "none today", and why.
- **Where:** the file and lines.
- **Symptom:** what a user would see, or "latent: no user-visible effect
  today", naming the guard that prevents it.
- **Origin:** on the base already, or introduced by this PR (possible only under
  the hostile-input rule).
- **How it's reached:** the path that reaches it, or why nothing does yet. For
  hostile input: the input, where you looked for legitimate input that reaches
  the same code, and whether it needs valid credentials.
- **Fix:** one or two sentences.
- **Severity as an issue of its own:** Critical, Moderate or Minor, by the rules
  above.

Offer to file them; don't file anything unless asked. File a crash or hang that
hostile input triggers only in `questdb/questdb-enterprise`, with the label
`crash`, and keep it out of everything public, PR comments included
(CLAUDE.md). When `gh repo view questdb/questdb-enterprise` fails, keep the
draft in the console report and file nothing. Adjacent findings don't count
toward the seven-finding limit. If one is severe enough that shipping this PR
without its fix is unsafe, because this PR moves code onto a path where the
old bug now fires, it is not adjacent: it is a finding in unchanged code that
this PR breaks. Step 8 rates it like any other Critical finding, and the report
says why. This never applies to a failure the hostile-input rule covers.

#### Title and description

The result of Step 2: the title, the body and the labels, each either fine or
with its problems and a replacement, ready to paste (the whole body in a code
block). When all three are fine, say so in one sentence. Without a PR, say
that there was no PR to check. These are not findings: they skip Step 7, the
severity rules and the seven-finding limit, and never affect the verdict.
Never list one under Blocking, Deferred, Moderate or Minor, not even a body
that describes the change wrongly.

#### Tests

In plain sentences:

- which tests ran, at which revision, and their result; for a fix, whether the
  new tests fail at the base;
- how much of the changed code the tests ran, such as "118 of the 124 changed
  lines and 40 of the 46 changed branches ran", and which tests that covers;
  or that coverage was not measured, and why;
- changed code that no test runs and that makes no finding, each with its
  reason, such as "it only builds the text of an error message" or "it runs
  only for non-WAL tables, and a probe there returned the right rows". This
  records what no test runs; it asks for no test.

Missing tests that matter are findings above; don't repeat them here. Keep the
full coverage map private unless the user asks for it, and don't count its
rows.

### The verdict

Exactly one of:

- **approve:** no Blocking finding is open. Deferred, Moderate and Minor items
  may remain; list them and approve. This is the expected outcome for
  competent work, and withholding it when nothing blocks is a review failure.
- **approve with comments:** nothing blocks, but you want specific Moderate
  items fixed; name them.
- **request changes:** at least one Blocking finding is open, a Blocking
  missing test included.
- **needs discussion:** the change needs a product, architecture or
  compatibility decision that a reviewer can't make alone.

Missing tests, zero test changes or a bug-fix label start the analysis of
Step 5; they never force "request changes" by themselves, and neither urgency
nor the effort a test takes excuses a Blocking finding. Never make the verdict
depend on splitting the PR. Deferred findings, Adjacent findings and dropped
concerns never affect it.

### Before you post

Check each finding against its evidence, not against the text you wrote:

- it survived the strongest disproof you could build;
- a supported operation, version or setting creates every condition of its
  trigger;
- the agent that proved it never saw the discovery reasoning, a severity, a fix
  or other agents' opinions;
- every claim about races, ordering, retries, restarts, file state or
  compatibility has a run at the reviewed revision, and the same trigger's
  result at the base;
- nothing depends on a claim that was dropped;
- severity came after the proof, and no missing evidence turned into a higher
  severity;
- each risk rating passed the checks of Step 8, and no risk assessor saw the
  severity, the Impact line, the discovery reasoning or the fix.

Drop what fails, and decide the verdict again. Then read the report as its
author would, with only the report in front of you, and rewrite any sentence
that needs the PR thread, an earlier review or this skill to make sense.
