---
name: review-pr
description: Review a GitHub pull request or local Git range against QuestDB coding standards
argument-hint: "[PR number or URL | --range=<base>..<head>] [--level=0..3]"
allowed-tools: Bash, Read, Grep, Glob, Agent
---

# Review a QuestDB pull request

**Usage:** `/review-pr [PR number or URL | --range=<base>..<head>] [--level=0..3]`

Review the PR or local range identified by the invocation arguments. When this skill
is run as `/skill:review-pr <args>`, the `<args>` are appended as a `User:` message;
treat that text as `$ARGUMENTS`. Parse exactly one review target: a PR number/URL,
or `--range=<base>..<head>`. The range head may be omitted (`--range=<base>..`) to
review the working tree, including uncommitted changes. If both targets are supplied,
stop and ask which was intended. If neither is supplied, ask for one.

**Tools this skill uses:** `Bash` for read-only `gh` and Git queries, `Read`, `Grep`,
`Glob`, and fresh-context agents through the Agent tool. Do not edit files or push.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. QuestDB is mission-critical software: bugs can cause data loss or system failures in production deployments that are expensive to patch. Be critical, thorough, and opinionated. Your job is to catch problems that would hurt a user before they ship — not to be nice, and not to demonstrate thoroughness by volume.

**A review that blocks on everything blocks on nothing.** Every finding costs the author a CI round-trip, and an inflated one costs the whole report its credibility. Reserve blocking severity for defects with a real user consequence, report everything else honestly at the severity it deserves, and approve when the gates pass. "Approve" is a normal, expected outcome of reviewing competent work — not a failure of rigour.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed. Treat the diff as the entry point, not the scope.
- **Discovery is not a finding.** Treat every concern — including one produced by several agents — as an untrusted hypothesis until it passes the Step 3b admission gate. Report every *admitted* issue at the severity its evidence earns; omit everything else. A review with zero findings is a successful outcome.
- **Falsify before you explain.** Search for the missing producer, unsupported configuration, omitted caller, retry, guard, downstream offset, and merge-base behavior before building a narrative. Failure to disprove a hypothesis is not evidence for it, and uncertainty is never promoted to severity.
- **Keep the blast radius of the PR small.** This PR should fix what it set out to fix, plus anything this change demonstrably breaks. Pre-existing bugs, residual hardening opportunities whose behavior is unchanged from base, and propositions that only support another candidate are never findings against this PR and never affect its verdict. The one exception is a pre-existing bug that this PR demonstrably moves onto a live path. Small blast radius governs what this PR must *fix*, not what the review is allowed to *know*: a pre-existing bug proved to the same evidence bar leaves as a Step 4 adjacent issue draft rather than being thrown away.
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, ask: what inputs break this? What happens under concurrent access? What if this runs on a 10-billion-row table? What if the column is NULL? What if the partition is empty?
- **Demand optimal algorithms where they matter.** QuestDB is a performance-first database. On data paths, "works
  correctly" is not sufficient — a linear scan where a hash lookup exists, two passes where one suffices, or a per-row
  allocation on a scan is a blocking defect. Off the data path, apply judgement: a bounded, non-scaling cost during SQL
  compilation, DDL, or startup is worth reporting as Moderate, not worth blocking a merge over. Ask "is there a faster
  way?" for every loop, traversal, and data-structure choice — then ask "does the user feel the difference?" before
  choosing the severity.
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing documentation for non-obvious behavior.
- **Untested changed behavior is a coverage risk, not proof of a defect.** Missing tests alone cannot make a finding Critical. A Critical coverage gap must identify a supported, reachable user/operator population and a credible regression mode with material impact. A named test with a real failure link remains the strongest evidence; when none exists, assess change risk and the least fragile meaningful test rather than blocking by category. Test difficulty never reduces the severity of an actual functional, security, availability, corruption, or data-loss defect.
- **Urgency is neither evidence nor an exemption.** It may inform delivery sequencing only after user impact, regression risk, and stable-test feasibility are established. "Urgent", "simple", and "hard to test" are conclusions to prove, not reasons to skip analysis.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmarks or reason about the algorithmic change — does it actually improve things, or could it regress in other cases? Even if the PR doesn't claim to be about performance, evaluate whether the chosen algorithms and data structures are optimal — sub-optimal code that "works" is still a finding. If it says "simplify", verify the new code is actually simpler and doesn't drop behavior. Treat the PR description as an unverified hypothesis, not a statement of fact.
- **Read the full context of changed files** when the diff alone is ambiguous. Use `Read` plus ripgrep (`rg` with Bash) and `fd` to inspect the surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem
  requires physically impossible conditions (billions of columns, corrupted JNI inputs, values that no caller can
  produce), it is not a real finding — drop it. Focus on bugs that real workloads can trigger, not theoretical edge
  cases that exist only in the type system.
- **QuestDB runs with Java assertions enabled (`-ea`).** Assertions are a valid guard for invariants that indicate
  corruption or internal bugs. Do NOT flag `assert` as insufficient — it is the preferred mechanism for conditions
  that should never occur in a non-corrupt database. Only flag an `assert` if the condition can plausibly be triggered
  by normal (non-corrupt) user operations.

## Review level

Parse `$ARGUMENTS` for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level token and any `--range=` token before feeding the remainder (PR number or URL) to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs (replication, JNI boundary changes, on-disk format, public API, security/ACL).

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 2.4, 2.6, 4. Skip Step 2.5 and agent fanout. Review the diff inline for correctness, NULL handling, **algorithmic optimality**, tests, and QuestDB standards. Build the Step 2.6 coverage map inline. Every candidate still passes the Step 3b admission gate inline from a blank evidence form; do not draft severity, a fix, or report prose first. |
| **1** | Adds Step 2.5a and Step 2.5e when test code is present. Run Agent 1 plus at most **two** applicable roles chosen from Agents 3, 5, 6, 12, and 13. Run an independent falsification task for each surviving atomic candidate. |
| **2** | Full Step 2.5, with 2.5b restricted to `public`/`protected` symbols. Run Agent 1 plus at most **four** change-relevant roles from Agents 2-8 and 11-13. Run an independent falsification task for each surviving atomic candidate. |
| **3** | Full Step 2.5 and the complete admission protocol. Select at most **six** applicable discovery roles from Agents 1-14: Agent 1 always; Agent 9 for changed symbols with out-of-diff callers; Agents 2-8 and 11 only when their domain is touched; Agents 12-14 only for changed tests or a fix claim; Agent 10 only when a distinct adversarial pass is warranted. Depth comes from producer/reachability evidence and independent falsification, not agent count. |

State the chosen level in one line at the start of the review so the user knows what they're getting (e.g., "Reviewing PR #1234 at level 2"). If the level was defaulted, mention that level 3 exists for full review.

## Spawning review agents

Steps 3 and 3b use fresh-context agents through the Agent tool, one task
per role or atomic falsification candidate. Each task is self-contained and read-only.
Discovery tasks receive the diff, Step 2.4 provenance verdicts, the Step 2.5 surface
map, the Step 2.6 coverage map, role instructions, and the candidate contract. Agents
10 and 11 are deliberate reduced-context exceptions. Step 3b falsifiers receive only
the neutral proposition, revision identities, relevant files, and raw artifact paths.
The parent owns role selection, the private ledger, admission, severity, and output.

Use a shared temporary artifact for large maps rather than pasting them repeatedly.
Never pass the discovery narrative, proposed severity/fix, votes, or verification
claims to a falsifier. Agents 10 and 11 receive only the diff and changed-file names,
as their role descriptions require. The parent owns synthesis, deduplication, and the
final report; children return candidates or falsification evidence only.

## Step 1: Gather PR context

Every mode must end this step with **`$BASE`** set — the commit the change is measured against. `$BASE` is required by Step 2.4 and by every behavioral finding's same-trigger base check; a review that never established it cannot attribute anything.

### GitHub PR

Capture the PR identifier in `$PR` after stripping the level token, then fetch metadata, diff, comments, and the base revision:

```bash
PR='<PR number or URL from $ARGUMENTS, with any level token removed>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr view "$PR" --comments
BASE=$(gh pr view "$PR" --json baseRefOid --jq .baseRefOid)
```

### Local range (`--range`)

When `--range=<base>..<head>` is given there is no PR, no description, and no
labels. Take the diff from Git instead:

```bash
BASE='<base from --range>'
HEAD='<head from --range, or empty for the working tree>'
git diff "$BASE"${HEAD:+"...$HEAD"} --stat
git diff "$BASE"${HEAD:+"...$HEAD"}
git diff "$BASE"${HEAD:+"...$HEAD"} --name-only
```

With `<head>` empty the diff includes uncommitted working-tree changes, which is
the normal case when reviewing a `fix-pr` result before it is pushed. Untracked
files do not appear in `git diff` — list them with `git status --porcelain` and
read any that are part of the change, especially new test files, or the coverage
map in Step 2.6 will silently miss them.

In range mode: **skip Step 2 entirely** (there is no title or description to
check) and say so in the report. Every other step runs unchanged — the diff is
still the entry point, callsite analysis still walks outward beyond the changed
files, and findings are still classified by the same rubric. Restricting the
review to the changed files would disable the out-of-diff breakage analysis that
is the most valuable part of this skill.

## Step 2: PR title and description

**Skipped in `--range` mode** — a local range has no PR metadata. State that it was skipped and continue at Step 2.4.

Check against CLAUDE.md conventions:
- Title follows Conventional Commits: `type(scope): description`
- Description repeats the verb and explains user impact
- If fixing an issue, `Fixes #NNN` is at the top of the body
- Tone is level-headed and analytical, with no superlatives or bold emphasis on numbers
- Labels match the PR scope (SQL, Performance, Core, etc.)
- Bundled related fixes are allowed; do not demand a split

## Step 2.4: Submodule provenance (mandatory at every level)

A changed submodule pointer is not automatically a change this PR makes. Before reviewing **any** content inside a submodule, classify the pointer move. This step is cheap, runs at every level including 0, and gates whether an entire repository's worth of diff is in scope. Skipping it is how a review attributes months of already-released upstream work to the PR in front of it.

List the pointer moves, then for each one resolve the submodule's default branch and test whether the new commit is already on it:

```bash
git diff "$BASE...HEAD" --submodule=short | grep -E '^(diff --git|[+-]Subproject commit)'

cd <submodule path>
git fetch origin --quiet
DEF=$(git symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's|^origin/||')   # e.g. main, master
git merge-base --is-ancestor <new-sha> "origin/$DEF" && echo UPSTREAM-SYNC || echo OFF-DEFAULT
git branch -r --contains <new-sha>
```

Classify each pointer move as exactly one of:

- **UPSTREAM-SYNC** — the new commit is an ancestor of the submodule's default branch. The work inside it **already landed upstream**; this PR only advances the pointer to pick it up. **Its contents are not in-diff and are not this PR's responsibility.** Do not review them as changes, do not attribute their behaviour changes, breaking or otherwise, to this PR, and do not build findings out of them. The only legitimate finding here is a genuine *integration* defect: the code in this diff calls the newly-synced code incorrectly. That finding lives at the callsite in this diff, not inside the submodule.
- **OFF-DEFAULT** — the new commit exists only on a feature or PR branch. The submodule's changes **are** part of this logical change and are reviewed in-diff.
- **UNRESOLVED** — the branch cannot be determined (no network, shallow clone, missing remote). Say so explicitly in the report, treat it as OFF-DEFAULT for safety, and state that the scope decision was made without provenance.

Record the verdict per submodule in one line each, and repeat it in the Step 4 report so the scope decision is auditable. Nested submodules are classified independently: an OFF-DEFAULT OSS pointer says nothing about the client pointer nested inside it, which is frequently an UPSTREAM-SYNC in the same PR.

## Step 2.5: Map the change surface

Before launching review agents, produce a structured change surface map. This step is mandatory and must use Grep and Glob — do not reason about callsites from memory. The output of this step is required input for every agent in Step 3.


### 2.5a Semantic delta per changed symbol

For every modified or added function, method, trait, struct field, SQL operator/function, or public constant, write:

- **Symbol:** fully-qualified name
- **Before:** signature, return type, error/exception behavior, panic behavior, mutation (`&self` vs `&mut self`, `final` vs not), ordering/idempotency guarantees, allocation behavior, thread-safety
- **After:** same fields
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "improved", "simplified" are not acceptable deltas. State the actual behavioral difference. If nothing semantically changed, write "no behavioral change" — but only after checking, not as a default.

### 2.5b Callsite inventory

For every changed symbol that is `public`, `protected`, package-private, or exported (`pub` / `pub(crate)` in Rust), run `rg` across the entire repository to find every callsite, implementation, override, or reference outside the diff.

Produce a list grouped by file. For Java, also search for:
- subclasses that override the method
- interfaces that declare it
- reflection-based callers (`getMethod`, `getDeclaredField`, `Class.forName`)
- SQL function/operator registrations (`FunctionFactory`, `OperatorRegistry`)
- service loader entries

For Rust, also search for:
- trait impls
- macro expansions
- JNI exports and their Java callers
- `extern "C"` boundaries


A changed `pub`/`protected`/package-private symbol with zero recorded `rg` calls in the trace is a skill violation. The model is not allowed to assert "this is only used here" without showing the search.

### 2.5c Implicit contract list

For each changed symbol, walk this checklist and write one line per item, stating before vs after:

- Panics or throws on which inputs
- Error variants returned and which `?`/`throws` chains propagate them
- Iteration order, sort stability, NULL ordering
- Idempotency and re-entrancy
- Lock acquisition order and which locks are held on return
- Allocation on hot vs compile-time path
- `Send`/`Sync`, thread-affinity, JFR/JNI thread attachment requirements
- Whether `null` and sentinel-NULL (`Numbers.LONG_NULL`, `Numbers.INT_NULL`, etc.) are still distinguished
- Cancellation/drop behavior (Rust) and finally/close behavior (Java)
- SQL: does the symbol now appear in new clauses (WHERE, GROUP BY, JOIN ON, ORDER BY, window frames, partition predicates, materialized view definitions) where it didn't before? List which.

### 2.5d Cross-context exposure list

End this step with an explicit list of "places this change is visible from but the diff does not touch". This is the highest-priority input for the bug-hunting subagents in Step 3.

The list groups the callsites from 2.5b by execution context: hot data paths, SQL compilation, async runtime, JNI boundary, replication, materialized views, parallel execution workers, etc. Every entry on this list must be reviewed in Step 3.


### 2.5e Test surface & helper inventory

Run this only when the PR adds or changes test code. It is the test-code counterpart to 2.5b and feeds Agents 12-14. Use real Grep/Glob searches — do not reason about helpers from memory.

- **Existing-infrastructure inventory:** search the changed test files' package and module for base test classes, shared `@Before`/`@After`, helper methods, fixtures, and assertion utilities the new tests could reuse (`rg` for `extends Abstract.*Test`, `class .*TestUtils`, `assertMemoryLeak`, `assertQuery`, `assertSql`, shared `protected` helpers in the base class). This list is the baseline Agent 13 uses to flag reinvented boilerplate — a "you stamped boilerplate instead of reusing helper X" finding requires X to appear in this inventory.
- **Changed shared helpers as symbols:** if the PR changes a shared test base class, helper, or fixture, run the 2.5b callsite inventory for it too — a changed test base class can silently break every subclassing test.
- **Exercised-symbol map:** for each new or changed test, list which production symbols from 2.5a it actually exercises, so Agents 12 and 14 can check efficacy and regression value.

## Step 2.6: Test coverage map (mandatory at every level)

This step runs at EVERY review level, for EVERY PR that touches production code — including (especially) PRs that add or change no test code at all. A PR with zero test changes does not skip test scrutiny; it concentrates it here. At level 0, derive the behavioral-change rows directly from the diff (2.5a is skipped); at level 1+ use the 2.5a semantic deltas.

Build a coverage table with one row per behavioral change: every changed symbol whose delta is not "no behavioral change", broken down further by every new or changed branch, error path, and NULL/boundary case inside it. For each row, record:

- **Change:** symbol + the specific behavior/branch/path.
- **Test:** the exact test class and method that exercises it — found via real `rg`/`fd` searches across the test tree (search for the symbol name, the SQL function/operator name, the error message text, the config key). Citing a test without a recorded search command in the trace is a skill violation, same as 2.5b. "Existing tests probably cover it" is banned.
- **Failure link:** what that test asserts and why the assertion fails if this behavior regresses. "The test calls the method" is not a failure link.
- **Reachability / population:** the supported operation, configuration, or event that reaches the changed path and the users/operators affected. "Public" or "user-visible" is not a population.
- **Credible regression consequence:** a concrete plausible mutation or recurrence and what that population would observe. Distinguish material harm from cosmetic output, routine verbosity, or developer-only inconvenience.
- **Change risk:** semantic complexity, branch/callsite breadth, state/concurrency/resource sensitivity, and the strength of existing surrounding coverage or compile-time/downstream safeguards.
- **Stable test design:** the least invasive assertion and observation seam considered, including cheaper unit, integration, fault-injection, or existing-helper alternatives.
- **Effort / fragility evidence:** setup cost, production seams required only for testing, global-state or asynchronous log capture, timing/nondeterminism, platform dependence, and alternatives searched. Bare "hard to test" is not evidence.
- **Dimensions:** applicable happy, error, NULL, boundary, concurrency, and resource-cleanup dimensions, each covered / uncovered / N-A with a reason.
- **Disposition rationale:** why the row is `COVERED`, `CRITICAL GAP`, `MODERATE GAP`, `ACCEPTED GAP`, or `EXEMPT`.

Rows with no effective test are marked **UNTESTED**, then classified by evidence rather than category:

- **Critical gap (blocking):** only when the changed path and affected population are supported and reachable, a credible regression would cause a material Critical consequence (data loss/corruption, security failure, outage/hang, compatibility break, unbounded resource loss, or similarly material operator harm), existing controls do not contain that risk, and the row passes Step 3b. The label "bug fix", "public API", "user-visible", "concurrency", or "security" never makes a gap Critical by itself.
- **Moderate gap:** meaningful but bounded regression exposure, including most bug fixes without a regression test, internal/error paths with a distinct but non-Critical consequence, or material uncertainty that does not meet the Critical burden.
- **Accepted gap:** low-risk, localized or mechanical behavior where recorded analysis shows that the least invasive meaningful test is disproportionate or more fragile than the code under test and existing safeguards make residual user risk small. Example: a routine log-level correction whose stable assertion would require invasive global asynchronous log capture. Keep the rationale private unless it is material to the verdict.
- **Exempt:** verified no-behavioral-change rows (pure rename, dead-code removal, comment/doc/CI-only).

A bug-fix label or zero test changes triggers this analysis; neither predetermines severity or verdict. Urgency cannot waive an actual defect or an admitted Critical gap. The coverage map is required internal evidence for Agent 5, Agents 12-14, and the Step 4 test gate. Publish only admitted gaps; keep COVERED, ACCEPTED, EXEMPT, and omitted rows private unless the user asks. At level 0, rows may be per-symbol to bound cost, but new error/exception and NULL/boundary paths still get separate rows.

## Step 3: Change-specific candidate discovery

Run this step with the Agent tool using fresh-context agents. Select only roles whose domain is materially touched, obey the level's discovery cap, and launch those roles as fresh-context, read-only `reviewer` tasks. Agent count is never evidence and unused roles are skipped.

Every selected agent receives:
1. The PR or local-range diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list)
3. The test coverage map from Step 2.6

The diff plus surface map can be large — write them to a shared file (e.g., under a temp/chain dir) and point each task at it via its `reads`/task text, rather than pasting the whole payload into every task. Agents 10 and 11 are deliberate exceptions and receive reduced context (see their entries).

### Candidate-discovery directive (applies to all agents)

- You are a **hypothesis generator**, not an authority to publish a finding. Output atomic propositions for independent falsification. Do not assign severity, propose fixes, write persuasive titles, or use “verified”, “proved”, or “confirmed”. Any role text below that mentions a finding or severity describes what to inspect, not what you may conclude.
- For each candidate, cite the exact changed hunk or unchanged callsite contract allegedly broken. Out-of-diff impact is valuable only after the PR-caused contract delta is established.
- Name the **supported-state producer**: the exact user operation, configuration, writer/version, event source, or code path that creates every required trigger condition. If you cannot locate it, write `producer: unknown`; do not invent a deployment or state.
- Give the reachability chain, head observation, same-trigger merge-base observation, user-visible symptom, and raw evidence paths/commands. Mark anything not actually checked as `unknown`.
- Actively seek disproof: unsupported/experimental status, absent format writer, omitted caller, retry, guard, lock, validation, downstream recovery, or unchanged/better base behavior. Record the strongest counterevidence.
- Claims containing **never**, **only**, **exactly one**, **no retry**, or equivalent universal negatives require an exhaustive caller/event-source inventory, not one traced path.
- A proposition with no independent consequence is evidence for its parent candidate, not a standalone candidate. If the parent falls, its dependent propositions fall with it.
- Pre-existing bugs and residual hardening whose same-trigger behavior is unchanged or better than base are outside this PR's findings and verdict. Never file them as findings and never propose them as changes to this PR. A fully proved one leaves as a Step 4 adjacent issue draft; an unproved one stays in the private ledger.
- Two agents repeating the same reasoning are one hypothesis, not corroboration. Corroboration requires independent evidence types and still does not bypass Step 3b.
- Returning no candidate is valid and preferred to returning a speculative one.

### Agents

Use the following as a role catalog. Select only the roles allowed by the chosen level and change surface; do not launch the whole catalog.

**Agent 1 — Correctness & bugs:** NULL handling, edge cases, logic errors, off-by-one, operator precedence, error paths. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite. When the diff touches the QWP ingress / role-gating path, an in-place switch or failover, a `questdb` submodule bump that carries client or ingress changes, or the `questdb-ent/e2e` failover/switch suites, also verify the "Store-and-forward & pool startup invariants" checklist — a change that lets a running SF drainer surface transport errors to the producer, imposes a reconnect time budget on it, or hard-fails it on a transient outage is a Critical (data-loss) finding.

**Agent 2 — Concurrency:** Race conditions, shared mutable state, missing volatile, lock ordering, thread-safety of data structures. Use the implicit contract list (lock order, thread-affinity) and check every callsite from 2.5b for violations of the new contract.

**Agent 3 — Performance & algorithmic optimality:** This agent enforces the principle that QuestDB code must use the
best known algorithm for each task — not merely "avoid quadratic."

For every new or changed loop, traversal, data structure, or computation:

1. **Algorithm optimality:** State the time complexity. Then ask: does a better algorithm exist? O(n) where O(1) is
   achievable (hash lookup vs linear scan, direct indexing vs search) is a finding. O(n log n) where O(n) suffices is a
   finding. The bar is not "avoid quadratic" — the bar is "use the best known approach."
2. **Multi-pass vs single-pass:** If the code makes multiple passes over the same data (parsing, validation,
   transformation), determine whether they can be fused into a single pass. Multiple passes over the same input is a
   finding unless each pass has a structural dependency on the output of the previous one.
3. **Redundant computation:** Flag values that are recomputed on every call but could be computed once and cached. Flag
   repeated lookups of the same key. Flag re-parsing of already-parsed data.
4. **Data structure choice:** For each collection or map, ask whether the chosen data structure is optimal. Linear
   search through a list where a hash set gives O(1) membership test. Sorted array where a heap gives better
   insert/extract-min. ArrayList where a direct-indexed array suffices.
5. **Unnecessary copies and conversions:** Copying data that could be referenced in place. Converting between
   representations (String ↔ CharSequence, byte[] ↔ DirectByteCharSequence) when the original form would work.
6. **Zero-GC violations:** `java.util.*` collections vs `io.questdb.std`, string creation/concatenation on hot paths,
   capturing lambdas, autoboxing. Even a single GC allocation on a per-row data path is a finding.
7. **SIMD and vectorization:** Where the code processes arrays or columns element-by-element, check whether a
   SIMD/vectorized alternative exists in QuestDB's native layer or could be added.
8. **Compile-time vs data-path:** GC allocations during SQL compilation are acceptable. Algorithmic inefficiency during
   compilation is still a finding — slow compilation means slow first-query latency — but its severity depends on
   whether the cost scales: a multi-pass parse or O(n^2) plan enumeration is serious; a bounded fixed cost paid once per
   compilation is a minor-impact finding. Report both, distinguished by item 9.

9. **Magnitude (required on every performance finding):** state what the cost multiplies by — rows scanned, values
   converted, pages read, partitions opened — or, if it does not scale with data, the fixed bound that caps it (column
   count, config-key count, once per query compilation, once at startup). Say plainly whether the cost is on a data path
   the user waits for, or off it. The parent uses this to assign severity: scaling-with-data costs block the merge,
   bounded off-path costs do not. A finding with no magnitude cannot be classified and will be dropped.

For changed symbols now reachable from new contexts (per 2.5d), check whether any of those new contexts is a hot path
that amplifies an otherwise-acceptable cost.

**Agent 4 — Resource management:** Leaks on all code paths (especially errors), try-with-resources, native memory, pool management. Walk every callsite from 2.5b that constructs, owns, or transfers ownership of changed types and verify cleanup on all paths. When the diff adds or changes a native allocation site, also apply the "Per-query memory tracker integration" checklist below: confirm large, unbounded, data-scaled allocators are wired into the per-query `MemoryTracker` and bounded / process-lived ones are deliberately left out, that malloc and its matching free charge the same tracker, and that newly wired sites have breach / success / leak-loop tests.

**Agent 5 — Test review & coverage:** Coverage gaps, error path tests, NULL tests, boundary conditions, regression tests, test quality, `assertMemoryLeak()` usage. Cross-reference 2.5d: every cross-context exposure should have a test that exercises the changed symbol from that context. For each missing cross-context test, add an `UNTESTED` Step 2.6 row; do not predetermine its severity or publication. Consume the Step 2.6 coverage map: re-verify every claimed test and failure link (read the assertion, don't trust the map), and hunt for behavioral changes the map missed. Then run a **mutation spot-check**: pick the 3-5 most dangerous changed lines (boundary comparisons, error handling, null checks, off-by-one candidates) and ask, per line, "which test fails if this line is wrong — inverted condition, off-by-one, dropped null check?" When no assertion would catch a mutation, add an `UNTESTED` map row even if a test nominally executes the line; classify it under Step 2.6 and publish it only after Step 3b admission. **Enforce the "SQL test assertions (builder API — strict)" checklist on every added/modified test line: any new `assertSql(...)`/`assertPlanNoLeakCheck(...)`/`getPlan(...)`/`TestUtils.assertSql(...)` is Critical; any new `.returnsOnce(...)` on a deterministic (non-RNG, non-time-varying) query is Critical; a lone `assertQuery(...)` wrapped in `assertMemoryLeak(...)` is a finding.** Test *efficacy* (whether tests actually exercise the change and could fail) and test-*code* quality are handled by Agents 12-14 — here, focus only on whether coverage exists for every new or changed path.

**Agent 6 — Code quality & standards:** Code smell, member ordering, naming conventions, modern Java features, dead code, third-party dependencies. **Also check for unclosed LOG statements**: QuestDB logging uses a builder pattern (`LOG.info().$("msg").$()`) and every chain MUST end with `.$()` or `.I$()`. A missing close holds a ring buffer slot forever, causing other log producer threads to busy-wait in `nextBully()`, and the log consumer `logging_0` thread cannot progress either. Also watch for `.put()` instead of `.$()` in LOG chains — `.put()` returns `Utf16Sink`, not `LogRecord`, breaking the chain. Also flag throw-capable expressions inside LOG chains (`LOG.info().$(func()).$()`): arguments are evaluated after the ring slot is acquired, so a throwing `func()` unwinds past the terminator and leaks the slot; the call must be hoisted into a local before the chain starts.

**Agent 7 — PR metadata & conventions:** Title format, description quality, commit messages, labels, SQL style in tests.

**Agent 8 — Rust safety (only if PR contains .rs files):** Check for any code that can panic at runtime — `unwrap()`,
`expect()`, array indexing without bounds checks, `panic!()`, `unreachable!()`, `todo!()`, integer overflow in release
mode, `slice::from_raw_parts` with invalid inputs. In mission-critical software a panic in Rust code called via JNI/FFI
will abort the entire JVM process with no recovery. Every fallible operation must use `Result`/`Option` with proper
error propagation. Flag every potential panic site.

**Agent 9 — Cross-context caller impact:** Walk the callsite inventory from 2.5b. For every callsite, fetch the surrounding code (the calling function plus its callers up two levels) and answer:

- Does this caller pass inputs the new behavior handles incorrectly?
- Does this caller depend on a contract from the implicit contract list (2.5c) that the change broke?
- Is this caller in a context (WHERE clause, async runtime, JNI thread, holding lock X, error path, hot loop, parallel worker, replication path, materialized view refresh) where the new behavior misbehaves even if the inputs are valid?
- For SQL functions/operators: is the symbol now resolvable in clauses where it didn't compile before (WHERE on indexed column, JOIN ON, GROUP BY key, ORDER BY, window frame, materialized view definition), and does it actually work there end to end?
- For changed Java methods overridden by subclasses: do all overrides still satisfy the new contract?
- For changed Rust types with trait impls: do all impls still satisfy the new invariants?
- For changed JNI signatures: do all Java callers pass the right types and lifetimes?

This agent's output is structured per callsite, not per failure mode. Each callsite gets a verdict: SAFE / CANDIDATE / INSUFFICIENT_EVIDENCE. A CANDIDATE is only an atomic hypothesis for Step 3b; it has no severity yet.

Select this role whenever changed symbols have meaningful out-of-diff callers. It counts toward the level's discovery cap; small diffs to widely used symbols usually justify it.

**Agent 10 — Fresh-context adversarial:** Dispatched separately from agents 1-9 to escape checklist anchoring. This agent operates under different rules from the rest:

- It receives ONLY the PR diff and the names of the changed files. It does NOT receive the change surface map from Step 2.5, the implicit contract list, the cross-context exposure list, or any of the review checklists below.
- Its sole instruction: “generate a small set of falsifiable ways this code could be wrong, and try to disprove each before returning it.” No category list, failure-mode taxonomy, or QuestDB-specific style guide.
- It is free to use Read and ripgrep (Grep/Glob with Bash) to explore the repository however it wants.
- Each surviving output follows the candidate contract: atomic proposition, changed attribution, producer, reachability, head/base observations, symptom, counterevidence, and missing evidence. No severity or fix.

The point is to escape the structured frame, not to create privileged findings. A unique hypothesis is not high signal by itself, and overlap is not corroboration unless it supplies an independent evidence type.

Select this role only when a distinct adversarial pass is warranted; it counts toward the level's discovery cap.

**Agent 11 — Adversarial performance:** Dispatched separately from Agent 3 to escape checklist anchoring. This agent
operates under different rules:

- It receives the PR diff plus the full source files that the diff touches (not just the changed lines). It does NOT
  receive the performance checklist, the change surface map, or Agent 3's findings.
- For every function or method the diff adds or modifies, read the full implementation and ask one question: **"What is
  the theoretically fastest way to implement this, and does the code match it?"**
- Work bottom-up from the code, not top-down from a checklist. Trace data flow through each function: what is read, how
  many times, in what order. Look for:
    - Passes over data that could be eliminated or fused
    - Lookups that could be O(1) but aren't
    - Allocations that could be avoided by reusing buffers
    - Branching that could be replaced with branchless arithmetic
    - Scalar loops over column data that could be vectorized
    - Sorting or searching where the input has structure (sorted, partitioned, bounded) that the code ignores
    - Work done unconditionally that is only needed conditionally
    - Intermediate collections built and then iterated once (build + iterate = two passes; a single streaming pass may
      suffice)
- Use Read and ripgrep (Grep/Glob with Bash) freely. Read callers to understand actual input sizes and access patterns — an O(n) scan that
  runs once at startup is different from one that runs per row.
- Each finding states: what the code does now (with complexity), what the optimal approach is (with complexity), and why
  it matters (call frequency, data scale, or hot-path placement).
- Do not duplicate zero-GC or style findings — focus purely on algorithmic and computational efficiency.

Select this role only when the diff changes loops, algorithms, data structures, allocation behavior, or a plausible hot path; it counts toward the level's discovery cap.

**Test-code agents (Agents 12-14) — eligible only when the diff adds or changes test code or claims a bug fix.** A production change with no test code is still handled by the Step 2.6 gate. Select only the applicable test roles within the level's discovery cap. Each receives the diff, the change surface map, and the test surface inventory from 2.5e. Tests are not second-class code — apply the same adversarial rigor here as to production.

**Agent 12 — Test efficacy & correctness (adversarial):** Prove each test actually exercises the production change and could fail if that change regressed.
- **Vacuous assertions:** flag every assertion that cannot fail — `assertTrue(true)`, `assertFalse(false)`, `assertEquals(x, x)`, asserting a literal against the same literal, asserting on a value the test itself just hard-coded, or a `@Test` body with no assertion and no `expected=`/`assertThrows`.
- **Tests that don't reach the changed code:** the assertion passes whether or not the production change is present. Trace the data flow from the changed symbol to the assertion.
- **Happy-path-only:** no assertion on the error/exception/NULL path the production change added.
- **Concurrency-test correctness:** races in the test harness itself, missing latches/barriers, an `AssertionError` thrown on a spawned thread where it is swallowed instead of failing the test, `Thread.sleep`-based synchronization that is timing-dependent and flaky.
- **Test setup/teardown resource handling:** native memory allocated in setup/`@Before` that leaks on a failing path, missing `assertMemoryLeak()` wrapping.
- Each finding states the exact assertion and why it cannot fail or what it fails to cover.

**Agent 13 — Test-code quality & maintainability:** Review the test as code.
- **Reflection overuse:** flag `setAccessible(true)`, `getDeclaredField`/`getDeclaredMethod`, `Field.set`, `Class.forName`, and similar when a public API, an existing test helper, or a constructor reaches the same state. Reflection in tests is a last resort; if a neater non-reflective path exists, the reflection is a finding — name the alternative.
- **No code reuse / boilerplate stamping:** before accepting repeated setup or assertion blocks, run Grep/Glob for existing helpers, base test classes, and fixtures (e.g., `extends Abstract.*Test`, `TestUtils`, `*TestUtils`, shared `assert*`, shared `@Before`) using the 2.5e inventory. If a helper already exists that the new test reimplements inline, flag it and name the helper. Duplicated blocks across new test methods that should be a single helper or a parameterized test are findings.
- **Javadoc bloat:** flag multi-paragraph javadoc on `@Test` methods, javadoc that merely restates the test name, and stacked/duplicated javadoc ("javadoc piled on javadoc"). Test intent belongs in a precise test name plus, at most, a one-line comment.
- **Residue and smells:** dead code, commented-out code, copy-paste leftovers (a `testFoo` that actually tests bar), `System.out.println` debugging, `@Ignore` without a referenced ticket, magic numbers >= 5 digits without `_` separators.
- **Which standards apply:** zero-GC and `io.questdb.std`-over-`java.util` do NOT apply to test code — do not flag `java.util` collections or allocations in tests. Member ordering, `is/has` boolean naming, and SQL style DO apply.

**Agent 14 — Regression-test efficacy verification:** For any PR that claims to fix a bug, verify the regression test would actually fail without the production change. Reason about reverting the production hunk and confirm the new or changed test's assertions would then fail. If the test still passes with the fix reverted, it is not a regression test — flag it. State, per test, which production line the test depends on and what its assertion would do if that line were reverted. Run only when the PR is a fix; skip for pure features or refactors.

Combine agent outputs into a private **candidate ledger**. Split compound narratives into atomic propositions, deduplicate by proposition plus evidence, and record dependencies. Do not draft report prose, severity, or a suggested fix. A candidate is not a finding.

## Step 3b: Independently falsify, prove, and admit candidates

Use this state machine with no shortcuts:

`HYPOTHESIS → FALSIFYING → PROVEN → ADMITTED`

Any missing proof, unresolved contradiction, failed reproduction, unsupported producer, or dependence on an omitted premise ends at `OMITTED`. There is no `DOWNGRADED` state for an unproven behavioral claim, and “could not disprove” never means `PROVEN`.

At levels 1-3, launch one fresh-context falsifier per atomic candidate. The falsifier receives only (a) the neutral proposition, (b) target repository, base/head revision identities (commit SHAs, or a captured diff hash for an uncommitted working tree) and relevant file names, and (c) raw evidence/artifact paths. **Do not send** the discovery narrative, proposed severity, suggested fix, author identity, other agents' votes, or statements that the claim was verified. At level 0, the parent applies the same protocol inline from a blank evidence form before writing any report prose.

The falsifier's first task is to construct the strongest disproof: find a missing state producer, unsupported deployment, impossible version/format pairing, omitted caller or event source, retry, guard, lock, validation, downstream offset, or identical/better base behavior. Only if the candidate survives does it assemble affirmative proof.

A behavioral candidate is admitted only when every field below is backed by cited evidence:

- **Attribution:** exact changed hunk, or exact unchanged callsite plus the contract this PR changed.
- **Supported-state producer:** exact supported operation/configuration/writer/version/event that creates every trigger condition. A reachable consumer branch is not proof that any producer can create its input.
- **Reachability:** complete producer-to-symptom path, including callers, event sources, retries, guards, locks, and offsets.
- **Head observation:** executed trigger and observed output/state at the reviewed revision.
- **Base observation:** the identical trigger and observed output/state at `$BASE`, or `N/A — genuinely new surface` with proof.
- **User symptom:** independently observable consequence; a statement that merely justifies another candidate is not a finding.
- **Counterevidence search:** strongest attempted disproof and why it does not apply.
- **Artifact:** exact command/test, output, environment/config, and revision identity (commit SHA or captured diff hash). Race, ordering, retry, restart, filesystem-state, compatibility, and on-disk-format claims always require runtime evidence; static source reading alone cannot admit them.

For static findings fully proved by source — compile errors, direct standards violations, or malformed LOG chains — mark producer/head/base/runtime fields `N/A — static` and cite the complete source proof. For a coverage gap, recorded searches may statically prove only that an effective test is absent; they never make the supported-state producer, reachability, affected population, credible regression consequence, or user impact `N/A`. A Critical coverage gap must prove those fields under Step 2.6. `N/A` is forbidden whenever a load-bearing premise concerns runtime shape, reachability, or impact.

Special burdens:

- A format/version/state compatibility claim must identify an actual producer that writes the alleged state in a supported deployment. A constant comparison or reader guard is not a producer.
- A claim containing **never**, **only**, **exactly one**, **no retry**, or an equivalent universal negative must include an exhaustive inventory plus an executed probe. One path proves only that path.
- A concurrency or ordering claim must force or observe the interleaving; timing prose is not evidence.
- A regression-test claim must run the test on head and against the reverted production hunk.
- If a parent premise is omitted, omit every candidate that depends on it; do not preserve its supporting propositions as Moderate findings.

If required execution is impossible, record the validation limitation in the private ledger and omit the candidate from the public findings. Never fall back from failed or unavailable execution to confident prose.

After a candidate satisfies this admission schema, apply the domain-specific checks below:

1. **Read the actual source code** at the exact lines cited. Do not rely on the agent's description alone.
2. **Trace the full code path**: follow callers, inheritance hierarchies, and runtime types. A method called on a base-class reference may dispatch to a subclass override (e.g., `PartitionDescriptor.clear()` vs `OwnedMemoryPartitionDescriptor.clear()`).
3. **Check both sides of JNI/FFI boundaries**: if a finding involves Java↔Rust interaction, read both the Java caller and the Rust JNI function. Verify ownership transfer, error propagation, and cleanup on both sides.
4. **For resource leak claims**: trace every allocation to its corresponding free/close on ALL code paths (happy path,
   error path, finally blocks). Check for polymorphic `close()`/`clear()` overrides. Before claiming a leak between
   allocation and cleanup registration, verify that the intervening code can actually throw.
5. **For Rust panic claims**: verify whether the panic site is actually reachable. Trace control flow backwards — a
   preceding guard or early return may make it unreachable.
6. **For Rust panic claims via JNI**: trace the Java caller to check whether it can actually pass parameters that
   trigger the panic. If every caller validates inputs before the JNI call, the panic is unreachable — drop it.
7. **For Rust numeric overflow claims**: check whether the overflow is reachable at realistic scale. QuestDB handles
   billions to a few trillion rows, thousands of tables, and thousands of columns — not billions of columns or
   quintillions of rows. If overflow requires values beyond that scale, drop it.
8. **For performance claims**: verify the finding is technically accurate (correct complexity analysis, correct
   identification of the hot/cold path) **and then establish its magnitude**. State what the cost multiplies by — rows
   scanned, values converted, pages read, partitions opened — or, if it does not scale with data, state the fixed bound
   (column count, config-key count, once per query compilation). A performance claim with neither a multiplier nor a
   bound is not verified. Do not drop a technically correct finding because today's tables are small — data grows. Do
   move it from Critical to Moderate when the cost is structurally bounded and off the data path: that is the whole
   difference between IO amplification that hits every row and a few hundred nanoseconds spent once per SQL
   compilation. Sub-optimal algorithm choice is always reportable; whether it *blocks* is decided by the magnitude.
9. **For cross-context findings (Agent 9)**: re-read the callsite in full, including its callers up two levels, and confirm the broken behavior is reachable from production code paths. Cross-context findings are high-value but also the easiest to overstate — verify carefully.
10. **For test-efficacy candidates (Agents 12, 14)**: re-read the cited assertion in full context and confirm it can fail for the claimed regression. For “would pass without the fix” claims, use a scratch `git worktree` (never the primary working tree): run the new test at the reviewed revision, then revert the production hunks (`git checkout <base> -- <files>`) and run it again. Admission requires green-on-head and red-without-fix artifacts. If the environment cannot build or run the test, omit the candidate and record the validation limitation privately; do not fall back to confident reasoning. The same rule applies to every dynamic Critical candidate: execute the claimed trigger and attach the observed output, or omit it.
11. **For coverage-gap candidates (UNTESTED rows from 2.6)**: verify the recorded test search and failure-link analysis, then try to falsify the risk with existing indirect assertions, guards, type/compile guarantees, constrained inputs, downstream validation, or operational controls. Establish supported reachability, an affected population, a credible regression mode, and its material consequence before assigning Critical. Evaluate the least fragile meaningful test and concrete alternatives. Reject bare "simple", "urgent", "hard to test", or "covered indirectly" claims; test-feasibility evidence counts only when it names the proposed observation seam, why it is invasive/unstable, and why cheaper stable alternatives do not work. A Critical gap may be counterfactual about whether the code is currently wrong, but never about reachability or impact. Test difficulty does not downgrade an independently proved functional defect.
12. **For test-code-quality findings (Agent 13)**: confirm a flagged reflective access really has a non-reflective alternative (some QuestDB internals genuinely require reflection in tests) before reporting it. Confirm a "reinvented helper" finding by actually locating the helper with `rg` and checking its signature fits the test's need.
13. **For "swallowed exception → silent wrong results / leak / corrupt state" claims**: a `catch` block is defensive coding, not evidence that anything throws. Before reporting, name **all three** of:
    (a) the **concrete exception type** and the **exact statement** that raises it — quote the throwing line, don't infer it from the presence of a `try`;
    (b) proof that this type is actually **caught by the specific catch clause cited** — `catch (SqlException | CairoException)` does NOT catch `OutOfMemoryError`, `IllegalArgumentException`, `NullPointerException`, or any other unlisted `Error`/`RuntimeException`. An `Error` that escapes the catch means the query **fails loudly**, which inverts the finding;
    (c) that the throwing statement is reachable with the arguments the callsite actually passes (constants, pre-reserved capacity, and guarded early returns frequently make it unreachable).
    If any of (a)-(c) cannot be established, omit the candidate. Do not relabel the unproven mechanism as a latent invariant or hardening finding; it may remain private supporting analysis only.
    Also check for the **non-throwing** sibling: a `void` method that silently drops or frees its argument on an early return (`if (x) { free(arg); return; }`) breaks the same invariant with no exception at all, is usually far more reachable than the throw, and is not fixed by reordering statements around the call. Report that path instead of, or in addition to, the throw.
14. **Verify the conjunction, not just the links.** A multi-step candidate ("A publishes early → B can throw → C swallows → D reads stale → wrong rows") is only as true as its weakest step. Identify the single **load-bearing step** — usually “this supported state can actually occur” — and try to falsify it first. Per-line support for each isolated link does not prove their conjunction.
    Reading code is not verification when the load-bearing step is a runtime-shape claim — “the plan contains factory X”, “the guard does not fire”, “this branch is taken”, or any claim about races, ordering, retries, restarts, or filesystem state. Such a step requires an attached execution artifact produced or independently re-run by the falsifier at the cited revision. An agent's prose is not an artifact. Votes do not count as corroboration; even independent evidence types must still satisfy every admission field.
15. **Derive a fix only after admission, then verify it compiles and closes the window.** A plausible fix is never evidence that the finding is real. Once admitted, check that every referenced variable is in scope and non-`null`, that ownership transfers do not create a double-free or leak, and that the fix closes every admitted path.
16. **Determine net user impact, then classify.** Step 4 assigns severity only after this determination. A behavioral candidate missing it is `OMITTED` and never reaches Step 4.

    **(a) Net user impact — answer all five, in order:**
    - **Population** — who reaches it: every user, every user of a named feature, a specific query/DDL/ingest shape, or an operator-only path. “Any user in principle” is not a population. If no supported user or operator population can execute the producer, omit a behavioral candidate; do not preserve it as Moderate.
    - **Delta vs base** — what that population observes differently from the merge base for the identical executed trigger. Static comparison is allowed only for a fully static finding; every behavioral claim requires observed head and base artifacts at every review level.
    - **Magnitude and frequency** — how much and how often: per row, per query, per restart, once ever. Reuse the 3b.8 multiplier or bound.
    - **Offsets** — what recovers this downstream before the user sees anything. Code offsets: a later validation, a retry, a checksum, a caller that discards the value, a guard the same PR added elsewhere. **Process offsets count too**: an established team procedure, a merge or release convention, a CI gate, or a deployment step that resolves the condition before it can reach anyone. A state the team's normal workflow always corrects is offset — treat it as such rather than assuming the worst path is taken. Name the offset, or write "none found, searched <where>".
    - **Net** — exactly one of:
      - **net-negative** — the population is measurably worse off than base. Only net-negative behavioral candidates can be admitted.
      - **net-neutral** — no observable regression versus base. Omit it from PR findings.
      - **net-positive** — the population is better off than base. Omit it from PR findings.

    A **coverage-gap** row is counterfactual only about whether an unobserved regression currently exists. Its producer, reachable path, population, credible regression consequence, magnitude, offsets, change risk, and stable-test feasibility must be evidenced under Step 2.6. Coverage absence affects confidence; it does not manufacture impact. Static code-quality findings are assessed directly from changed lines.

    A behavioral net determination missing a supported population or same-trigger base delta is not a determination. A coverage-gap Critical missing material reachable impact or test-feasibility evidence is not Critical; classify it Moderate, accept it with evidence, or omit it as the admission schema warrants.

    **(b) Classify ledger entries** as:
    - **ADMITTED in-diff** — every applicable admission field is proved and the defect is inside the diff
    - **ADMITTED out-of-diff-breakage** — every applicable field is proved, and an unchanged callsite is broken by a contract this PR changed
    - **OMITTED pre-existing/not-attributed** — base has the same or worse behavior and this PR does not expose a new path
    - **OMITTED false** — counterevidence disproves the proposition
    - **OMITTED unverified** — any required producer, reachability, observation, artifact, or dependency is missing

**Enumerated candidates are admitted per item.** Never sample N instances and publish the unverified remainder. Every rendered item needs its own producer/trigger and evidence; otherwise omit that item.

Keep omitted candidates and their disproofs in the private ledger. Do not publish a Downgraded, retracted, rejected, or “possible issue” section, and do not report candidate counts. **OMITTED pre-existing/not-attributed** is the one exception: an entry whose producer, reachability, and observation are all proved leaves the ledger as a Step 4 adjacent issue draft. **OMITTED false** and **OMITTED unverified** entries never do.

Fresh falsifiers may run in parallel, but each receives only its neutral proposition and raw evidence contract. The parent independently checks every returned admission form before writing Step 4.

## Review checklists

Review the diff for:

### Correctness & bugs
- NULL handling: distinguish sentinel NULL vs actual NULL
- Edge cases and error paths
- SqlException positions point at the offending character, not the expression start
- Logic errors, off-by-one, incorrect bounds, wrong operator precedence
- **Reachability expansion:** for each changed symbol, list the SQL clauses, async contexts, error paths, parallel workers, and lock-held states it can now appear in but didn't before. Verify it works in each.

### Concurrency
- Race conditions: unsynchronized shared mutable state, missing volatile, unsafe publication
- Lock ordering issues that could cause deadlocks
- Thread-safety of data structures used across threads
- For every changed symbol, check whether it is now called from a thread or context (per 2.5d) where the previous concurrency assumptions don't hold

### Performance & algorithmic optimality

QuestDB is a performance-first database. On data paths the standard is not "avoid regressions" — it is "use the best
known algorithm", and a violation blocks the merge. Off data paths (SQL compilation, DDL, startup, metadata operations)
the standard is the same, but a *bounded* violation is a Moderate finding, not a blocker. Every new loop, traversal,
data structure choice, and computation must be justified as optimal or near-optimal — and every finding must say which
of the two categories it lands in, per the magnitude rule in Step 4.

#### Algorithm optimality (highest priority)

- For every new or changed loop/traversal, state the time complexity. Then ask: does a better algorithm exist? Flag:
    - O(n) linear scan where O(1) hash lookup or direct indexing is possible
    - O(n log n) sort where O(n) alternative exists
    - O(n^2) nested iteration where O(n) or O(n log n) would work
    - Any sub-optimal complexity where a better algorithm is known, at any scale
- Multi-pass vs single-pass: if the code traverses the same data multiple times (parsing, validating, transforming,
  collecting then iterating), determine whether the passes can be fused into one. Multiple passes is a finding unless
  each pass structurally depends on the completed output of the previous one.
- Redundant computation: values recomputed on every call that could be computed once and cached. Repeated map/list
  lookups for the same key. Re-parsing of already-parsed data. Re-traversal of an already-visited structure.
- Data structure fitness: is the chosen data structure optimal for the access pattern? Linear search in a list where a
  hash set gives O(1). Sorted array where a heap gives better insert/extract-min. Linked traversal where an indexed
  array gives O(1) random access. ArrayList where a pre-sized array suffices.
- Unnecessary copies and conversions: copying data that could be referenced in place. String ↔ CharSequence, byte[] ↔
  DirectByteCharSequence conversions when the original form works.

#### Zero-GC and allocation discipline

- Unnecessary allocations on data paths (zero-GC requirement) — even a single GC allocation on a per-row path is a
  finding
- Use of `java.util.*` collections (HashMap, ArrayList, etc.) instead of QuestDB's own zero-GC collections in `io.questdb.std`
- String creation or concatenation on hot paths (use CharSink, StringSink, or direct char[] instead)
- Capturing lambdas on hot paths — lambdas that capture local variables or instance fields allocate a new object on every invocation. Non-capturing lambdas (static method refs, no closed-over state) are safe as the JVM caches them. Flag any capturing lambda on a data path.
- Autoboxing on hot paths — primitive-to-wrapper conversions (`int` → `Integer`, `long` → `Long`, etc.) allocate silently. Watch for primitives passed to generic methods, stored in `java.util.*` collections, or returned from methods with wrapper return types.

#### Vectorization and native acceleration

- Missing SIMD or vectorization opportunities where QuestDB's native layer could process column data in bulk
- Inefficient algorithms where QuestDB already provides optimized alternatives

#### Compile-time paths

- GC allocations during SQL compilation are acceptable
- Algorithmic inefficiency during compilation is still a finding — slow compilation means slow first-query latency. A
  multi-pass parse, O(n^2) plan enumeration, or redundant AST traversals in the compiler are real problems. Severity
  follows the magnitude rule: compile cost that scales with input (O(n^2) in column/term count, re-parsing on every
  invocation, work repeated per row of a cursor) is Critical; a bounded fixed cost paid once per compilation — an extra
  small allocation, a linear scan over column count, a few hundred nanoseconds — is Moderate.

### Code quality
- Code smell: overly complex methods, deep nesting, unclear intent, dead code
- No third-party Java dependencies on data paths

### QuestDB coding standards
- Class members grouped by kind (static vs instance) and visibility
- Boolean names use `is...` / `has...` prefix
- Modern Java features: enhanced switch, multiline strings, pattern variables in instanceof

### Logging
- Every LOG chain MUST end with `.$()` or `.I$()` — a missing close holds a ring buffer slot forever and stalls the `logging_0` consumer
- Watch for `.put()` instead of `.$()` in LOG chains — `.put()` returns `Utf16Sink`, not `LogRecord`, breaking the chain
- No throw-capable expressions inside LOG chains: arguments are evaluated after the slot is acquired, so `LOG.info().$(func()).$()` leaks the slot if `func()` throws — hoist into a local first (`var a = func(); LOG.info().$(a).$();`)

### Resource management
- Resources properly closed in all code paths (especially error paths)
- try-with-resources used where applicable
- Native memory freed correctly

### Per-query memory tracker integration (if PR adds or changes native-memory allocation sites)

QuestDB caps how much native memory a single bounded workload (user SQL query, materialized view refresh, WAL apply batch) may allocate through a per-query `MemoryTracker`. The tracker is bound on `SqlExecutionContext` (`getMemoryTracker()` / `setMemoryTracker(...)`) and threaded into the tracker-aware `Unsafe.malloc` / `realloc` / `free` / `getNativeAllocator(tag, tracker)` overloads (and the Rust `QdbAllocator`). A `null` tracker degrades to global-RSS-only accounting. Apply this checklist whenever the diff adds or changes a native allocation site, a factory/cursor that owns growing native buffers, or a pooled memory class (`Map`, `RecordChain`, `RecordArray`, sort/tree chains, `GroupByAllocator`, join-key maps, etc.).

**The tracker is for large, potentially unbounded allocations only — that is the whole decision rule.** Do not treat "wire everything" as the safe default; over-wiring is itself a finding.

- **Wire it** when the allocation grows with the data or query cardinality and has no structural cap: map / hash-table backing, sort / tree / record chains, hash-join key (and match-id) maps, the group-by allocator and aggregate function state, `LATEST BY` rowid lists and maps, set-operation maps, encoded and top-K `ORDER BY ... LIMIT N` sort buffers (parallel and single-threaded), secondary / markout-horizon cross-join buffers, window-join and horizon-join aggregation maps, window partition maps and RANGE-frame ring buffers, SAMPLE BY fill, parquet decode buffers. These are the runaway vectors the limit exists to catch. An unbounded site that passes `null` (or omits the tracker overload entirely) is a coverage-gap candidate: record the runaway query path and classify it through Step 2.6. It is Critical only when that path independently proves the required material reachable impact.
- **Leave it on the global counter only** when the allocation is structurally bounded, self-capped, or process / session-lived: page-frame buffers, JIT buffers, `string_agg`, fixed-size heaps (e.g. the single-column long top-K heap), ROWS-frame window buffers, table reader / writer columns, symbol tables, connection buffers, memory-mapped pages. Wiring one of these is a finding in its own right: it adds two atomic counter updates per malloc/free on both the Java and Rust paths for no protective benefit, and tracker-aware pooled classes give up cross-query backing retention (they free native backing on cursor close and re-allocate on next use), so charging a bounded or retained allocator to the tracker trades away a pool optimization for nothing.

For each new or changed allocation site, verify:

- **Same tracker for malloc and its matching free.** A site that allocates with a tracker but frees with `null` (or vice versa) desyncs the counter and trips the live `recordPerQueryMemAlloc` balance assert. Trace every free / close path — error paths and `toTop()` / `clear()` / cursor-close reuse included — and confirm the identical tracker is used on both ends.
- **Nested SQL inherits the outer tracker.** Subqueries, the mat-view refresh inner SELECT, and WAL apply inner SQL must inherit the tracker already bound on the context, not acquire their own. A new acquisition site that acquires unconditionally (instead of only when no outer tracker is present) double-counts — flag it.
- **Coverage has a test.** A newly wired allocator needs a `*MemoryTrackerTest` proving (a) a breach throws the per-query out-of-memory message, (b) an under-limit run succeeds, and (c) a `getCursor()`-to-close leak loop stays balanced. Record a missing tracker test or an unpinned factory-class routing guard as an `UNTESTED` Step 2.6 row; classify and publish it only through the normal proportionality and admission gates.

### Store-and-forward & pool startup invariants (QWP client contract)
Apply this whenever the diff touches the QWP ingress path (upgrade/role
gating, in-place demote / lifecycle switch, connection handling on role
change), replication failover, a `questdb` submodule bump that carries
client (`java-questdb-client`) or ingress changes, or tests that drive a
producer through a failover/switch window (e.g. the `questdb-ent/e2e`
failover/switch suites). These are the CLIENT's store-and-forward
guarantees (the client code lives in the nested `questdb/java-questdb-client`
submodule); server-side changes and tests in this repo must be reviewed
against them. A violation here is a **Critical** finding: the whole point of
store-and-forward is that a running producer never loses data and never
hard-fails on a transient outage.

**Drainer (steady state — once the pool is running).**
- Once the pool is running, an async drainer thread ships buffered SF data to
  the server. It MUST NOT propagate server / transport errors back to the
  client (`Sender` producer calls, `flush()`, the pooled handle). The ONLY
  error a running drainer may surface to the caller is **SF out of space** (the
  on-disk / backing buffer is full and can accept no more rows). Flag any other
  failure class (connect-refused, DNS, unreachable/black-hole, TLS/cert, auth,
  role-reject, upgrade/protocol timeout, reset) that can escape the drainer
  onto a producer or borrow call.
- Primary reconnect MUST be fully contained inside the drainer thread and MUST
  have **no time limit** — no `reconnect_max_duration_millis`-style budget, no
  deadline, no "give up and latch terminal after N ms". A budget that latches
  the sender terminal on a long outage is a Critical violation: it drops a
  producer that store-and-forward promised to keep alive. Flag any bounded
  reconnect loop, `deadlineNanos` / `while (now < deadline)`, or terminal
  `SenderError` reachable from the running drainer's reconnect path.
- The drainer must retry with **exponential backoff** and handle every connect
  failure class gracefully, without a hard fail — it keeps buffering and keeps
  retrying until the wire is back. The per-attempt backoff may be capped (a max
  delay between attempts), but the RETRY LOOP ITSELF must be unbounded. Flag a
  capped total retry duration or an attempt-count cap on the steady-state
  drainer.
- **Sanctioned terminals (orphan-slot drainer only).** The orphan drainer
  (`BackgroundDrainer`) MAY quarantine its slot (`.failed` sentinel,
  human-in-the-loop) on conditions that are terminal by design: auth failure,
  a non-421 upgrade reject, and a genuine cluster-wide durable-ack capability
  gap that exhausted its documented settle budget (16 consecutive
  capability-gap sweeps, or a wall-clock budget anchored at the FIRST
  capability-gap error of the episode — whichever is hit first). These are
  NOT violations of the no-budget rule above. The settle budget applies ONLY
  to consecutive capability-gap attempts: transient classes (role reject,
  transport error) must never increment it or burn its wall clock — a
  transient state consuming the terminal budget (shared attempt counter,
  entry-anchored deadline) IS a Critical violation of this checklist.
- **Mid-stream server NACKs (no drop policy).** The NACK policy must mirror
  the connect-time tiering. A rejection category that a transient cluster
  state can produce (`WRITE_ERROR`, `INTERNAL_ERROR`, `UNKNOWN` — and any
  future status byte) is RETRIABLE: recycle the wire and replay from
  `ackedFsn+1`. It must NEVER drop the batch and NEVER latch a terminal /
  quarantine a slot on first sight. Only rejections deterministic under
  byte-identical replay (`SCHEMA_MISMATCH`, `PARSE_ERROR`, `SECURITY_ERROR`
  on a writable node) may go TERMINAL. A client that advances the ack
  watermark past a NACKed frame is silently losing data — Critical. A frame
  repeatedly rejected with no ack progress must escalate through the
  poison-frame detector (bounded consecutive strikes at the same head FSN),
  not through a WS close-code list — close codes carry no policy semantics.
  `UNKNOWN` must fail OPEN (retry), never closed (terminal): a status byte
  from a newer server must degrade to retry, not to a dead sender.

**Pool startup — two modes; the mode decides who sees connectivity errors.**
- `lazy_connect=true`: `build()` MUST succeed with **no server present**. The
  producing `Sender` must work immediately (writes buffer via SF), and once the
  server comes up the read side must also connect and read (reads are deferred,
  not disabled).
- `lazy_connect=false` (default): `build()` / the initial connect MUST expose
  connectivity problems to the caller — DNS errors, connect-refused /
  unreachable, TLS/cert, authentication/authorization, and connect/upgrade
  timeouts must all surface as a thrown exception at startup, not be swallowed.
- **In BOTH modes the boundary is the same:** connectivity errors are only
  ever the caller's problem DURING initialization. Once the client has
  connected and is past initialization, the running drainer reverts to the
  steady-state contract above — it must NEVER expose transport problems, NEVER
  impose a reconnect time budget, and NEVER hard-fail on a transient outage.

**Server-side & test application (this repo).**
- The server MUST NOT rely on producer-visible role errors: an in-place
  demote CLOSES QWP ingress connections (no per-write SECURITY_ERROR to an SF
  sender). A server change that reintroduces per-write role errors on the QWP
  ingress path breaks the containment contract above.
- Flag any test (unit, integration, or e2e) that uses QWP producer-visible
  role errors as evidence of the REPLICA write gate — under the containment
  contract the producer is silent by design. Write-gate evidence belongs on
  pg-wire probes, frozen commit counts on the settled replica, and
  post-promotion SF drain (durable-ack await barriers + dense oracles).
- Dense/count oracles over rows produced through an SF sender must account
  for at-least-once replay: durably ack (await) seed rows before the
  disturbance, or use a DEDUP table — otherwise the oracle reports replay
  duplicates as data corruption.

### SQL conventions (if tests or SQL involved)
- Keywords in UPPERCASE
- `expr::TYPE` cast syntax preferred over CAST()
- Underscores in numbers >= 5 digits (e.g., 1_000_000)
- Multiline strings for complex queries
- No DELETE statements (suggest DROP PARTITION or soft delete)
- Tests use the `assertQuery(...)` builder for SQL assertions (see "SQL test assertions" below) and `execute()` for DDL
- Single INSERT for multiple rows

### SQL test assertions (builder API — strict, blocking)

QuestDB has migrated SQL test assertions to the fluent `AbstractCairoTest.assertQuery(query)` builder. These rules are blocking — treat violations as **Critical** findings, not style nits. Apply them to every test line the diff **adds or modifies** (a residual pattern that the PR merely moves or reindents is not a finding; a newly written or edited one is).

- **`assertSql(...)` has been REMOVED — there is no query-result `assertSql(...)`/`TestUtils.assertSql(...)` to fall back to.** Any new or changed test code that asserts query results with `assertSql(...)` / `TestUtils.assertSql(...)` is a Critical finding (it will not even compile against the current base class); the author must use the builder instead:
    - data: `assertQuery(sql).returns(expected)` — chain `.timestamp(...)`, `.expectSize()`, `.noRandomAccess()`, `.sizeMayVary()`, `.ddl(...)`, `.mutateWith(...)`, `.withEngine(...)`, `.withContext(...)` as needed.
    - plans: `assertQuery(sql).assertsPlan(plan)` / `.assertsPlanContaining(...)` / `.assertsPlanNotContaining(...)`, or fold the plan into a data assertion via `.withPlan(...)` / `.withPlanContaining(...)` / `.withPlanNotContaining(...)`.
  Do **not** accept "the surrounding file already uses `assertSql`" — there is no such helper anymore, so the diff's lines must use the new API. Flag `assertPlanNoLeakCheck(...)`, `getPlan(...)`, `assertPlanDoesNotContain(...)`, and direct `TestUtils.assertSql(...)` in new/changed test code for the same reason. The one `assertSql` that legitimately survives is the live-`ServerMain` wrapper `TestServerMain.assertSql(sql, expected)`: it is a convenience for the running-server context, internally drives the builder via `returnsOnce()` (single pass, because a live server's state mutates between reads), and is NOT the banned query-result helper — do not flag it.

- **`.returnsOnce(...)` is a correctness smell — flag every newly added use.** `returnsOnce` runs the query through a SINGLE cursor pass and deliberately SKIPS the second read, the `calculateSize()` pass, the variable-column check, and the factory-property assertions (`supportsRandomAccess`, `expectSize`) that `.returns(...)` performs. Those skipped checks catch real bugs: cursors that don't reset correctly on `toTop()`, `size()` that disagrees between passes, random-access records that return wrong values via `recordAt()`. `returnsOnce` is **only** justified when the query's output is genuinely unstable across two reads with no underlying data change — e.g. an unseeded `rnd_*` in the projection, `now()`/`sysdate()`/`systimestamp()`-style time-varying output, or inherently non-deterministic row order. For a `.returnsOnce(...)` on a deterministic query this is a Critical finding: demand `.returns(...)`. Require the author to state *why* the query is unstable; "it was simpler" is not a reason — the shortcut leaves real bugs untested.

- **Anti-pattern: a lone `assertQuery(...)` wrapped in `assertMemoryLeak(() -> { ... })`.** The builder runs its OWN memory-leak check by default (it wraps internally unless `.noLeakCheck()` is set). When an `assertMemoryLeak(...)` lambda's only meaningful statement is a single `assertQuery(...)` chain, the outer wrapper is redundant and almost always forces a `.noLeakCheck()` on the builder — which disables the builder's leak check and replaces it with a hand-rolled one, defeating the point. Flag it: drop the `assertMemoryLeak` wrapper and the `.noLeakCheck()`, letting the builder leak-check itself. The wrapper is only legitimate when the lambda genuinely holds multiple statements (DDL + inserts + several assertions) that must share one leak-check scope; a single builder call does not.

### Permission hooks (if PR adds an ALTER operation or other state-mutating SQL)

This check decides on two `rg` searches in this repo — run them instead of reasoning about them.

- **A new ALTER TABLE operation, or any new statement that mutates table state, needs a `SecurityContext.authorize*()` call on its execution path.** Cite the callsite — the `AlterOperation.apply()` dispatch for ALTER, the op or compiler class for everything else. Absence is a proven finding, not a speculative one: the evidence is the search that finds the new operation and the search that finds no `authorize*` call covering it. Classify it with the standard rubric — a state-mutating operation no security context can refuse is a privilege bypass, which the severity table already lists as Critical.
- **A new `authorize*()` method must be implemented wherever it is abstract:** `AllowAllSecurityContext` and `ReadOnlySecurityContext` (`DenyAllSecurityContext` extends the latter), plus any test `SecurityContext` implementations the compiler does not already catch. If the PR instead adds the method with a permissive `default` body, every implementation that does not override it — including enterprise ones this checkout cannot see — silently grants the permission. The interface does use `default` deliberately in places, so ask for the rationale; treat a missing one as the finding, not the `default` itself.
- **Enterprise wiring is out of scope for a review run in this repo.** `Permission.java` registration, `PermissionParser` GRANT/REVOKE parsing, `EntSecurityContextBase` / `AdminSecurityContext` implementations, and replica `deniedOnReplica()` gating all live in a separate repository and cannot be verified from this checkout. Note them once as an enterprise follow-up when the PR adds an `authorize*()` method; do not raise them as findings and do not let them affect the verdict.

### Test review
- **Coverage gaps are impact- and proportionality-assessed:** consume the Step 2.6 map. Missing tests alone are not blocking. For every uncovered path, establish user/operator impact, change risk, existing safeguards, and the least fragile meaningful test before choosing Critical, Moderate, accepted, or exempt. Do not accept unsupported "simple" or "hard to test" claims, and do not demand a brittle/invasive test whose demonstrated cost and fragility outweigh a small residual user risk. Add every discovered path to the private map; publish only admitted gaps.
- **Execution-mode dimensions (QuestDB-specific):** where the changed code is sensitive to them, demand coverage across the modes that alter its behavior: WAL vs non-WAL tables, O3 (out-of-order) writes vs append-only, JIT-compiled vs interpreted filters, parallel vs single-threaded execution (parallel GROUP BY/filter workers), partitioned vs non-partitioned tables. A SQL-engine change tested in only one mode is a coverage gap in the others — name the untested modes.
- **Fuzz coverage:** for parser, encoder/decoder, ingestion-protocol, or O3/WAL-merge changes, search the test tree for existing fuzz tests (`rg -l Fuzz`) covering the changed surface. If one exists and was neither extended nor mentioned as run against the change, add an `UNTESTED` Step 2.6 row; classify and publish it only through the normal proportionality and admission gates.
- **Cross-context coverage:** For every entry in the cross-context exposure list (2.5d), verify a test exercises the changed symbol from that context. Record each missing cross-context test as an `UNTESTED` Step 2.6 row; classify and publish it only through the normal proportionality and admission gates.
- **Error path coverage:** Are failure cases, exceptions, and edge conditions tested — not just the happy path?
- **NULL tests:** Are NULL inputs, NULL columns, and NULL expression results tested?
- **Boundary conditions:** Empty tables, empty partitions, single-row tables, max-value inputs, zero-length strings.
- **Concurrency tests:** If the code touches shared state, are there tests that exercise concurrent access?
- **Resource leak tests:** Tests must use `assertMemoryLeak()` for anything that allocates native memory.
- **Test quality:** Are tests actually asserting the right thing? Watch for tests that pass trivially, assert on wrong values, or test implementation details instead of behavior.
- **Regression tests:** If this PR fixes a bug, is there a test that reproduces the original bug and would fail without the fix?
- Use Grep and Glob to find existing test files for the changed classes and verify they cover the new behavior.

### Test code quality
- **No vacuous assertions.** Every assertion must be able to fail. Flag `assertTrue(true)`, `assertFalse(false)`, `assertEquals(x, x)`, asserting a literal against the same literal, or a `@Test` body with no assertion and no `expected=`/`assertThrows`.
- **Reflection is a last resort.** Flag `setAccessible(true)`, `getDeclaredField`/`getDeclaredMethod`, `Field.set`, `Class.forName` when a public API, existing helper, or constructor would reach the same state. Name the non-reflective path.
- **Reuse before reinventing.** Search for existing helpers, base classes, and fixtures before accepting inline setup. Duplicated setup/assert blocks an existing helper or a parameterized test would cover are findings; name the helper.
- **No javadoc bloat.** No multi-paragraph javadoc on `@Test` methods, no javadoc that restates the test name, no stacked/duplicated javadoc. Prefer a precise test name and at most a one-line comment.
- **Test-appropriate standards.** zero-GC and `io.questdb.std`-over-`java.util` rules do NOT apply to tests — do not flag them there. Member ordering, `is/has` naming, and SQL style DO apply.
- **No debugging residue.** No `System.out.println`, no commented-out code, no `@Ignore` without a referenced ticket.

### Unresolved TODOs and FIXMEs
- Scan the diff for `TODO`, `FIXME`, `HACK`, `XXX`, and `WORKAROUND` comments. For each one found:
  - Is it a pre-existing comment that was just moved/reformatted, or newly introduced in this PR?
  - If newly introduced: does it represent unfinished work that should block the merge, or a known limitation that is acceptable to ship? Flag any that look like deferred bugs or incomplete implementations.
  - If the TODO references a ticket/issue number, verify the reference exists.

### Commit messages
- Plain English titles (no Conventional Commits prefix), under 50 chars
- Full long-form body description, line breaks at 72 chars
- Active voice, naming the acting subject

## Step 4: Output

Present only **ADMITTED** findings. Omitted candidates, disproofs, retractions, agent counts, candidate counts, and the private ledger never appear in the public review. Do not publish a hypothesis and retract it later; finish falsification first. It is valid to report no findings. The single exception is the **Adjacent findings** section below, which carries proved pre-existing bugs as issue drafts — not findings against this PR, and weightless in every gate.

**Proportionality.** Keep the report actionable in one sitting. If a normal-sized PR yields more than about seven total findings, re-run the admission gate on every item and remove dependent, duplicate, not-attributed, and low-value prose. Removing a not-attributed item means moving it to Adjacent findings, not discarding it. Review depth is demonstrated by evidence, not report length.


**Every finding — at every severity — opens with three one-line summaries, before any prose:**

- **Problem:** what is wrong. ≤ 12 words. No mechanism or fix.
- **Net impact:** supported population and magnitude. ≤ 12 words. A behavioral item with no net regression is omitted.
- **Evidence:** the decisive artifact or static proof, including the reviewed revision identity.

Write these lines last from the completed admission form, never first from a hunch. Then give only the minimal producer → path → symptom trace, base comparison, and suggested fix.

```
Problem: Symbol column read twice per scanned row.
Net impact: ~2x column IO on every filtered scan.
Evidence: benchmark.sh output at abc123; base 8ms, head 16ms.

Problem: WAL segment leaks a file descriptor on the error path.
Net impact: Ingestion stalls after ~1k failed commits.
Evidence: WalLeakTest red at abc123, green at base def456.
```

Structure as:

### Severity classification (impact-first — severity is the user's consequence, not the finding's category)

Severity is a function of **what the user loses**, not of which checklist the finding came from. Classify by the worst *user-visible* consequence on a *reachable* path. Do not classify up "to be safe": an inflated Critical costs exactly what a real one costs and teaches the author to skim the report.

**"The user" means a QuestDB database user or a production operator** — someone running queries, ingesting data, or operating a deployment. It does **not** mean a QuestDB developer, a CI job, or the release process. A finding whose only affected population is the team — a slower build, a broken local setup, an awkward merge — is never Critical, whatever its symptom. Developer-experience problems are Moderate at most, and most are Minor.

**The Critical test — name the symptom.** A finding is Critical only if you can complete this sentence with something a user, operator, or on-call engineer would actually observe: *"Because of this, the user sees ___."* The valid completions are:

- **wrong or missing data** — incorrect query results, silent truncation, lost or duplicated rows, corrupted on-disk state, divergent replica, wrong materialized-view content;
- **a crash, hang, or unavailability** — panic, deadlock, livelock, unbounded loop, OOM, fd/thread/connection exhaustion, or a leak that grows without bound under a repeatable operation;
- **a security or ACL failure** — privilege bypass, permission not enforced, credential or cross-tenant data exposure;
- **a broken or misleading failure mode** — an operation that fails with no error or the wrong error, an error message the user cannot act on, an exception swallowed so failure looks like success, a fault lost or unlogged such that an incident cannot be diagnosed;
- **a compatibility break** — on-disk format, wire protocol, public/SQL/JNI API, or config semantics changed so existing clients, existing data, or a rolling upgrade break;
- **a performance or IO regression the user can feel** — per the magnitude rule below;
- **an admitted Critical coverage gap** — the changed path is supported and reachable, a named population can execute it, a credible regression would produce one of the material consequences above, existing controls do not contain it, and Step 2.6 shows why stable coverage is warranted. The gap is counterfactual only about whether the regression currently exists; it is not counterfactual about trigger, reachability, or impact.

**Every completion needs a trigger.** A symptom sentence must name the concrete query shape, ingest pattern, API call, config value, or operation sequence a user/operator can run: *"user does X → sees Y"*. For a coverage gap use *"user does X; if this changed path regressed as Y, the user would see Z"*. "Could theoretically return wrong results" is not evidence.

If a behavioral candidate cannot name and execute a supported trigger with one of the consequences above, omit it; do not preserve the mechanism as Moderate. Concrete static standards, maintainability, and coverage findings may still be Moderate or Minor when fully established directly from changed source.

**Magnitude rule for performance and IO.** Cost blocks only when it is user-observable. Ask two questions: does the cost **scale with data** (per row, per value, per page, per partition, per scanned block), and is it on a path the user **waits for or repeats**?

- **Critical:** per-row/per-value/per-page work on a data path; extra IO multiplied across a scan (reading a column, page, partition, or file that need not be opened); an added pass over data; O(n²) in row or partition count; an algorithmic class change on a query execution path — anything that measurably moves query latency, ingestion throughput, or disk/network volume.
- **Moderate:** a bounded, non-scaling cost off the data path — a few hundred nanoseconds during SQL compilation, one extra allocation per query (not per row), a linear scan over a small fixed set (column count, partition unit, config keys), work at startup, DDL, or metadata-change time. Worth reporting and worth fixing; not worth blocking a merge. **Sub-optimal but bounded is Moderate**, even when a better algorithm plainly exists — name the better algorithm and state the bound that makes it non-blocking.
- To file a performance finding as Critical you must state the magnitude: the multiplier and what it multiplies ("one extra 4KB page read per scanned row", "a second full pass over the partition", "O(n²) in partition count"). A Critical performance finding with no stated magnitude is mis-filed.

**Config-divergence rule.** "The same statement is accepted under config A and rejected under config B" (a flag-dependent plan shape changing what a guard sees, a validation only some execution mode runs) is a finding in its own right — an inconsistency an operator can observe across nodes — and is classified on the consequence of the divergence itself. It does not inherit the severity of the worst case reachable through the more permissive configuration; that worst case is a separate finding that must pass the symptom test, the trigger requirement, and the base-behavior check on its own.

**Out of scope — these are not findings.** Three classes get reported constantly and are worth nothing. Drop them before they reach the report:

- **Merge mechanics.** The reviewed artifact is the code change, not the merge event. Submodule pin position, merge order between repos, branch existence, labels, and anything true only of the PR's in-flight state are properties of *how it lands*, not of *what it does*. A PR that bumps a vendored submodule pointer necessarily pins whatever commit it was built against — that is a property of the pointer, not a defect in the change.
- **Tautologies.** Before filing, ask: *"would this finding appear on every PR of this shape?"* If yes, it describes the workflow, not this change. A finding that can never be absent is not a defect, and reporting it teaches the author to skim the report.
- **Overridden project decisions.** When the project's own tooling explicitly permits something — a CI check that passes by design, a documented exception, a convention the PR body already names — that is a decision, not an oversight. Overriding it requires evidence the decision is *wrong*, not merely that it is permissive. "CI allows this but I would not" is not a finding.
- **Upstream submodule content.** Anything inside a submodule whose pointer move Step 2.4 classified **UPSTREAM-SYNC**. That code already landed on the submodule's default branch; this PR did not write it, did not review it, and cannot be asked to fix it. A breaking change discovered there is upstream's, released independently, and belongs in an issue against that repository — never a Critical against this PR. The one exception is an integration defect at a callsite *inside this diff*, which is filed against that callsite with its own symptom and trigger.

**Moderate.** Admitted, attributable defects with bounded or developer-facing impact: a concrete changed-line standards violation, proved weak test, missing internal-path coverage, documentation defect, or bounded off-data-path cost. An unreachable runtime theory, unchanged residual hardening opportunity, or proposition that only supports another candidate is not Moderate; omit it.

**Minor.** Cosmetics: member ordering, naming, formatting, comment wording, import order.

Do not inflate and do not deflate. Filing a real user-visible defect as Moderate is a review failure; so is filing a bounded compile-time micro-cost as Critical. Where two readings are defensible, pick the one you can evidence.

### Critical
Blocking issues introduced or exposed by this PR, ordered worst user impact first. Each must include:
- The three summary lines (**Problem** / **Net impact** / **Evidence**) before anything else
- The **net determination** from 3b.16(a): population, delta vs base, magnitude/frequency, offsets, and a net of **net-negative** — a Critical that is net-neutral or net-positive is mis-filed by definition
- Exact file path and line numbers
- The **symptom sentence** with its supported trigger: "user does X → sees Y". For a coverage-gap Critical: "user does X; if this changed path regressed as Y, the user would see Z"
- For a coverage-gap Critical: the credible mutation/recurrence, existing safeguards and offsets, change-risk assessment, least-fragile stable test considered, and concrete evidence that cheaper alternatives are inadequate
- Whether the finding is **in-diff** or **out-of-diff-breakage** (an unchanged callsite this PR breaks) — both are this PR's responsibility
- Code path trace showing why the bug is real and reachable
- **Base behavior for the identical trigger** (required): executed at the merge base. If base shows the same or worse user-visible outcome, omit the candidate as not attributed to this PR. For a genuinely new surface, write `N/A — new surface` and prove that base cannot express the trigger. Base rejection is the absence of a wrong-result defect, not a worse defect outcome.
- For out-of-diff-breakage: the callsite that triggers it, plus the violated contract — cite it from 2.5c at level 2+, or state it inline at levels 0-1 where 2.5c is not built
- For performance findings: the magnitude statement (the multiplier and what it multiplies)
- Suggested fix, written to be applied in THIS PR

Pre-existing/not-attributed observations are never Critical; a fully proved one belongs under Adjacent findings instead.

### Moderate
Non-blocking admitted issues worth fixing. Every item must still include the three summary lines and its decisive evidence. Dynamic behavioral speculation is not allowed here.

### Minor
Concrete cosmetics on changed lines. Non-blocking, optional.

### Adjacent findings (not blocking — file as GitHub issues)

Bugs that already exist on the merge base, found in code this review visited (changed files, callers from the callsite inventory, cross-context exposures), which this PR does not introduce, break, or worsen. They are **not findings against this PR**: they never appear under Critical/Moderate/Minor, never influence the verdict, and are never proposed as changes to this PR. Discarding them instead is pure waste — the investigation is already paid for, and nobody re-finds them later.

They are held to the same evidence bar as a published finding. An adjacent draft comes only from a candidate that reached **OMITTED pre-existing/not-attributed** with its producer, reachability, and observation proved. A candidate that ended **OMITTED false** or **OMITTED unverified** stays in the private ledger; this section is not a home for speculation that failed falsification.

Report each as a ready-to-file issue draft, so it can move to GitHub without re-investigation:

- **Problem:** ≤ 12 words — doubles as the issue title
- **Net impact:** ≤ 12 words — population and magnitude, or "None — <reason>"
- **Location:** file path + line numbers
- **Symptom:** what a user would observe — or "latent: no user-visible impact today", naming the guard that prevents it
- **Reachability:** the path that reaches it, or why nothing does yet
- **Suggested fix:** one or two lines
- **Severity if filed standalone:** Critical / Moderate / Minor per the rubric above

Offer to file them; do not file anything without being asked. Their count and severity sit outside the finding-proportionality budget and outside every gate in the Summary. If one is severe enough that shipping this PR without it is genuinely unsafe — because this PR moves code onto a path where the pre-existing bug now fires — then it is not adjacent: it is out-of-diff-breakage, it belongs under Critical, and you state that argument explicitly.

### Coverage map
State the test-gate result and the number of **admitted** coverage gaps only. Render admitted gap rows with their recorded search and failure link. Do not expose counts for omitted candidates or private UNTESTED rows; keep the full Step 2.6 matrix private unless the user asks to see it.

### Summary
- **Verdict**, exactly one of:
  - **approve** — no open Critical findings and the test gate passes. Moderate and Minor items may remain open; list them and approve anyway. This is the expected outcome for competent work, and withholding it when both gates pass is itself a review failure.
  - **approve with comments** — both gates pass; you want specific Moderate items addressed but will not block on them. Name which ones.
  - **request changes** — at least one Critical is open, or the test gate fails.
  - **needs discussion** — the change requires a product, architecture, or compatibility decision a reviewer cannot make alone.
- **Correctness gate (hard rule):** the verdict cannot be "approve" while any **ADMITTED** Critical finding remains open, including an admitted Critical coverage gap. Omitted hypotheses never affect the verdict.

  Before finalizing, rerun the admission audit from evidence fields rather than from report prose:
  - **falsification:** state the strongest attempted disproof for each rendered behavioral finding;
  - **producer:** confirm a supported operation/version/configuration actually creates every trigger state;
  - **independence:** confirm the admitting verifier did not receive the discovery narrative, severity, fix, or votes;
  - **dynamic evidence:** confirm races, ordering, retries, restarts, filesystem states, and compatibility claims have an executed artifact at the reviewed revision and the same-trigger base result;
  - **dependency:** remove every item whose parent premise was omitted;
  - **severity:** classify only after admission. Never promote missing evidence or uncertainty to Critical.

  If any field fails, omit the candidate and rerun the verdict. If the admitted Critical list is empty and the test gate passes, approve plainly; zero findings is expected for correct changes.
- **Test gate (hard rule):** the gate fails only while an **ADMITTED Critical coverage gap** remains open. Zero test changes, a bug-fix label, or missing regression coverage triggers the Step 2.6 analysis but never automatically forces `request changes`. Moderate gaps may accompany `approve with comments`; accepted gaps do not affect the verdict. Any independently admitted functional Critical still fails the correctness gate regardless of test effort or urgency.
- State the test-gate result and admitted coverage-gap count. Do not publish total UNTESTED or omitted-candidate counts from the private map.
- Highlight any regressions or tradeoffs
- Never make the verdict conditional on splitting the PR. Pre-existing and not-attributed observations never affect the verdict, whether they were omitted or delivered as adjacent issue drafts.
- Do **not** state agent counts, candidate counts, rejected/false-positive counts, or retraction history.
- State the Step 2.4 submodule provenance verdicts, one line per changed pointer (e.g., "questdb: OFF-DEFAULT — in scope; java-questdb-client: UPSTREAM-SYNC — out of scope"). If a pointer moved and no verdict is stated, the scope of the review is unknown and the report is incomplete.
- State only the admitted split: in-diff / out-of-diff-breakage. At levels 0-1, describe the limited callsite analysis rather than implying a clean bill of health.
- State the severity distribution. If the report is long or severity-heavy, re-run admission; do not compensate by preserving weak items at a lower severity.
