---
name: review-pr
description: Review a GitHub pull request against QuestDB coding standards
argument-hint: [PR number or URL] [--level=0..3]
allowed-tools: Bash(gh *), Read, Grep, Glob, Agent
---

Review the pull request `$ARGUMENTS`.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. QuestDB is mission-critical software: bugs can cause data loss or system failures in production deployments that are expensive to patch. Be critical, thorough, and opinionated. Your job is to catch problems that would hurt a user before they ship — not to be nice, and not to demonstrate thoroughness by volume.

**A review that blocks on everything blocks on nothing.** Every finding costs the author a CI round-trip, and an inflated one costs the whole report its credibility. Reserve blocking severity for defects with a real user consequence, report everything else honestly at the severity it deserves, and approve when the gates pass. "Approve" is a normal, expected outcome of reviewing competent work — not a failure of rigour.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed. Treat the diff as the entry point, not the scope.
- **Report every issue you find, at the severity it actually deserves.** Do not soften language or hedge — say "this is wrong", not "this might be an issue". But do not launder a nit into a blocker either: getting severity right is part of the review, not a softening of it.
- **Keep the blast radius of the PR small.** This PR should fix what it set out to fix, plus anything this change breaks. Pre-existing bugs you walk past are reported as **adjacent findings** — standalone issue drafts for GitHub — not as change requests against this PR. Bundling unrelated fixes grows the diff, grows the callsite inventory, grows the next review, and delays the change under review. The one exception is a pre-existing bug that this PR moves onto a live path: that one this PR must handle, and you say why.
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, ask: what inputs break this? What happens under concurrent access? What if this runs on a 10-billion-row table? What if the column is NULL? What if the partition is empty?
- **Demand optimal algorithms where they matter.** QuestDB is a performance-first database. On data paths, "works
  correctly" is not sufficient — a linear scan where a hash lookup exists, two passes where one suffices, or a per-row
  allocation on a scan is a blocking defect. Off the data path, apply judgement: a bounded, non-scaling cost during SQL
  compilation, DDL, or startup is worth reporting as Moderate, not worth blocking a merge over. Ask "is there a faster
  way?" for every loop, traversal, and data-structure choice — then ask "does the user feel the difference?" before
  choosing the severity.
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing documentation for non-obvious behavior.
- **Untested user-visible behavior is broken code.** Every new or changed behavior a user can observe — query result, public API, config key, error message or failure mode — ships with a test whose assertion would fail if it regressed, or it does not ship. "The change is simple" and "existing tests probably cover it" are not evidence; a named test located by a recorded search, with a stated failure link, is. Untested *internal* branches are a Moderate coverage gap, not a blocker.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmarks or reason about the algorithmic change — does it actually improve things, or could it regress in other cases? Even if the PR doesn't claim to be about performance, evaluate whether the chosen algorithms and data structures are optimal — sub-optimal code that "works" is still a finding. If it says "simplify", verify the new code is actually simpler and doesn't drop behavior. Treat the PR description as an unverified hypothesis, not a statement of fact.
- **Read the full context of changed files** when the diff alone is ambiguous. Use `Read` plus Grep and Glob to inspect the surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem
  requires physically impossible conditions (billions of columns, corrupted JNI inputs, values that no caller can
  produce), it is not a real finding — drop it. Focus on bugs that real workloads can trigger, not theoretical edge
  cases that exist only in the type system.
- **QuestDB runs with Java assertions enabled (`-ea`).** Assertions are a valid guard for invariants that indicate
  corruption or internal bugs. Do NOT flag `assert` as insufficient — it is the preferred mechanism for conditions
  that should never occur in a non-corrupt database. Only flag an `assert` if the condition can plausibly be triggered
  by normal (non-corrupt) user operations.

## Review level

Parse the invocation arguments for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level token and any `--range=` token before feeding the remainder (PR number or URL) to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs (replication, JNI boundary changes, on-disk format, public API, security/ACL).

| Level | What runs |
|-------|-----------|
| **0 (default)** | Steps 1, 2, 2.4, 2.6, 4. Skip Step 2.5. Skip Step 3 — no agent fanout; review the diff inline in the main loop, using Read/Grep on demand to resolve ambiguities. Skip Step 3b — verify each finding inline as you write it. Single-pass review covering correctness, NULL handling, **algorithmic optimality**, tests, and QuestDB standards on the diff itself. The performance checklist (including algorithm optimality) is mandatory at every level. When the diff touches test code, also apply the test-efficacy and test-code-quality anti-pattern checks inline (vacuous assertions, reflection overuse, reinvented helpers, javadoc bloat). Step 2.6 (test coverage map) is mandatory here as at every level — build it inline before writing findings; derive the behavioral-change rows directly from the diff since 2.5a is skipped. |
| **1** | Adds Step 2.5a (semantic delta only — skip 2.5b/2.5c/2.5d) plus Step 2.5e when test code is present. In Step 3, launch Agent 1 (correctness), Agent 3 (performance), Agent 5 (tests), Agent 6 (code quality), and — when the diff touches test code — Agent 12 (test efficacy) and Agent 13 (test-code quality) as parallel Agent-tool tasks. Skip all other agents. Skip Step 3b — verify findings inline as you draft the report. |
| **2** | Full Step 2.5 (including 2.5e when test code is present), but in 2.5b restrict the callsite inventory to `public`/`protected` symbols (skip package-private and `pub(crate)`). In Step 3, launch Agents 1-8 (Agent 8 only if `.rs` files are present), plus Agent 11 (adversarial performance), plus Agents 12 and 13 when the diff touches test code. Skip Agent 9 (cross-context), Agent 10 (adversarial fresh-context), and Agent 14 (regression-test efficacy verification). Step 3b uses a single batched verification agent for all findings instead of one per finding. |
| **3** | Every step below as written, all 14 agents, per-finding verification. The full mission-critical pass. |

State the chosen level in one line at the start of the review so the user knows what they're getting (e.g., "Reviewing PR #1234 at level 2"). If the level was defaulted, mention that level 3 exists for full review.

## Spawning review agents

Steps 3 and 3b below call for "agents" launched "in parallel". In Claude Code, launch
them with the Agent tool — one task per agent role, dispatched in parallel. Each task
must be self-contained because the child does not inherit the parent conversation.
Give every review task:

1. the PR diff,
2. the Step 2.4 submodule provenance verdicts, one line per submodule — an agent that does not know a pointer move is an UPSTREAM-SYNC will review upstream code as if this PR wrote it,
3. the full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list) plus the test coverage map from Step 2.6,
4. the specific role instructions for that agent (Agent 1..14 below),
5. an explicit "review only — do not edit any files" constraint,
6. the scope policy: every finding must be tagged as exactly one of (a) **in-diff**, (b) **out-of-diff-breakage** — an unchanged callsite that this PR breaks, which blocks and is fixed here — (c) **adjacent** — a pre-existing bug in visited code that this PR does not introduce, break, or worsen, which is reported as a standalone GitHub-issue draft and never blocks — or (d) **incomplete-hardening** — a residual gap in a guard, check, or guarantee that this PR itself introduces, where behavior for the gap input is identical to the merge base (nothing regressed; enforcement is new but partial). Agents must not propose adjacent findings as edits to this PR.

The diff plus surface map can be large — write them to a shared file under a temp directory and point each task at that path rather than pasting the whole payload into every task. The fresh-context adversarial agents (Agent 10, Agent 11) must NOT receive the change surface map, the test coverage map, or checklists; give them only the diff and changed-file names, per their instructions. The parent session owns synthesis, deduplication, and the final report — children only return findings.

## Step 1: Gather PR context

Every mode must end this step with **`$BASE`** set — the commit the change is measured against. `$BASE` is required by Step 2.4 and by the base-behavior check on every Critical; a review that never established it cannot attribute anything.

Capture the PR identifier in `$PR` (the part of the invocation arguments left after stripping the level token), then fetch metadata, diff, and review comments in a single bash call so `$PR` is in scope for all three `gh` invocations:

```bash
PR='<PR number or URL from the arguments, with any --level=N / -lN / bare-digit level token removed>'
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
- Description repeats the verb (e.g., `fix(sql): fix ...` not `fix(sql): DECIMAL ...`)
- Description speaks to end-user impact, not implementation internals
- If fixing an issue, `Fixes #NNN` is at the top of the body
- Tone is level-headed and analytical, no superlatives or bold emphasis on numbers
- Labels match the PR scope (SQL, Performance, Core, etc.)

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

- **UPSTREAM-SYNC** — the new commit is an ancestor of the submodule's default branch. The work inside it **already landed upstream**; this PR only advances the pointer to pick it up. **Its contents are not in-diff and are not this PR's responsibility.** Do not review them as changes, do not attribute their behaviour changes, breaking or otherwise, to this PR, and do not build findings out of them. The only legitimate finding here is a genuine *integration* defect: code in this diff calls the newly-synced code incorrectly. That finding lives at the callsite in this diff, not inside the submodule.
- **OFF-DEFAULT** — the new commit exists only on a feature or PR branch. The submodule's changes **are** part of this logical change and are reviewed in-diff.
- **UNRESOLVED** — the branch cannot be determined (no network, shallow clone, missing remote). Say so explicitly in the report, treat it as OFF-DEFAULT for safety, and state that the scope decision was made without provenance.

Record the verdict per submodule in one line each, and repeat it in the Step 4 report so the scope decision is auditable. This repository vendors several submodules — `java-questdb-client`, `jemalloc`, `simdjson`, `zlib`, `fsst`, `async-profiler`, `parquet-testing`. Each is classified independently, and a pointer bump that merely picks up already-released upstream work is an UPSTREAM-SYNC whose contents this PR does not answer for.

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

For every changed symbol that is `public`, `protected`, package-private, or exported (`pub` / `pub(crate)` in Rust), run Grep across the entire repository to find every callsite, implementation, override, or reference outside the diff.

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

A changed `pub`/`protected`/package-private symbol with zero recorded Grep calls in the trace is a skill violation. The model is not allowed to assert "this is only used here" without showing the search.

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

End this step with an explicit list of "places this change is visible from but the diff does not touch". This is the highest-priority input for the bug-hunting agents in Step 3.

The list groups the callsites from 2.5b by execution context: hot data paths, SQL compilation, async runtime, JNI boundary, replication, materialized views, parallel execution workers, etc. Every entry on this list must be reviewed in Step 3.

### 2.5e Test surface & helper inventory

Run this only when the PR adds or changes test code. It is the test-code counterpart to 2.5b and feeds Agents 12-14. Use real Grep/Glob searches — do not reason about helpers from memory.

- **Existing-infrastructure inventory:** search the changed test files' package and module for base test classes, shared `@Before`/`@After`, helper methods, fixtures, and assertion utilities the new tests could reuse (Grep for `extends Abstract.*Test`, `class .*TestUtils`, `assertMemoryLeak`, `assertQuery`, `assertSql`, shared `protected` helpers in the base class). This list is the baseline Agent 13 uses to flag reinvented boilerplate — a "you stamped boilerplate instead of reusing helper X" finding requires X to appear in this inventory.
- **Changed shared helpers as symbols:** if the PR changes a shared test base class, helper, or fixture, run the 2.5b callsite inventory for it too — a changed test base class can silently break every subclassing test.
- **Exercised-symbol map:** for each new or changed test, list which production symbols from 2.5a it actually exercises, so Agents 12 and 14 can check efficacy and regression value.

## Step 2.6: Test coverage map (mandatory at every level)

This step runs at EVERY review level, for EVERY PR that touches production code — including (especially) PRs that add or change no test code at all. A PR with zero test changes does not skip test scrutiny; it concentrates it here. At level 0, derive the behavioral-change rows directly from the diff (2.5a is skipped); at level 1+ use the 2.5a semantic deltas.

Build a coverage table with one row per behavioral change: every changed symbol whose delta is not "no behavioral change", broken down further by every new or changed branch, error path, and NULL/boundary case inside it. For each row, record:

- **Change:** symbol + the specific behavior/branch/path
- **Test:** the exact test class and method that exercises it — found via real Grep/Glob searches across the test tree (search for the symbol name, the SQL function/operator name, the error message text, the config key). Citing a test without a recorded search command in the trace is a skill violation, same as 2.5b. "Existing tests probably cover it" is banned.
- **Failure link:** one line stating what that test asserts and why the assertion fails if this specific change regresses. "The test calls the method" is not a failure link — the assertion must observe the changed behavior.
- **Dimensions:** which applicable dimensions the tests cover, each marked covered / uncovered / N-A: happy path, error/exception path, NULL inputs/results, boundaries (empty table, single row, max values), concurrency (if shared state is touched), resource cleanup / `assertMemoryLeak()` (if native memory is allocated). An N-A mark requires a one-line reason ("no shared state → concurrency N/A"); an unexplained N-A counts as uncovered — "not applicable" is the easiest place to hide a gap.

Rows with no test, or with a test that has no plausible failure link, are marked **UNTESTED** and carry a default severity:

- **Critical (blocking):** UNTESTED new **or changed** user-visible behavior (SQL function/operator/clause result, public API, config key, error message or failure mode), UNTESTED bug fix — **a fix PR with no regression test is automatically Critical, no agent analysis needed** — UNTESTED security/ACL enforcement change, and UNTESTED concurrency, native-memory, or resource-lifecycle changes whose failure mode is data loss, corruption, or unbounded resource growth.
- **Moderate:** UNTESTED internal branch, internal error path, or defensive guard that is reachable but produces no distinct user-visible outcome, and UNTESTED behavior that is hard to trigger in isolation — the finding must name what a test would need to do to reach it. Reported, but does not block the verdict.
- **Exempt:** only rows whose delta is verified "no behavioral change" (pure rename, dead-code removal, comment/doc/CI-only). The exemption must be stated per row, citing the verified delta. "Refactor" claimed by the PR description is not an exemption — only a verified no-behavioral-change delta is.

The coverage map is required input for Agent 5, Agents 12-14, and the Step 4 verdict. Every UNTESTED row must surface as a finding in the Step 4 report under its severity section, and the full map must be rendered in the report's "Coverage map" section — a map that exists only as summary totals is unauditable and does not count. At level 0, rows may be kept per-symbol instead of per-branch to bound cost, but new error/exception paths and NULL/boundary handling introduced by the change must still get their own rows.

## Step 3: Parallel review

Run this step with the Agent tool. Launch the agents below as **parallel tasks**, one task per agent, each a fresh-context agent so every reviewer starts unanchored. Pass each task a read-only mandate (no edits) and the inputs it needs.

Every agent receives:
1. The PR diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list)
3. The test coverage map from Step 2.6

The diff plus surface map can be large — write them to a shared file (e.g., under a temp/chain dir) and point each task at it via its `reads`/task text, rather than pasting the whole payload into every task. Agents 10 and 11 are deliberate exceptions and receive reduced context (see their entries).

### Anti-anchoring directive (applies to all agents)

- **Bugs this PR causes at callsites outside the diff are the highest-value findings.** A changed symbol whose new behavior breaks an unchanged caller is a P0 blocking finding — this PR broke it, so this PR fixes it. Tag it **out-of-diff-breakage**.
- **"Looks correct in isolation" is not a valid conclusion.** Before clearing a changed symbol, the agent must walk the callsite inventory from 2.5b and explicitly state, per callsite, whether the new behavior is still correct there.
- **The diff is the entry point for a blast-radius analysis, not for a codebase audit.** If the change surface map shows the symbol is reachable from N other files, the review reads N+1 files — looking for what this change breaks there, not auditing those files' unrelated logic.
- A single finding of the form "in `FooReader.java` the new behavior of `Bar.x()` causes Y" is worth more than five style findings inside the diff.
- **Every finding states its user-visible consequence** in one sentence: "Because of this, the user sees ___" — wrong data, a crash or hang, a security failure, a wrong/missing/misleading error, a compatibility break, or a performance cost (with its magnitude). If no such sentence can be written, say so explicitly and state why the defect is not observable today.
- **Every finding supplies the net-impact inputs** the parent needs to compute severity (3b.16a), as four short lines: **population** (which users or operations reach it — name the operation, never "any user in principle"), **delta vs base** (what they observe differently from the merge base, or "unknown — did not check"), **magnitude/frequency** (per row, per query, per restart, once ever), and **offsets** (what recovers this downstream before the user sees anything — a later validation, a retry, a caller that discards the value — or "none found, searched <where>"). Searching for the offset is part of the finding, not the parent's job: a finding that never looked for one is incomplete.
- **Do not report merge mechanics, tautologies, or overridden project decisions.** "The user" is a QuestDB database user or production operator — never a developer, CI, or the release process. Submodule pin position, merge order, branch/label state and anything true only while the PR is open are not findings. Before filing, ask whether the same finding would appear on every PR of this shape; if it would, it describes the workflow and you drop it. If the project's own tooling or the PR body already permits the thing you are flagging, that is a decision — report it only with evidence the decision is wrong.
- **Respect the Step 2.4 submodule verdicts supplied with your task.** Content inside a submodule marked **UPSTREAM-SYNC** already landed on that repository's default branch and is out of scope entirely — do not review it, do not diff it, and never attribute its behaviour to this PR. Only a submodule marked **OFF-DEFAULT** carries changes belonging to this review.
- The parent assigns severity from the consequence sentence plus these inputs, so a finding without them cannot be classified and will be dropped. Do not assign severity labels yourself.
- **Pre-existing bugs in visited code are reported separately and never block.** If a changed file, a caller from the callsite inventory, or a cross-context exposure surfaces a bug that already exists on master and that this diff does not introduce, break, or worsen, report it as an **adjacent** finding — a standalone issue draft for GitHub, not a change request against this PR. Do not propose a fix for it here. Small PRs are a deliberate goal: an unrelated fix bundled in expands the diff, the callsite inventory, and the next review round.
- **The exception:** if this PR moves code onto a path where a pre-existing bug now actually fires, it is not adjacent — it is out-of-diff-breakage, it blocks, and you make that argument explicitly.


### Agents

Launch the following agents in parallel (each as one Agent-tool task).

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

**Agent 5 — Test review & coverage:** Coverage gaps, error path tests, NULL tests, boundary conditions, regression tests, test quality, `assertMemoryLeak()` usage. Cross-reference 2.5d: every cross-context exposure should have a test that exercises the changed symbol from that context. Missing tests for cross-context callsites is a high-priority finding. Consume the Step 2.6 coverage map: re-verify every claimed test and failure link (read the assertion, don't trust the map), and hunt for behavioral changes the map missed. Then run a **mutation spot-check**: pick the 3-5 most dangerous changed lines (boundary comparisons, error handling, null checks, off-by-one candidates) and ask, per line, "which test fails if this line is wrong — inverted condition, off-by-one, dropped null check?" A dangerous line no assertion would catch is an UNTESTED finding even if a test nominally executes it. **Enforce the "SQL test assertions (builder API — strict)" checklist on every added/modified test line: any new `assertSql(...)`/`assertPlanNoLeakCheck(...)`/`getPlan(...)`/`TestUtils.assertSql(...)` is Critical; any new `.returnsOnce(...)` on a deterministic (non-RNG, non-time-varying) query is Critical; a lone `assertQuery(...)` wrapped in `assertMemoryLeak(...)` is a finding.** Test *efficacy* (whether tests actually exercise the change and could fail) and test-*code* quality are handled by Agents 12-14 — here, focus only on whether coverage exists for every new or changed path.

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

This agent's output is structured per callsite, not per failure mode. Each callsite gets a verdict: SAFE / BROKEN / NEEDS VERIFICATION. Every BROKEN entry is a P0 finding regardless of whether the file is in the diff.

This agent is not optional even when the diff is small. Small diffs to widely-used symbols have the largest blast radius.

**Agent 10 — Fresh-context adversarial:** Dispatched separately from agents 1-9 to escape checklist anchoring. This agent operates under different rules from the rest:

- It receives ONLY the PR diff and the names of the changed files. It does NOT receive the change surface map from Step 2.5, the implicit contract list, the cross-context exposure list, or any of the review checklists below.
- Its sole instruction: "find ways this code is wrong". No category list, no failure-mode taxonomy, no QuestDB-specific style guide.
- It is free to use `Read` and Grep/Glob to explore the repository however it wants.
- Findings are not pre-classified by category. Each finding states: what's wrong, why it's wrong, and the code path that demonstrates it.

The point of this agent is to surface bugs the structured agents cannot see because they are reasoning inside the same frame. A finding here that none of agents 1-9 produced is high signal — it means the structured review missed it. A finding here that overlaps with agents 1-9 is corroboration.

Run this agent in parallel with agents 1-9. It is mandatory regardless of diff size.

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
- Use `Read` and Grep/Glob freely. Read callers to understand actual input sizes and access patterns — an O(n) scan that
  runs once at startup is different from one that runs per row.
- Each finding states: what the code does now (with complexity), what the optimal approach is (with complexity), and why
  it matters (call frequency, data scale, or hot-path placement).
- Do not duplicate zero-GC or style findings — focus purely on algorithmic and computational efficiency.

Run this agent in parallel with agents 1-10. It is mandatory regardless of diff size.

**Test-code agents (Agents 12-14) — run only when the diff adds or changes test code.** When a PR changes production behavior but adds or changes NO test code, do not treat this gate as letting the PR off the hook — the Step 2.6 coverage map already classifies every uncovered behavioral change, and a fix PR with no regression test is an automatic Critical finding without any agent run. Launch them in the same parallel batch as agents 1-11. Each receives the diff, the change surface map, and the test surface inventory from 2.5e. They are the test-code counterparts to the production agents: Agent 12 mirrors Agent 1 (correctness), Agent 13 mirrors Agent 6 (code quality), and Agent 14 verifies regression-test efficacy. Tests are not second-class code — apply the same adversarial rigor here as to production.

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

Combine all agent findings into a single deduplicated **draft** report. Do NOT present this draft to the user yet — it goes straight into verification.

## Step 3b: Verify every finding against source code

The parallel review agents work from the diff plus the change surface map and frequently produce false positives — especially around memory ownership, polymorphic dispatch, Rust control-flow guarantees, and JNI lifecycle conventions. Every finding MUST be verified before it is reported.

For each finding in the draft report:

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
10. **For test-efficacy findings (Agents 12, 14)**: re-read the cited assertion in full context and confirm it truly cannot fail — a "vacuous assertion" claim is a false positive if production code actually recomputes the asserted value. For "would pass without the fix" claims, trace what the assertion observes against the reverted production hunk before reporting. **At level 3, verify empirically where practical:** in a scratch `git worktree` (never the primary working tree), run the new test on the PR branch (must pass), then revert the production hunks (`git checkout <base> -- <files>`) and run it again (must fail). A regression test that passes with the fix reverted is Critical — attach the run output as evidence. If the environment cannot build or run tests, state so explicitly and fall back to reasoning; remove the worktree afterwards. The same scratch-worktree mechanism extends at level 3 to any Critical finding's trigger: where practical, execute the claimed "user does X" and attach the observed output; a Critical whose executed trigger does not produce the claimed symptom moves to Downgraded with the run output as the disproof.
11. **For coverage-gap findings (UNTESTED rows from 2.6)**: the ONLY valid reasons to downgrade are (a) a concrete test, located by a recorded search, whose assertion demonstrably fails if the change regresses — name the test and the assertion — or (b) a verified no-behavioral-change delta. "The change is simple", "obviously correct", "hard to test", or "covered indirectly" are NOT valid downgrades; treat any of these in a verification agent's output as a red flag, not a resolution.
12. **For test-code-quality findings (Agent 13)**: confirm a flagged reflective access really has a non-reflective alternative (some QuestDB internals genuinely require reflection in tests) before reporting it. Confirm a "reinvented helper" finding by actually locating the helper with Grep and checking its signature fits the test's need.
13. **For "swallowed exception → silent wrong results / leak / corrupt state" claims**: a `catch` block is defensive coding, not evidence that anything throws. Before reporting, name **all three** of:
    (a) the **concrete exception type** and the **exact statement** that raises it — quote the throwing line, don't infer it from the presence of a `try`;
    (b) proof that this type is actually **caught by the specific catch clause cited** — `catch (SqlException | CairoException)` does NOT catch `OutOfMemoryError`, `IllegalArgumentException`, `NullPointerException`, or any other unlisted `Error`/`RuntimeException`. An `Error` that escapes the catch means the query **fails loudly**, which inverts the finding;
    (c) that the throwing statement is reachable with the arguments the callsite actually passes (constants, pre-reserved capacity, and guarded early returns frequently make it unreachable).
    If any of (a)-(c) cannot be established, the finding is **not** a silent-wrong-results bug. It may still be reportable as a **latent invariant violation / hardening** item — file it that way, state explicitly that no user-visible impact exists today, and say what future change would make it live.
    Also check for the **non-throwing** sibling: a `void` method that silently drops or frees its argument on an early return (`if (x) { free(arg); return; }`) breaks the same invariant with no exception at all, is usually far more reachable than the throw, and is not fixed by reordering statements around the call. Report that path instead of, or in addition to, the throw.
14. **Verify the conjunction, not just the links.** A multi-step finding ("A publishes early → B can throw → C swallows → D reads stale → wrong rows") is only as true as its weakest step, but per-line verification (item 1) confirms each step **in isolation** and will happily mark all of them CONFIRMED. Before filing any finding whose argument is a chain of three or more propositions, identify the single **load-bearing step** — the one that, if false, collapses the whole thing (usually "this can actually happen", not "this line says what the reporter says it says") — and verify **that** step first and hardest. Record it in the finding as "load-bearing step: <X>, verified by <evidence>". A finding whose every link is individually true can still be a false positive.
    Reading code is not verification when the load-bearing step is a runtime-shape claim — "the plan contains factory X", "the guard does not fire", "this branch is taken", or any agent sentence of the form "proved / verified / reproduced live". At level 2+, such a step requires attached execution evidence: an EXPLAIN plan, or the executed SQL/commands with their observed output and the commit SHA they ran against, produced or re-run by the verifier — an agent's repro claim without its artifact is demoted to hypothesis and cannot be classified CONFIRMED until the artifact exists. Corroboration across agents counts only when the evidence types differ (a static trace plus an executed repro); two agents reaching the same conclusion by the same search is one finding once, not two confirmations.
15. **Verify the proposed fix compiles and closes the window.** Re-read the fix against the surrounding code before including it: check that every variable it references is still in scope and non-`null` at the point it runs (statements like `a = b = c = null;` and ownership transfers routinely invalidate "just move this call later" advice), that it does not introduce a double-free or leak in the `finally`, and that it closes **every** path identified in item 13 — not just the one the reporter noticed. A fix that doesn't compile or that leaves the real path open discredits an otherwise valid finding.
16. **Determine net user impact, then classify.** Step 4 assigns severity from this determination, so it must exist before the finding is classified. A finding that reaches Step 4 without one is unverified, not Critical.

    **(a) Net user impact — answer all five, in order:**
    - **Population** — who reaches it: every user, every user of a named feature, a specific query/DDL/ingest shape, an operator-only path, or nobody today. "Any user in principle" is not a population — name the operation. If the only population you can name is the development team, CI, or the release process, **stop**: this is not a user-impact finding, and it caps at Moderate no matter how the symptom reads.
    - **Delta vs base** — what that population observes differently from the merge base. Not "the code is wrong": what different rows, bytes, errors, or latency they get. Reuse the base-behavior evidence standard (static at levels 0-1, executed at 2+).
    - **Magnitude and frequency** — how much and how often: per row, per query, per restart, once ever. Reuse the 3b.8 multiplier or bound.
    - **Offsets** — what recovers this downstream before the user sees anything. Code offsets: a later validation, a retry, a checksum, a caller that discards the value, a guard the same PR added elsewhere. **Process offsets count too**: an established team procedure, a merge or release convention, a CI gate, or a deployment step that resolves the condition before it can reach anyone. A state the team's normal workflow always corrects is offset — treat it as such rather than assuming the worst path is taken. Name the offset, or write "none found, searched <where>".
    - **Net** — exactly one of:
      - **net-negative** — the population is measurably worse off than base. **Only net-negative findings may be Critical.**
      - **net-neutral** — no observable change versus base for that population, whether by attribution, offset, or empty population. Caps at Moderate.
      - **net-positive** — the population is better off than base despite the residual gap: this PR improved the situation. Not a finding. Record it in Downgraded naming the improvement, or as an optional hardening note — never as a change request.

    Two exceptions. A **coverage-gap** row (2.6) is assessed counterfactually: population and delta are those of the behavior that would regress unnoticed, not of a present defect. An **incomplete-hardening** item (see below) is net-neutral by construction — that is what the tag means — and escalates only through its own two escalations.

    A net determination missing a population or a delta is not a determination. The finding stays unverified and cannot be filed above Moderate, however certain the mechanism looks.

    **(b) Classify each finding** as:
    - **CONFIRMED in-diff** — the bug is real and inside the diff
    - **CONFIRMED out-of-diff-breakage** — the bug is in an unchanged file because the changed symbol is used there in a way that's now broken (cite the file and the contract from 2.5c that was violated). Blocking; fixed in this PR
    - **CONFIRMED adjacent (pre-existing)** — the bug exists on master independent of this diff, found in code the review visited, and this PR does not introduce, break, or worsen it. Reported in the Adjacent findings section as a standalone issue draft; it does not block and no fix is proposed for this PR. "Not caused by this PR" is NOT grounds for FALSE POSITIVE — it is grounds for the adjacent tag. If this PR moves the bug onto a live path, reclassify it as out-of-diff-breakage and say why
    - **CONFIRMED incomplete-hardening** — this PR introduces a guard, check, or guarantee, and the finding is a residual gap in it: an input the new mechanism misses, where this PR's behavior for that input is identical to the merge base. Nothing regressed — enforcement went from absent to partial. Caps at Moderate, with two escalations: if the residual behavior is *worse* than base for some input, that input is a regression — reclassify it as in-diff or out-of-diff-breakage; if the trigger is realistic AND the PR documents or tests the guarantee as absolute, the contract mismatch itself is Critical — fixable either by closing the gap or by scoping the promise in the tests/docs, and the report must offer both options
    - **FALSE POSITIVE** — the code is actually correct (explain why)
    - **CONFIRMED with nuance** — the issue exists but is less severe than stated (explain)

**Enumerated findings verify per item.** A finding that lists N instances of one pattern ("these five classes miss override X", "these four callsites skip check Y") is N findings sharing a mechanism, not one finding with N bullets. Establish a trigger per item: items with a demonstrated trigger keep their earned severity; items without one are split out as unverified hardening (Moderate at most) with any suggested fix marked optional; items whose path is shown unreachable (a parser rule, validation, or type check — cite it) are dropped from the fix list entirely. An enumeration never inherits, for every member, the severity earned by its strongest member. On wide enumerations, bounded sampling is legitimate: verify the strongest member (worst consequence if real) and the most-doubted member; if both triggers hold, the remainder may be filed as unverified hardening (Moderate, fixes optional) without individual triggers — one confirmed Critical member already blocks the merge, so proving all N adds cost without changing the verdict. State that sampling was used and which members were verified.

**Move false positives to a separate "Downgraded" section** at the end of the report. For each, give a one-line explanation of why it was dismissed. This lets the PR author verify the reasoning and catch verification mistakes.

Launch verification agents in parallel (fresh-context agents via the Agent tool) where findings are independent. Each verification agent should read surrounding source files, not just the diff.

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
- Class members grouped by kind (static vs instance) and visibility, sorted alphabetically
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

- **Wire it** when the allocation grows with the data or query cardinality and has no structural cap: map / hash-table backing, sort / tree / record chains, hash-join key (and match-id) maps, the group-by allocator and aggregate function state, `LATEST BY` rowid lists and maps, set-operation maps, encoded and top-K `ORDER BY ... LIMIT N` sort buffers (parallel and single-threaded), secondary / markout-horizon cross-join buffers, window-join and horizon-join aggregation maps, window partition maps and RANGE-frame ring buffers, SAMPLE BY fill, parquet decode buffers. These are the runaway vectors the limit exists to catch — an unbounded site that passes `null` (or omits the tracker overload entirely) is a **Critical** coverage gap; flag it with the runaway query path that reaches it.
- **Leave it on the global counter only** when the allocation is structurally bounded, self-capped, or process / session-lived: page-frame buffers, JIT buffers, `string_agg`, fixed-size heaps (e.g. the single-column long top-K heap), ROWS-frame window buffers, table reader / writer columns, symbol tables, connection buffers, memory-mapped pages. Wiring one of these is a finding in its own right: it adds two atomic counter updates per malloc/free on both the Java and Rust paths for no protective benefit, and tracker-aware pooled classes give up cross-query backing retention (they free native backing on cursor close and re-allocate on next use), so charging a bounded or retained allocator to the tracker trades away a pool optimization for nothing.

For each new or changed allocation site, verify:

- **Same tracker for malloc and its matching free.** A site that allocates with a tracker but frees with `null` (or vice versa) desyncs the counter and trips the live `recordPerQueryMemAlloc` balance assert. Trace every free / close path — error paths and `toTop()` / `clear()` / cursor-close reuse included — and confirm the identical tracker is used on both ends.
- **Nested SQL inherits the outer tracker.** Subqueries, the mat-view refresh inner SELECT, and WAL apply inner SQL must inherit the tracker already bound on the context, not acquire their own. A new acquisition site that acquires unconditionally (instead of only when no outer tracker is present) double-counts — flag it.
- **Coverage has a test.** A newly wired allocator needs a `*MemoryTrackerTest` proving (a) a breach throws the per-query out-of-memory message, (b) an under-limit run succeeds, and (c) a `getCursor()`-to-close leak loop stays balanced. Missing tracker tests for a newly wired site is a high-priority finding; so is a factory-class routing guard that no longer pins the test to the intended plan.

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
  Do **not** accept "the surrounding file already uses `assertSql`" — there is no such helper anymore, so the diff's lines must use the new API. Flag `assertPlanNoLeakCheck(...)`, `getPlan(...)`, `assertPlanDoesNotContain(...)`, and direct `TestUtils.assertSql(...)` in new/changed test code for the same reason. The one `assertSql` that legitimately survives is the live-`ServerMain` wrapper `TestServerMain.assertSql(sql, expected)` (and the enterprise `EntGriffinServerMain.assertSql(...)`): it is a convenience for the running-server context, internally drives the builder via `returnsOnce()` (single pass, because a live server's state mutates between reads), and is NOT the banned query-result helper — do not flag it.

- **`.returnsOnce(...)` is a correctness smell — flag every newly added use.** `returnsOnce` runs the query through a SINGLE cursor pass and deliberately SKIPS the second read, the `calculateSize()` pass, the variable-column check, and the factory-property assertions (`supportsRandomAccess`, `expectSize`) that `.returns(...)` performs. Those skipped checks catch real bugs: cursors that don't reset correctly on `toTop()`, `size()` that disagrees between passes, random-access records that return wrong values via `recordAt()`. `returnsOnce` is **only** justified when the query's output is genuinely unstable across two reads with no underlying data change — e.g. an unseeded `rnd_*` in the projection, `now()`/`sysdate()`/`systimestamp()`-style time-varying output, or inherently non-deterministic row order. For a `.returnsOnce(...)` on a deterministic query this is a Critical finding: demand `.returns(...)`. Require the author to state *why* the query is unstable; "it was simpler" is not a reason — the shortcut leaves real bugs untested.

- **Anti-pattern: a lone `assertQuery(...)` wrapped in `assertMemoryLeak(() -> { ... })`.** The builder runs its OWN memory-leak check by default (it wraps internally unless `.noLeakCheck()` is set). When an `assertMemoryLeak(...)` lambda's only meaningful statement is a single `assertQuery(...)` chain, the outer wrapper is redundant and almost always forces a `.noLeakCheck()` on the builder — which disables the builder's leak check and replaces it with a hand-rolled one, defeating the point. Flag it: drop the `assertMemoryLeak` wrapper and the `.noLeakCheck()`, letting the builder leak-check itself. The wrapper is only legitimate when the lambda genuinely holds multiple statements (DDL + inserts + several assertions) that must share one leak-check scope; a single builder call does not.

### Enterprise permissions & ACL (if PR introduces new SQL statements or ALTER operations)
- New ALTER TABLE operations almost always require a new enterprise permission. If the PR adds a new ALTER statement (or any new SQL statement that modifies state), flag it if there is no corresponding `SecurityContext.authorize*()` call in the execution path.
- New features in OSS should have an enterprise counterpart that wires up ACL. Check whether the PR introduces `authorize*` methods in `SecurityContext` and whether all enterprise `SecurityContext` implementations (`EntSecurityContextBase`, `AdminSecurityContext`, `AbstractReplicaSecurityContext`, and test mocks) are updated.
- New permissions must be registered in `Permission.java` (constant, name maps, and included in `TABLE_PERMISSIONS`/`ALL_PERMISSIONS` as appropriate).
- The `PermissionParser` must be able to parse GRANT/REVOKE for the new permission name — especially if the name contains SQL keywords like `ON`, `TO`, or `FROM` that could conflict with parser grammar.
- Replica security contexts must deny new write operations (`deniedOnReplica()`).

### Test review
- **Coverage gaps are blocking, not advisory:** consume the Step 2.6 coverage map. Every UNTESTED row carries its default severity from 2.6 (Critical for new user-visible behavior, bug fixes, error paths, concurrency, resource lifecycles). Do not downgrade an UNTESTED Critical to Moderate because the change "looks simple" — simplicity is not coverage. For every new or changed code path not in the map, flag it explicitly as "missing test for X" and add it to the map.
- **Execution-mode dimensions (QuestDB-specific):** where the changed code is sensitive to them, demand coverage across the modes that alter its behavior: WAL vs non-WAL tables, O3 (out-of-order) writes vs append-only, JIT-compiled vs interpreted filters, parallel vs single-threaded execution (parallel GROUP BY/filter workers), partitioned vs non-partitioned tables. A SQL-engine change tested in only one mode is a coverage gap in the others — name the untested modes.
- **Fuzz coverage:** for parser, encoder/decoder, ingestion-protocol, or O3/WAL-merge changes, search the test tree for existing fuzz tests (`rg -l Fuzz`) covering the changed surface. If one exists and was neither extended nor mentioned as run against the change, flag it.
- **Cross-context coverage:** For every entry in the cross-context exposure list (2.5d), verify a test exercises the changed symbol from that context. Missing cross-context tests are high-priority findings.
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

Present ONLY verified findings (false positives are excluded).

**Proportionality.** The report must be actionable in one sitting. Rank Critical findings by user impact, worst first. If a normal-sized PR yields more than ~7 Criticals, treat that as a signal you are inflating severity rather than evidence the PR is exceptionally bad — re-run the symptom test on each before publishing. A report where everything is Critical carries no information about what to fix first.

**Every finding — at every severity — opens with two one-line summaries, before any prose:**

- **Problem:** what is wrong. ≤ 12 words. No file path, no mechanism, no fix.
- **Net impact:** what the user loses, from the 3b.16(a) determination — population and magnitude. ≤ 12 words. If none, say so: "None — <reason>".

These are triage lines: a reader must be able to sort and prioritise the entire report by reading only them. They are not the symptom sentence — that carries the trigger and the evidence; these carry the summary. Write them last, from the finished finding, never first from the hunch. If the Net impact line reads "none" on a Critical, the finding is mis-filed.

```
Problem: Symbol column read twice per scanned row.
Net impact: ~2x column IO on every filtered scan.

Problem: WAL segment leaks a file descriptor on the error path.
Net impact: Ingestion stalls after ~1k failed commits.

Problem: New validation guard misses NaN input.
Net impact: None — base rejects NaN earlier; unchanged.
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
- **an unverified user-visible behavior change** — a coverage-gap row that the Step 2.6 gate marks Critical. This one completion is **counterfactual**: the sentence is *"if this regressed, the user would see ___"*, completed with one of the consequences above. The defect is the missing assertion that would catch the regression, so no present-tense symptom and no trigger are required — the 2.6 gate is the evidence. Coverage-gap Criticals are real Criticals and block exactly like the rest.

**Every other completion needs a trigger.** A symptom sentence is valid only if it names the concrete thing that produces it — the query shape, ingest pattern, API call, config value, or operation sequence a user could actually run — in the form *"user does X → sees Y"*. "Could theoretically return wrong results" is a guess, not a symptom; Step 3b.13 and 3b.14 already specify how to establish the trigger and identify the load-bearing step. Name the trigger or the finding is not Critical.

If you cannot complete the sentence with one of the above, the finding is **not** Critical, however real the defect is. File it Moderate and state why it is not user-visible today.

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

**Moderate.** Real findings this PR does not make user-visible. Two kinds share the bucket, with different required sentences — writing the wrong sentence type is a failed audit:

- **Latent** — no user can observe the defect today: invariant violations whose trigger is currently unreachable (Step 3b.13), bounded performance or allocation costs off the data path, sub-optimal-but-bounded algorithms, coverage gaps on internal branches, weak or brittle test assertions, missing docs on non-obvious behavior, structural problems in code that will be maintained. The required sentence states why the user cannot observe it today — naming the specific guard, catch clause, or early return that makes it unreachable, or the bound that caps the cost.
- **Not-attributed (incomplete-hardening, 3b.16)** — a user *can* observe the behavior, but this PR did not cause it: the merge base produces the same or worse outcome for the same trigger, and the PR narrowed the exposure without closing it. The required sentence is the base-delta citation: the merge-base commit and the observed base outcome for the same trigger.

**Minor.** Cosmetics: member ordering, naming, formatting, comment wording, import order.

Do not inflate and do not deflate. Filing a real user-visible defect as Moderate is a review failure; so is filing a bounded compile-time micro-cost as Critical. Where two readings are defensible, pick the one you can evidence.

### Critical
Blocking issues introduced or exposed by this PR, ordered worst user impact first. Each must include:
- The two summary lines (**Problem** / **Net impact**) before anything else
- The **net determination** from 3b.16(a): population, delta vs base, magnitude/frequency, offsets, and a net of **net-negative** — a Critical that is net-neutral or net-positive is mis-filed by definition
- Exact file path and line numbers
- The **symptom sentence** with its trigger: "user does X → sees Y". For a coverage-gap Critical, the counterfactual form: "if this regressed, the user would see Y" — no trigger needed
- Whether the finding is **in-diff** or **out-of-diff-breakage** (an unchanged callsite this PR breaks) — both are this PR's responsibility
- Code path trace showing why the bug is real and reachable
- **Base behavior at the merge base for the same trigger** (required): what the same input produces on the merge base. If base shows the same or worse user-visible outcome, the finding is not "introduced or exposed by this PR" — reclassify it as incomplete-hardening (3b.16, capped at Moderate) or adjacent before publishing. Two boundary rules: **net-new surface** — if the trigger cannot be expressed on base (a new function, syntax, endpoint, or config key that base rejects as unrecognized), the comparison is N/A — write "N/A — new surface" and the finding stands or falls on the symptom test alone; **rejection is not "worse"** — a base that rejects or errors on the input is the absence of the defect, not a worse outcome: "worse" compares defect outcomes only, so "base errored, PR returns wrong data" is a new defect, never grounds to reclassify. At levels 0-1 establish base behavior statically (`git show <base>:<file>`, `git log -S<symbol>`) and label the citation *static*; at level 2+ prefer executed evidence per 3b.14
- For out-of-diff-breakage: the callsite that triggers it, plus the violated contract — cite it from 2.5c at level 2+, or state it inline at levels 0-1 where 2.5c is not built
- For performance findings: the magnitude statement (the multiplier and what it multiplies)
- Suggested fix, written to be applied in THIS PR

Pre-existing bugs this PR does not break belong under **Adjacent findings**, whatever their standalone severity.

### Moderate
Non-blocking issues worth fixing. Each carries its required sentence per the severity rubric: a latent item states why the user cannot observe it today; an incomplete-hardening item is tagged **[incomplete-hardening]** and carries the base-delta citation (merge-base commit + the same trigger's observed base outcome) plus its residual-gap fix marked optional — or the alternative of scoping the documented promise, per 3b.16. These do **not** block approval — the author may address them in this PR or not.

### Minor
Cosmetics. Non-blocking, optional.

### Downgraded (false positives)
Findings from the initial review that were dismissed after source code verification. For each, state:
- The original claim (one line)
- Why it was dismissed (one line, citing the specific code that disproves it)

### Adjacent findings (not blocking — file as GitHub issues)
Bugs that already exist on master, found in code this review visited (changed files, callers from the callsite inventory, cross-context exposures), which this PR does not introduce, break, or worsen.

These are **out of scope for this PR** and never affect the verdict. Report each as a ready-to-file issue draft so it can be moved to GitHub without re-investigation:
- **Problem:** ≤ 12 words — doubles as the issue title
- **Net impact:** ≤ 12 words — population and magnitude, or "None — <reason>"
- **Location:** file path + line numbers
- **Symptom:** what a user would observe — or "latent: no user-visible impact today", naming the guard that prevents it
- **Reachability:** the path that reaches it, or why nothing does yet
- **Suggested fix:** one or two lines
- **Severity if filed standalone:** Critical / Moderate / Minor per the rubric above

Do not propose these as changes to this PR, and do not let their count or severity influence the verdict. If one is severe enough that shipping this PR without it is genuinely unsafe — because this PR moves code onto a path where the pre-existing bug now fires — then it is not adjacent: it is out-of-diff-breakage, it belongs under Critical, and you state that argument explicitly.

### Coverage map
Render the full Step 2.6 coverage map: one row per behavioral change with its test, failure link, dimension marks (including justified N-As), and TESTED / UNTESTED / EXEMPT verdict. EXEMPT rows must show the verified no-behavioral-change delta. This section is mandatory whenever the PR touches production code — it is the audit trail for the test gate below; a review without it is incomplete.

### Summary
- **Verdict**, exactly one of:
  - **approve** — no open Critical findings and the test gate passes. Moderate and Minor items may remain open; list them and approve anyway. This is the expected outcome for competent work, and withholding it when both gates pass is itself a review failure.
  - **approve with comments** — both gates pass; you want specific Moderate items addressed but will not block on them. Name which ones.
  - **request changes** — at least one Critical is open, or the test gate fails.
  - **needs discussion** — the change requires a product, architecture, or compatibility decision a reviewer cannot make alone.
- **Correctness gate (hard rule):** the verdict cannot be "approve" while any Critical finding remains open — that is, any confirmed, reachable defect with a stated user-visible symptom, either in-diff or broken by this PR at an out-of-diff callsite, plus any coverage-gap row the 2.6 gate marks Critical. Adjacent (pre-existing) findings never block, and neither do Moderate or Minor items.

  Before finalizing, run the severity audit in **both** directions and record that you ran both:
  - **downward:** re-audit each Critical and confirm its symptom sentence names a real trigger. Move any that cannot to Moderate.
  - **upward:** re-audit each Moderate and ask whether a symptom sentence *can* now be completed for it. Move any that can to Critical — with one attribution exception: an incomplete-hardening item's symptom sentence is completable by design, because the same sentence was already completable on the merge base; a completable sentence alone does not promote it, and promotion happens only through 3b.16's two escalations (an input where behavior is worse than base, or an absolute documented guarantee with a realistic trigger). A Moderate whose inertness justification is missing, hand-waved, or of the form "probably fine", "unlikely in practice", or "only on an edge case" has **not** been verified inert — it is a Critical until someone shows otherwise. The one-sentence justification is the whole safeguard; an absent or vague one is a failed audit, not a pass. Three justification forms are evidence-backed downgrades, not hand-waves — each stands only with its citation: (a) **precondition-gated** — the trigger requires a flag, feature, or combination that documentation or code marks dev-only, test-only, experimental, or unsupported for production — cite the marking — together with the explicit assertion that no supported production deployment profile enables it; a supported production feature that is merely off by default (replication, TLS, OIDC, cold storage) is NOT this form — for every deployment running the feature the precondition is always on, so the config belongs in the trigger ("user with <feature> enabled does X → sees Y") and severity is judged on that population's consequence; (b) **delta-vs-base zero** — the merge base produces the same or worse outcome for the same trigger (incomplete-hardening, 3b.16); (c) **structurally unreachable** — a parser, validation, or type rule forbids the input the escalation needs, cited by file:line or by an executed probe. The difference from a banned hand-wave is the citation: "unlikely" asserts a probability from nowhere; these name the marking that keeps the flag out of supported production, the base behavior that shows nothing regressed, or the rule that blocks the input.

  Only after both passes: if the Critical list is empty, the verdict is approve — say so plainly rather than reaching for a reason to block. If it is not empty, the verdict is request changes, however small the remaining item looks. Skipping the upward pass to reach an approve is the same review failure as inflating everything to Critical, in the opposite direction.
- **Test gate (hard rule):** the verdict cannot be "approve" while (a) any UNTESTED user-visible-behavior row remains Critical in the Step 2.6 coverage map, or (b) the PR claims a fix but ships no regression test with a verified failure link. Untested internal branches are Moderate and do not block. If the PR changes production code and adds zero tests, the verdict is "request changes" unless every behavioral delta is verified "no behavioral change" — state that justification explicitly here.
- State the coverage-map totals: behavioral changes total, tested, UNTESTED (e.g., "coverage map: 12 behavioral changes, 9 tested, 3 UNTESTED") — the totals must match the rendered Coverage map section row-for-row
- Highlight any regressions or tradeoffs
- Never make the verdict conditional on splitting the PR — "approve once X is moved to another PR" is not a valid verdict. Conversely, never require that adjacent (pre-existing) findings be fixed here; they leave as issue drafts and the PR is judged without them.
- State how many draft findings were verified vs dropped as false positives (e.g., "8 findings verified, 4 false positives removed")
- State the Step 2.4 submodule provenance verdicts, one line per changed pointer (e.g., "questdb: OFF-DEFAULT — in scope; java-questdb-client: UPSTREAM-SYNC — out of scope"). If a pointer moved and no verdict is stated, the scope of the review is unknown and the report is incomplete.
- State the split: in-diff / out-of-diff-breakage / incomplete-hardening / adjacent (e.g., "5 in-diff, 2 out-of-diff-breakage, 1 incomplete-hardening, 3 adjacent"). At levels 0-1 the callsite inventory (2.5b) and the exposure list (2.5d) are not built, so out-of-diff-breakage covers only callers the inline review actually opened — report a zero there as "callsite analysis not run at this level", never as a clean bill of health. At level 2+, if the diff is non-trivial and out-of-diff-breakage is zero, either the change is genuinely well-contained — say so — or the cross-context pass underran: re-check the 2.5d exposure list, and Agent 9's output at level 3, before finalizing.
- State the severity distribution (e.g., "3 Critical, 6 Moderate, 2 Minor"). Read it in both directions: nearly all Critical means the symptom test was not applied and should be re-run, not that the PR is unusually bad; nearly all Moderate on a PR with real behavioral change means the upward pass was skipped — go back and check that each Moderate's inertness justification actually holds.
