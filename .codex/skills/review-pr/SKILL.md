---
name: review-pr
description: Review GitHub pull requests against QuestDB coding standards using gh, local source inspection, review levels 0-3, and verified Critical, Moderate, Minor, and Downgraded findings. Use when Codex is asked to review a PR number or URL, run review-pr, inspect a GitHub PR, validate PR metadata, analyze diffs and callsites, or perform a QuestDB blocking code review.
---

# QuestDB PR Review

Review the pull request identifier supplied by the user. Accept a PR number or URL plus optional level tokens such as `--level=2`, `-l2`, or `2`.

Use Codex tools for shell commands (`gh`, `rg`, `rg --files`, `git`, `sed`, and `nl`), local file reads for source inspection, and subagents when the environment exposes multi-agent tools. If subagents are required by the selected level but unavailable, run the same passes inline and state that limitation in the summary.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. QuestDB is mission-critical software deployed on spacecraft — bugs can cause data loss or system failures that cannot be patched after deployment. There is zero tolerance for correctness issues, resource leaks, or undefined behavior. Be critical, thorough, and opinionated. Your job is to catch problems before they ship, not to be nice.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed. Treat the diff as the entry point, not the scope.
- **Flag every issue you find**, no matter how small. Do not soften language or hedge. Say "this is wrong" not "this might be an issue".
- **"Out of scope" is not a valid disposition — fix it while we're in there.** CI is slow and every PR round-trip is expensive; the team deliberately consolidates fixes. Never advise splitting work into a follow-up PR, filing a ticket "for later", or deferring a confirmed bug because it pre-dates the diff. A pre-existing bug discovered in code the review visits — changed files, callers from the callsite inventory, cross-context exposures — is a reportable finding tagged **pre-existing**, with a suggested fix intended for THIS PR. The only acceptable reason to leave a confirmed bug unfixed is that fixing it would genuinely destabilize the change — then say so explicitly, naming the risk. Conversely, never penalize a PR for bundling multiple fixes; review each change on its merits.
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, ask: what inputs break this? What happens under concurrent access? What if this runs on a 10-billion-row table? What if the column is NULL? What if the partition is empty?
- **Demand optimal algorithms.** QuestDB is a performance-first database. "Works correctly" is necessary but not
  sufficient — the implementation must use the best known algorithm and data structure for the job. If a hash lookup
  gives O(1) but the code does a linear scan, that is a finding. If two passes over the data can be collapsed into one,
  that is a finding. If a value is recomputed on every call but could be cached, that is a finding. Do not accept "good
  enough" — ask "is there a faster way?" for every loop, every traversal, every data structure choice.
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing documentation for non-obvious behavior.
- **Untested code is broken code.** Treat every new or changed production behavior that ships without a test proving it as a defect, not a nice-to-have. The burden of proof is on the PR: a behavioral change ships with a test whose assertion would fail if the change regressed, or it does not ship. "The change is simple" and "existing tests probably cover it" are not evidence — a named test located by a recorded search, with a stated failure link, is.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it
  says "improve performance", look for benchmarks or reason about the algorithmic change — does it actually improve
  things, or could it regress in other cases? Even if the PR doesn't claim to be about performance, evaluate whether the
  chosen algorithms and data structures are optimal — sub-optimal code that "works" is still a finding. If it says "
  simplify", verify the new code is actually simpler and doesn't drop behavior. Treat the PR description as an
  unverified hypothesis, not a statement of fact.
- **Read the full context of changed files** when the diff alone is ambiguous. Use local file reads and repository search (`rg` / `rg --files`) to inspect the surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem
  requires physically impossible conditions (billions of columns, corrupted JNI inputs, values that no caller can
  produce), it is not a real finding — drop it. Focus on bugs that real workloads can trigger, not theoretical edge
  cases that exist only in the type system.
- **QuestDB runs with Java assertions enabled (`-ea`).** Assertions are a valid guard for invariants that indicate
  corruption or internal bugs. Do NOT flag `assert` as insufficient — it is the preferred mechanism for conditions
  that should never occur in a non-corrupt database. Only flag an `assert` if the condition can plausibly be triggered
  by normal (non-corrupt) user operations.

## Review level

Parse the user's request for a level token: `--level=N`, `-lN`, or a bare single digit `0`-`3`. **If no level is given, default to 0.** Strip the level token before feeding the remainder (PR number or URL) to `gh` commands.

The level controls how much of the review below actually runs. Lower levels keep the same review *spirit* — adversarial, blocking, no praise — but cut the breadth of the analysis. Higher levels have significantly higher token cost; reserve level 3 for high-stakes PRs (replication, JNI boundary changes, on-disk format, public API, security/ACL).

| Level           | What runs                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **0 (default)** | Steps 1, 2, 2.6, 4. Skip Step 2.5. Skip Step 3 — no agent spawn; review the diff inline in the main loop, using local file reads and `rg` on demand to resolve ambiguities. Skip Step 3b — verify each finding inline as you write it. Single-pass review covering correctness, NULL handling, **algorithmic optimality**, tests, and QuestDB standards on the diff itself. The performance checklist (including algorithm optimality) is mandatory at every level. When the diff touches test code, also apply the test-efficacy and test-code-quality anti-pattern checks inline (vacuous assertions, reflection overuse, reinvented helpers, javadoc bloat). Step 2.6 (test coverage map) is mandatory here as at every level — build it inline before writing findings; derive the behavioral-change rows directly from the diff since 2.5a is skipped. |
| **1**           | Adds Step 2.5a (semantic delta only — skip 2.5b/2.5c/2.5d) plus Step 2.5e when test code is present. In Step 3, launch Agent 1 (correctness), **Agent 3 (performance)**, Agent 5 (tests), Agent 6 (code quality), and — when the diff touches test code — Agent 12 (test efficacy) and Agent 13 (test-code quality) in parallel. Skip all other agents. Skip Step 3b — verify findings inline as you draft the report.                                                                                                                                                            |
| **2**           | Full Step 2.5 (including 2.5e when test code is present), but in 2.5b restrict the callsite inventory to `public`/`protected` symbols (skip package-private and `pub(crate)`). In Step 3, launch Agents 1-8 (Agent 8 only if `.rs` files are present), plus **Agent 11 (adversarial performance)**, plus Agents 12 and 13 when the diff touches test code. Skip Agent 9 (cross-context), Agent 10 (adversarial fresh-context), and Agent 14 (regression-test efficacy verification). Step 3b uses a single batched verification agent for all findings instead of one per finding.                  |
| **3**           | Every step below as written, all 14 agents, per-finding verification. The full mission-critical pass.                                                                                                                                                                                                                                                                                                                                          |

State the chosen level in one line at the start of the review so the user knows what they're getting (e.g., "Reviewing PR #1234 at level 2"). If the level was defaulted, mention that level 3 exists for full review.

## Step 1: Gather PR context

Capture the PR identifier in `$PR` (the part of the user's request left after stripping the level token), then fetch metadata, diff, and review comments in a single bash call so `$PR` is in scope for all three `gh` invocations:

```bash
PR='<PR number or URL from the user request, with any --level=N / -lN / bare-digit level token removed>'
gh pr view "$PR" --json number,title,body,labels,state
gh pr diff "$PR"
gh pr view "$PR" --comments
```

## Step 2: PR title and description

Check against CLAUDE.md conventions:
- Title follows Conventional Commits: `type(scope): description`
- Description repeats the verb (e.g., `fix(sql): fix ...` not `fix(sql): DECIMAL ...`)
- Description speaks to end-user impact, not implementation internals
- If fixing an issue, `Fixes #NNN` is at the top of the body
- Tone is level-headed and analytical, no superlatives or bold emphasis on numbers
- Labels match the PR scope (SQL, Performance, Core, etc.)
- Bundled adjacent fixes ("while we're in there") are acceptable and expected — do not flag a PR for containing multiple related fixes. Flag only a description that fails to LIST what was bundled; each bundled fix should get one line in the body

## Step 2.5: Map the change surface

Before launching review agents, produce a structured change surface map. This step is mandatory and must use repository search (`rg` / `rg --files`) — do not reason about callsites from memory. The output of this step is required input for every agent in Step 3.

### 2.5a Semantic delta per changed symbol

For every modified or added function, method, trait, struct field, SQL operator/function, or public constant, write:

- **Symbol:** fully-qualified name
- **Before:** signature, return type, error/exception behavior, panic behavior, mutation (`&self` vs `&mut self`, `final` vs not), ordering/idempotency guarantees, allocation behavior, thread-safety
- **After:** same fields
- **Delta:** one line stating what semantically changed

"Refactored", "cleaned up", "improved", "simplified" are not acceptable deltas. State the actual behavioral difference. If nothing semantically changed, write "no behavioral change" — but only after checking, not as a default.

### 2.5b Callsite inventory

For every changed symbol that is `public`, `protected`, package-private, or exported (`pub` / `pub(crate)` in Rust), run repository search across the entire repository to find every callsite, implementation, override, or reference outside the diff.

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

A changed `pub`/`protected`/package-private symbol with zero recorded search commands in the trace is a skill violation. The model is not allowed to assert "this is only used here" without showing the search.

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

Run this only when the PR adds or changes test code. It is the test-code counterpart to 2.5b and feeds Agents 12-14. Use real repository searches (`rg` / `rg --files`) — do not reason about helpers from memory.

- **Existing-infrastructure inventory:** search the changed test files' package and module for base test classes, shared `@Before`/`@After`, helper methods, fixtures, and assertion utilities the new tests could reuse (`rg` for `extends Abstract.*Test`, `class .*TestUtils`, `assertMemoryLeak`, `assertQuery`, `assertSql`, shared `protected` helpers in the base class). This list is the baseline Agent 13 uses to flag reinvented boilerplate — a "you stamped boilerplate instead of reusing helper X" finding requires X to appear in this inventory.
- **Changed shared helpers as symbols:** if the PR changes a shared test base class, helper, or fixture, run the 2.5b callsite inventory for it too — a changed test base class can silently break every subclassing test.
- **Exercised-symbol map:** for each new or changed test, list which production symbols from 2.5a it actually exercises, so Agents 12 and 14 can check efficacy and regression value.

## Step 2.6: Test coverage map (mandatory at every level)

This step runs at EVERY review level, for EVERY PR that touches production code — including (especially) PRs that add or change no test code at all. A PR with zero test changes does not skip test scrutiny; it concentrates it here. At level 0, derive the behavioral-change rows directly from the diff (2.5a is skipped); at level 1+ use the 2.5a semantic deltas.

Build a coverage table with one row per behavioral change: every changed symbol whose delta is not "no behavioral change", broken down further by every new or changed branch, error path, and NULL/boundary case inside it. For each row, record:

- **Change:** symbol + the specific behavior/branch/path
- **Test:** the exact test class and method that exercises it — found via real `rg` searches across the test tree (search for the symbol name, the SQL function/operator name, the error message text, the config key). Citing a test without a recorded search command in the trace is a skill violation, same as 2.5b. "Existing tests probably cover it" is banned.
- **Failure link:** one line stating what that test asserts and why the assertion fails if this specific change regresses. "The test calls the method" is not a failure link — the assertion must observe the changed behavior.
- **Dimensions:** which applicable dimensions the tests cover, each marked covered / uncovered / N-A: happy path, error/exception path, NULL inputs/results, boundaries (empty table, single row, max values), concurrency (if shared state is touched), resource cleanup / `assertMemoryLeak()` (if native memory is allocated). An N-A mark requires a one-line reason ("no shared state → concurrency N/A"); an unexplained N-A counts as uncovered — "not applicable" is the easiest place to hide a gap.

Rows with no test, or with a test that has no plausible failure link, are marked **UNTESTED** and carry a default severity:

- **Critical (blocking):** UNTESTED new **or changed** user-visible behavior (SQL function/operator/clause result, API, config key), UNTESTED bug fix — **a fix PR with no regression test is automatically Critical, no agent analysis needed** — UNTESTED error/exception path introduced by the change, UNTESTED concurrency-sensitive change, UNTESTED native-memory or resource-lifecycle change.
- **Moderate:** UNTESTED internal branch that is reachable but hard to trigger in isolation — the finding must name what a test would need to do to reach it.
- **Exempt:** only rows whose delta is verified "no behavioral change" (pure rename, dead-code removal, comment/doc/CI-only). The exemption must be stated per row, citing the verified delta. "Refactor" claimed by the PR description is not an exemption — only a verified no-behavioral-change delta is.

The coverage map is required input for Agent 5, Agents 12-14, and the Step 4 verdict. Every UNTESTED row must surface as a finding in the Step 4 report under its severity section, and the full map must be rendered in the report's "Coverage map" section — a map that exists only as summary totals is unauditable and does not count. At level 0, rows may be kept per-symbol instead of per-branch to bound cost, but new error/exception paths and NULL/boundary handling introduced by the change must still get their own rows.

## Step 3: Parallel review

Every agent receives:
1. The PR diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list) plus the test coverage map from Step 2.6
3. The specific role instructions for that agent (Agent 1..11 below)
4. An explicit "review only — do not edit any files" constraint
5. The scope policy: pre-existing bugs found in visited code (changed files, callers, cross-context exposures) are in-scope findings — tag them **pre-existing**; never recommend deferring a fix to a follow-up PR, filing a ticket instead of fixing, or splitting this PR.

The fresh-context adversarial agents (Agent 10, Agent 11) are the exception — they must NOT receive the change surface map, the test coverage map, or checklists; give them only the diff and changed-file names, per their instructions below. The parent session owns synthesis, deduplication, and the final report — children only return findings.

### Anti-anchoring directive (applies to all agents)

- **Bugs at callsites outside the diff outrank bugs inside the diff.** A confirmed bug in a file the PR did not touch but that calls a changed symbol is a P0 finding.
- **"Looks correct in isolation" is not a valid conclusion.** Before clearing a changed symbol, the agent must walk the callsite inventory from 2.5b and explicitly state, per callsite, whether the new behavior is still correct there.
- **The diff is the entry point, not the scope.** If the change surface map shows the symbol is reachable from N other files, the review covers N+1 files.
- A single finding of the form "in `FooReader.java` the new behavior of `Bar.x()` causes Y" is worth more than five findings inside the diff.
- **Pre-existing bugs in visited code are in scope.** If reviewing a changed file, a caller from the callsite inventory, or a cross-context exposure surfaces a bug that already exists on master independent of this diff, report it tagged **pre-existing** — do not drop it as "not caused by this PR" and do not defer it to a separate PR; fixing it here is cheaper than a second trip through CI. This is a license to report what the review path visits, not to audit unrelated code — stay on the callsite/exposure trail.

### Agents

Launch the following agents in parallel.

**Agent 1 — Correctness & bugs:** NULL handling, edge cases, logic errors, off-by-one, operator precedence, error paths. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite. When the diff touches the QWP ingress / role-gating path, an in-place switch or failover, a `java-questdb-client` submodule bump, or store-and-forward test suites, also verify the "Store-and-forward & pool startup invariants" checklist — a change that lets a running SF drainer surface transport errors to the producer, imposes a reconnect time budget on it, or hard-fails it on a transient outage is a Critical (data-loss) finding.

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
8. **Compile-time vs data-path:** GC allocations during SQL compilation are acceptable. But algorithmic inefficiency
   during compilation is NOT acceptable — slow compilation means slow query latency. A multi-pass parse or O(n^2) plan
   enumeration in the compiler is still a finding.

For changed symbols now reachable from new contexts (per 2.5d), check whether any of those new contexts is a hot path
that amplifies an otherwise-acceptable cost.

**Agent 4 — Resource management:** Leaks on all code paths (especially errors), try-with-resources, native memory, pool management. Walk every callsite from 2.5b that constructs, owns, or transfers ownership of changed types and verify cleanup on all paths. When the diff adds or changes a native allocation site, also apply the "Per-query memory tracker integration" checklist below: confirm large, unbounded, data-scaled allocators are wired into the per-query `MemoryTracker` and bounded / process-lived ones are deliberately left out, that malloc and its matching free charge the same tracker, and that newly wired sites have breach / success / leak-loop tests.

**Agent 5 — Test review & coverage:** Coverage gaps, error path tests, NULL tests, boundary conditions, regression tests, test quality, `assertMemoryLeak()` usage. Cross-reference 2.5d: every cross-context exposure should have a test that exercises the changed symbol from that context. Missing tests for cross-context callsites is a high-priority finding. Consume the Step 2.6 coverage map: re-verify every claimed test and failure link (read the assertion, don't trust the map), and hunt for behavioral changes the map missed. Then run a **mutation spot-check**: pick the 3-5 most dangerous changed lines (boundary comparisons, error handling, null checks, off-by-one candidates) and ask, per line, "which test fails if this line is wrong — inverted condition, off-by-one, dropped null check?" A dangerous line no assertion would catch is an UNTESTED finding even if a test nominally executes it. **Enforce the "SQL test assertions (builder API — strict)" checklist on every added/modified test line: any new `assertSql(...)`/`assertPlanNoLeakCheck(...)`/`getPlan(...)`/`TestUtils.assertSql(...)` is Critical; any new `.returnsOnce(...)` on a deterministic (non-RNG, non-time-varying) query is Critical; a lone `assertQuery(...)` wrapped in `assertMemoryLeak(...)` is a finding.** Test *efficacy* (whether tests actually exercise the change and could fail) and test-*code* quality are handled by Agents 12-14 — here, focus only on whether coverage exists for every new or changed path.

**Agent 6 — Code quality & standards:** Code smell, member ordering, naming conventions, modern Java features, dead code, third-party dependencies.

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

- It receives ONLY the PR diff and the names of the changed files. It does NOT receive the change surface map from Step 2.5, the test coverage map from Step 2.6, the implicit contract list, the cross-context exposure list, or any of the review checklists below.
- Its sole instruction: "find ways this code is wrong". No category list, no failure-mode taxonomy, no QuestDB-specific style guide.
- It is free to use local file reads and repository search to explore the repository however it wants.
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
- Use local file reads and repository search freely. Read callers to understand actual input sizes and access patterns — an O(n) scan that
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
- **No code reuse / boilerplate stamping:** before accepting repeated setup or assertion blocks, run repository searches for existing helpers, base test classes, and fixtures (e.g., `extends Abstract.*Test`, `TestUtils`, `*TestUtils`, shared `assert*`, shared `@Before`) using the 2.5e inventory. If a helper already exists that the new test reimplements inline, flag it and name the helper. Duplicated blocks across new test methods that should be a single helper or a parameterized test are findings.
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
   identification of the hot/cold path). Do NOT downgrade a finding just because the current data size is small
   or the saving seems negligible — algorithmic inefficiency compounds at scale and under concurrent load.
   Sub-optimal algorithm choice (O(n) vs O(1), multi-pass vs single-pass, redundant traversals) is always
   a valid finding regardless of measured cost. The only valid reason to downgrade is if the analysis is
   technically wrong (e.g., the "linear scan" is actually bounded by a small constant like column count).
9. **For cross-context findings (Agent 9)**: re-read the callsite in full, including its callers up two levels, and confirm the broken behavior is reachable from production code paths. Cross-context findings are high-value but also the easiest to overstate — verify carefully.
10. **For test-efficacy findings (Agents 12, 14)**: re-read the cited assertion in full context and confirm it truly cannot fail — a "vacuous assertion" claim is a false positive if production code actually recomputes the asserted value. For "would pass without the fix" claims, trace what the assertion observes against the reverted production hunk before reporting. **At level 3, verify empirically where practical:** in a scratch `git worktree` (never the primary working tree), run the new test on the PR branch (must pass), then revert the production hunks (`git checkout <base> -- <files>`) and run it again (must fail). A regression test that passes with the fix reverted is Critical — attach the run output as evidence. If the environment cannot build or run tests, state so explicitly and fall back to reasoning; remove the worktree afterwards.
11. **For coverage-gap findings (UNTESTED rows from 2.6)**: the ONLY valid reasons to downgrade are (a) a concrete test, located by a recorded search, whose assertion demonstrably fails if the change regresses — name the test and the assertion — or (b) a verified no-behavioral-change delta. "The change is simple", "obviously correct", "hard to test", or "covered indirectly" are NOT valid downgrades; treat any of these in a verification agent's output as a red flag, not a resolution.
12. **For test-code-quality findings (Agent 13)**: confirm a flagged reflective access really has a non-reflective alternative (some QuestDB internals genuinely require reflection in tests) before reporting it. Confirm a "reinvented helper" finding by actually locating the helper with `rg` and checking its signature fits the test's need.
13. **Classify each finding** as:
    - **CONFIRMED in-diff** — the bug is real and inside the diff
    - **CONFIRMED at out-of-diff callsite** — the bug is in an unchanged file because the changed symbol is used there in a way that's now broken (cite the file and the contract from 2.5c that was violated)
    - **CONFIRMED pre-existing** — the bug exists on master independent of this diff, found in code the review visited; still reportable, with a fix proposed for THIS PR. "Not caused by this PR" is NOT grounds for FALSE POSITIVE
    - **FALSE POSITIVE** — the code is actually correct (explain why)
    - **CONFIRMED with nuance** — the issue exists but is less severe than stated (explain)

**Move false positives to a separate "Downgraded" section** at the end of the report. For each, give a one-line explanation of why it was dismissed. This lets the PR author verify the reasoning and catch verification mistakes.

Launch verification agents in parallel where findings are independent. Each verification agent should read surrounding source files, not just the diff.

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

QuestDB is a performance-first database. The standard is not "avoid regressions" — it is "use the best known algorithm."
Every new loop, traversal, data structure choice, and computation must be justified as optimal or near-optimal.

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
- Algorithmic inefficiency during compilation is NOT acceptable — slow compilation means slow first-query latency. A
  multi-pass parse, O(n^2) plan enumeration, or redundant AST traversals in the compiler are findings just as they would
  be on a data path.

### Code quality
- Code smell: overly complex methods, deep nesting, unclear intent, dead code
- No third-party Java dependencies on data paths

### QuestDB coding standards
- Class members grouped by kind (static vs instance) and visibility, sorted alphabetically
- Boolean names use `is...` / `has...` prefix
- Modern Java features: enhanced switch, multiline strings, pattern variables in instanceof

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
change), replication failover, a `java-questdb-client` submodule bump, or
tests that drive a producer through a failover/switch window. These are the
CLIENT's store-and-forward guarantees (the client code lives in the
`java-questdb-client` submodule); server-side changes and tests in this repo
must be reviewed against them. A violation here is a **Critical** finding:
the whole point of store-and-forward is that a running producer never loses
data and never hard-fails on a transient outage.

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
- **Fuzz coverage:** for parser, encoder/decoder, ingestion-protocol, or O3/WAL-merge changes, use `rg` to find existing fuzz tests (search for `Fuzz`) covering the changed surface. If one exists and was neither extended nor mentioned as run against the change, flag it.
- **Cross-context coverage:** For every entry in the cross-context exposure list (2.5d), verify a test exercises the changed symbol from that context. Missing cross-context tests are high-priority findings.
- **Error path coverage:** Are failure cases, exceptions, and edge conditions tested — not just the happy path?
- **NULL tests:** Are NULL inputs, NULL columns, and NULL expression results tested?
- **Boundary conditions:** Empty tables, empty partitions, single-row tables, max-value inputs, zero-length strings.
- **Concurrency tests:** If the code touches shared state, are there tests that exercise concurrent access?
- **Resource leak tests:** Tests must use `assertMemoryLeak()` for anything that allocates native memory.
- **Test quality:** Are tests actually asserting the right thing? Watch for tests that pass trivially, assert on wrong values, or test implementation details instead of behavior.
- **Regression tests:** If this PR fixes a bug, is there a test that reproduces the original bug and would fail without the fix?
- Use repository search to find existing test files for the changed classes and verify they cover the new behavior.

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

Present ONLY verified findings (false positives are excluded). Structure as:

### Critical
Issues that must be fixed before merge. Each must include:
- Exact file path and line numbers (including out-of-diff files)
- Whether the finding is **in-diff**, **out-of-diff** (broken by this change at a callsite), or **pre-existing** (already broken on master, found while in there)
- Code path trace showing why the bug is real
- For out-of-diff findings: the contract from 2.5c that was violated and the callsite that triggers it
- Suggested fix, written to be applied in THIS PR — never "address in a follow-up PR" or "file a ticket". A pulled-in pre-existing fix becomes a behavioral change like any other: it gets its own coverage-map row and needs its own regression test

### Moderate
Issues worth addressing but not blocking.

### Minor
Style nits and suggestions.

### Downgraded (false positives)
Findings from the initial review that were dismissed after source code verification. For each, state:
- The original claim (one line)
- Why it was dismissed (one line, citing the specific code that disproves it)

### Coverage map
Render the full Step 2.6 coverage map: one row per behavioral change with its test, failure link, dimension marks (including justified N-As), and TESTED / UNTESTED / EXEMPT verdict. EXEMPT rows must show the verified no-behavioral-change delta. This section is mandatory whenever the PR touches production code — it is the audit trail for the test gate below; a review without it is incomplete.

### Summary
- One-line verdict: approve, request changes, or needs discussion
- **Test gate (hard rule):** the verdict cannot be "approve" while (a) any UNTESTED Critical row remains in the Step 2.6 coverage map, (b) the PR claims a fix but has no regression test with a verified failure link, or (c) new user-visible behavior ships without tests on its error and NULL paths. If the PR changes production code and adds zero tests, the verdict is "request changes" unless every behavioral delta is verified "no behavioral change" — and that justification must be stated explicitly here.
- State the coverage-map totals: behavioral changes total, tested, UNTESTED (e.g., "coverage map: 12 behavioral changes, 9 tested, 3 UNTESTED") — the totals must match the rendered Coverage map section row-for-row
- Highlight any regressions or tradeoffs
- Never make the verdict conditional on splitting the PR or extracting fixes to follow-ups — "approve once X is moved to another PR" is not a valid verdict. If confirmed pre-existing findings exist, the expectation is that they are fixed (with tests) in this PR; name them
- State how many draft findings were verified vs dropped as false positives (e.g., "8 findings verified, 4 false positives removed")
- State the in-diff / out-of-diff / pre-existing split (e.g., "5 findings in-diff, 3 out-of-diff, 2 pre-existing"). If the diff is non-trivial and out-of-diff is zero, the cross-context pass likely underran — re-invoke Agent 9 with a wider grep before finalizing.
