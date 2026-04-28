---
name: review-pr
description: Review a GitHub pull request against QuestDB coding standards
argument-hint: [PR number or URL]
allowed-tools: Bash(gh *), Read, Grep, Glob, Agent
---

Review the pull request `$ARGUMENTS`.

## Review mindset

You are a senior QuestDB engineer performing a blocking code review. QuestDB is mission-critical software deployed on spacecraft — bugs can cause data loss or system failures that cannot be patched after deployment. There is zero tolerance for correctness issues, resource leaks, or undefined behavior. Be critical, thorough, and opinionated. Your job is to catch problems before they ship, not to be nice.

- **Assume nothing is correct until you've verified it.** Read surrounding code to understand context — don't just look at the diff in isolation.
- **The diff is a hint, not the boundary of the review.** The highest-value bugs almost always live at callsites outside the diff that depend on contracts the diff quietly changed. Treat the diff as the entry point, not the scope.
- **Flag every issue you find**, no matter how small. Do not soften language or hedge. Say "this is wrong" not "this might be an issue".
- **Do not praise the code.** Skip "looks good", "nice work", "clever approach". Focus entirely on problems and risks.
- **Think adversarially.** For each change, ask: what inputs break this? What happens under concurrent access? What if this runs on a 10-billion-row table? What if the column is NULL? What if the partition is empty?
- **Check what's missing**, not just what's there. Missing tests, missing error handling, missing edge cases, missing documentation for non-obvious behavior.
- **Verify every claim.** If the PR title says "fix", verify the bug actually existed and the fix is correct. If it says "improve performance", look for benchmarks or reason about the algorithmic change — does it actually improve things, or could it regress in other cases? If it says "simplify", verify the new code is actually simpler and doesn't drop behavior. Treat the PR description as an unverified hypothesis, not a statement of fact.
- **Read the full context of changed files** when the diff alone is ambiguous. Use Read/Grep/Glob to inspect the surrounding code, callers, and related tests.
- **Assess reachability before reporting.** For every potential bug, trace the actual callers and inputs. If a problem
  requires physically impossible conditions (billions of columns, corrupted JNI inputs, values that no caller can
  produce), it is not a real finding — drop it. Focus on bugs that real workloads can trigger, not theoretical edge
  cases that exist only in the type system.
- **QuestDB runs with Java assertions enabled (`-ea`).** Assertions are a valid guard for invariants that indicate
  corruption or internal bugs. Do NOT flag `assert` as insufficient — it is the preferred mechanism for conditions
  that should never occur in a non-corrupt database. Only flag an `assert` if the condition can plausibly be triggered
  by normal (non-corrupt) user operations.

## Step 1: Gather PR context

Fetch PR metadata, diff, and any review comments:

```bash
gh pr view $ARGUMENTS --json number,title,body,labels,state
gh pr diff $ARGUMENTS
gh pr view $ARGUMENTS --comments
```

## Step 2: PR title and description

Check against CLAUDE.md conventions:
- Title follows Conventional Commits: `type(scope): description`
- Description repeats the verb (e.g., `fix(sql): fix ...` not `fix(sql): DECIMAL ...`)
- Description speaks to end-user impact, not implementation internals
- If fixing an issue, `Fixes #NNN` is at the top of the body
- Tone is level-headed and analytical, no superlatives or bold emphasis on numbers
- Labels match the PR scope (SQL, Performance, Core, etc.)

## Step 2.5: Map the change surface

Before launching review agents, produce a structured change surface map. This step is mandatory and must use Grep/Glob — do not reason about callsites from memory. The output of this step is required input for every agent in Step 3.

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

## Step 3: Parallel review

Every agent receives:
1. The PR diff
2. The full change surface map from Step 2.5 (semantic deltas, callsite inventory, implicit contracts, cross-context exposure list)

### Anti-anchoring directive (applies to all agents)

- **Bugs at callsites outside the diff outrank bugs inside the diff.** A confirmed bug in a file the PR did not touch but that calls a changed symbol is a P0 finding.
- **"Looks correct in isolation" is not a valid conclusion.** Before clearing a changed symbol, the agent must walk the callsite inventory from 2.5b and explicitly state, per callsite, whether the new behavior is still correct there.
- **The diff is the entry point, not the scope.** If the change surface map shows the symbol is reachable from N other files, the review covers N+1 files.
- A single finding of the form "in `FooReader.java` the new behavior of `Bar.x()` causes Y" is worth more than five findings inside the diff.

### Agents

Launch the following agents in parallel.

**Agent 1 — Correctness & bugs:** NULL handling, edge cases, logic errors, off-by-one, operator precedence, error paths. Cross-reference every changed symbol against its callsite inventory and verify the new behavior is correct at each callsite.

**Agent 2 — Concurrency:** Race conditions, shared mutable state, missing volatile, lock ordering, thread-safety of data structures. Use the implicit contract list (lock order, thread-affinity) and check every callsite from 2.5b for violations of the new contract.

**Agent 3 — Performance & allocations:** Regressions, zero-GC violations, `java.util.*` collections vs `io.questdb.std`, string creation/concatenation on hot paths, SIMD opportunities. Algorithmic complexity: for each new loop, traversal, or data structure, analyze how it scales with data size (row count, partition count, join fan-out). Flag any O(n^2) or worse patterns that could regress on large tables (1M+ rows, 1000+ partitions). Check whether new code paths are compile-time-only or data-path — compile-time allocations are acceptable, data-path allocations are not. For changed symbols now reachable from new contexts (per 2.5d), check whether any of those new contexts is a hot path.

**Agent 4 — Resource management:** Leaks on all code paths (especially errors), try-with-resources, native memory, pool management. Walk every callsite from 2.5b that constructs, owns, or transfers ownership of changed types and verify cleanup on all paths.

**Agent 5 — Test review & coverage:** Coverage gaps, error path tests, NULL tests, boundary conditions, regression tests, test quality, `assertMemoryLeak()` usage. Cross-reference 2.5d: every cross-context exposure should have a test that exercises the changed symbol from that context. Missing tests for cross-context callsites is a high-priority finding.

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

- It receives ONLY the PR diff and the names of the changed files. It does NOT receive the change surface map from Step 2.5, the implicit contract list, the cross-context exposure list, or any of the review checklists below.
- Its sole instruction: "find ways this code is wrong". No category list, no failure-mode taxonomy, no QuestDB-specific style guide.
- It is free to use Read, Grep, and Glob to explore the repository however it wants.
- Findings are not pre-classified by category. Each finding states: what's wrong, why it's wrong, and the code path that demonstrates it.

The point of this agent is to surface bugs the structured agents cannot see because they are reasoning inside the same frame. A finding here that none of agents 1-9 produced is high signal — it means the structured review missed it. A finding here that overlaps with agents 1-9 is corroboration.

Run this agent in parallel with agents 1-9. It is mandatory regardless of diff size.

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
8. **For performance claims**: check whether the cost is measurable in a realistic scenario. Downgrade to a nit if the
   saving is negligible relative to the surrounding work. Exception: GC allocations on a hot path are always worth
   flagging, even a single one.
9. **For cross-context findings (Agent 9)**: re-read the callsite in full, including its callers up two levels, and confirm the broken behavior is reachable from production code paths. Cross-context findings are high-value but also the easiest to overstate — verify carefully.
10. **Classify each finding** as:
    - **CONFIRMED in-diff** — the bug is real and inside the diff
    - **CONFIRMED at out-of-diff callsite** — the bug is in an unchanged file because the changed symbol is used there in a way that's now broken (cite the file and the contract from 2.5c that was violated)
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

### Performance
- Performance regressions: changes that make hot paths slower or increase complexity
- Unnecessary allocations on data paths (zero-GC requirement)
- Use of `java.util.*` collections (HashMap, ArrayList, etc.) instead of QuestDB's own zero-GC collections in `io.questdb.std`
- String creation or concatenation on hot paths (use CharSink, StringSink, or direct char[] instead)
- Capturing lambdas on hot paths — lambdas that capture local variables or instance fields allocate a new object on every invocation. Non-capturing lambdas (static method refs, no closed-over state) are safe as the JVM caches them. Flag any capturing lambda on a data path.
- Autoboxing on hot paths — primitive-to-wrapper conversions (`int` → `Integer`, `long` → `Long`, etc.) allocate silently. Watch for primitives passed to generic methods, stored in `java.util.*` collections, or returned from methods with wrapper return types.
- Missing SIMD or vectorization opportunities
- Inefficient algorithms where QuestDB already provides optimized alternatives
- Algorithmic complexity at scale: for each new loop or traversal, what is the time complexity as a function of row count, partition count, or join fan-out? Flag O(n^2) or worse patterns. Consider: what happens with 1M outer rows? 10K partitions? 100-way fan-out per row?
- Compile-time vs data-path distinction: allocations and O(n) scans during SQL compilation/optimization are acceptable; the same on per-row data paths are not

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

### SQL conventions (if tests or SQL involved)
- Keywords in UPPERCASE
- `expr::TYPE` cast syntax preferred over CAST()
- Underscores in numbers >= 5 digits (e.g., 1_000_000)
- Multiline strings for complex queries
- No DELETE statements (suggest DROP PARTITION or soft delete)
- Tests use `assertMemoryLeak()`, `assertQueryNoLeakCheck()`, `execute()` for DDL
- Single INSERT for multiple rows

### Enterprise permissions & ACL (if PR introduces new SQL statements or ALTER operations)
- New ALTER TABLE operations almost always require a new enterprise permission. If the PR adds a new ALTER statement (or any new SQL statement that modifies state), flag it if there is no corresponding `SecurityContext.authorize*()` call in the execution path.
- New features in OSS should have an enterprise counterpart that wires up ACL. Check whether the PR introduces `authorize*` methods in `SecurityContext` and whether all enterprise `SecurityContext` implementations (`EntSecurityContextBase`, `AdminSecurityContext`, `AbstractReplicaSecurityContext`, and test mocks) are updated.
- New permissions must be registered in `Permission.java` (constant, name maps, and included in `TABLE_PERMISSIONS`/`ALL_PERMISSIONS` as appropriate).
- The `PermissionParser` must be able to parse GRANT/REVOKE for the new permission name — especially if the name contains SQL keywords like `ON`, `TO`, or `FROM` that could conflict with parser grammar.
- Replica security contexts must deny new write operations (`deniedOnReplica()`).

### Test review
- **Coverage gaps:** For every new or changed code path, verify a corresponding test exists. If not, flag it explicitly as "missing test for X".
- **Cross-context coverage:** For every entry in the cross-context exposure list (2.5d), verify a test exercises the changed symbol from that context. Missing cross-context tests are high-priority findings.
- **Error path coverage:** Are failure cases, exceptions, and edge conditions tested — not just the happy path?
- **NULL tests:** Are NULL inputs, NULL columns, and NULL expression results tested?
- **Boundary conditions:** Empty tables, empty partitions, single-row tables, max-value inputs, zero-length strings.
- **Concurrency tests:** If the code touches shared state, are there tests that exercise concurrent access?
- **Resource leak tests:** Tests must use `assertMemoryLeak()` for anything that allocates native memory.
- **Test quality:** Are tests actually asserting the right thing? Watch for tests that pass trivially, assert on wrong values, or test implementation details instead of behavior.
- **Regression tests:** If this PR fixes a bug, is there a test that reproduces the original bug and would fail without the fix?
- Use Grep/Glob to find existing test files for the changed classes and verify they cover the new behavior.

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
- Whether the finding is **in-diff** or **out-of-diff**
- Code path trace showing why the bug is real
- For out-of-diff findings: the contract from 2.5c that was violated and the callsite that triggers it
- Suggested fix

### Moderate
Issues worth addressing but not blocking.

### Minor
Style nits and suggestions.

### Downgraded (false positives)
Findings from the initial review that were dismissed after source code verification. For each, state:
- The original claim (one line)
- Why it was dismissed (one line, citing the specific code that disproves it)

### Summary
- One-line verdict: approve, request changes, or needs discussion
- Highlight any regressions or tradeoffs
- State how many draft findings were verified vs dropped as false positives (e.g., "8 findings verified, 4 false positives removed")
- State the in-diff vs out-of-diff split (e.g., "5 findings in-diff, 3 findings out-of-diff"). If the diff is non-trivial and out-of-diff is zero, the cross-context pass likely underran — re-invoke Agent 9 with a wider grep before finalizing.
