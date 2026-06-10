---
name: review-pr
description: Review GitHub pull requests against QuestDB coding standards using gh, rg, local source inspection, optional subagents, review levels 0-3, and verified Critical/Moderate/Minor findings. Use when Codex is asked to review a PR number or URL, inspect a QuestDB PR diff, validate PR metadata, check changed callsites and contracts, or enforce QuestDB correctness, performance, SQL test, resource, Rust safety, and ACL standards.
---

# QuestDB PR Review

Perform a blocking QuestDB code review. Treat the PR diff as the entry point, not the full scope: changed contracts can break unchanged callsites.

## Argument Parsing

Parse the user's request for:

- PR identifier: PR number or URL. Require one unless the current branch clearly has an associated PR that the user asked to review.
- Review level: parse `--level=N`, `-lN`, or a bare single digit `0`-`3`. Default to level 0.

Strip the level token before passing the PR identifier to `gh`. At the start of the review, state one line such as `Reviewing PR #1234 at level 2.` If the level defaulted to 0, mention that level 3 exists for the full pass.

## Mindset

- Be critical and verification-driven. QuestDB is mission-critical database software; correctness, resource handling, and deterministic performance matter.
- Do not praise the code. Lead with defects, risks, regressions, and missing tests.
- Verify claims against source. Do not report a finding until the reachable code path is confirmed.
- Read surrounding code, callers, tests, and contracts when the diff alone is ambiguous.
- Assess reachability before reporting. Drop issues that require impossible inputs, corrupted state, or values no caller can produce.
- Remember that QuestDB runs Java assertions enabled with `-ea`. Assertions are valid for internal invariants; flag `assert` only when normal user operations can trigger it.
- Demand optimal algorithms. Working code is not enough when a better data structure, one-pass algorithm, direct index, cache, or vectorized path is available.

## Review Levels

| Level | Scope |
| --- | --- |
| 0 | Default. Gather context, review PR metadata, inspect the diff inline, use `rg` on demand, verify findings inline, and cover correctness, NULL handling, algorithmic optimality, tests, and QuestDB standards. |
| 1 | Add semantic delta mapping. Use available subagents for correctness, performance, tests, and code quality; otherwise run those passes inline. |
| 2 | Full change surface map for public and protected symbols. Use subagents for correctness, concurrency, performance, resources, tests, code quality, metadata, Rust safety when needed, and adversarial performance. Batch-verify findings. |
| 3 | Full mission-critical pass. Map all changed symbols, run every review pass including cross-context and fresh-context adversarial review, then verify each finding independently. |

The performance checklist is mandatory at every level.

## Step 1: Gather PR Context

Use `gh` to fetch the PR metadata, diff, comments, and commits:

```bash
gh pr view "<PR>" --json number,title,body,labels,state,headRefName
gh pr diff "<PR>"
gh pr view "<PR>" --comments
gh pr view "<PR>" --json commits
```

Also inspect the local repository state and relevant changed files. Prefer `rg`, `rg --files`, `git diff`, `git show`, `sed`, and `nl` for local exploration. Use `multi_tool_use.parallel` for independent file reads and searches when available.

## Step 2: Check PR Metadata

Check QuestDB PR conventions:

- Title follows Conventional Commits: `type(scope): description`.
- Description repeats the verb, e.g. `fix(sql): fix ...`.
- Description explains user impact, not only implementation internals.
- Fixing an issue has `Fixes #NNN` at the top of the body.
- Tone is analytical; avoid superlatives and bold emphasis on numbers.
- Labels match the scope.
- Commit titles are plain English, no Conventional Commits prefix, under 50 characters, with long-form body lines wrapped at 72 characters.

## Step 2.5: Map the Change Surface

Run this step according to the selected level. Use `rg` and local source reads; do not infer callsites from memory.

### Semantic Delta

For each modified or added function, method, trait, struct field, SQL operator/function, public constant, or exported Rust item, record:

- Symbol: fully-qualified name.
- Before: signature, return type, throws/errors/panics, mutation, ordering, idempotency, allocation behavior, thread-safety.
- After: the same fields.
- Delta: the actual behavior change. If none, write `no behavioral change` only after checking.

### Callsite Inventory

For each changed symbol in scope for the level, search every callsite, override, implementation, and reference outside the diff. A changed public, protected, package-private, `pub`, or `pub(crate)` symbol with no recorded search is a review failure.

For Java, also search:

- subclasses and interface declarations
- `getMethod`, `getDeclaredField`, `Class.forName`
- `FunctionFactory`, `OperatorRegistry`, and SQL registrations
- service loader entries

For Rust, also search:

- trait impls
- macro uses
- JNI exports and Java callers
- `extern "C"` boundaries

### Implicit Contracts

For changed symbols, compare before and after:

- thrown exceptions, returned errors, panics
- iteration order, sort stability, NULL ordering
- idempotency and re-entrancy
- lock acquisition order and locks held on return
- hot-path vs compile-time allocation
- `Send`/`Sync`, thread affinity, JFR/JNI thread attachment
- Java `null` vs sentinel NULL such as `Numbers.LONG_NULL` and `Numbers.INT_NULL`
- Rust drop behavior and Java `finally`/`close`
- SQL clause exposure: WHERE, GROUP BY, JOIN ON, ORDER BY, window frames, partition predicates, materialized views

### Cross-Context Exposure

List places where the changed behavior is visible but the diff does not touch: hot data paths, SQL compilation, async runtime, JNI, replication, materialized views, parallel workers, error paths, and lock-held contexts.

## Step 3: Review Passes

Use subagents for levels that require them when the Codex environment exposes multi-agent tools. Discover them with `tool_search` if needed. If subagents are unavailable, run the required passes inline and state that limitation in the summary.

Every structured pass except fresh-context adversarial review receives the diff and the change surface map.

### Correctness

Check NULL handling, edge cases, off-by-one errors, operator precedence, error paths, SqlException positions, changed SQL clause reachability, and caller expectations.

### Concurrency

Check races, unsafe publication, missing volatile, shared mutable state, lock ordering, thread-affinity, and whether new callsites violate old concurrency assumptions.

### Performance and Algorithmic Optimality

For every new or changed loop, traversal, data structure, or computation:

- State complexity and ask whether a better known algorithm exists.
- Flag O(n) where O(1) direct indexing or hash lookup is available.
- Flag O(n log n) where O(n) is available and O(n^2) where a better bound is practical.
- Collapse multiple passes unless each pass depends structurally on the previous pass.
- Flag redundant computation, repeated lookups, reparsing, and retraversal.
- Check collection choice against access pattern.
- Avoid copies and representation conversions when references can be reused.
- Enforce zero-GC on data paths: avoid `java.util.*`, string creation, concatenation, capturing lambdas, and autoboxing.
- Consider SIMD or native/vectorized alternatives for column and array processing.
- Treat SQL compilation as allocation-tolerant but not algorithmically lax.

Do not downgrade a technically correct algorithmic finding merely because current data sizes are small. Downgrade only when the analysis is wrong or the bound is genuinely constant.

### Resource Management

Trace allocation, ownership transfer, close/free/clear, native memory, pools, `try-with-resources`, error paths, and polymorphic cleanup methods.

### Tests

Verify coverage for every changed behavior, regression, error path, NULL case, boundary condition, concurrency path, resource leak path, and cross-context exposure. Use `assertMemoryLeak()` for native-memory tests.

### Code Quality and Standards

Check member ordering, naming, dead code, complexity, modern Java use, third-party dependencies on data paths, and SQL test style:

- SQL keywords in uppercase.
- Prefer `expr::TYPE` casts.
- Use underscores in numbers with at least five digits.
- Use multiline strings for complex queries.
- Avoid DELETE in tests unless the behavior specifically requires it.
- Use single INSERT for multiple rows.

### SQL Test Assertions

QuestDB SQL test assertions should use the fluent `AbstractCairoTest.assertQuery(query)` builder.

- Treat new or changed query-result `assertSql(...)` or `TestUtils.assertSql(...)` as Critical; require `assertQuery(sql).returns(expected)` or plan builder assertions instead.
- Treat new or changed `assertPlanNoLeakCheck(...)`, `getPlan(...)`, and `assertPlanDoesNotContain(...)` as Critical when they are used instead of the builder plan assertions.
- Do not flag live-server wrappers such as `TestServerMain.assertSql(...)`, `serverMain.assertSql(...)`, or `EntGriffinServerMain.assertSql(...)`.
- Treat `.returnsOnce(...)` on deterministic queries as Critical. It is only justified for genuinely unstable output such as unseeded `rnd_*`, `now()`, `sysdate()`, `systimestamp()`, or non-deterministic row order.
- Flag a lone `assertQuery(...)` chain wrapped in `assertMemoryLeak(...)`; the builder performs its own leak check unless a broader multi-statement leak scope is genuinely needed.

### Logging

QuestDB logging uses a builder chain. Every `LOG` chain must end with `.$()` or `.I$()`. Missing closure holds a ring buffer slot and can stall producers and the `logging_0` consumer. Watch for `.put()` where `.$()` was intended, because `.put()` returns `Utf16Sink` and breaks the chain.

### Rust Safety

When `.rs` files change, check every reachable panic site: `unwrap`, `expect`, indexing, `panic!`, `unreachable!`, `todo!`, release-mode integer overflow, and unsafe slice construction. Rust panics across JNI/FFI can abort the JVM, so fallible operations should use `Result` or `Option` with propagation. Drop panic claims that callers make unreachable.

### Enterprise Permissions and ACL

If the PR introduces SQL statements, ALTER operations, or state mutation:

- Verify a corresponding `SecurityContext.authorize*()` call exists.
- Check enterprise implementations when available: `EntSecurityContextBase`, `AdminSecurityContext`, `AbstractReplicaSecurityContext`, and test mocks.
- Verify `Permission.java` constants, name maps, and `TABLE_PERMISSIONS` or `ALL_PERMISSIONS`.
- Verify `PermissionParser` can parse GRANT and REVOKE for the new permission name.
- Ensure replica security contexts deny new writes.

### TODOs and FIXMEs

Scan the diff for `TODO`, `FIXME`, `HACK`, `XXX`, and `WORKAROUND`. Determine whether each is pre-existing or newly introduced. Flag newly introduced deferred bugs or incomplete implementation. Verify referenced ticket numbers exist when practical.

### Fresh-Context and Adversarial Passes

At level 3, run a fresh-context pass that receives only the diff and changed file names with the instruction to find ways the code is wrong. Run a separate adversarial performance pass that receives the diff plus full touched files and asks for the theoretically fastest implementation of each changed method.

## Step 3b: Verify Findings

Do not present draft findings. Verify each one first:

- Read exact source lines and surrounding code.
- Trace callers, subclasses, overrides, runtime dispatch, and callers up two levels for cross-context issues.
- For JNI/FFI, read both Java and Rust sides.
- For leaks, trace allocation to cleanup on happy and error paths; check whether code between allocation and cleanup registration can throw.
- For Rust panics and overflows, prove reachability from realistic QuestDB scale and callers.
- For performance, verify the complexity and hot/cold path.
- Classify internally as confirmed in-diff, confirmed out-of-diff callsite, confirmed with nuance, or false positive.

Report only confirmed findings. Include a `Downgraded` section only when useful to explain dismissed draft findings.

## Step 4: Output

Use code-review format with findings first. Do not include praise.

### Critical

Blocking issues. Include exact file path and line number, in-diff or out-of-diff classification, code path trace, violated contract for out-of-diff findings, and suggested fix.

### Moderate

Issues worth addressing but not necessarily blocking.

### Minor

Style nits and small suggestions.

### Downgraded

False positives dismissed during verification. For each, state the original claim and the source fact that disproves it.

### Summary

Include one-line verdict: approve, request changes, or needs discussion. State how many draft findings were verified vs dropped and the in-diff vs out-of-diff split. If a non-trivial diff has zero out-of-diff findings, widen the cross-context search before finalizing.
