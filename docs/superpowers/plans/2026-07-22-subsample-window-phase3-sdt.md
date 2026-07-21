# SUBSAMPLE-as-window Phase 3: `sdt` (Swinging Door Trending)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Bring the already-built, already-reviewed `sdt(ts, value, compdev)` keep-flag window function from branch `feat/swinging-door` into `subsample-fixes` (making it user-visible alongside uniform/cadence/m4/minmax/lttb), then add `SUBSAMPLE sdt(value, compdev)` as a desugaring to it. Completes the six-algorithm keep-flag family.

**Architecture:** `sdt` is error-bound (an error tolerance `compdev`, variable/unbounded output), unlike the five count-based methods. It is its OWN standalone TWO_PASS window function (`SdtWindowFunctionFactory` + `SwingingDoor` state machine) — it does NOT use `BucketSelectWindowFunction`. Pass1 runs the swinging door once (back-patching the one-row lookback into an O(1)-per-row keep buffer); pass2 materializes the buffered flags. `SUBSAMPLE sdt(value, compdev)` desugars exactly like the other value-inspecting methods — to `sdt(ts, value, compdev) OVER (ORDER BY ts)` + a `WHERE __keep` filter — reusing the shared `desugarValueInspectingSubsample` + `desugarSubsample` tail. **Key difference from the other five:** `sdt` is NOT in the old `SubsampleRecordCursorFactory`, so there is no cursor to fall through to. Therefore the `sdt` gate must be TOTAL: for `token=="sdt"` it either migrates (valid shape) or throws a specific `SqlException` at rewrite time — it must NEVER leave an `sdt` subsample node for codegen (whose `generateSubsample` would emit the misleading "unknown subsample method: sdt. Supported methods: lttb, m4, minmax, uniform, cadence").

**Tech Stack:** Java, QuestDB griffin window framework, JUnit4.

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; build/test from `/home/nick/claude/wt/oss/subsample-fixes`.
- Auto-register via classpath scan; Apache header on every new `.java`; fluent `assertQuery` tests (except old-cursor cross-checks — but sdt has NO old cursor, so cross-check against the sdt WINDOW FUNCTION directly via the bespoke `assertSql` helper where random access is needed).
- Result `ColumnType.BOOLEAN`; sdt is TWO_PASS (framework) / single algorithmic pass. Require ORDER BY; reject framing; PARTITION BY supported (the ported factory has a partitioned variant). Value column numeric; `compdev` a compile-time constant `double`, `>= 0`, finite.
- **PORT `SwingingDoor` + `SdtWindowFunctionFactory` VERBATIM from `feat/swinging-door`** (they are unit-green + whole-branch-reviewed there: 24/24, `SwingingDoorTest` 11 + `SdtWindowFunctionTest` 13). Reconcile ONLY for window-framework API drift on master; do NOT re-design the algorithm or the factory. The correctness finding (dropped points lie within `compdev` of the swinging-door ENVELOPE; piecewise reconstruction between kept points can reach ~2×`compdev` on asymmetric/step data — matches PI/IoTDB) and its pinning test port as-is.
- **`sdt` gate is TOTAL** (see Architecture). Every `token=="sdt"` path in `rewriteSubsample` ends in migrate-or-throw.
- **NULL handling:** `SUBSAMPLE sdt` has no `IGNORE/RESPECT NULLS` syntax; the desugared `OVER` clause omits it, so the window function's default (RESPECT NULLS — a null forces a kept boundary + resets the door, flushing the last real sample before the gap) applies. SUBSAMPLE's timestamp filtering already drops `LONG_NULL` ts.
- Do NOT touch the old cursor's runtime path, the other five algorithms, the shared `BucketSelectWindowFunction`, or the uniform/cadence gates.

**Commands:**
```bash
cd /home/nick/claude/wt/oss/subsample-fixes && export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -q -pl core -am compile -DskipTests
mvn -q -pl core -Dtest='SwingingDoorTest,SdtWindowFunctionTest' test
mvn -q -pl core -Dtest=SubsampleTest test
```

---

## File Structure

- `core/.../engine/functions/window/SwingingDoor.java` — ported pure state machine (verbatim).
- `core/.../engine/functions/window/SdtWindowFunctionFactory.java` — ported `sdt(NDd)` factory (verbatim, reconciled for API drift only).
- `core/.../test/.../engine/window/SwingingDoorTest.java`, `SdtWindowFunctionTest.java` — ported tests (verbatim).
- `core/.../griffin/SqlOptimiser.java` — extend `rewriteSubsample` with a TOTAL `sdt` gate; reuse `desugarValueInspectingSubsample` + `desugarSubsample`.
- `core/.../test/.../griffin/SubsampleTest.java` — new `SUBSAMPLE sdt` desugar + cross-check + error tests.

---

## Task 1: Port the `sdt` window function

**Files:**
- Create (port): `SwingingDoor.java`, `SdtWindowFunctionFactory.java`, `SwingingDoorTest.java`, `SdtWindowFunctionTest.java`
- Reference: the same files on `feat/swinging-door` (`git show feat/swinging-door:<path>`), and this branch's other window factories for the current `AbstractWindowFunctionFactory`/`BaseWindowFunction`/`WindowContext` API.

**Interfaces:**
- Produces: window function `sdt(NDd)`, BOOLEAN, `sdt(ts, value, compdev) OVER (ORDER BY ts)`, non-partitioned + partitioned; RESPECT/IGNORE NULLS supported.

- [ ] **Step 1: Verify `sdt` free as a function name on THIS branch.** `grep -rn "getSignature\|SIGNATURE" core/src/main/java/io/questdb/griffin/engine/functions/ | grep -iw sdt` → nothing. Confirm the 4 target files do not already exist here.
- [ ] **Step 2: Port the files.** `git checkout feat/swinging-door -- core/src/main/java/io/questdb/griffin/engine/functions/window/SwingingDoor.java core/src/main/java/io/questdb/griffin/engine/functions/window/SdtWindowFunctionFactory.java core/src/test/java/io/questdb/test/griffin/engine/window/SwingingDoorTest.java core/src/test/java/io/questdb/test/griffin/engine/window/SdtWindowFunctionTest.java`. (Do NOT bring the docs/spec/plan files — those stay on their branch.)
- [ ] **Step 3: Compile; reconcile API drift ONLY.** `mvn -q -pl core -am compile -DskipTests`. If it fails, the cause is window-framework API changes on master since the branch point (method signatures on `AbstractWindowFunctionFactory`, `BaseWindowFunction`, `WindowContext`, `WindowSPI`, `Reopenable`, memory/record-chain helpers). Fix by matching THIS branch's API — compare against a sibling factory (e.g. `EmaDoubleWindowFunctionFactory`, or the migrated `UniformFunctionFactory`). Change ONLY what the compiler/API demands; do not alter algorithm logic, validation messages, or the BOOLEAN keep-slot mechanism. Record every reconciliation in the report.
- [ ] **Step 4: Run the ported tests.** `mvn -q -pl core -Dtest='SwingingDoorTest,SdtWindowFunctionTest' test`. Expected: all green (24/24 on the branch). If a test fails, determine whether it's (a) a real API-drift reconciliation miss (fix the port) or (b) a genuine behavior difference on master (STOP, report — do NOT weaken the test).
- [ ] **Step 5: Sanity — sdt is registered + usable.** Add ONE fluent smoke test in `SdtWindowFunctionTest` (or confirm an existing one) that runs `sdt(ts, value, 0.5) OVER (ORDER BY ts)` end-to-end and asserts a known keep-set, proving classpath registration works on this branch. Lock its EXPLAIN plan.
- [ ] **Step 6: Commit**
```bash
git add core/src/main/java/io/questdb/griffin/engine/functions/window/SwingingDoor.java \
        core/src/main/java/io/questdb/griffin/engine/functions/window/SdtWindowFunctionFactory.java \
        core/src/test/java/io/questdb/test/griffin/engine/window/SwingingDoorTest.java \
        core/src/test/java/io/questdb/test/griffin/engine/window/SdtWindowFunctionTest.java
git commit --no-verify -m "feat(window): port sdt() swinging-door keep-flag window function from feat/swinging-door"
```

---

## Task 2: `SUBSAMPLE sdt(value, compdev)` desugaring

**Files:** Modify `SqlOptimiser.java`; Test `SubsampleTest.java`.
**Interfaces:** Consumes `sdt` (Task 1). `SUBSAMPLE sdt(value, compdev)` → `SELECT <cols> FROM (SELECT <cols>, sdt(ts, value, compdev) OVER (ORDER BY ts) __keep FROM src) WHERE __keep`.

- [ ] **Step 1: Baseline** — `SubsampleTest` green (150). Confirm `SUBSAMPLE sdt(...)` currently errors at codegen with "unknown subsample method: sdt" (there is no cursor).
- [ ] **Step 2: Failing tests** in `SubsampleTest.java`:
  - `testSdtDesugarsToWindowFilter` — plan test: `SELECT ts, price FROM x SUBSAMPLE sdt(price, 0.5)` rewrites to the window+`__keep`-filter plan (fill actual after implementing).
  - `testSdtMatchesWindowFunction` — cross-check (bespoke `assertSql`, random access): `... SUBSAMPLE sdt(price, 0.5)` returns byte-identical rows to the explicit `SELECT ts, price FROM (SELECT *, sdt(ts, price, 0.5) OVER (ORDER BY ts) k FROM x) WHERE k`. This is the oracle (the desugaring's own target), since sdt has no cursor.
  - `testSdtNullFlush` — data with a null value mid-series: confirm the default RESPECT-NULLS flush (last real sample before the gap kept), matching the window function.
  - Error tests (each asserts a SPECIFIC message + position, NOT the generic codegen "unknown method"): `testSdtNonConstantCompdev` (`sdt(price, x)` bind var / non-constant), `testSdtNegativeCompdev` (`sdt(price, -1.0)`), `testSdtNonNumericValue` (`sdt(sym, 0.5)`), `testSdtWrongArity` (`sdt(price)` and `sdt(price,0.5,1)`), `testSdtInJoin` (`SUBSAMPLE sdt` over an ASOF JOIN → a clear "not supported inside a join" style error, NOT "unknown method"), `testSdtNoDesignatedTs`.
- [ ] **Step 3: RED** — `mvn -q -pl core -Dtest=SubsampleTest test` fails on the new tests.
- [ ] **Step 4: Implement the TOTAL `sdt` gate in `rewriteSubsample`.**
  - Add an `sdt` branch (mutually exclusive with the others by token). Migrate when: `token=="sdt"`; `paramCount==2`; designated ts present; non-agg; NOT in a join context (reuse the `subsampleInJoinContext` flag from Phase 2c); value arg (arg[0]) is a bare-column `LITERAL`; `compdev` (arg[1]) is a compile-time constant `double`, `>= 0`, finite (add an `isConstantSdtCompdev` helper — mirror `isConstantCadenceSeed`: compile the arg, require `isConstant()`, read as double, reject NaN / negative; a non-constant / non-numeric / negative / NaN compdev makes the helper return false).
  - Build the node via `desugarValueInspectingSubsample` (unchanged — it already builds a 3-arg `(ts, value, arg2)` node back-to-front; here arg2 = the `compdev` node): 2-arg SUBSAMPLE → 3-arg `sdt(ts, value, compdev)` window node → route through the shared `desugarSubsample` tail. The factory reads `getQuick(0)=ts, (1)=value, (2)=compdev`.
  - **TOTALITY:** when `token=="sdt"` but the shape is NOT migrable, throw a specific `SqlException` at the subsample position instead of leaving the node for codegen. Distinct messages: join context → `"SUBSAMPLE sdt is not supported inside a join"`; missing designated ts → reuse the existing `"SUBSAMPLE requires a designated timestamp column"`; wrong arity → `"sdt() requires exactly 2 arguments: column and compdev"`; non-literal value → `"SUBSAMPLE sdt requires a plain column as its first argument"`; non-constant/negative/NaN compdev → `"SUBSAMPLE sdt requires a constant, non-negative compdev"`; aggregation context → the existing agg-context message. (For the non-numeric-VALUE case — a literal naming a SYMBOL/VARCHAR column — existence/type isn't known at rewrite time, so that one legitimately migrates and the `sdt` window factory rejects it with its own numeric-type message; assert THAT message in `testSdtNonNumericValue`, mirroring how lttb handles it.)
  - Keep uniform/cadence/m4/minmax/lttb branches unchanged.
- [ ] **Step 5: Lock the plan test** (paste the actual rewritten `sdt` plan).
- [ ] **Step 6: GREEN** — full `mvn -q -pl core -Dtest=SubsampleTest test`. All new sdt tests pass; all 150 prior cases still green (other methods untouched). Fill cross-check expected from the window-function run.
- [ ] **Step 7: Broad sanity** `mvn -q -pl core -Dtest='SubsampleTest,SdtWindowFunctionTest,LttbWindowFunctionTest,M4WindowFunctionTest,SqlOptimiserTest' test`.
- [ ] **Step 8: Commit** `feat(sql): desugar SUBSAMPLE sdt to the sdt keep-flag window function (total gate; no cursor fallback)`.

---

## Self-Review

**Spec coverage:** Port `SwingingDoor` + `sdt` window function (Task 1) + `SUBSAMPLE sdt(value, compdev)` desugaring (Task 2), completing the six-algorithm keep-flag family (design spec §Phase 3). sdt is error-bound (own state machine, not `BucketSelectWindowFunction`); TWO_PASS-framework / single-algorithmic-pass, matching the spec table. The unbounded-output + envelope-vs-reconstruction caveats are carried in the ported tests/docs.

**Placeholder scan:** plan pastes (plan text, cross-check expected) are deliberate lock-the-output steps filled from real runs. The gate's TOTALITY and per-failure messages are concrete. The port is specified as verbatim-with-API-reconciliation against an authoritative source branch.

**Type consistency:** `sdt(NDd)` → BOOLEAN; args `ts, value, compdev` at getQuick(0,1,2); desugaring adds back-to-front via the existing `desugarValueInspectingSubsample`; `compdev` constant double ≥0 finite via `isConstantSdtCompdev`. No fall-through for sdt (no cursor) — migrate-or-throw.

## Execution Handoff
(Provided after user review.)
