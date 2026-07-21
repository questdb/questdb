# SUBSAMPLE-as-window Phase 2c: `lttb`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add user-visible `lttb(ts, value, target [, gap])` boolean keep-flag window function(s) and migrate `SUBSAMPLE lttb(value, target [, gap])` to them, byte-identically, with javier's ~84 lttb SUBSAMPLE test cases as the oracle. Completes the count/value-based algorithms (uniform, cadence, m4, minmax already migrated).

**Architecture:** `lttb` is value-inspecting TWO_PASS and REUSES the shared `BucketSelectWindowFunction` (from `M4FunctionFactory`) with a `LttbAlgorithm` instance — `LttbAlgorithm implements SubsampleAlgorithm`, and its gap threshold lives in the instance (`new LttbAlgorithm(gapThresholdMicros)`), so no base changes are needed (the base's NULL/NaN filtering, `count<=target` keepAll, native buffer, and ascending-order pass2 walk all apply — LTTB emits first + per-bucket + last (and per-segment) in ascending order). Two overloads mirror cadence's two-class optional-arg pattern: `lttb(NDl)` (no gap → gap=0) and `lttb(NDls)` (gap = a constant interval string parsed to micros via `TimestampSamplerFactory`, reproducing the old cursor's parse exactly). The desugaring extends the value-inspecting gate for lttb (2 or 3 args) building a 3- or 4-arg call node.

**Tech Stack:** Java, QuestDB griffin window framework, JUnit4.

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; build/test from `/home/nick/claude/wt/oss/subsample-fixes`.
- Auto-register via classpath scan; Apache header on every new `.java`; fluent `assertQuery` tests (except where comparing against the old `SUBSAMPLE` cursor, which needs random access — then match `SubsampleTest`'s bespoke `assertSql` helper, as m4's null test did).
- Result `ColumnType.BOOLEAN`; TWO_PASS; require ORDER BY; reject framing + PARTITION BY; value column numeric; target constant `>= 2`.
- **REUSE `BucketSelectWindowFunction` with `new LttbAlgorithm(gapMicros)` — do NOT re-copy the base or the LTTB math.** Widen `LttbAlgorithm` (class + `LttbAlgorithm(long)` constructor + `select` if not already public) to `public`, no logic change.
- **Signatures:** `lttb(NDl)` = (ts, double value, const long target); `lttb(NDls)` = (…, const string gap). Two public factory classes (scanner instantiates by reflection — precedent: `CadenceFunctionFactory`/`CadenceSeedFunctionFactory`).
- **Gap parsing:** reproduce the old cursor EXACTLY. Read `SqlCodeGenerator.generateSubsample` around line 7157 (the lttb gap/"tolerance" branch using `TimestampSamplerFactory.findPositiveIntervalEndIndex` + `parsePositiveInterval`). Parse the constant gap string to `gapThresholdMicros` the same way (same units {s,m,h,d}, same errors/positions). `gap == 0`/absent → non-gap LTTB.
- **Arg order in the factory:** `getQuick(0)=ts, (1)=value, (2)=target, (3)=gap`. Desugaring builds the node args BACK-TO-FRONT: 3-arg → `add(target), add(value), add(ts)`; 4-arg → `add(gap), add(target), add(value), add(ts)`.
- **Oracle:** every `SUBSAMPLE lttb` case in `SubsampleTest.java` (~84) stays green byte-identical; uniform/cadence/m4/minmax stay green. `lttb` shapes that can't be byte-identical (non-constant target, non-bare-column value per the m4/minmax rule, non-constant/invalid gap, aggregation context, no designated ts, >3 args) fall through to the untouched cursor.
- Do NOT touch the old cursor's runtime path, the other algorithms, or the shared base's logic.

**Commands:**
```bash
cd /home/nick/claude/wt/oss/subsample-fixes && export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -q -pl core -am compile -DskipTests
mvn -q -pl core -Dtest=LttbWindowFunctionTest test
mvn -q -pl core -Dtest=SubsampleTest test
```

---

## File Structure

- `core/.../engine/functions/window/LttbFunctionFactory.java` — `lttb(NDl)` factory: thin, instantiates `BucketSelectWindowFunction` with `new LttbAlgorithm(0)`.
- `core/.../engine/functions/window/LttbGapFunctionFactory.java` — `lttb(NDls)` factory: parses the gap string → micros, instantiates `BucketSelectWindowFunction` with `new LttbAlgorithm(gapMicros)`.
- (accessibility) `LttbAlgorithm.java` — widen to public.
- `core/.../griffin/SqlOptimiser.java` — extend the value-inspecting gate for `lttb` (2/3 args + gap constant); reuse/extend `desugarValueInspectingSubsample`.
- Tests: `LttbWindowFunctionTest.java`; SubsampleTest plan test.

---

## Task 1: `lttb` window function(s)

**Files:**
- Create: `LttbFunctionFactory.java`, `LttbGapFunctionFactory.java`, `LttbWindowFunctionTest.java`
- Modify: `LttbAlgorithm.java` (visibility only)
- Reference: `M4FunctionFactory.java` (the shared `BucketSelectWindowFunction` + the value-inspecting validation pattern), `CadenceFunctionFactory`/`CadenceSeedFunctionFactory` (two-class optional-arg registration), `SqlCodeGenerator.generateSubsample` ~7157 (the exact gap parse), `LttbAlgorithm.java` (constructor + select).

**Interfaces:**
- Produces: window functions `lttb(NDl)` and `lttb(NDls)`, BOOLEAN, `lttb(ts,value,target[,gap]) OVER (ORDER BY ts)`.

- [ ] **Step 1: Verify `lttb` free as a function name.** `grep -rn "SIGNATURE\|getSignature" core/src/main/java/io/questdb/griffin/engine/functions/ | grep -iw lttb` → nothing; else STOP.
- [ ] **Step 2: Write failing tests** (`LttbWindowFunctionTest.java`, fluent). Cases: basic lttb (keep first/last + triangle picks — fill from stable output, cross-checked vs old cursor); gap-preserving `lttb(ts,v,N,'1h')` over data with a gap (verify a segment split); keepAll when non-null count <= target; null/NaN row filtering (inherited); non-constant target rejection; a `lttb(ts, s, 4)` non-numeric value rejection (reproduce old message). Use the `SUBSAMPLE lttb(...)` cross-check (bespoke `assertSql`) for the tricky cases (gap, null) — byte-identity.
- [ ] **Step 3: RED** — `mvn -q -pl core -Dtest=LttbWindowFunctionTest test` fails.
- [ ] **Step 4: Widen `LttbAlgorithm` visibility** (class + `LttbAlgorithm(long)` ctor + `select` as needed) to public. No logic change. Compile.
- [ ] **Step 5: Implement the two factories.**
  - `LttbFunctionFactory`: `getSignature()="lttb(NDl)"`. newInstance validates ORDER BY / default frame / no PARTITION BY / numeric value (reproduce m4's message) / constant target `>= 2` (reuse the m4 validation shape). Returns `new M4FunctionFactory.BucketSelectWindowFunction(tsArg, valueArg, target, new LttbAlgorithm(0), NAME)`.
  - `LttbGapFunctionFactory`: `getSignature()="lttb(NDls)"`. Same validations + the gap: arg[3] must be a constant string; parse it to `gapMicros` via the SAME `TimestampSamplerFactory` calls the old cursor uses (read ~7157), reproducing the exact interval-parse errors/positions. Return `new BucketSelectWindowFunction(..., new LttbAlgorithm(gapMicros), NAME)`.
  - Both NAME=`"lttb"`, BOOLEAN. Confirm `BucketSelectWindowFunction` is reachable (it's the same package as M4FunctionFactory — window package; LttbAlgorithm is now public in the table package).
- [ ] **Step 6: GREEN + fill/lock** — run; fill deterministic expected from stable output (cross-check vs `SUBSAMPLE lttb(...)`); lock an EXPLAIN plan test; fix `.fails` positions to actual.
- [ ] **Step 7: Commit**
```bash
git add core/src/main/java/io/questdb/griffin/engine/functions/window/LttbFunctionFactory.java \
        core/src/main/java/io/questdb/griffin/engine/functions/window/LttbGapFunctionFactory.java \
        core/src/test/java/io/questdb/test/griffin/engine/window/LttbWindowFunctionTest.java \
        core/src/main/java/io/questdb/griffin/engine/table/LttbAlgorithm.java
git commit -m "feat(window): add lttb() value-inspecting keep-flag window function with optional gap"
```

---

## Task 2: `SUBSAMPLE lttb(...)` desugaring

**Files:** Modify `SqlOptimiser.java`; Test `SubsampleTest.java`.
**Interfaces:** Consumes `lttb` (Task 1).

- [ ] **Step 1: Baseline** — `SubsampleTest` green (148); list the ~84 lttb cases; classify migrate vs fall-through under the gate.
- [ ] **Step 2: Failing plan test** `testLttbDesugarsToWindowFilter` (fill later).
- [ ] **Step 3: Confirm current lttb plan is the old cursor.**
- [ ] **Step 4: Extend the value-inspecting gate for `lttb`.**
  - Migrate when: token=="lttb"; paramCount 2 or 3; designated ts; non-agg; value arg (arg[0]) is a bare column LITERAL (same rule m4/minmax now use); target (arg[1]) constant `>= 2` (reuse `isConstantUniformTarget`); and IF a 3rd arg (gap) is present, it is a CONSTANT string that parses to a valid positive interval (add an `isConstantLttbGap` helper mirroring `isConstantCadenceSeed`/the old cursor's parse; on non-constant/invalid gap → fall through so the cursor's exact error is preserved).
  - Build the node via `desugarValueInspectingSubsample` extended for an optional gap: 2-arg SUBSAMPLE → 3-arg window node (target,value,ts back-to-front); 3-arg SUBSAMPLE → 4-arg window node (gap,target,value,ts back-to-front). Route through the shared `desugarSubsample` tail. Keep uniform/cadence/m4/minmax paths unchanged.
- [ ] **Step 5: Lock the plan test** (paste actual rewritten lttb plan for both a 2-arg and a 3-arg/gap case).
- [ ] **Step 6: FULL SubsampleTest — oracle green.** All ~84 lttb cases byte-identical (migrated or fallen-through); other methods still green. Debug any divergence against the specific case — a gap-preserving or null case divergence likely means the gap parse or the inherited filtering is off; fix precisely, do not weaken tests.
- [ ] **Step 7: Broad sanity** `mvn -q -pl core -Dtest='SubsampleTest,LttbWindowFunctionTest,M4WindowFunctionTest,SqlOptimiserTest,WindowFunctionTest' test`.
- [ ] **Step 8: Commit** `feat(sql): desugar SUBSAMPLE lttb to the lttb keep-flag window function`.

---

## Self-Review

**Spec coverage:** lttb value-inspecting keep-flag window function with optional gap (Task 1) + desugaring migration (Task 2), reusing `BucketSelectWindowFunction` + `LttbAlgorithm` (no base/math re-copy). Gap parse reproduces the old cursor. Byte-identical oracle (~84 cases). TWO_PASS (needs global range/segments), consistent with the spec. Completes the five count/value algorithms; sdt is Phase 3.

**Placeholder scan:** `FILL_FROM_FIRST_RUN`/plan pastes are deliberate lock-the-output steps. The gap parse is specified by pointing at the authoritative old-cursor code to reproduce (exact, not vague). The value-arg LITERAL rule and gap-constant rule are concrete fall-through conditions driven by the oracle.

**Type consistency:** `lttb(NDl)`/`lttb(NDls)` → BOOLEAN; args ts,value,target[,gap] at getQuick(0,1,2,3); desugaring adds back-to-front; reuses `BucketSelectWindowFunction`, `desugarValueInspectingSubsample` (extended for the gap), and the shared `desugarSubsample` tail; `LttbAlgorithm` widened public.

## Execution Handoff
(Provided after user review.)
