# SUBSAMPLE-as-window Phase 4a: kill-switch + window-vs-cursor benchmark

> **Historical phase plan:** This body records the temporary A/B decision phase. The authoritative final state is the completed [Phase 5 plan](2026-07-22-subsample-window-phase5-delete-cursor.md): the cursor, kill-switch, max-row property, and obsolete A/B benchmarks are deleted.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Get the missing evidence to decide whether to delete the old `SubsampleRecordCursorFactory`: (1) add a default-on kill-switch config `cairo.subsample.window.enabled` gating the keep-flag-window migration (a genuine production safety valve AND the benchmark lever), then (2) benchmark the window-migrated path vs the old cursor end-to-end on identical queries. **No cursor is deleted in this phase** — the user chose "benchmark first, then decide."

**Architecture:** The five count/value methods (uniform, cadence, m4, minmax, lttb) have a cursor equivalent, so the kill-switch, when OFF, makes them fall through to the untouched cursor (their pre-migration behavior) — giving an apples-to-apples A/B on the same SQL. `sdt` has NO cursor, so it is UNAFFECTED by the switch (always migrates; the switch gates only the five). The benchmark constructs the engine twice (switch on / switch off) and times identical `SUBSAMPLE` queries across methods × row-counts × targets.

**Tech Stack:** Java, QuestDB griffin, JMH.

## Global Constraints

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`; build/test from `/home/nick/claude/wt/oss/subsample-fixes`.
- Config default = **true** (window path stays the default; the switch only exists to disable it). Mirror an existing default-true boolean flag end-to-end (`isGroupByPresizeEnabled` is a good template — grep ALL its sites).
- The switch gates ONLY the five count/value migrations in `rewriteSubsample`. `sdt`'s total gate is untouched (sdt always migrates; with the switch off it still migrates, since it has no cursor).
- Do NOT delete or alter the old cursor, the window functions, or the desugaring logic (beyond adding the one `if (windowEnabled)` guard around the five-method migration).
- Benchmark goes in `benchmarks/`, modeled on the existing `SubsampleSortFusionBenchmark.java`. JMH run needs the same `--add-exports` JVM args that benchmark uses.

**Commands:**
```bash
cd /home/nick/claude/wt/oss/subsample-fixes && export JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64
mvn -q -pl core -am compile -DskipTests
mvn -q -pl core -Dtest=SubsampleTest test
mvn -q -pl benchmarks -am package -DskipTests
```

---

## File Structure

- `core/.../cairo/CairoConfiguration.java` — new `boolean isSubsampleWindowEnabled();` (default true via the default impl).
- `core/.../cairo/DefaultCairoConfiguration.java` (+ any test default) — return true.
- `core/.../PropServerConfiguration.java` + `PropertyKey.java` — parse `cairo.subsample.window.enabled` (default true).
- `core/.../griffin/SqlOptimiser.java` — read the flag once in `rewriteSubsample`; guard the five-method migration (sdt unaffected).
- `core/.../test/.../griffin/SubsampleTest.java` — A/B test: same query, switch on == switch off (byte-identical), and switch-off plan shows the cursor ("Subsample"), switch-on shows the window filter.
- `benchmarks/.../SubsampleWindowVsCursorBenchmark.java` — the JMH A/B.

---

## Task 1: `cairo.subsample.window.enabled` kill-switch

**Files:** Modify `CairoConfiguration.java`, `DefaultCairoConfiguration.java` (+ test config default if separate), `PropServerConfiguration.java`, `PropertyKey.java`, `SqlOptimiser.java`; Test `SubsampleTest.java`.

**Interfaces:**
- Produces: `CairoConfiguration.isSubsampleWindowEnabled()` (default true); property `cairo.subsample.window.enabled`.

- [ ] **Step 1: Find the template.** `grep -rn "isGroupByPresizeEnabled\|group.by.presize\|GROUP_BY_PRESIZE" core/src/main/java` to enumerate every site a default-true boolean flag touches (interface, default impl, PropServerConfiguration parse, PropertyKey enum, any config-dump/reload). List them.
- [ ] **Step 2: Write the failing A/B test** `testSubsampleWindowKillSwitch` in `SubsampleTest.java`: using a config override that sets `isSubsampleWindowEnabled()`=false, assert `SELECT ts, price FROM x SUBSAMPLE m4(price, 4)` (a) returns byte-identical rows to the switch-on (default) run, and (b) its EXPLAIN plan contains the cursor "Subsample" node (not the window `__keep` filter), while the switch-on plan shows the window filter. Find how SubsampleTest / AbstractCairoTest overrides a boolean config (look for an existing `overrideProperty` / config-override idiom in the test base). Also assert `SUBSAMPLE sdt(price, 0.5)` STILL migrates (window plan) with the switch off — sdt is unaffected.
- [ ] **Step 3: RED** — compile/test fails (method doesn't exist).
- [ ] **Step 4: Add the config** across all sites from Step 1, default true. Property name `cairo.subsample.window.enabled`.
- [ ] **Step 5: Guard the migration in `rewriteSubsample`.** Read `configuration.isSubsampleWindowEnabled()` once (reach the config via the execution context, the way other optimiser methods read config). Wrap ONLY the five count/value migration arms (uniform/cadence/m4/minmax/lttb) so that when the flag is false they DON'T migrate (fall through → cursor). Leave the `sdt` branch's total gate exactly as-is (sdt migrates regardless).
- [ ] **Step 6: GREEN** — `mvn -q -pl core -Dtest=SubsampleTest test`. The A/B test passes; all 161 prior cases still green (default true → unchanged).
- [ ] **Step 7: Commit** `feat(sql): add cairo.subsample.window.enabled kill-switch (default on) gating keep-flag-window migration`.

---

## Task 2: window-vs-cursor benchmark

**Files:** Create `benchmarks/src/main/java/org/questdb/SubsampleWindowVsCursorBenchmark.java`.
**Interfaces:** Consumes the Task 1 kill-switch.

- [ ] **Step 1: Model on the existing benchmark.** Read `benchmarks/src/main/java/org/questdb/SubsampleSortFusionBenchmark.java` for the engine setup, data generation, compile/execute idiom, and the exact `--add-exports` JVM args.
- [ ] **Step 2: Write the JMH benchmark.** Params: `@Param method` ∈ {uniform, cadence, m4, minmax, lttb}; `@Param rows` ∈ {100_000, 1_000_000, 10_000_000}; `@Param target` ∈ {500, 4}. Two states: window (config `isSubsampleWindowEnabled`=true) vs cursor (=false) — construct the engine/config accordingly (a `@Param boolean windowEnabled` driving a `DefaultCairoConfiguration` override, or two `@Setup` engines). Each iteration: compile once in setup, execute the `SUBSAMPLE <method>(value, target)` query and fully drain the cursor (count rows) so the work isn't elided. Generate a realistic ts+value table (monotonic ts, noisy double value) once per trial. Include a correctness guard: assert window and cursor return the same row count for a given param combo (sanity, outside the timed loop).
- [ ] **Step 3: Build** `mvn -q -pl benchmarks -am package -DskipTests`.
- [ ] **Step 4: Run** a short but meaningful config (e.g. `-f 1 -wi 3 -i 5`) across the param grid; capture the results table. (If 10M×full-grid is too slow for a first pass, run {100k, 1M} fully and note 10M as a follow-up single-point run.)
- [ ] **Step 5: Record results** in the report: a window-vs-cursor table (ms or ops/s) per method × rows × target, with the speedup ratio, and a one-paragraph read of where the window path wins/loses vs the cursor (recall the earlier fusion finding: serial-sort fused wins ≤5M, engine parallel-sort wins ≥10M — this benchmark tests the REAL migrated path, which uses the engine sort).
- [ ] **Step 6: Commit** `bench: add SUBSAMPLE window-vs-cursor JMH benchmark + results`.

---

## Self-Review

**Spec coverage:** kill-switch (Task 1) + A/B benchmark (Task 2) deliver the "benchmark first, then decide" evidence without deleting anything. The switch is a real production safety valve (default on) and the benchmark lever. sdt correctly exempt (no cursor).

**Placeholder scan:** benchmark param grid + run config are concrete; results are a deliberate fill-from-run step. The config sites are enumerated by mirroring an existing flag (authoritative), not guessed.

**Type consistency:** `isSubsampleWindowEnabled()` boolean, default true; guards only the five count/value arms; sdt total gate untouched. Benchmark drains cursors (no dead-code elision) and cross-checks row counts for correctness.

## Execution Handoff
(Provided after user review.)
