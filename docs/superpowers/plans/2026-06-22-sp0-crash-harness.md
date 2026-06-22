# SP0 — Crash-Consistency Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an in-process, deterministic, CI-gating fault-injection harness that simulates power loss (truncate-to-last-fsync) so every later corruption fix lands with a red-before/green-after crash test.

**Architecture:** A `CrashFaultFilesFacade` (subclass of `TestFilesFacadeImpl`) tracks, per file, the size that is durable (advanced only by `fsync`/`fsyncAndClose`, never by `msync`). `crash()` truncates every file under the db root to its durable size; `tornTail()` zeroes a chosen sub-range for deterministic recovery-validation tests. An `AbstractCrashConsistencyTest` injects the facade via the existing `assertMemoryLeak(ff, code)` overload (which routes through `StaticOverrides.getFilesFacade()` → `AbstractCairoTest.ff`) and provides crash/reopen/assert helpers. Two validation tests prove the harness reproduces the already-fixed VARCHAR bug and quantifies the P2 (msync-without-fsync) gap.

**Tech Stack:** Java, JUnit4, QuestDB test framework (`AbstractCairoTest`, `TestFilesFacadeImpl`), Maven (`mvn test -pl core`). Crash/baseline file walks use `java.nio.file` directly (test-only, simplest).

**Spec:** `docs/superpowers/specs/2026-06-22-crash-consistency-design.md` (§3 = SP0).

---

## File structure

- Create `core/src/test/java/io/questdb/test/cairo/crash/CrashSimulationError.java` — unwind signal (extends `Error`).
- Create `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java` — the fault-injection `FilesFacade`.
- Create `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacadeTest.java` — unit tests for the facade in isolation (no engine).
- Create `core/src/test/java/io/questdb/test/cairo/crash/AbstractCrashConsistencyTest.java` — reusable base for engine-level crash tests.
- Create `core/src/test/java/io/questdb/test/cairo/crash/VarcharCrashConsistencyTest.java` — harness proof against the shipped fix.
- Create `core/src/test/java/io/questdb/test/cairo/crash/Phase2DurabilityProbeTest.java` — P2 quantifier (asserts current buggy SYNC behaviour).

---

## Task 1: CrashSimulationError + facade skeleton with fsync-tracked durable size

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/crash/CrashSimulationError.java`
- Create: `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java`
- Test: `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacadeTest.java`

- [ ] **Step 1: Write the failing test**

Create `CrashFaultFilesFacadeTest.java`:

```java
package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class CrashFaultFilesFacadeTest extends AbstractTest {

    @Test
    public void testCrashTruncatesToLastFsyncedSize() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("a.d")) {
            long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 32, (byte) 1);
                // write 16 bytes, fsync -> durable size = 16
                Assert.assertEquals(16, ff.write(fd, buf, 16, 0));
                ff.fsync(fd);
                // write 16 more bytes, NO fsync -> durable size stays 16
                Assert.assertEquals(16, ff.write(fd, buf, 16, 16));
                Assert.assertEquals(32, ff.length(fd));
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            // crash: file must be rolled back to the last fsynced size (16)
            ff.crash(dir);
            Assert.assertEquals(16, ff.length(path.$()));
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=CrashFaultFilesFacadeTest#testCrashTruncatesToLastFsyncedSize -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|ERROR.*COMPIL|cannot find symbol"`
Expected: COMPILATION ERROR — `CrashFaultFilesFacade` / `crash` do not exist.

- [ ] **Step 3: Write minimal implementation**

Create `CrashSimulationError.java`:

```java
package io.questdb.test.cairo.crash;

/**
 * Thrown by {@link CrashFaultFilesFacade} to unwind the stack at a chosen durability op,
 * simulating a power loss mid-commit. Extends Error (not Exception) so production
 * {@code catch (CairoException)} / {@code catch (Throwable)} handlers do not absorb it;
 * only the harness driver catches it.
 */
public class CrashSimulationError extends Error {
    public CrashSimulationError(int durabilityOp) {
        super("simulated crash at durability op " + durabilityOp);
    }
}
```

Create `CrashFaultFilesFacade.java`:

```java
package io.questdb.test.cairo.crash;

import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Fault-injection FilesFacade that models the OS durability contract:
 * msync flushes data pages but only fsync makes an extended file's size durable.
 * On {@link #crash}, every file is truncated to its last-fsynced size.
 */
public class CrashFaultFilesFacade extends TestFilesFacadeImpl {
    private final Map<Long, String> fdToPath = new HashMap<>();
    private final Map<String, Long> durableSize = new HashMap<>();

    @Override
    public long openRW(LPSZ name, int opts) {
        long fd = super.openRW(name, opts);
        if (fd > -1) {
            fdToPath.put(fd, Utf8s.toString(name));
        }
        return fd;
    }

    @Override
    public long openRO(LPSZ name) {
        long fd = super.openRO(name);
        if (fd > -1) {
            fdToPath.put(fd, Utf8s.toString(name));
        }
        return fd;
    }

    @Override
    public boolean close(long fd) {
        fdToPath.remove(fd);
        return super.close(fd);
    }

    @Override
    public void fsync(long fd) {
        super.fsync(fd);
        recordDurable(fd);
    }

    @Override
    public void fsyncAndClose(long fd) {
        recordDurable(fd); // fd still valid here
        super.fsyncAndClose(fd);
        fdToPath.remove(fd);
    }

    /** Roll every file under {@code dbRoot} back to its last-fsynced size. */
    public void crash(CharSequence dbRoot) {
        walk(dbRoot, p -> {
            String key = p.toAbsolutePath().toString();
            Long durable = durableSize.get(key);
            long target = durable != null ? durable : 0L; // never fsynced since baseline -> not durable
            try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE)) {
                if (ch.size() > target) {
                    ch.truncate(target);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void recordDurable(long fd) {
        String p = fdToPath.get(fd);
        if (p != null) {
            durableSize.put(p, super.length(fd));
        }
    }

    private static void walk(CharSequence dbRoot, java.util.function.Consumer<Path> fileFn) {
        Path root = Paths.get(dbRoot.toString());
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> s = Files.walk(root)) {
            s.filter(Files::isRegularFile).forEach(fileFn);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=CrashFaultFilesFacadeTest#testCrashTruncatesToLastFsyncedSize -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD"`
Expected: `Tests run: 1, Failures: 0, Errors: 0` / `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/varchar-corruption
git add core/src/test/java/io/questdb/test/cairo/crash/CrashSimulationError.java core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacadeTest.java
git commit -m "test(core): crash-fault FilesFacade with fsync-tracked durable size"
```

---

## Task 2: markDurableBaseline + tornTail

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java`
- Test: `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacadeTest.java`

- [ ] **Step 1: Write the failing test**

Add to `CrashFaultFilesFacadeTest`:

```java
@Test
public void testBaselineKeepsPriorDataAndTornTailZeroesRange() throws Exception {
    final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
    final String dir = temp.newFolder("crashroot2").getAbsolutePath();
    try (io.questdb.std.str.Path path = new io.questdb.std.str.Path().of(dir).concat("b.d")) {
        long fd = ff.openRW(path.$(), io.questdb.cairo.CairoConfiguration.O_NONE);
        long buf = io.questdb.std.Unsafe.malloc(64, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        try {
            io.questdb.std.Unsafe.getUnsafe().setMemory(buf, 64, (byte) 7);
            ff.write(fd, buf, 64, 0); // 64 bytes, never fsynced
        } finally {
            io.questdb.std.Unsafe.free(buf, 64, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
        // baseline: treat the current 64 bytes as durable even though never fsynced
        ff.markDurableBaseline(dir);
        // tornTail: zero bytes [60,64)
        ff.tornTail(path.$(), 60, 4);
        ff.crash(dir);
        Assert.assertEquals("baseline size preserved", 64, ff.length(path.$()));
        // verify the torn range is zeroed and the rest intact
        long rd = ff.openRO(path.$());
        long rb = io.questdb.std.Unsafe.malloc(64, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(64, ff.read(rd, rb, 64, 0));
            Assert.assertEquals((byte) 7, io.questdb.std.Unsafe.getUnsafe().getByte(rb + 59));
            Assert.assertEquals((byte) 0, io.questdb.std.Unsafe.getUnsafe().getByte(rb + 60));
            Assert.assertEquals((byte) 0, io.questdb.std.Unsafe.getUnsafe().getByte(rb + 63));
        } finally {
            io.questdb.std.Unsafe.free(rb, 64, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            ff.close(rd);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=CrashFaultFilesFacadeTest#testBaselineKeepsPriorDataAndTornTailZeroesRange -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|cannot find symbol"`
Expected: COMPILATION ERROR — `markDurableBaseline` / `tornTail` missing.

- [ ] **Step 3: Write minimal implementation**

Add to `CrashFaultFilesFacade` (a `tornTail` queue applied during `crash`):

```java
    private final java.util.List<long[]> tornRanges = new java.util.ArrayList<>(); // unused placeholder removed below
```

Replace that line — instead add these fields and methods (and apply torn ranges inside `crash`):

```java
    // path -> list of {offset,len} ranges to zero during crash()
    private final Map<String, java.util.List<long[]>> tornTails = new HashMap<>();

    /** Record current sizes of all files under dbRoot as durable ("prior committed, long journaled"). */
    public void markDurableBaseline(CharSequence dbRoot) {
        Path root = Paths.get(dbRoot.toString());
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> s = Files.walk(root)) {
            s.filter(Files::isRegularFile).forEach(p -> {
                try {
                    durableSize.put(p.toAbsolutePath().toString(), Files.size(p));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Zero [offset, offset+len) of the given file when crash() runs (deterministic torn-write injection). */
    public void tornTail(LPSZ name, long offset, long len) {
        tornTails.computeIfAbsent(Utf8s.toString(name), k -> new java.util.ArrayList<>())
                .add(new long[]{offset, len});
    }
```

Then update `crash()` to apply torn ranges after truncation:

```java
    public void crash(CharSequence dbRoot) {
        walk(dbRoot, p -> {
            String key = p.toAbsolutePath().toString();
            Long durable = durableSize.get(key);
            long target = durable != null ? durable : 0L;
            try (FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
                if (ch.size() > target) {
                    ch.truncate(target);
                }
                java.util.List<long[]> ranges = tornTails.get(key);
                if (ranges != null) {
                    java.nio.ByteBuffer zeros = null;
                    for (long[] r : ranges) {
                        int n = (int) r[1];
                        if (zeros == null || zeros.capacity() < n) {
                            zeros = java.nio.ByteBuffer.allocate(n);
                        }
                        zeros.clear();
                        zeros.limit(n);
                        ch.write(zeros, r[0]);
                    }
                    ch.force(true);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
```

Remove the stray `tornRanges` placeholder line if it was added.

- [ ] **Step 4: Run test to verify it passes**

Run both facade tests: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=CrashFaultFilesFacadeTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD"`
Expected: `Tests run: 2, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/varchar-corruption
git add core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacadeTest.java
git commit -m "test(core): add markDurableBaseline + tornTail to crash facade"
```

---

## Task 3: AbstractCrashConsistencyTest base + crashAndReopen + Bar-1/Bar-2 asserts

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/crash/AbstractCrashConsistencyTest.java`
- Test: itself, exercised by Task 5/6 (this task adds a tiny self-test inside it).

- [ ] **Step 1: Write the failing test**

Create `AbstractCrashConsistencyTest.java` with a self-test method:

```java
package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCrashConsistencyTest extends AbstractCairoTest {

    protected CrashFaultFilesFacade crashFf;

    /** Run {@code body} with the crash facade installed as the engine's FilesFacade. */
    protected void runWithCrashFacade(TestUtils.LeakProneCode body) throws Exception {
        crashFf = new CrashFaultFilesFacade();
        assertMemoryLeak(crashFf, body);
    }

    /** Mark everything committed so far as durable (prior, long-journaled state). */
    protected void markDurableBaseline() {
        crashFf.markDurableBaseline(engine.getConfiguration().getDbRoot());
    }

    /**
     * Simulate power loss: release handles (clean close never fsyncs) then roll files back.
     * No explicit reload needed — the next reader/writer reopens from disk (proven by the
     * shipped VarcharPowerLossCorruptionTest, which uses exactly releaseAllReaders/Writers).
     */
    protected void crashAndReopen() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        crashFf.crash(engine.getConfiguration().getDbRoot());
    }

    /** Bar 1: every expected row reads back equal, OR a CairoException/CairoError is raised. Never silent garbage. */
    protected void assertNoSilentCorruption(String tableName, String column, List<String> expected) {
        try {
            List<String> actual = readColumn(tableName, column);
            int n = Math.min(actual.size(), expected.size());
            for (int i = 0; i < n; i++) {
                Assert.assertEquals("row " + i + " silently wrong", expected.get(i), actual.get(i));
            }
        } catch (CairoException | CairoError e) {
            // acceptable: corruption detected loudly
        }
    }

    /** Bar 2: every committed row present and correct after crash. */
    protected void assertSyncDurable(String tableName, String column, List<String> expected) {
        List<String> actual = readColumn(tableName, column);
        Assert.assertEquals("row count after SYNC crash", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals("row " + i, expected.get(i), actual.get(i));
        }
    }

    protected List<String> readColumn(String tableName, String column) {
        List<String> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select " + column + " from " + tableName)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Record r = c.getRecord();
                while (c.hasNext()) {
                    CharSequence v = r.getStrA(0);
                    out.add(v == null ? null : v.toString());
                }
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    @Test
    public void testHarnessSelfCheck() throws Exception {
        runWithCrashFacade(() -> {
            execute("create table t (ts timestamp, s string) timestamp(ts) partition by none");
            execute("insert into t values (0, 'hello-world-0000')");
            markDurableBaseline();
            crashAndReopen();
            // baseline row survives a crash
            assertSyncDurable("t", "s", java.util.List.of("hello-world-0000"));
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=AbstractCrashConsistencyTest#testHarnessSelfCheck -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|cannot find symbol"`
Expected: PASS once the class compiles (the self-check seeds one row, baselines, crashes, and asserts the row survives — it does, because a fsync'd-or-baselined file isn't truncated). If it does not compile, fix imports only; the reopen path needs no reload call (confirmed against the shipped test).

- [ ] **Step 3: Write minimal implementation**

The class above IS the implementation. If Step 2 surfaced an API mismatch on the reopen line, fix `crashAndReopen()` accordingly (the only line in question). The `select(...)`, `execute(...)`, `assertMemoryLeak(ff, code)`, and `engine` members are all inherited from `AbstractCairoTest` (verified). `getStrA` reads STRING columns; varchar columns are read via a type-aware override added in Task 5.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=AbstractCrashConsistencyTest#testHarnessSelfCheck -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD"`
Expected: `Tests run: 1, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/varchar-corruption
git add core/src/test/java/io/questdb/test/cairo/crash/AbstractCrashConsistencyTest.java
git commit -m "test(core): AbstractCrashConsistencyTest base with crash/reopen + bar asserts"
```

---

## Task 4: Exhaustive crash-point driver

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java`
- Modify: `core/src/test/java/io/questdb/test/cairo/crash/AbstractCrashConsistencyTest.java`
- Test: `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacadeTest.java`

- [ ] **Step 1: Write the failing test**

Add to `CrashFaultFilesFacadeTest`:

```java
@Test
public void testArmCrashThrowsAfterNthDurabilityOp() throws Exception {
    final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
    final String dir = temp.newFolder("crashroot3").getAbsolutePath();
    try (io.questdb.std.str.Path path = new io.questdb.std.str.Path().of(dir).concat("c.d")) {
        long fd = ff.openRW(path.$(), io.questdb.cairo.CairoConfiguration.O_NONE);
        ff.armCrashAt(2); // crash on the 2nd durability op
        long buf = io.questdb.std.Unsafe.malloc(8, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        try {
            ff.fsync(fd); // op 1
            try {
                ff.fsync(fd); // op 2 -> throws
                Assert.fail("expected CrashSimulationError");
            } catch (CrashSimulationError expected) {
                Assert.assertEquals(2, ff.durabilityOpCount());
            }
        } finally {
            io.questdb.std.Unsafe.free(buf, 8, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=CrashFaultFilesFacadeTest#testArmCrashThrowsAfterNthDurabilityOp -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|cannot find symbol"`
Expected: COMPILATION ERROR — `armCrashAt` / `durabilityOpCount` missing.

- [ ] **Step 3: Write minimal implementation**

Add to `CrashFaultFilesFacade` the counter + arming, and call `bumpDurabilityOp()` from `fsync`, `fsyncAndClose`, and `msync`:

```java
    private int durabilityOps = 0;
    private int crashAtOp = -1;

    public void armCrashAt(int n) {
        this.crashAtOp = n;
    }

    public int durabilityOpCount() {
        return durabilityOps;
    }

    private void bumpDurabilityOp() {
        durabilityOps++;
        if (crashAtOp > 0 && durabilityOps >= crashAtOp) {
            crashAtOp = -1;
            throw new CrashSimulationError(durabilityOps);
        }
    }
```

Add `bumpDurabilityOp()` as the LAST statement of `fsync` and `fsyncAndClose`, and override `msync` (which must NOT advance durableSize, only the op counter):

```java
    @Override
    public void msync(long addr, long len, boolean async) {
        super.msync(addr, len, async);
        bumpDurabilityOp();
    }
```

(Insert `bumpDurabilityOp();` at the end of the existing `fsync` and `fsyncAndClose` bodies.)

Then add the exhaustive driver to `AbstractCrashConsistencyTest`:

```java
    /**
     * Re-run {@code workload} crashing after each successive durability op (1..N) and run
     * {@code assertion} on the reopened engine each time, until the workload completes untripped.
     * Finds the worst crash window (proves ordering fixes).
     */
    protected void forEachCrashPoint(Runnable seed, Runnable workload, Runnable assertion, int maxPoints) {
        for (int k = 1; k <= maxPoints; k++) {
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.clear();
            seed.run();
            markDurableBaseline();
            boolean tripped = false;
            crashFf.armCrashAt(k);
            try {
                workload.run();
            } catch (CrashSimulationError e) {
                tripped = true;
            } finally {
                crashFf.armCrashAt(-1);
            }
            crashAndReopen();
            assertion.run();
            if (!tripped) {
                LOG.info().$("forEachCrashPoint: workload completed untripped at k=").$(k).$();
                return;
            }
        }
        Assert.fail("forEachCrashPoint exhausted " + maxPoints + " points without completing the workload");
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=CrashFaultFilesFacadeTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD"`
Expected: `Tests run: 3, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/varchar-corruption
git add core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java core/src/test/java/io/questdb/test/cairo/crash/AbstractCrashConsistencyTest.java core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacadeTest.java
git commit -m "test(core): exhaustive crash-point driver (armCrashAt + forEachCrashPoint)"
```

---

## Task 5: VarcharCrashConsistencyTest — harness proof against the shipped fix

**Files:**
- Modify: `core/src/test/java/io/questdb/test/cairo/crash/AbstractCrashConsistencyTest.java` (add varchar-aware read)
- Create: `core/src/test/java/io/questdb/test/cairo/crash/VarcharCrashConsistencyTest.java`

- [ ] **Step 1: Write the failing test**

Add a varchar-aware reader override to `AbstractCrashConsistencyTest` (varchar columns need `getVarcharA`):

```java
    protected List<String> readVarcharColumn(String tableName, String column) {
        List<String> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select " + column + " from " + tableName)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                Record r = c.getRecord();
                while (c.hasNext()) {
                    io.questdb.std.str.Utf8Sequence v = r.getVarcharA(0);
                    out.add(v == null ? null : v.toString());
                }
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }
```

Create `VarcharCrashConsistencyTest.java`:

```java
package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Proves the harness reproduces the already-fixed VARCHAR torn-aux bug: zeroing the last aux
 * entry's offset bytes via tornTail and reopening must NOT silently overwrite committed rows.
 * Green on HEAD (guard throws); would be RED on pre-fix 7e861e7239 (silent overwrite).
 */
public class VarcharCrashConsistencyTest extends AbstractCrashConsistencyTest {

    private static final String SPLIT = "AAAABBBBCCCCDDDDEEEE"; // 20 bytes -> split

    @Test
    public void testTornLastAuxEntryNeverSilentlyCorrupts() throws Exception {
        runWithCrashFacade(() -> {
            final int rows = 10;
            execute("create table v (ts timestamp, x varchar) timestamp(ts) partition by none");
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                String val = "row" + String.format("%02d", i) + SPLIT;
                execute("insert into v values (" + (i * 1_000_000L) + ", '" + val + "')");
                expected.add(val);
            }
            markDurableBaseline();

            // queue torn-tail: zero bytes 8-15 of the last aux entry (the shipped-bug trigger)
            TableToken tt = engine.verifyTableName("v");
            try (Path aux = new Path()) {
                aux.of(engine.getConfiguration().getDbRoot()).concat(tt)
                        .concat(TableUtils.DEFAULT_PARTITION_NAME).slash();
                TableUtils.iFile(aux, "x", TableUtils.COLUMN_NAME_TXN_NONE);
                long base = (long) (rows - 1) * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                crashFf.tornTail(aux.$(), base + 8L, 8L);
            }

            crashAndReopen();

            // attempt an append (drives setAppendPosition recovery) — guard should throw, not corrupt
            boolean detected = false;
            try {
                execute("insert into v values (" + (rows * 1_000_000L) + ", 'newrow" + SPLIT + "')");
            } catch (CairoException | CairoError e) {
                detected = true;
            }
            engine.releaseAllWriters();

            // row 0 must never be silently wrong regardless of detection
            List<String> actual = readVarcharColumn("v", "x");
            if (!actual.isEmpty()) {
                Assert.assertEquals("row 0 silently corrupted", expected.get(0), actual.get(0));
            }
            Assert.assertTrue("torn last aux entry must be detected on reopen", detected);
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails (then passes on HEAD)**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=VarcharCrashConsistencyTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD|cannot find symbol"`
Expected on HEAD: `Tests run: 1, Failures: 0` (the shipped guard makes it green). If it fails to compile, fix imports. To confirm the harness has teeth, optionally `git stash` is NOT needed — the existing `VarcharPowerLossCorruptionTest` already proves red-on-revert; this test proves the *harness path* reaches the same guard.

- [ ] **Step 3: (no separate implementation)** — the test exercises shipped code; if it is red on HEAD, that is a harness bug to fix in `crashAndReopen`/`tornTail`, not a product change.

- [ ] **Step 4: Run to verify pass**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=VarcharCrashConsistencyTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD"`
Expected: `Tests run: 1, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
cd ~/claude/wt/oss/varchar-corruption
git add core/src/test/java/io/questdb/test/cairo/crash/AbstractCrashConsistencyTest.java core/src/test/java/io/questdb/test/cairo/crash/VarcharCrashConsistencyTest.java
git commit -m "test(core): varchar crash-consistency test proving the harness path"
```

---

## Task 6: Phase2DurabilityProbeTest — quantify the P2 (msync-without-fsync) gap

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/crash/Phase2DurabilityProbeTest.java`

- [ ] **Step 1: Write the test (asserts CURRENT buggy behaviour so CI stays green)**

```java
package io.questdb.test.cairo.crash;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Quantifies P2: in SYNC mode the engine msyncs but never fsyncs, so a file extend's size is
 * not journaled and a crash after a committed transaction can lose it. This probe asserts the
 * CURRENT (buggy) behaviour — the just-committed extra rows are LOST after crash — so CI stays
 * green. SP2 (Phase B fdatasync-on-extend) will INVERT this to assertSyncDurable(full).
 */
public class Phase2DurabilityProbeTest extends AbstractCrashConsistencyTest {

    @Test
    public void testSyncCommitLosesExtendOnCrash_currentBehaviour() throws Exception {
        // force SYNC commit mode (established idiom; Overrides has no setCommitMode).
        // Must be set before the engine opens the table, i.e. before runWithCrashFacade's body runs.
        setProperty(io.questdb.PropertyKey.CAIRO_COMMIT_MODE, "sync");
        try {
            runWithCrashFacade(() -> {
                execute("create table p (ts timestamp, s string) timestamp(ts) partition by none");
                execute("insert into p values (0, 'seed-row-000000')");
                markDurableBaseline(); // seed is "old, journaled"

                // a fresh SYNC-committed transaction that GROWS the files
                final int extra = 200;
                List<String> all = new ArrayList<>();
                all.add("seed-row-000000");
                for (int i = 1; i <= extra; i++) {
                    String v = "grow-row-" + String.format("%06d", i);
                    execute("insert into p values (" + (i * 1_000_000L) + ", '" + v + "')");
                    all.add(v);
                }

                crashAndReopen();

                List<String> actual = readColumn("p", "s");
                // CURRENT behaviour: msync'd-but-not-fsync'd extend is lost -> fewer than all rows.
                // (SP2 will change this assertion to: Assert.assertEquals(all.size(), actual.size()).)
                Assert.assertTrue(
                        "P2 probe: expected SYNC extend to be lost on crash (current behaviour); got "
                                + actual.size() + " of " + all.size(),
                        actual.size() < all.size());
                // containment still holds: whatever survived is a correct prefix
                for (int i = 0; i < actual.size(); i++) {
                    Assert.assertEquals(all.get(i), actual.get(i));
                }
            });
        } finally {
            setProperty(io.questdb.PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        }
    }
}
```

- [ ] **Step 2: Run test**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=Phase2DurabilityProbeTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD|cannot find symbol"`
Expected: `Tests run: 1, Failures: 0`. `setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync")` is the established idiom (used by `PGCommitFailureTest`, `AlterTableConvertPartitionTest`). If the probe shows NO loss (actual.size() == all.size()), the model truncation is not engaging for this schema — confirm the data `.d`/`.i` files actually grew past baseline and that `markDurableBaseline()` ran after only the seed row.

- [ ] **Step 3: (no implementation)** — probe documents current behaviour; no product change here.

- [ ] **Step 4: Re-run to confirm green**

Run: `cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=Phase2DurabilityProbeTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD"`
Expected: `Tests run: 1, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit + run the whole crash package**

```bash
cd ~/claude/wt/oss/varchar-corruption
mvn test -pl core -Dtest="io.questdb.test.cairo.crash.*" -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD"
git add core/src/test/java/io/questdb/test/cairo/crash/Phase2DurabilityProbeTest.java
git commit -m "test(core): P2 durability probe quantifying msync-without-fsync gap"
```

Expected final: all crash-package tests green (`Tests run: 7+, Failures: 0`).

---

## Self-review notes (resolved)

- **Spec coverage:** SP0 §3.1 components → Tasks 1–6; §3.2 model (baseline/fsync/msync/crash/tornTail/dirLost) → Tasks 1–2 (dirLost deferred: not needed until SP2/P4 tests — add then); §3.3 fixed-point → Task 3, exhaustive → Task 4; §3.4 Bar 1/2 asserts → Task 3 (Bar 3 `assertNosyncContainment` deferred to SP3 when first needed); §3.5 done-criteria → Tasks 5 (varchar proof) + 6 (P2 probe).
- **Deferred-by-design (YAGNI):** `dirLost` (P4) and `assertNosyncContainment` (Bar 3) are added in the SP that first needs them, to avoid untested dead code now. Noted so SP2/SP3 plans pick them up.
- **API-risk lines flagged inline with fallbacks:** the engine reopen call in `crashAndReopen` (Task 3) and `setCommitMode` (Task 6) each carry a verification grep + fallback, since those are the two spots most likely to differ from assumption.
- **Type consistency:** `crashFf`, `crash(dbRoot)`, `markDurableBaseline(dbRoot)`, `tornTail(LPSZ,off,len)`, `armCrashAt(int)`, `durabilityOpCount()`, `forEachCrashPoint`, `readColumn`/`readVarcharColumn`, `assertNoSilentCorruption`/`assertSyncDurable` are used consistently across tasks.
