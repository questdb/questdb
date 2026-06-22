/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewCheckpointBlockType;
import io.questdb.cairo.lv.LiveViewCheckpointManifest;
import io.questdb.cairo.lv.LiveViewCheckpointReader;
import io.questdb.cairo.lv.LiveViewCheckpointWriter;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewFunctionSnapshot;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.mp.Job;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Smoke tests: confirm the new CREATE LIVE VIEW syntax (FLUSH EVERY,
 * IN MEMORY, PARTITION BY, BACKFILL reject) is parsed and validated, and that
 * creating + dropping a live view goes through the engine end-to-end.
 * <p>
 * Asserted-wording validation tests will go in a dedicated file once the full
 * suite is rewritten in delta plan task #8.
 */
public class LiveViewSmokeTest extends AbstractCairoTest {

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    // Drives the named view's backfill sweep to completion across however many
    // turns the configured budget needs (each drainJob burst is capped at 64
    // turns), reusing the caller's refresh job. Re-fetches the instance each
    // pass so it survives a simulated restart that rebuilds the registry, and
    // applies the LV WAL at the end. The caller owns the job's lifecycle: a
    // backfill test must drive the whole sweep through a single
    // LiveViewRefreshJob, since tearing one down mid-sweep and resuming on a
    // fresh one is not a path production takes (the pool keeps jobs alive).
    private void driveBackfillToCompletion(LiveViewRefreshJob job, String viewName) {
        for (int i = 0; i < 500; i++) {
            LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance(viewName);
            if (inst == null
                    || inst.getStateReader().getBackfillState() != LiveViewState.BACKFILL_STATE_BACKFILLING) {
                break;
            }
            drainJob(job);
        }
        drainWalQueue();
    }

    // Removes the view's rolling backfill checkpoint file, simulating a crash
    // before the .bcp the latest committed turn would have written (or a view
    // whose functions cannot snapshot). Recovery then has no resume source and
    // re-sweeps from offset 0, skip-writing the on-disk prefix.
    private void unlinkBackfillCheckpointFile(LiveViewInstance instance) {
        long key = instance.getHeadBackfillCpKey();
        if (key == Numbers.LONG_NULL) {
            return;
        }
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot())
                    .concat(instance.getLiveViewToken())
                    .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                    .slash();
            LiveViewCheckpointWriter.appendBcpFileName(p, key);
            engine.getConfiguration().getFilesFacade().removeQuiet(p.$());
        }
    }

    // Walks the LV's compiled factory to its WindowRecordCursorFactory and
    // returns its window function list. Mirrors the unwrap logic in
    // LiveViewRefreshJob; tests use this to reach non-anchored windows which
    // do not show up via LiveViewInstance.getAnchorWindow().
    private static ObjList<WindowFunction> unwrapWindowFunctions(LiveViewInstance instance) {
        RecordCursorFactory f = instance.getCompiledFactory();
        while (f != null) {
            if (f instanceof WindowRecordCursorFactory wf) {
                return wf.getWindowFunctions();
            }
            if (f instanceof QueryProgress) {
                f = f.getBaseFactory();
                continue;
            }
            break;
        }
        throw new IllegalStateException("compiled factory does not contain a WindowRecordCursorFactory");
    }

    // Drives a partitioned bounded-frame avg(DECIMAL) live view: asserts the
    // windowed average is correct (also proving CREATE-accept), then performs a
    // byte-exact snapshot/restore round-trip of the function's partition state
    // (write -> toTop -> restore -> write again, comparing the two payloads).
    private void assertAvgDecimalFrameRoundTrip(
            String decimalType,
            String frameClause,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d " + decimalType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts " + frameClause + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Drives a partitioned bounded RANGE first_value(DECIMAL) frame live view:
    // asserts the windowed first value is correct (also proving CREATE-accept),
    // then performs a byte-exact snapshot/restore round-trip of the function's
    // partition state (write -> toTop -> restore -> write again, comparing the
    // two payloads). Covers both RESPECT NULLS (FirstValue base) and IGNORE NULLS
    // (FirstNotNull subclass, full physical-ring serialization).
    private void assertFirstValueDecimalFrameRoundTrip(
            boolean ignoreNulls,
            String decimalType,
            String frameClause,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d " + decimalType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, first_value(d)" + (ignoreNulls ? " IGNORE NULLS" : "") + " OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts " + frameClause + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Drives a partitioned unbounded-preceding (anchor) first_value(DECIMAL) live
    // view: asserts the captured first value is correct (also proving
    // CREATE-accept), then performs a byte-exact snapshot/restore round-trip of
    // the function's partition state (write -> toTop -> restore -> write again,
    // comparing the two payloads). Covers both RESPECT NULLS and IGNORE NULLS.
    private void assertFirstValueDecimalUnboundedRoundTrip(
            boolean ignoreNulls,
            String decimalType,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d " + decimalType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, first_value(d)" + (ignoreNulls ? " IGNORE NULLS" : "") + " OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertMaxMinDecimalFrameRoundTrip(
            String fnName,
            String decimalType,
            String frameClause,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d " + decimalType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " + fnName + "(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts " + frameClause + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Drives a partitioned bounded-frame first_value(TIMESTAMP|DATE) live view
    // (RESPECT or IGNORE NULLS): asserts the windowed value is correct (also
    // proving CREATE-accept), then performs a byte-exact snapshot/restore
    // round-trip of the function's partition state.
    private void assertFirstValueTimestampDateFrameRoundTrip(
            String valueType,
            String frameClause,
            boolean ignoreNulls,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v " + valueType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, first_value(v)" + (ignoreNulls ? " IGNORE NULLS" : "") + " OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts " + frameClause + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, v) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Same as assertFirstValueTimestampDateFrameRoundTrip but for the
    // unbounded-preceding (anchored) shape; the function lives on the anchor
    // window, not the main factory.
    private void assertFirstValueTimestampDateUnboundedRoundTrip(
            String valueType,
            boolean ignoreNulls,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v " + valueType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, first_value(v)" + (ignoreNulls ? " IGNORE NULLS" : "") + " OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, v) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Drives a partitioned bounded-frame last_value(DECIMAL) live view (RESPECT or
    // IGNORE NULLS): asserts the windowed value is correct (also proving
    // CREATE-accept), then performs a byte-exact snapshot/restore round-trip of the
    // function's partition state (write -> toTop -> restore -> write again, comparing
    // the two payloads). RESPECT NULLS must use a frame ending strictly before the
    // current row (a frame ending AT the current row routes to the un-migrated
    // IncludeCurrent shape and is rejected at CREATE); IGNORE NULLS may end at the
    // current row.
    private void assertLastValueDecimalFrameRoundTrip(
            boolean ignoreNulls,
            String decimalType,
            String frameClause,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d " + decimalType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, last_value(d)" + (ignoreNulls ? " IGNORE NULLS" : "") + " OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts " + frameClause + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Drives a partitioned bounded-frame last_value(TIMESTAMP|DATE) live view:
    // asserts the windowed value is correct (also proving CREATE-accept), then
    // performs a byte-exact snapshot/restore round-trip of the function's
    // partition state (write -> toTop -> restore -> write again, comparing).
    // The frame must end strictly before the current row (e.g. N PRECEDING AND
    // M PRECEDING): a frame ending AT the current row routes to the un-migrated
    // IncludeCurrent shape and is rejected at CREATE.
    private void assertLastValueTimestampDateFrameRoundTrip(
            String valueType,
            String frameClause,
            boolean ignoreNulls,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v " + valueType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, last_value(v)" + (ignoreNulls ? " IGNORE NULLS" : "") + " OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts " + frameClause + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, v) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Drives a partitioned bounded-frame max/min(TIMESTAMP|DATE) live view:
    // asserts the windowed value is correct (also proving CREATE-accept), then
    // performs a byte-exact snapshot/restore round-trip of the function's
    // partition state (write -> toTop -> restore -> write again, comparing).
    private void assertMaxMinTimestampDateFrameRoundTrip(
            String fnName,
            String valueType,
            String frameClause,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v " + valueType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " + fnName + "(v) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts " + frameClause + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, v) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Same as assertMaxMinTimestampDateFrameRoundTrip but for the unbounded-preceding
    // (anchored) shape; the function lives on the anchor window, not the main factory.
    private void assertMaxMinTimestampDateUnboundedRoundTrip(
            String fnName,
            String valueType,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v " + valueType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " + fnName + "(v) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, v) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Drives a partitioned anchored-unbounded nth_value(DECIMAL) live view (the
    // OverUnboundedPartitionFrameFunction shape, reached via a bare ANCHOR =
    // default UNBOUNDED PRECEDING AND CURRENT ROW frame): asserts the nth value is
    // correct (proving CREATE-accept), then a byte-exact write/toTop/restore/write
    // round-trip of the partition state.
    private void assertNthValueDecimalAnchoredRoundTrip(
            String decimalType,
            int nthN,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d " + decimalType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(d, " + nthN + ") OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Drives a partitioned bounded/unbounded-frame nth_value(DECIMAL) live view
    // (the RANGE, bounded-ROWS, or unbounded-ROWS partition shapes): asserts the
    // nth value is correct (proving CREATE-accept), then a byte-exact
    // write/toTop/restore/write round-trip of the partition state.
    private void assertNthValueDecimalFrameRoundTrip(
            String decimalType,
            int nthN,
            String frameClause,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d " + decimalType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(d, " + nthN + ") OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts " + frameClause + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertSumDecimalFrameRoundTrip(
            String decimalType,
            String frameClause,
            String insertValues,
            String expectedRows
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d " + decimalType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts " + frameClause + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " + insertValues);
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns(expectedRows);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateAndDropLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 5s PARTITION BY DAY AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateLiveViewDefaultsInMemoryToFlushEvery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 500ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateLiveViewMakesCheckpointsDir() throws Exception {
        // CREATE LIVE VIEW must materialize _checkpoints/
        // inside the LV directory so the flush cycle's checkpoint write hook
        // has a target directory ready.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            try {
                TableToken token = engine.verifyTableName("lv");
                FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path path = new Path()) {
                    path.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
                    Assert.assertTrue(
                            "_checkpoints/ must exist after CREATE LIVE VIEW [path=" + path + ']',
                            ff.exists(path.$())
                    );
                }
            } finally {
                execute("DROP LIVE VIEW lv");
            }
        });
    }

    @Test
    public void testCreateLiveViewNameVisibleOnlyAfterFilesDurable() throws Exception {
        // Registry name commit must follow
        // the durable _lv.s + _lv writes. A concurrent name lookup that races
        // with CREATE must therefore resolve nothing until both files are on
        // disk - never a half-built LV. The hook below observes the registry
        // state at the moment the _lv block file opens for writing.
        final AtomicBoolean observedDuringLvWrite = new AtomicBoolean(false);
        final AtomicBoolean nameMissingDuringLvWrite = new AtomicBoolean(true);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME)) {
                    observedDuringLvWrite.set(true);
                    if (engine.getTableTokenIfExists("lv") != null) {
                        nameMissingDuringLvWrite.set(false);
                    }
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            Assert.assertTrue("expected _lv write to be observed during CREATE",
                    observedDuringLvWrite.get());
            Assert.assertTrue("registry name must not resolve while _lv is being written",
                    nameMissingDuringLvWrite.get());
            Assert.assertNotNull("name must resolve after CREATE returns",
                    engine.getTableTokenIfExists("lv"));
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateLiveViewRollsBackOnDefinitionWriteFailure() throws Exception {
        // _lv is the atomic CREATE marker; if the write fails, the rollback
        // must leave neither a registered name, an on-disk LV directory, nor a
        // sequencer entry, so a retry with the same name succeeds cleanly.
        // Pre-fix the registry committed before _lv landed and a failure left
        // a phantom registered-but-half-built LV.
        final AtomicBoolean failLvWrite = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failLvWrite.get() && Utf8s.endsWithAscii(name, LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            failLvWrite.set(true);
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected _lv write failure to abort CREATE");
            } catch (CairoException expected) {
                // expected
            } finally {
                failLvWrite.set(false);
            }
            Assert.assertNull("LV name must not resolve after a failed _lv write",
                    engine.getTableTokenIfExists("lv"));
            Assert.assertNull("LV instance must not be in the in-memory registry",
                    engine.getLiveViewRegistry().getViewInstance("lv"));

            // Retry CREATE with the same name; rollback must have cleared
            // sequencer + FS state so the retry succeeds and the LV is queryable.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            Assert.assertNotNull(engine.getTableTokenIfExists("lv"));
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectLiveViewOverMissingBase() throws Exception {
        assertMemoryLeak(() -> {
            final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM does_not_exist";
            try {
                execute(sql);
                Assert.fail("expected missing-base reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("base table does not exist"));
                Assert.assertEquals(
                        "position must point at the base table name",
                        sql.lastIndexOf("does_not_exist"),
                        e.getPosition()
                );
            }
        });
    }

    @Test
    public void testRejectLiveViewOverNonWalBase() throws Exception {
        assertMemoryLeak(() -> {
            // No WAL — bypass-WAL is the default for non-partitioned plain tables.
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts)");
            final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base";
            try {
                execute(sql);
                Assert.fail("expected non-WAL-base reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must be a WAL table"));
                Assert.assertEquals(
                        "position must point at the base table name",
                        sql.lastIndexOf("base"),
                        e.getPosition()
                );
            }
        });
    }

    @Test
    public void testRejectLiveViewOverLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv1 FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final String sql = "CREATE LIVE VIEW lv2 FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM lv1";
            try {
                execute(sql);
                Assert.fail("expected live-on-live reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live views are not allowed as base tables in V1")
                );
                // Position must point at the base table name, not the view
                // name.
                Assert.assertEquals(
                        "position must point at the base table name",
                        sql.lastIndexOf("lv1"),
                        e.getPosition()
                );
            }
            execute("DROP LIVE VIEW lv1");
        });
    }

    @Test
    public void testBackfillSweepEmitsHistoricalRows() throws Exception {
        // CREATE LIVE VIEW ... BACKFILL captures the base table's pre-CREATE
        // history. Without BACKFILL the LV is empty until new commits arrive;
        // with BACKFILL the sweep covers everything <= backfillTargetSeqTxn
        // and the lifecycle flips to ACTIVE on completion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 1), " +
                    "('2026-04-01T00:00:01.000000Z', 2), " +
                    "('2026-04-01T00:00:02.000000Z', 3)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals(
                    "view must start in BACKFILLING",
                    LiveViewState.BACKFILL_STATE_BACKFILLING,
                    instance.getStateReader().getBackfillState()
            );

            // Drive the sweep through the refresh worker.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            Assert.assertEquals(
                    "sweep must flip backfillState to ACTIVE",
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    instance.getStateReader().getBackfillState()
            );
            Assert.assertEquals(
                    "ACTIVE flip must clear backfillTargetSeqTxn",
                    Numbers.LONG_NULL,
                    instance.getStateReader().getBackfillTargetSeqTxn()
            );
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillSweepOnEmptyBaseFlipsToActive() throws Exception {
        // CREATE LIVE VIEW ... BACKFILL on an empty base must still flip to
        // ACTIVE on the first refresh tick; the sweep walks zero rows and
        // the lifecycle advances immediately.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals(
                    LiveViewState.BACKFILL_STATE_BACKFILLING,
                    instance.getStateReader().getBackfillState()
            );

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            Assert.assertEquals(
                    "empty-base sweep must still flip to ACTIVE",
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    instance.getStateReader().getBackfillState()
            );
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReadAggregateOverPrunedColumn() throws Exception {
        // Aggregating a non-timestamp column (max(rn), sum(rn), count_distinct(rn))
        // prunes the timestamp out of the LV read projection, so the cursor's
        // resolved timestampColumnIndex is -1. The read must serve from disk
        // without probing the absent timestamp column (regressions there read
        // out of bounds and crash the JVM).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            execute("INSERT INTO base (ts, x) " +
                    "SELECT timestamp_sequence('2026-04-01T00:00:00.000000Z', 1_000_000), x FROM long_sequence(40)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n40\n");
            assertQuery("SELECT max(rn) FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("max\n40\n");
            assertQuery("SELECT sum(rn) FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("sum\n820\n");
            assertQuery("SELECT count_distinct(rn) FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count_distinct\n40\n");
            // A single-column read also prunes the timestamp; content stays correct.
            assertQuery("SELECT rn FROM lv ORDER BY rn LIMIT 3").noLeakCheck().expectSize().returns("rn\n1\n2\n3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReadAggregateOverPrunedColumnMultiPartitionAtScale() throws Exception {
        // The pruned-projection read must cross page-frame and partition
        // boundaries cleanly. 600 rows at 1-hour steps span 25 day-partitions.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            execute("INSERT INTO base (ts, x) " +
                    "SELECT timestamp_sequence('2026-04-01T00:00:00.000000Z', 3_600_000_000), x FROM long_sequence(600)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n600\n");
            assertQuery("SELECT max(rn) FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("max\n600\n");
            assertQuery("SELECT sum(rn) FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("sum\n180300\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testReadAggregateOverPrunedColumnWithInMemoryTier() throws Exception {
        // With an in-mem tier configured, a pruned read (max(rn)) must find the
        // tier ineligible and serve from disk; a full-schema read remains
        // eligible for the tier. Neither path may crash.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 10s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            execute("INSERT INTO base (ts, x) " +
                    "SELECT timestamp_sequence('2026-04-01T00:00:00.000000Z', 1_000_000), x FROM long_sequence(40)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            // Pruned read: tier present but not eligible -> disk-only.
            assertQuery("SELECT max(rn) FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("max\n40\n");
            // Full-schema read: tier eligible; content correct.
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts LIMIT 2").noLeakCheck().timestamp("ts").expectSize().returns("ts\tx\trn\n" +
                            "2026-04-01T00:00:00.000000Z\t1\t1\n" +
                            "2026-04-01T00:00:01.000000Z\t2\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillCoexistsWithFollowOnInserts() throws Exception {
        // After BACKFILL completes, subsequent inserts go through the normal
        // incremental refresh path. End-to-end row count must include both
        // the backfilled history and the post-CREATE inserts.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 1), " +
                    "('2026-04-01T00:00:01.000000Z', 2)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            // Drive the backfill sweep.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            Assert.assertEquals(
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    instance.getStateReader().getBackfillState()
            );
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            // Insert more rows post-CREATE; the incremental drain handles them.
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:10.000000Z', 10), " +
                    "('2026-04-01T00:00:11.000000Z', 11)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillAnchoredViewResumesAcrossRestart() throws Exception {
        // An anchored window's per-partition state (lastAnchorValue) must
        // survive a restart mid-sweep: the day-2 'a' row resets row_number to 1
        // only if the restored anchor state knows the day-1 anchor was crossed.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 'a', 1), " +
                    "('2026-04-01T00:00:01.000000Z', 'b', 4), " +
                    "('2026-04-01T00:00:05.000000Z', 'a', 2), " +
                    "('2026-04-02T00:00:00.000000Z', 'a', 3), " +
                    "('2026-04-02T00:00:02.000000Z', 'b', 5)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, sym, x, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");

            // Partial sweep, then simulate a restart mid-sweep, all on one job.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.run();
                drainWalQueue();
                Assert.assertEquals(
                        "partial sweep must still be BACKFILLING",
                        LiveViewState.BACKFILL_STATE_BACKFILLING,
                        engine.getLiveViewRegistry().getViewInstance("lv").getStateReader().getBackfillState()
                );

                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                driveBackfillToCompletion(job, "lv");
            }

            Assert.assertEquals(
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    engine.getLiveViewRegistry().getViewInstance("lv").getStateReader().getBackfillState()
            );
            assertQuery("SELECT ts, sym, x, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tx\trn\n" +
                            "2026-04-01T00:00:00.000000Z\ta\t1\t1\n" +
                            "2026-04-01T00:00:01.000000Z\tb\t4\t1\n" +
                            "2026-04-01T00:00:05.000000Z\ta\t2\t2\n" +
                            "2026-04-02T00:00:00.000000Z\ta\t3\t1\n" +
                            "2026-04-02T00:00:02.000000Z\tb\t5\t1\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillCompletionRetiresCheckpointFiles() throws Exception {
        // On completion the rolling .bcp is unlinked and a steady head .cp is
        // materialised, so the ACTIVE phase has a restart/O3 anchor.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) " +
                    "SELECT timestamp_sequence('2026-04-01T00:00:00.000000Z', 1_000_000), x FROM long_sequence(4)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            // Capture the rolling .bcp key from a partial sweep before finishing.
            final long bcpKey;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.run();
                drainWalQueue();
                bcpKey = engine.getLiveViewRegistry().getViewInstance("lv").getHeadBackfillCpKey();
                Assert.assertNotEquals("a .bcp must have been written mid-sweep", Numbers.LONG_NULL, bcpKey);
                driveBackfillToCompletion(job, "lv");
            }

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertEquals(
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    instance.getStateReader().getBackfillState()
            );
            Assert.assertEquals(
                    "completion must clear the in-memory .bcp key",
                    Numbers.LONG_NULL,
                    instance.getHeadBackfillCpKey()
            );
            Assert.assertNotEquals(
                    "completion must leave a steady head .cp",
                    Numbers.LONG_NULL,
                    instance.getHeadCheckpointLvSeqTxn()
            );
            // The mid-sweep .bcp file must be gone after completion.
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                        .slash();
                LiveViewCheckpointWriter.appendBcpFileName(p, bcpKey);
                Assert.assertFalse("no .bcp must survive completion", ff.exists(p.$()));
            }
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillFilteredViewResumesAcrossRestart() throws Exception {
        // A WHERE filter drops base rows, so the sweep's data-cursor offset
        // outruns the output-row count. The restart must resume at the correct
        // data offset (via FilteringRecordCursor's base-rows-consumed counter)
        // and reproduce exactly the filtered result.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 1), " +
                    "('2026-04-01T00:00:01.000000Z', 2), " +
                    "('2026-04-01T00:00:02.000000Z', 3), " +
                    "('2026-04-01T00:00:03.000000Z', 4), " +
                    "('2026-04-01T00:00:04.000000Z', 5), " +
                    "('2026-04-01T00:00:05.000000Z', 6), " +
                    "('2026-04-01T00:00:06.000000Z', 7), " +
                    "('2026-04-01T00:00:07.000000Z', 8)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x >= 5");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.run();
                drainWalQueue();
                Assert.assertEquals(
                        LiveViewState.BACKFILL_STATE_BACKFILLING,
                        engine.getLiveViewRegistry().getViewInstance("lv").getStateReader().getBackfillState()
                );

                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                driveBackfillToCompletion(job, "lv");
            }

            // Only x >= 5 survive the filter; rn is the filtered sweep order.
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tx\trn\n" +
                            "2026-04-01T00:00:04.000000Z\t5\t1\n" +
                            "2026-04-01T00:00:05.000000Z\t6\t2\n" +
                            "2026-04-01T00:00:06.000000Z\t7\t3\n" +
                            "2026-04-01T00:00:07.000000Z\t8\t4\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillRestartRestoresO3DetectionWatermark() throws Exception {
        // Finding 2b regression: row_number() OVER () under BACKFILL + O3 + restart.
        // A restart resets the in-memory O3 detection watermark (latestSeenTs) to
        // null, so tryRestoreFromHead must re-seed it from the head .cp's
        // maxTimestamp. Without that, the first post-restart commit - here a
        // back-dated (O3) row - slips past O3 detection and gets forward-appended
        // in arrival order, so the late row keeps too high a row number instead of
        // being re-sequenced by ts.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            setCurrentMicros(0L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 1), " +
                    "('2026-04-01T00:00:01.000000Z', 2)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            // The job re-fetches the instance from the registry each tick, so a
            // single job survives the registry rebuild that simulates a restart.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveBackfillToCompletion(job, "lv");

                // In-order forward-append row; the head .cp records maxTs=00:00:10.
                setCurrentMicros(currentMicros + 250_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:10.000000Z', 3)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Restart: rebuild the registry so the instance (and its
                // latestSeenTs) is reconstructed from disk.
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();

                // First post-restart commit is a back-dated (O3) row sitting
                // between the backfilled data and the forward-appended row. It
                // must trigger a re-sequencing replay, not a forward append.
                setCurrentMicros(currentMicros + 250_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:05.000000Z', 4)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns(
                    "ts\tx\trn\n" +
                            "2026-04-01T00:00:00.000000Z\t1\t1\n" +
                            "2026-04-01T00:00:01.000000Z\t2\t2\n" +
                            "2026-04-01T00:00:05.000000Z\t4\t3\n" +
                            "2026-04-01T00:00:10.000000Z\t3\t4\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillRestartResumesFromCheckpoint() throws Exception {
        // A restart mid-sweep finds the surviving .bcp, stamps its key, resumes
        // from the recorded data offset, and produces the full, gap-free output.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 10);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) " +
                    "SELECT timestamp_sequence('2026-04-01T00:00:00.000000Z', 1_000_000), x FROM long_sequence(40)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.run();
                drainWalQueue();
                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals(
                        LiveViewState.BACKFILL_STATE_BACKFILLING,
                        instance.getStateReader().getBackfillState()
                );
                Assert.assertNotEquals(
                        "a .bcp must exist before restart",
                        Numbers.LONG_NULL,
                        instance.getHeadBackfillCpKey()
                );

                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotEquals(
                        "recovery must stamp the surviving .bcp key so the sweep resumes",
                        Numbers.LONG_NULL,
                        reloaded.getHeadBackfillCpKey()
                );

                driveBackfillToCompletion(job, "lv");
            }

            Assert.assertEquals(
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    engine.getLiveViewRegistry().getViewInstance("lv").getStateReader().getBackfillState()
            );
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n40\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillRestartWithoutCheckpointReSweeps() throws Exception {
        // If no .bcp survives (crash before the first cadence write, or a
        // non-snapshot-capable view), recovery re-sweeps from offset 0 and
        // skip-writes the on-disk prefix - the result is still complete and
        // gap-free with no duplicates.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 10);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) " +
                    "SELECT timestamp_sequence('2026-04-01T00:00:00.000000Z', 1_000_000), x FROM long_sequence(40)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.run();
                drainWalQueue();
                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals(
                        LiveViewState.BACKFILL_STATE_BACKFILLING,
                        instance.getStateReader().getBackfillState()
                );
                // Drop the .bcp so recovery has no resume source.
                unlinkBackfillCheckpointFile(instance);

                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();

                LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals(
                        "no .bcp survives, so no resume key is stamped",
                        Numbers.LONG_NULL,
                        reloaded.getHeadBackfillCpKey()
                );

                driveBackfillToCompletion(job, "lv");
            }

            Assert.assertEquals(
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    engine.getLiveViewRegistry().getViewInstance("lv").getStateReader().getBackfillState()
            );
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n40\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillResumedSweepThenIncrementalDrain() throws Exception {
        // After a restart-resumed sweep flips to ACTIVE, post-CREATE inserts
        // drain incrementally and continue the row_number sequence.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 10);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) " +
                    "SELECT timestamp_sequence('2026-04-01T00:00:00.000000Z', 1_000_000), x FROM long_sequence(40)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.run();
                drainWalQueue();
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                driveBackfillToCompletion(job, "lv");
            }
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n40\n");

            // Post-backfill inserts go through the incremental drain.
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:01:00.000000Z', 41), " +
                    "('2026-04-01T00:01:01.000000Z', 42)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n42\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillYieldsAcrossTurns() throws Exception {
        // checkpoint.rows=10 caps each turn at 10 rows, so a 40-row sweep
        // yields across several turns instead of monopolising the worker: one
        // turn leaves the view BACKFILLING with a partial result, and draining
        // the rest completes it in order.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 10);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) " +
                    "SELECT timestamp_sequence('2026-04-01T00:00:00.000000Z', 1_000_000), x FROM long_sequence(40)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.run();
                drainWalQueue();
                Assert.assertEquals(
                        "one turn must not complete a 40-row sweep",
                        LiveViewState.BACKFILL_STATE_BACKFILLING,
                        instance.getStateReader().getBackfillState()
                );
                assertQuery("SELECT count() > 0 AND count() < 40 AS c FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("c\ntrue\n");
                driveBackfillToCompletion(job, "lv");
            }

            Assert.assertEquals(
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    instance.getStateReader().getBackfillState()
            );
            // count() catches any over- or under-emission across the turn yields.
            // The exact row_number values are verified at small scale by the
            // anchored/filtered tests, which read the full output column set.
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n40\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillStatusVisibleInCatalogue() throws Exception {
        // live_views().view_status reads "backfilling" while the sweep is in
        // progress; backfill_target_seqtxn surfaces the captured target.
        // After the sweep, the status flips to "active" and the target column
        // returns to LONG_NULL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("view_status\nbackfilling\n");
            Assert.assertTrue(
                    "backfill_target_seqtxn must be non-NULL while BACKFILLING",
                    instance.getStateReader().getBackfillTargetSeqTxn() >= 0
            );

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT view_status, backfill_target_seqtxn FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("view_status\tbackfill_target_seqtxn\n" +
                            "active\tnull\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testShowCreateEmitsBackfillClause() throws Exception {
        // SHOW CREATE LIVE VIEW round-trips the BACKFILL clause so the emitted
        // DDL re-creates an equivalent view.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            assertQuery("SHOW CREATE LIVE VIEW lv").noLeakCheck().noRandomAccess().returns("ddl\n" +
                            "CREATE LIVE VIEW 'lv' FLUSH EVERY 200ms IN MEMORY 200ms PARTITION BY DAY BACKFILL AS (\n" +
                            "SELECT ts, x, row_number() OVER () AS rn FROM base\n" +
                            ");\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillAcceptedAtCreate() throws Exception {
        // BACKFILL parses, the CORE_DEFINITION block stores
        // backfillRequested=true, and the CORE_STATE block stores
        // BACKFILL_STATE_BACKFILLING plus the captured target seqTxn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 1), " +
                    "('2026-04-01T00:00:01.000000Z', 2)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 500ms BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertTrue(
                    "BACKFILL clause must round-trip to definition",
                    instance.getDefinition().getBackfillRequested()
            );
            Assert.assertEquals(
                    "BACKFILL CREATE must persist BACKFILLING state",
                    LiveViewState.BACKFILL_STATE_BACKFILLING,
                    instance.getStateReader().getBackfillState()
            );
            // backfillTargetSeqTxn captures base.head at CREATE; with the two
            // inserts above (one commit), head must be >= 0 and equal to the
            // sequencer's writer txn.
            Assert.assertTrue(
                    "backfillTargetSeqTxn must be set",
                    instance.getStateReader().getBackfillTargetSeqTxn() >= 0
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillInitialLvConsumedSeqTxnIsTargetMinusOne() throws Exception {
        // A BACKFILLING view publishes backfillTargetSeqTxn - 1 as its initial WAL
        // purge floor (WAL retention coupling): the snapshot reader MVCC-pins
        // everything <= the target, while one extra base segment stays retained for
        // the deferred ring drain after the sweep. That is one lower than the
        // subscribeFromSeqTxn - 1 floor a non-BACKFILL view starts at.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 1)," +
                    "('2026-04-01T00:00:01.000000Z', 2)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            long target = instance.getStateReader().getBackfillTargetSeqTxn();
            Assert.assertTrue("backfillTargetSeqTxn must be captured", target >= 0);
            Assert.assertEquals(
                    "BACKFILLING initial lvConsumedSeqTxn must be backfillTargetSeqTxn - 1",
                    target - 1,
                    instance.getStateReader().getLvConsumedSeqTxn()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectBareUnboundedWindow() throws Exception {
        // A window with PARTITION BY and the default (UNBOUNDED PRECEDING ...
        // CURRENT ROW) frame must have an ANCHOR clause, otherwise partition
        // count grows without bound.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, symbol SYMBOL, price DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");

            // (a) named WINDOW shape.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, symbol, sum(price) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY symbol ORDER BY ts)");
                Assert.fail("expected bare unbounded named WINDOW reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported"));
            }

            // (b) inline OVER (...) shape.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, symbol, sum(price) OVER (PARTITION BY symbol ORDER BY ts) AS s FROM base");
                Assert.fail("expected bare unbounded inline OVER reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported"));
            }

            // (c) inline OVER nested inside an arithmetic expression must still be caught.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, symbol, sum(price) OVER (PARTITION BY symbol ORDER BY ts) + 1 AS s FROM base");
                Assert.fail("expected bare unbounded nested OVER reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported"));
            }

            // (d) bounded ROWS frame without ANCHOR is accepted.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, symbol, sum(price) OVER (PARTITION BY symbol ORDER BY ts " +
                    "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base");
            execute("DROP LIVE VIEW lv");

            // (e) ANCHOR DAILY satisfies the rule for the same window shape.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, symbol, sum(price) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY symbol ORDER BY ts ANCHOR DAILY '00:00')");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectFlushEveryBelow100Ms() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 50ms AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected FLUSH EVERY <100ms reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("FLUSH EVERY must be at least 100ms"));
            }
        });
    }

    @Test
    public void testRejectInMemoryAboveCap() throws Exception {
        // Default cap is 60min, which the formatter renders as the largest clean
        // divisor "1h". The reject must include the value so the operator knows
        // what they need to override.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 2h AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected IN MEMORY > cap reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "IN MEMORY must be at most cairo.live.view.in.memory.max (1h)"));
            }
        });
    }

    @Test
    public void testRejectInMemoryBelowFlushEvery() throws Exception {
        // The asserted message requires the FLUSH EVERY value in parentheses so the
        // operator sees what the floor was (1s in this case).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 500ms AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected IN MEMORY < FLUSH EVERY reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "IN MEMORY must be at least FLUSH EVERY (1s)"));
            }
        });
    }

    @Test
    public void testWalPurgeHonorsLvConsumedSeqTxnFloor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // First insert + drain — base table applies seqTxn 1.
            execute("INSERT INTO base (ts, x) VALUES ('2026-06-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            // Force a fresh segment for the next insert by releasing pooled writers.
            engine.releaseInactive();
            // Second insert + drain — base table applies seqTxn 2 in segment 1.
            execute("INSERT INTO base (ts, x) VALUES ('2026-06-01T00:01:00.000000Z', 2)");
            drainWalQueue();
            engine.releaseInactive();

            // Purge with the LV still at lvConsumedSeqTxn = 0: segment 0 must be
            // retained because the LV hasn't consumed seqTxn 1 yet.
            drainPurgeJob();
            assertSegmentExistence(true, "base", 1, 0);

            // Refresh + persist. lvConsumedSeqTxn advances to base's head.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Now the LV no longer needs the old segment. Purge must reap it.
            engine.releaseInactive();
            drainPurgeJob();
            assertSegmentExistence(false, "base", 1, 0);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInvalidatedViewHoldsWalRetentionFloor() throws Exception {
        // An INVALID live view keeps holding its WAL purge floor at the last
        // published value until it is dropped (WAL retention coupling). Releasing
        // it would let the base WAL be purged past the LV's last-applied seqTxn,
        // foreclosing the invalid-and-readable -> re-CREATE recovery path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            // Initial published floor for a non-BACKFILL view is subscribeFromSeqTxn
            // - 1. The view never refreshes, so the floor stays at this value.
            long floorBefore = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue("LV must publish an initial floor", floorBefore > -1);

            // A base commit lands in segment 0, above the LV's floor and never
            // consumed by the view.
            execute("INSERT INTO base (ts, x) VALUES ('2026-06-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            engine.releaseInactive();

            // Invalidate the LV. The floor must stay at the last published value.
            engine.invalidateLiveView(instance, "test retention floor");
            Assert.assertTrue("LV must be invalid", instance.isInvalid());
            Assert.assertEquals(
                    "invalid LV must hold its floor at the last published value",
                    floorBefore,
                    instance.getStateReader().getLvConsumedSeqTxn()
            );

            // Purge: segment 0 holds the unconsumed seqTxn, which sits above the
            // held floor, so the segment must survive. Were the floor released on
            // invalidation, the purge would clamp only to the base head and reap it.
            drainPurgeJob();
            assertSegmentExistence(true, "base", 1, 0);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInvalidationFreesRuntimeState() throws Exception {
        // An INVALID view frees its non-cursor-pinned runtime state (compiled
        // factory, anchor window, anchor function, in-mem tier) instead of
        // pinning it until DROP / shutdown. The view stays in the registry and
        // queryable from the on-disk tier.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1h AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                    "('2026-11-01T00:00:20.000000Z', 'a', 2.0)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            // After a refresh the anchored LV holds all four runtime-state fields.
            Assert.assertNotNull("compiled factory populated after refresh", instance.getCompiledFactory());
            Assert.assertNotNull("anchor window populated after refresh", instance.getAnchorWindow());
            Assert.assertNotNull("anchor function populated after refresh", instance.getAnchorFunction());
            Assert.assertNotNull("in-mem tier populated after refresh", instance.getInMemoryTier());

            // Invalidate. The unified path frees the runtime state.
            engine.invalidateLiveView(instance, "test free runtime state");
            Assert.assertTrue("view must be invalid", instance.isInvalid());
            Assert.assertNull("invalidation frees the compiled factory", instance.getCompiledFactory());
            Assert.assertNull("invalidation frees the anchor window", instance.getAnchorWindow());
            Assert.assertNull("invalidation frees the anchor function", instance.getAnchorFunction());
            Assert.assertNull("invalidation frees the in-mem tier", instance.getInMemoryTier());

            // The view stays queryable from the on-disk tier and reports INVALID.
            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-11-01T00:00:10.000000Z\ta\t1.0\n" +
                            "2026-11-01T00:00:20.000000Z\ta\t3.0\n");
            assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("view_status\n" +
                            "invalid\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testWriterStallWhenBothSlotsPinned() throws Exception {
        // When both slots are reader-pinned, the
        // slow-path tryAcquireWrite returns null and the refresh worker
        // records the start of the stall streak. writer_stall_micros surfaces
        // the duration via live_views(). We pin both slots through the test
        // by manipulating the tier directly, then run a refresh — the in-mem
        // populate path stalls but the on-disk apply still advances.
        // Force slow-path on every cycle (growth=0 disables fast-path) so the
        // setup's "publishedIdx flipped after each cycle" precondition holds.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Seed the tier via an initial refresh so both slots have valid
            // shapes (the second slot is allocated but never written-to without
            // this seed; the test pins the seeded slot + the other one).
            setCurrentMicros(0L);
            execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000001Z', 1)");
            drainWalQueue();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                int publishedAfterSeed = tier.getPublishedIdx();

                // A second cycle so both slots have been writer-touched
                // (publishedIdx flips on each swap, so this seeds the
                // currently-non-published slot).
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000002Z', 2)");
                drainWalQueue();
                drainJob(job);
                Assert.assertNotEquals("publishedIdx flipped after second cycle",
                        publishedAfterSeed, tier.getPublishedIdx());

                // Pin both slots — first the currently-published one, then
                // flip publishedIdx via a noop pseudo-publish (we just pin
                // both). Standard usage doesn't expose this state, but the
                // test mirrors what concurrent long readers do in production.
                int pinA = tier.acquireRead();
                int pinB;
                // Force a "phantom" reader on the OTHER slot by manipulating
                // publishedIdx via a write+swap cycle while still pinning A.
                // The simplest way is: take write sentinel on otherIdx, swap,
                // then acquireRead on the now-current slot.
                int otherIdx = 1 - pinA;
                LiveViewInMemoryBuffer otherWrite = tier.tryAcquireWrite(otherIdx);
                Assert.assertNotNull("seed write on other slot must succeed", otherWrite);
                tier.publishSwap(otherIdx);
                pinB = tier.acquireRead();
                Assert.assertEquals("pinB must land on the now-published slot",
                        otherIdx, pinB);

                try {
                    // Now both slots are reader-pinned. A new refresh cycle
                    // cannot take the writer sentinel on either. The on-disk
                    // tier still advances; only the in-mem swap stalls.
                    Assert.assertEquals("writer_stall_micros must start at 0",
                            Numbers.LONG_NULL, instance.getWriterStallStartUs());

                    setCurrentMicros(500_000L);
                    execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000003Z', 3)");
                    drainWalQueue();
                    drainJob(job);

                    Assert.assertEquals(
                            "writerStallStartUs must equal the wall-clock at refresh time",
                            500_000L,
                            instance.getWriterStallStartUs()
                    );

                    // live_views() must surface the stall duration (now - stallStart).
                    setCurrentMicros(700_000L);
                    assertQuery("SELECT writer_stall_micros FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("writer_stall_micros\n200000\n");
                } finally {
                    tier.releaseRead(pinA);
                    tier.releaseRead(pinB);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInMemEvictionPastInMemoryWindow() throws Exception {
        // Rows whose ts falls below latest - IN_MEMORY are
        // not copied into the new write slot during the slow-path swap. With
        // IN MEMORY 100ms and a 200ms gap between the two inserts (data ts),
        // the first row's ts is below the eviction threshold by the time the
        // second refresh cycle runs, so only the second row survives.
        // IN_MEMORY eviction runs on slow-path edges only — the
        // fast-path append leaves prior rows intact regardless of age. Force
        // slow-path (growth=0) to exercise the eviction policy.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 1)");
                drainWalQueue();
                drainJob(job);

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                Assert.assertEquals("first cycle: one row in tier", 1, tier.getSlot(tier.getPublishedIdx()).rowCount());

                // Second insert: data ts 200ms after the first, so latest -
                // IN_MEMORY = +100ms is past the first row's ts. Advance the
                // wall clock by 200ms so the FLUSH EVERY 100ms gate passes.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.200000Z', 2)");
                drainWalQueue();
                drainJob(job);

                LiveViewInMemoryBuffer published = tier.getSlot(tier.getPublishedIdx());
                Assert.assertEquals("post-second-cycle: only the new row survives", 1, published.rowCount());
                Assert.assertEquals("surviving row is the second insert", 2, published.getInt(0, 1));
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInMemEvictionDurabilityClampHoldsBackUnflushedRows() throws Exception {
        // In-mem eviction must clamp on
        // (ts < latest - IN_MEMORY) AND (seqTxn <= applied_watermark) so the
        // gap-free invariant between tiers stays intact when the disk tier
        // is behind. The in-mem publish happens only after a
        // successful apply, so the natural cycle always satisfies the clamp;
        // this test poisons the published slot's maxSeqTxn to Long.MAX_VALUE
        // before driving a second slow-path swap, simulating a future
        // hand-off-ring regime where the in-mem tier publishes rows ahead of
        // apply. The aged-out row must survive.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 1)");
                drainWalQueue();
                drainJob(job);

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                LiveViewInMemoryBuffer firstPublished = tier.getSlot(tier.getPublishedIdx());
                Assert.assertEquals("seed cycle: one row in tier", 1, firstPublished.rowCount());
                Assert.assertNotEquals(
                        "fast-path append must stamp slot maxSeqTxn",
                        Numbers.LONG_NULL,
                        firstPublished.maxSeqTxn()
                );

                // Poison the published slot's maxSeqTxn to outrun any
                // applied_watermark the next cycle can advance to. This
                // simulates a future regime where in-mem rows have been
                // published ahead of the disk-side apply.
                firstPublished.setMaxSeqTxn(Long.MAX_VALUE);

                // Second insert: data ts 200ms after the first, so the first
                // row's ts is below the IN_MEMORY threshold and would normally
                // be evicted. With the clamp engaged (the prior slot is not
                // durable yet), both rows must survive the slow-path swap.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.200000Z', 2)");
                drainWalQueue();
                drainJob(job);

                LiveViewInMemoryBuffer published = tier.getSlot(tier.getPublishedIdx());
                Assert.assertEquals(
                        "durability clamp must retain the aged-out row when prior slot is unflushed",
                        2,
                        published.rowCount()
                );
                Assert.assertEquals("first surviving row is the older insert", 1, published.getInt(0, 1));
                Assert.assertEquals("second surviving row is the newer insert", 2, published.getInt(1, 1));
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsExposesInMemBytes() throws Exception {
        // in_mem_bytes reports the
        // current footprint of both N=2 slots. Zero before any refresh; > 0
        // once a refresh has populated the tier.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Before any refresh: tier is unallocated; in_mem_bytes must read 0.
            assertQuery("SELECT in_mem_bytes FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("in_mem_bytes\n0\n");

            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 1), " +
                    "('2026-05-12T00:00:00.000002Z', 2)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            // After refresh, the tier is non-empty and footprint must be > 0.
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertNotNull("tier must be allocated after refresh", instance.getInMemoryTier());
            long footprint = instance.getInMemoryTier().footprintBytes();
            Assert.assertTrue("footprint must be > 0 after a refresh", footprint > 0);
            assertQuery("SELECT in_mem_bytes FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("in_mem_bytes\n" + footprint + "\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInMemTierReceivesRowsAfterRefresh() throws Exception {
        // Refresh worker mirrors LV outputs into a worker-local
        // staging buffer and runs a slow-path swap into the LV's N=2 in-mem
        // tier after the inline apply commits. Two rows match the WHERE
        // filter; both must show up in the published slot, in ts-ascending
        // order, with seamTs = the lowest ts.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 1), " +
                    "('2026-05-12T00:00:00.000002Z', 2), " +
                    "('2026-05-12T00:00:00.000003Z', -1)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("in-mem tier must be allocated after refresh", tier);
            LiveViewInMemoryBuffer published = tier.getSlot(tier.getPublishedIdx());
            Assert.assertEquals("published slot must have two rows", 2, published.rowCount());
            // ts column is col 0; matches the LV's designated ts.
            long ts0 = published.getLong(0, 0);
            long ts1 = published.getLong(1, 0);
            Assert.assertTrue("rows must be ordered by ts", ts0 < ts1);
            Assert.assertEquals("seamTs must equal the lowest retained ts", ts0, published.seamTs());
            // x column is col 1 — survived the WHERE filter (x > 0)
            Assert.assertEquals(1, published.getInt(0, 1));
            Assert.assertEquals(2, published.getInt(1, 1));
            // row_number outputs at col 2 — the SELECT's third column
            Assert.assertEquals(1L, published.getLong(0, 2));
            Assert.assertEquals(2L, published.getLong(1, 2));

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCursorPinKeepsTierAliveAcrossDrop() throws Exception {
        // A reader
        // holding an in-mem tier pin must survive a concurrent DROP LIVE VIEW
        // without segfault. The tier's deferred-close protocol marks it
        // closed on DROP but keeps native memory alive until the last pin
        // drains. Without the protocol, tier.releaseRead after DROP would
        // dereference a freed Unsafe pointer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000001Z', 7)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("tier must be allocated after refresh", tier);

            // Acquire a pin manually — stands in for a LiveViewRecordCursor
            // that opens before DROP and finishes its scan after.
            int pin = tier.acquireRead();
            Assert.assertTrue("acquireRead must succeed before DROP", pin >= 0);
            // Sanity: contents reachable while pinned.
            Assert.assertEquals(1L, tier.getSlot(pin).rowCount());
            Assert.assertEquals(7, tier.getSlot(pin).getInt(0, 1));

            // DROP LIVE VIEW now: liveViewRegistry.removeView -> markAsDropped ->
            // tryCloseIfDropped -> tier.close(). With deferred close the native
            // memory stays alive because the pin count is 1.
            execute("DROP LIVE VIEW lv");

            // Reader can still inspect the pinned slot — no use-after-free.
            Assert.assertEquals("pinned slot contents survive DROP",
                    7, tier.getSlot(pin).getInt(0, 1));
            // New acquires reject cleanly post-close.
            Assert.assertEquals("post-close acquireRead must return -1",
                    -1, tier.acquireRead());

            // Releasing the last pin triggers the actual native free.
            // assertMemoryLeak verifies no leak — the deferred-free path runs.
            tier.releaseRead(pin);
        });
    }

    @Test
    public void testSlowPathSwapFailureKeepsPreviousSlotPublished() throws Exception {
        // End-to-end: when publishToInMemoryTier's slow-path
        // swap throws after a successful tryAcquireWrite, the catch block must
        // call releaseWriteWithoutPublish so the previously-published slot
        // stays visible to readers and the held sentinel does not deadlock the
        // next refresh. The unit-level test
        // testReleaseWriteWithoutPublishKeepsPriorSlotPublished pins the tier
        // contract; this smoke test drives the same recovery via the refresh
        // worker so the production catch path itself is exercised.
        // The publishSwap injection point fires only on the
        // slow-path. Force slow-path (growth=0) so the failure injection runs.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // First refresh populates the tier with a known row. Establishes
            // the "previously-published" state the recovery path must preserve.
            setCurrentMicros(0L);
            execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000001Z', 7)");
            drainWalQueue();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull("tier must be allocated after first refresh", tier);
                int publishedBeforeFailure = tier.getPublishedIdx();
                LiveViewInMemoryBuffer pubSlotBeforeFailure = tier.getSlot(publishedBeforeFailure);
                Assert.assertEquals("first refresh seeded one row", 1, pubSlotBeforeFailure.rowCount());
                Assert.assertEquals(7, pubSlotBeforeFailure.getInt(0, 1));
                int retriesBefore = instance.getFlushRetryCount();
                Assert.assertEquals("no retries before injection", 0, retriesBefore);

                // Arm a one-shot publishSwap failure. Wall-clock advance so the
                // FLUSH EVERY gate (100ms) opens for the next refresh cycle.
                tier.setFailNextPublishSwap(new RuntimeException("test: simulated mid-swap failure"));
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000002Z', 13)");
                drainWalQueue();
                drainJob(job);

                // Recovery contract:
                //   1. Previously-published slot still holds the original row;
                //      readers never observe a zero-row slot.
                //   2. publishedIdx did not flip.
                //   3. Sentinel on the write slot is cleared, so a subsequent
                //      refresh can take it.
                Assert.assertEquals(
                        "publishedIdx must not flip after failed swap",
                        publishedBeforeFailure,
                        tier.getPublishedIdx()
                );
                LiveViewInMemoryBuffer pubSlotAfterFailure = tier.getSlot(tier.getPublishedIdx());
                Assert.assertEquals(
                        "previously-published slot must still hold the original row",
                        1,
                        pubSlotAfterFailure.rowCount()
                );
                Assert.assertEquals(7, pubSlotAfterFailure.getInt(0, 1));
                Assert.assertTrue(
                        "in-mem-tier swap failure must tick the flush retry counter",
                        instance.getFlushRetryCount() > retriesBefore
                );

                // The on-disk tier did advance — the inline apply committed
                // before publishToInMemoryTier ran. Both base rows must be
                // visible through SELECT (which reads from disk).
                assertQuery("SELECT x, rn FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("x\trn\n" +
                                "7\t1\n" +
                                "13\t2\n");

                // A subsequent refresh must succeed normally: the sentinel was
                // released, the staging buffer is repopulated from the next
                // commit, and publishSwap flips the tier into a slot containing
                // the retained row from the previously-published slot plus the
                // new staging row.
                //
                // Note: the row processed by the failed refresh cycle
                // (x=13) is durable on disk via the inline apply that committed
                // before publishToInMemoryTier ran, but it never made it into
                // the in-mem tier — the slow-path swap that would have copied
                // it threw. The tier therefore lags the on-disk tier by one
                // row until that row ages out of the IN MEMORY window. Reads
                // route through disk, so this gap is invisible to
                // SELECT; once seam_ts routing lands, the recovery path will
                // need to re-source the missed rows from disk (or reset the
                // seam) on the next successful swap. Documented here so the
                // assertion doesn't drift silently when that lands.
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000003Z', 21)");
                drainWalQueue();
                drainJob(job);
                Assert.assertNotEquals(
                        "third refresh must successfully flip publishedIdx",
                        publishedBeforeFailure,
                        tier.getPublishedIdx()
                );
                LiveViewInMemoryBuffer pubSlotAfterRecovery = tier.getSlot(tier.getPublishedIdx());
                Assert.assertEquals(
                        "post-recovery slot holds the retained row plus the new staging row",
                        2,
                        pubSlotAfterRecovery.rowCount()
                );
                Assert.assertEquals(7, pubSlotAfterRecovery.getInt(0, 1));
                Assert.assertEquals(21, pubSlotAfterRecovery.getInt(1, 1));
                Assert.assertEquals(0, instance.getFlushRetryCount());
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSqlCursorPinKeepsTierAliveAcrossDrop() throws Exception {
        // A SQL
        // RecordCursor opened against an LV pins the in-mem tier slot for its
        // lifetime; concurrent DROP LIVE VIEW marks the tier closed but
        // deferred-frees native memory until the cursor releases. The unit-
        // adjacent test testCursorPinKeepsTierAliveAcrossDrop drives the pin
        // directly through the tier API; this smoke test exercises the same
        // contract through the SQL path that production cursors take
        // (LiveViewRecordCursorFactory.getCursor -> LiveViewRecordCursor.of
        // -> tier.acquireRead, then on close -> tier.releaseRead).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 4), " +
                    "('2026-05-12T00:00:00.000002Z', 9)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("tier must be allocated after refresh", tier);

            // Open the cursor via the SQL path. LiveViewRecordCursorFactory
            // wraps the disk factory and pins the tier slot at getCursor; the
            // pin survives until cursor.close().
            try (RecordCursorFactory factory = select("SELECT x FROM lv ORDER BY ts")) {
                RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                try {
                    // Consume the first row to confirm the cursor is live.
                    Assert.assertTrue("cursor must yield first row", cursor.hasNext());
                    Record record = cursor.getRecord();
                    Assert.assertEquals(4, record.getInt(0));

                    // DROP while the cursor is mid-scan. Registry-level
                    // visibility is gone immediately; the tier is marked
                    // closed but native memory is held by the cursor's pin.
                    execute("DROP LIVE VIEW lv");

                    // A fresh acquireRead after DROP must reject cleanly.
                    Assert.assertEquals(
                            "post-DROP acquireRead must return -1",
                            -1,
                            tier.acquireRead()
                    );

                    // The cursor still works: the disk-side TableReader holds
                    // its partition versions independently of the LV
                    // registry, and DROP defers physical deletion until all
                    // readers release.
                    Assert.assertTrue("cursor must yield second row after DROP", cursor.hasNext());
                    Assert.assertEquals(9, record.getInt(0));
                    Assert.assertFalse("cursor must terminate after the second row", cursor.hasNext());
                } finally {
                    // Closing the cursor releases the tier pin; deferred-free
                    // runs because no other reader holds the tier. assertMemoryLeak
                    // verifies the native refcount block and column buffers are
                    // ultimately freed.
                    cursor.close();
                }
            }
        });
    }

    @Test
    public void testReadClonedSymbolTableResolvesSymbols() throws Exception {
        // newSymbolTable(int) on the LV read cursor must return a usable
        // symbol-table clone (the contract a parallel cursor consumer relies
        // on), not the throwing default. The cursor delegates the clone to the
        // disk cursor, which holds the LV table's symbol map; the same map also
        // resolves the in-mem tier's raw symbol ids (see getSymA), so a clone
        // is valid for either tier.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, x, row_number() OVER () AS rn FROM base");
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 'aapl', 1), " +
                    "('2026-04-01T00:00:01.000000Z', 'msft', 2), " +
                    "('2026-04-01T00:00:02.000000Z', 'aapl', 3), " +
                    "('2026-04-01T00:00:03.000000Z', 'goog', 4)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            final int symCol = 1;
            try (
                    RecordCursorFactory factory = select("SELECT ts, sym, x, rn FROM lv ORDER BY ts");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final SymbolTable clone = cursor.newSymbolTable(symCol);
                Assert.assertNotNull("newSymbolTable must return a clone, not null", clone);

                final SymbolTable live = cursor.getSymbolTable(symCol);
                final Record record = cursor.getRecord();
                int probeKey = SymbolTable.VALUE_NOT_FOUND;
                String probeValue = null;
                int rows = 0;
                while (cursor.hasNext()) {
                    final int key = record.getInt(symCol);
                    // The clone resolves the same value the record reports and
                    // the cursor's own (live) symbol table reports.
                    TestUtils.assertEquals(record.getSymA(symCol), clone.valueOf(key));
                    TestUtils.assertEquals(live.valueOf(key), clone.valueOf(key));
                    if (probeValue == null) {
                        probeKey = key;
                        probeValue = Chars.toString(record.getSymA(symCol));
                    }
                    rows++;
                }
                Assert.assertEquals(4, rows);

                // The clone is independent of the cursor's iteration state: it
                // still resolves keys after the cursor is exhausted, which is
                // exactly what a concurrent consumer relies on.
                TestUtils.assertEquals(probeValue, clone.valueOf(probeKey));
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInMemTierBypassedForVarLengthOutputSchema() throws Exception {
        // LiveViewInMemoryBuffer supports fixed-width column
        // types only; an LV that projects a var-length column (VARCHAR,
        // STRING, etc.) falls through to disk-only reads.
        // ensureStagingAndTier returns false, no tier is allocated, but the
        // on-disk path still produces correct results via TableReader.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym VARCHAR, x INT) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            // The LV projects a var-length column (sym VARCHAR), so the
            // in-mem tier allocation is skipped. PARTITION BY uses a fixed-
            // width INT key so the LV-side snapshot/restore contract applies
            // (variable-length partition keys are not yet supported by the
            // codec used to serialise key bytes into checkpoint blocks).
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, x, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '00:00')");
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 'a', 1), " +
                    "('2026-05-12T00:00:00.000002Z', 'b', 2), " +
                    "('2026-05-12T00:00:00.000003Z', 'a', 1)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertNull(
                    "var-length output schema must skip in-mem tier allocation",
                    instance.getInMemoryTier()
            );

            // Reads still return correct results from disk.
            assertQuery("SELECT ts, sym, x, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tx\trn\n" +
                            "2026-05-12T00:00:00.000001Z\ta\t1\t1\n" +
                            "2026-05-12T00:00:00.000002Z\tb\t2\t1\n" +
                            "2026-05-12T00:00:00.000003Z\ta\t1\t2\n");

            // in_mem_bytes must read 0 since no tier was allocated.
            assertQuery("SELECT in_mem_bytes FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("in_mem_bytes\n0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testGlobalApplyDoesNotTouchLiveView() throws Exception {
        // The global ApplyWal2TableJob.doRun skips LV tokens. We
        // ingest into the base, write a LIVE_VIEW_DATA block via the refresh worker,
        // then capture the LV's applied seqTxn. A subsequent drainWalQueue must NOT
        // advance the LV's _txn, since global apply ignores LV notifications.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 11)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            // Refresh writes + applies inline.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            long lvConsumedAfterInline = instance.getStateReader().getLvConsumedSeqTxn();
            long lastProcessedAfterInline = instance.getLastProcessedSeqTxn();
            Assert.assertEquals(
                    "inline apply must advance lvConsumed to lastProcessed",
                    lastProcessedAfterInline,
                    lvConsumedAfterInline
            );

            // Run global apply repeatedly — must be a no-op for the LV.
            for (int i = 0; i < 8; i++) {
                drainWalQueue();
            }
            Assert.assertEquals(
                    "global apply must not advance lvConsumed for LV tokens",
                    lvConsumedAfterInline,
                    instance.getStateReader().getLvConsumedSeqTxn()
            );
            Assert.assertEquals(
                    "global apply must not advance lastProcessed",
                    lastProcessedAfterInline,
                    instance.getLastProcessedSeqTxn()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshAppliesInlineAndAdvancesLvConsumedSeqTxn() throws Exception {
        // LV apply runs inline on the refresh worker after the
        // LIVE_VIEW_DATA block is written. The global ApplyWal2TableJob.doRun skips
        // LV tokens, so drainWalQueue is a no-op for the LV's own WAL. The temporal
        // contract: lvConsumedSeqTxn advances within a single refresh cycle, not on
        // a subsequent global apply tick.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 4), " +
                    "('2026-04-01T00:01:00.000000Z', 8)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            long preLvConsumed = instance.getStateReader().getLvConsumedSeqTxn();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(
                    "lastProcessedSeqTxn must advance after refresh",
                    instance.getLastProcessedSeqTxn() > preLvConsumed
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must advance inline with refresh",
                    instance.getLastProcessedSeqTxn(),
                    instance.getStateReader().getLvConsumedSeqTxn()
            );

            // drainWalQueue is a no-op for the LV's own WAL — global apply
            // skips LV tokens. Calling it must not change anything.
            long lvConsumedPostRefresh = instance.getStateReader().getLvConsumedSeqTxn();
            drainWalQueue();
            Assert.assertEquals(
                    "lvConsumedSeqTxn must not change on global drainWalQueue",
                    lvConsumedPostRefresh,
                    instance.getStateReader().getLvConsumedSeqTxn()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLvConsumedAdvancesPastNonDataCommit() throws Exception {
        // Regression: an ALTER on the base table rides through the WAL as a non-DATA
        // event. The LV refresh worker walks past the seqTxn (no rows to process) and
        // emits no LIVE_VIEW_DATA block, so the apply path has nothing to consume.
        // Pre-fix, lvConsumedSeqTxn stayed at the CREATE-time floor and held base WAL
        // retention forever. Post-fix, the no-row branch advances lvConsumedSeqTxn
        // directly via engine.advanceLiveViewConsumedSeqTxn on base-table data
        // removal.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            long baselineFloor = instance.getStateReader().getLvConsumedSeqTxn();

            // ALTER ADD COLUMN of a column the LV does not reference: dependency-set
            // narrowing keeps the LV ACTIVE; the SQL seqTxn lands on the base sequencer.
            execute("ALTER TABLE base ADD COLUMN y INT");
            drainWalQueue();
            Assert.assertFalse(
                    "LV must stay valid after ALTER touching only non-dependency columns",
                    instance.isInvalid()
            );

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            // Apply runs even though no LV WAL block was written, so we drain to make
            // sure the assertion does not race the post-cycle state publication.
            drainWalQueue();

            long postFloor = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue(
                    "lvConsumedSeqTxn must advance past the non-DATA seqTxn [baseline=" + baselineFloor
                            + ", post=" + postFloor + ']',
                    postFloor > baselineFloor
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must catch up to lastProcessedSeqTxn",
                    instance.getLastProcessedSeqTxn(),
                    postFloor
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLvConsumedAdvancesWhenAllRowsFilteredOut() throws Exception {
        // Regression: a DATA commit whose rows the LV's WHERE clause rejects produces
        // zero output rows. Pre-fix, no LIVE_VIEW_DATA block was emitted and
        // lvConsumedSeqTxn stalled, holding the base WAL segment that contained the
        // filtered seqTxn. Post-fix, the no-row branch advances lvConsumedSeqTxn so
        // base WAL purge can release the segment.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 1000000");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            long baselineFloor = instance.getStateReader().getLvConsumedSeqTxn();

            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 5)");
            drainWalQueue();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            long postFloor = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue(
                    "lvConsumedSeqTxn must advance past the all-rows-filtered seqTxn [baseline="
                            + baselineFloor + ", post=" + postFloor + ']',
                    postFloor > baselineFloor
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must catch up to lastProcessedSeqTxn",
                    instance.getLastProcessedSeqTxn(),
                    postFloor
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLvRowsTotalSurvivesRestart() throws Exception {
        // Per the MANIFEST schema, lvRowPosition is the cumulative live-view
        // row count through the checkpointed lvSeqTxn. Drive a refresh that
        // writes N rows, confirm the in-memory counter and the persisted
        // manifest both read N, restart, confirm the counter restored from
        // the head .cp, then write M more rows and confirm the next head
        // records N + M.
        //
        // Force the row-trigger cadence to 1 so each batch produces a
        // dedicated head .cp under test; default cadence is too high for a
        // few-row check.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, x, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");

            final long firstBatch = 4;
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-06-01T00:00:00.000000Z', 'a', 1), " +
                    "('2026-06-01T00:00:01.000000Z', 'a', 2), " +
                    "('2026-06-01T00:00:02.000000Z', 'b', 3), " +
                    "('2026-06-01T00:00:03.000000Z', 'b', 4)");
            drainWalQueue();

            final long firstHeadLvSeqTxn;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                drainWalQueue();
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                firstHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, firstHeadLvSeqTxn);
                Assert.assertEquals(
                        "in-memory counter matches the rows produced this cycle",
                        firstBatch,
                        instance.getLvRowsTotal()
                );
            }

            // Read the manifest off disk and assert lvRowPosition matches.
            try (Path cpPath = new Path()) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                cpPath.of(engine.getConfiguration().getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                        .slash();
                LiveViewCheckpointWriter.appendCpFileName(cpPath, firstHeadLvSeqTxn);
                try (LiveViewCheckpointReader reader = new LiveViewCheckpointReader(engine.getConfiguration())) {
                    reader.of(cpPath.$());
                    LiveViewCheckpointManifest manifest = new LiveViewCheckpointManifest();
                    reader.readManifestInto(manifest);
                    Assert.assertEquals(
                            "manifest.lvRowPosition reflects the rows the head covers",
                            firstBatch,
                            manifest.getLvRowPosition()
                    );
                }
            }

            // Simulated restart re-seeds the counter from the head manifest
            // on the first post-restart refresh cycle.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(0L, reloaded.getLvRowsTotal()); // fresh in-mem state pre-restore
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertEquals(
                    "post-restart counter restored from the head manifest",
                    firstBatch,
                    reloaded.getLvRowsTotal()
            );

            // A follow-up commit advances the counter; the next head .cp
            // records the cumulative total.
            final long secondBatch = 3;
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-06-02T00:00:00.000000Z', 'a', 5), " +
                    "('2026-06-02T00:00:01.000000Z', 'a', 6), " +
                    "('2026-06-02T00:00:02.000000Z', 'b', 7)");
            drainWalQueue();

            final long secondHeadLvSeqTxn;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                drainWalQueue();
                secondHeadLvSeqTxn = reloaded.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals("a follow-up head .cp must be written", firstHeadLvSeqTxn, secondHeadLvSeqTxn);
                Assert.assertEquals(
                        "counter accumulates against the restored total",
                        firstBatch + secondBatch,
                        reloaded.getLvRowsTotal()
                );
            }

            try (Path cpPath = new Path()) {
                cpPath.of(engine.getConfiguration().getDbRoot())
                        .concat(reloaded.getLiveViewToken())
                        .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                        .slash();
                LiveViewCheckpointWriter.appendCpFileName(cpPath, secondHeadLvSeqTxn);
                try (LiveViewCheckpointReader reader = new LiveViewCheckpointReader(engine.getConfiguration())) {
                    reader.of(cpPath.$());
                    LiveViewCheckpointManifest manifest = new LiveViewCheckpointManifest();
                    reader.readManifestInto(manifest);
                    Assert.assertEquals(
                            "follow-up manifest records the post-restart cumulative total",
                            firstBatch + secondBatch,
                            manifest.getLvRowPosition()
                    );
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testTruncateOnBaseIsTransparentToLiveView() throws Exception {
        // TRUNCATE on the base is transparent to the LV: the view stays
        // ACTIVE, its derived rows on disk are preserved byte-for-byte, the
        // in-memory tier is unchanged, and the refresh worker walks past the
        // TRUNCATE seqTxn (advancing last_processed_seqTxn and
        // lv_consumed_seqTxn) without rewriting LV state. Pre-fix,
        // ApplyWal2TableJob's TRUNCATE branch invalidated dependent LVs,
        // flipping the view to INVALID on every base TRUNCATE.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            execute(
                    "INSERT INTO base (ts, x) VALUES " +
                            "('2026-04-01T00:00:00.000000Z', 1)," +
                            "('2026-04-02T00:00:00.000000Z', 2)"
            );
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            long preFloor = instance.getStateReader().getLvConsumedSeqTxn();

            execute("TRUNCATE TABLE base");
            drainWalQueue();
            // FLUSH EVERY 200ms would otherwise rate-limit the back-to-back
            // refresh cycle and the walk-past would not run until 200ms later.
            instance.setLastFlushTimeUs(Numbers.LONG_NULL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            Assert.assertFalse(
                    "LV must stay valid after TRUNCATE on base",
                    instance.isInvalid()
            );
            assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("view_status\nactive\n");
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            long postFloor = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue(
                    "lv_consumed_seqTxn must advance past the TRUNCATE seqTxn [pre=" + preFloor
                            + ", post=" + postFloor + ']',
                    postFloor > preFloor
            );

            // Forward inserts past the LV's latestSeenTs continue to flow
            // through the normal path; the TRUNCATE did not reset state.
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            instance.setLastFlushTimeUs(Numbers.LONG_NULL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropPartitionOnBaseIsTransparentToLiveView() throws Exception {
        // DROP PARTITION on the base is transparent to the LV (same contract as
        // TRUNCATE, see testTruncateOnBaseIsTransparentToLiveView): the view
        // stays ACTIVE, its derived rows are preserved, and the refresh worker
        // walks past the DROP PARTITION seqTxn (advancing last_processed_seqTxn
        // and lv_consumed_seqTxn) without rewriting LV state. Pre-fix,
        // ApplyWal2TableJob's CMD_ALTER_TABLE branch forwarded executeAlter's
        // mat-view invalidation reason to invalidateLiveViewsForBaseTable,
        // flipping every dependent LV to INVALID on the first base partition
        // drop -- which broke any base using DROP PARTITION or TTL retention.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            execute(
                    "INSERT INTO base (ts, x) VALUES " +
                            "('2026-04-01T00:00:00.000000Z', 1)," +
                            "('2026-04-02T00:00:00.000000Z', 2)"
            );
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            long preFloor = instance.getStateReader().getLvConsumedSeqTxn();

            execute("ALTER TABLE base DROP PARTITION LIST '2026-04-01'");
            drainWalQueue();
            // FLUSH EVERY 200ms would otherwise rate-limit the back-to-back
            // refresh cycle and the walk-past would not run until 200ms later.
            instance.setLastFlushTimeUs(Numbers.LONG_NULL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            Assert.assertFalse(
                    "LV must stay valid after DROP PARTITION on base",
                    instance.isInvalid()
            );
            assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("view_status\nactive\n");
            // The dropped base partition does not retract already-computed LV
            // rows: the view's derived output is preserved.
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            long postFloor = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue(
                    "lv_consumed_seqTxn must advance past the DROP PARTITION seqTxn [pre=" + preFloor
                            + ", post=" + postFloor + ']',
                    postFloor > preFloor
            );

            // Forward inserts past the LV's latestSeenTs continue to flow
            // through the normal path; the DROP PARTITION did not reset state.
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            instance.setLastFlushTimeUs(Numbers.LONG_NULL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testApplyPersistFailureDoesNotAdvanceFloor() throws Exception {
        // Regression: pre-fix, advanceLiveViewConsumedSeqTxn mutated the in-memory
        // floor before persisting _lv.s and silently swallowed any persist error,
        // leaving the in-memory floor ahead of the durable contract WalPurgeJob
        // reads (WAL retention coupling). The fix reorders to persist
        // first and throws on failure.
        final AtomicBoolean failPersist = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failPersist.get() && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 4)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            TableToken token = instance.getLiveViewToken();
            long baselineFloor = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue("baseline lvConsumedSeqTxn must be > -1", baselineFloor > -1);

            // Inject persist failure and call advance directly with a strictly higher value.
            // Pre-fix the in-memory floor would advance and the persist error would be
            // swallowed; post-fix the call throws and the in-memory floor stays put.
            try (
                    BlockFileWriter blockFileWriter = new BlockFileWriter(
                            engine.getConfiguration().getFilesFacade(),
                            engine.getConfiguration().getCommitMode()
                    );
                    Path path = new Path()
            ) {
                failPersist.set(true);
                try {
                    long target = baselineFloor + 10;
                    try {
                        engine.advanceLiveViewConsumedSeqTxn(token, target, blockFileWriter, path);
                        Assert.fail("expected CairoException from failed _lv.s persist");
                    } catch (CairoException e) {
                        Assert.assertTrue(
                                "exception must mention the view name [msg=" + e.getFlyweightMessage() + "]",
                                Chars.contains(e.getFlyweightMessage(), token.getTableName())
                        );
                    }
                    Assert.assertEquals(
                            "in-memory lvConsumedSeqTxn must not advance when _lv.s persist fails",
                            baselineFloor,
                            instance.getStateReader().getLvConsumedSeqTxn()
                    );
                } finally {
                    failPersist.set(false);
                }

                // Sanity: with the failure flag cleared, the same advance succeeds and the
                // in-memory floor publishes the new value.
                engine.advanceLiveViewConsumedSeqTxn(token, baselineFloor + 10, blockFileWriter, path);
                Assert.assertEquals(
                        baselineFloor + 10,
                        instance.getStateReader().getLvConsumedSeqTxn()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshPersistFailureKeepsInMemoryAdvanced() throws Exception {
        // Refresh-side _lv.s write happens after the LV WAL block is committed, so
        // a failing persist cannot roll back the in-memory advance without producing
        // duplicate rows on retry. The refresh worker logs critical and moves on; the
        // next cycle resumes from the in-memory advance and the eventual successful
        // persist catches up the durable file.
        final AtomicBoolean failPersist = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failPersist.get() && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            // Pin the clock so the second refresh isn't skipped by the FLUSH-EVERY gate.
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 4)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                long baselineLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertTrue("baseline lastProcessedSeqTxn must be > -1", baselineLastProcessed > -1);

                // Advance past the FLUSH EVERY 1s window so the next refresh isn't gated.
                setCurrentMicros(2_000_000L);

                failPersist.set(true);
                try {
                    execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000001Z', 8)");
                    drainWalQueue();
                    drainJob(job);
                    // The persist threw; the refresh top-level catch logged critical. The
                    // key invariant: in-memory still advanced so the next refresh cycle
                    // does not re-process and double-write rows to the LV WAL.
                    Assert.assertTrue(
                            "refresh must keep lastProcessedSeqTxn advanced even when _lv.s persist fails",
                            instance.getLastProcessedSeqTxn() > baselineLastProcessed
                    );
                } finally {
                    failPersist.set(false);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshAdvancesLvConsumedSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:00.000000Z', 1), " +
                    "('2026-01-01T00:01:00.000000Z', 5), ('2026-01-01T00:02:00.000000Z', -3)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // The live view's last-processed seqTxn moved past 0 because we ingested at
            // least one DATA commit on the base.
            Assert.assertTrue(
                    "lastProcessedSeqTxn must advance past 0",
                    instance.getLastProcessedSeqTxn() > 0
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must equal lastProcessedSeqTxn after refresh",
                    instance.getLastProcessedSeqTxn(),
                    instance.getStateReader().getLvConsumedSeqTxn()
            );

            // Two rows match (x > 0); the live view's on-disk tier picks them up via
            // the standard ApplyWal2TableJob.
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRecoversLvState() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-02-01T00:00:00.000000Z', 7), " +
                    "('2026-02-01T00:01:00.000000Z', 9)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            long preLastProcessed = instance.getLastProcessedSeqTxn();
            Assert.assertTrue("preLastProcessed must be > 0", preLastProcessed > 0);

            // Simulate restart: clear the in-memory registry and rebuild from on-disk
            // _lv + _lv.s files via buildViewGraphs (the same path startup takes).
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("live view must be re-registered after restart", reloaded);
            Assert.assertEquals(
                    "lastProcessedSeqTxn must round-trip via _lv.s",
                    preLastProcessed,
                    reloaded.getLastProcessedSeqTxn()
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must round-trip via _lv.s",
                    preLastProcessed,
                    reloaded.getStateReader().getLvConsumedSeqTxn()
            );
            // Reads route through the standard TableReader cursor over the LV's
            // _meta + applied WAL. The on-disk tier survives restart so the row count
            // should reflect what the refresh wrote before the registry was cleared.
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillFieldsRoundTripAsActiveDefault() throws Exception {
        // Without the BACKFILL clause the CORE_DEFINITION / CORE_STATE blocks
        // persist the ACTIVE / LONG_NULL defaults. Round-trips across a
        // simulated restart so a regression that drops these fields breaks
        // visibly.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(
                    "backfillRequested defaults to false when BACKFILL is omitted",
                    instance.getDefinition().getBackfillRequested()
            );
            Assert.assertEquals(
                    "backfillState defaults to ACTIVE when BACKFILL is omitted",
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    instance.getStateReader().getBackfillState()
            );
            Assert.assertEquals(
                    "backfillTargetSeqTxn defaults to LONG_NULL when BACKFILL is omitted",
                    Numbers.LONG_NULL,
                    instance.getStateReader().getBackfillTargetSeqTxn()
            );

            // Round-trip across a simulated restart.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("live view must be re-registered after restart", reloaded);
            Assert.assertFalse(
                    "backfillRequested must round-trip via _lv",
                    reloaded.getDefinition().getBackfillRequested()
            );
            Assert.assertEquals(
                    "backfillState must round-trip via _lv.s",
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    reloaded.getStateReader().getBackfillState()
            );
            Assert.assertEquals(
                    "backfillTargetSeqTxn must round-trip via _lv.s",
                    Numbers.LONG_NULL,
                    reloaded.getStateReader().getBackfillTargetSeqTxn()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRoundTripsLvConsumedSeqTxn() throws Exception {
        // Refresh writes + applies inline, so a successful refresh
        // cycle leaves no unapplied LV WAL block in steady state. This pins the
        // round-trip of lvConsumedSeqTxn and lastProcessedSeqTxn through restart.
        // (Recovery from a crash mid-cycle — between commitLiveView and the inline
        // apply — is a narrow window covered by the durability ordering inside
        // engine.advanceLiveViewConsumedSeqTxn and lives outside the smoke suite.)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 4), " +
                    "('2026-04-01T00:01:00.000000Z', 8)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            long postLastProcessed = instance.getLastProcessedSeqTxn();
            long postLvConsumed = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue("refresh must advance lastProcessedSeqTxn", postLastProcessed > 0);
            Assert.assertEquals(
                    "inline apply must advance lvConsumed to lastProcessed",
                    postLastProcessed,
                    postLvConsumed
            );

            // Simulate restart.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("live view must be re-registered after restart", reloaded);
            Assert.assertEquals(
                    "lvConsumed must round-trip at the post-apply value",
                    postLvConsumed,
                    reloaded.getStateReader().getLvConsumedSeqTxn()
            );
            Assert.assertEquals(
                    "lastProcessed must round-trip at the post-apply value",
                    postLastProcessed,
                    reloaded.getLastProcessedSeqTxn()
            );

            // Data must already be visible without further apply.
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartContinuesProcessingNewBaseCommits() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // First batch lands and is fully refreshed/applied.
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");

            // Restart: the registry is rebuilt from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            // New base commits arrive after restart. The freshly-loaded LV instance
            // must pick them up via the refresh job's normal notification path. This
            // catches loader bugs where the registry is registered but the
            // liveViewStateStore base-table mapping is not, so notifications would
            // silently never enqueue a task for the reloaded view.
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-02T00:00:00.000000Z', 2), " +
                    "('2026-04-02T00:01:00.000000Z', 3)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testMultipleLiveViewsOverSameBaseRefreshTogether() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Two LVs over the same base with different SELECT shapes. A single base
            // commit must fan out to BOTH via getViewsForBaseTable; the per-instance
            // refresh latch must not block one LV from refreshing while the other is.
            execute("CREATE LIVE VIEW lv1 FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 5");

            execute("INSERT INTO base (ts, x) VALUES ('2026-05-01T00:00:00.000000Z', 3), " +
                    "('2026-05-01T00:01:00.000000Z', 7), " +
                    "('2026-05-01T00:02:00.000000Z', 12)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // lv1: x > 0 keeps all three rows.
            assertQuery("SELECT count() FROM lv1").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            // lv2: x > 5 keeps the 7 and 12.
            assertQuery("SELECT count() FROM lv2").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            // Both LVs must have actually advanced their state independently — verify
            // neither is stuck at zero (which would happen if refresh skipped one of them).
            LiveViewInstance i1 = engine.getLiveViewRegistry().getViewInstance("lv1");
            LiveViewInstance i2 = engine.getLiveViewRegistry().getViewInstance("lv2");
            Assert.assertTrue("lv1 must advance", i1.getLastProcessedSeqTxn() > 0);
            Assert.assertTrue("lv2 must advance", i2.getLastProcessedSeqTxn() > 0);

            execute("DROP LIVE VIEW lv1");
            execute("DROP LIVE VIEW lv2");
        });
    }

    @Test
    public void testLiveViewAsAsofJoinRhs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("CREATE TABLE probe (ts TIMESTAMP, label SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-03-01T00:00:00.000000Z', 10), ('2026-03-01T00:01:00.000000Z', 20)");
            execute("INSERT INTO probe (ts, label) VALUES " +
                    "('2026-03-01T00:00:30.000000Z', 'a'), ('2026-03-01T00:01:30.000000Z', 'b')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // ASOF JOIN against the LV as RHS: each probe row should pick up the
            // latest LV row at or before the probe ts.
            assertQuery("SELECT probe.ts, probe.label, lv.x, lv.rn " +
                            "FROM probe ASOF JOIN lv").noLeakCheck().timestamp("ts").noRandomAccess().expectSize().returns("ts\tlabel\tx\trn\n" +
                            "2026-03-01T00:00:30.000000Z\ta\t10\t1\n" +
                            "2026-03-01T00:01:30.000000Z\tb\t20\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDependencyColumnIndexesPopulated() throws Exception {
        assertMemoryLeak(() -> {
            // Base has four columns (ts, x, y, z); LV references only ts + x.
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT, z INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            // ts + x should be the dependency set; y and z must not appear.
            Assert.assertEquals(2, instance.getDependencyColumnNames().size());

            // Restart sweep: the dependency set lives in _lv, so reload restores it
            // before any refresh runs. A schema change between restart and first
            // refresh would otherwise fall back to the broad invalidation path.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertEquals(2, reloaded.getDependencyColumnNames().size());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFallbackScanPicksUpMissedNotifications() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Drain the notification state store BEFORE inserting — this simulates a
            // missed notification (e.g., the worker was busy elsewhere when the commit
            // landed, and the dedup gate dropped subsequent notifications).
            engine.getLiveViewStateStore().clear();

            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 11), ('2026-04-01T00:01:00.000000Z', 22)");
            drainWalQueue();

            // Notification queue is empty; the refresh job's fallback scan must catch
            // the lag and refresh the LV.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertTrue(
                    "fallback scan must advance lastProcessedSeqTxn",
                    instance.getLastProcessedSeqTxn() > 0
            );
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFlushEveryRateLimitsCommits() throws Exception {
        assertMemoryLeak(() -> {
            // Pin the test clock so the FLUSH EVERY 1s gate is exercised
            // deterministically: both batches land at t=0, so the second refresh
            // must be skipped; only after we advance the clock past 1s does
            // the LV catch up.
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1 at t=0: passes the gate (lastFlushTimeUs is unset).
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                long firstFlushUs = instance.getLastFlushTimeUs();
                Assert.assertEquals("first refresh must record the commit timestamp", 0L, firstFlushUs);
                long firstProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertTrue("first refresh must advance lastProcessedSeqTxn", firstProcessed > 0);

                // Batch 2 still at t=0: refresh must be skipped because we are
                // within the 1s rate-limit window.
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000001Z', 2)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "rate-limited refresh must not advance lastFlushTimeUs",
                        firstFlushUs,
                        instance.getLastFlushTimeUs()
                );
                Assert.assertEquals(
                        "rate-limited refresh must not advance lastProcessedSeqTxn",
                        firstProcessed,
                        instance.getLastProcessedSeqTxn()
                );

                // Advance past FLUSH EVERY: the fallback scan must pick up the
                // pending commit and refresh now succeeds.
                setCurrentMicros(2_000_000L);
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "post-window refresh must record the new commit timestamp",
                        2_000_000L,
                        instance.getLastFlushTimeUs()
                );
                Assert.assertTrue(
                        "post-window refresh must advance lastProcessedSeqTxn",
                        instance.getLastProcessedSeqTxn() > firstProcessed
                );
            }

            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFlushRetryBudgetExhaustionInvalidatesView() throws Exception {
        // Persist failures retry up to cairo.live.view.flush.retry.max
        // (or .duration, whichever fires first). On budget exhaustion the view is
        // invalidated via the unified path. We force consecutive _lv.s persist failures
        // on the refresh worker and assert the LV flips to INVALID after the configured
        // count.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, 2);
        // Set a duration cap large enough that the count trigger fires first.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX_DURATION_MICROS, Micros.HOUR_MICROS);

        final AtomicBoolean failPersist = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failPersist.get() && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Happy-path drain seeds lastFlushTimeUs and lvConsumedSeqTxn.
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000001Z', 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertFalse("LV must be valid after happy path", instance.isInvalid());

                failPersist.set(true);
                try {
                    // Two consecutive persist failures: first hits retryCount=1 (budget
                    // not exhausted, log only); second hits retryCount=2 == max → invalidate.
                    setCurrentMicros(200_000L);
                    execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000002Z', 2)");
                    drainWalQueue();
                    drainJob(job);
                    Assert.assertEquals("first failure must increment retryCount to 1",
                            1, instance.getFlushRetryCount());
                    Assert.assertFalse("LV must still be valid after one failure",
                            instance.isInvalid());

                    setCurrentMicros(400_000L);
                    execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000003Z', 3)");
                    drainWalQueue();
                    drainJob(job);
                    Assert.assertTrue("second failure (retryCount=2) must exhaust the budget",
                            instance.isInvalid());
                    Assert.assertTrue(
                            "invalidation reason must mention flush retry [reason=" + instance.getInvalidationReason() + "]",
                            Chars.contains(instance.getInvalidationReason(), "flush retry budget")
                    );
                } finally {
                    failPersist.set(false);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSchemaChangeNarrowsToReferencedColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT, z INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse("LV must start valid", instance.isInvalid());

            // Drop an unreferenced column — LV should stay ACTIVE.
            execute("ALTER TABLE base DROP COLUMN z");
            drainWalQueue();
            Assert.assertFalse(
                    "dropping an unreferenced column must not invalidate the LV",
                    instance.isInvalid()
            );

            // Drop a referenced column (x) — LV should flip to INVALID.
            execute("ALTER TABLE base DROP COLUMN x");
            drainWalQueue();
            Assert.assertTrue(
                    "dropping a referenced column must invalidate the LV",
                    instance.isInvalid()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropLiveViewWritesDropSentinelDurably() throws Exception {
        // dropLiveView's first durable step writes _lv.drop and fsyncs it
        // before any in-memory or on-disk teardown, so a crash mid-drop leaves
        // an unambiguous signal for the startup loader to reap. We verify the
        // ordering at the FilesFacade layer: the sentinel openRW must run
        // before liveViewRegistry.removeView mutates the in-memory state,
        // and an fsync against the same fd must run before the file closes.
        final AtomicBoolean sentinelOpened = new AtomicBoolean(false);
        final AtomicBoolean sentinelFsynced = new AtomicBoolean(false);
        final long[] sentinelFd = {-1L};
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (fd > 0 && Utf8s.endsWithAscii(name, LiveViewDefinition.LIVE_VIEW_DROP_SENTINEL_FILE_NAME)) {
                    sentinelOpened.set(true);
                    sentinelFd[0] = fd;
                }
                return fd;
            }

            @Override
            public void fsync(long fd) {
                if (sentinelFd[0] == fd) {
                    sentinelFsynced.set(true);
                }
                super.fsync(fd);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            execute("DROP LIVE VIEW lv");
            Assert.assertTrue("_lv.drop sentinel must be created during DROP",
                    sentinelOpened.get());
            Assert.assertTrue("_lv.drop sentinel must be fsynced during DROP",
                    sentinelFsynced.get());
        });
    }

    @Test
    public void testLoaderReapsLiveViewWhenDropSentinelExists() throws Exception {
        // Simulates a crash mid-DROP: the durable _lv.drop sentinel landed on
        // disk but the rest of the drop (sequencer mark + FS unlink) never
        // ran. On restart, buildViewGraphs must finish the drop instead of
        // re-registering a healthy-looking LV. Without the sentinel-reap
        // branch the loader would replay the LV as if DROP had never been
        // issued.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            TableToken token = engine.getLiveViewRegistry().getViewInstance("lv").getLiveViewToken();
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot())
                        .concat(token)
                        .concat(LiveViewDefinition.LIVE_VIEW_DROP_SENTINEL_FILE_NAME);
                Assert.assertTrue("sentinel touch must succeed", ff.touch(path.$()));
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            Assert.assertNull(
                    "LV must not be re-registered when _lv.drop is present",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );
            Assert.assertNull(
                    "loader must mark the dropped token as dropped",
                    engine.getTableTokenIfExists("lv")
            );
        });
    }

    @Test
    public void testLoaderReapsOrphanLiveViewDirectory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Simulate a CREATE crash that left the table directory and _lv.s
            // behind but never wrote the _lv commit marker. After restart, the
            // loader should reap the directory.
            TableToken token = engine.getLiveViewRegistry().getViewInstance("lv").getLiveViewToken();
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot())
                        .concat(token)
                        .concat(LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME);
                Assert.assertTrue(ff.removeQuiet(path.$()));

                // Sanity: the LV directory is still on disk before the loader runs.
                path.of(engine.getConfiguration().getDbRoot()).concat(token);
                Assert.assertTrue("LV directory must still exist before reap", ff.exists(path.$()));
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            Assert.assertNull(
                    "loader must not register an LV without a committed _lv",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );
            // The reap path goes through dropTableOrViewOrMatView, which marks
            // the WAL token dropped in the name registry. On-disk cleanup is
            // then handled by the standard WAL purge machinery; this assertion
            // captures the durable-side outcome (the LV name no longer resolves).
            Assert.assertNull(
                    "loader must mark the orphan token as dropped",
                    engine.getTableTokenIfExists("lv")
            );
        });
    }

    @Test
    public void testLoaderRejectsHalfCreatedLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Simulate a crash that left _lv on disk but not _lv.s. With the
            // engine's atomic write order (_lv.s first, _lv last) this state can
            // only occur via external corruption, but if it does the loader must
            // reject the LV rather than fall back to a default subscribeFromSeqTxn
            // that would re-replay the entire base table.
            TableToken token = engine.getLiveViewRegistry().getViewInstance("lv").getLiveViewToken();
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot())
                        .concat(token)
                        .concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME);
                Assert.assertTrue(ff.removeQuiet(path.$()));
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            Assert.assertNull(
                    "loader must refuse to register a live view whose _lv.s is missing",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );
            // No DROP here: the loader rejected the LV, so the SQL surface no
            // longer sees it. Per-test fixture cleans up the on-disk leftover.
        });
    }

    @Test
    public void testSchemaChangeNarrowsAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT, z INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Restart sweep before any refresh runs. The dependency set must come
            // back from _lv, otherwise the schema-change hook falls back to broad
            // invalidation and the LV flips INVALID on an unrelated DROP.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse("LV must come back valid", reloaded.isInvalid());

            execute("ALTER TABLE base DROP COLUMN z");
            drainWalQueue();
            Assert.assertFalse(
                    "post-restart unreferenced-column DROP must not invalidate the LV",
                    reloaded.isInvalid()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorSpecPersistsAndRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertNotNull("anchor spec must be captured at CREATE",
                    instance.getDefinition().getAnchorSpec());
            Assert.assertEquals("w", instance.getDefinition().getAnchorSpec().windowName);
            Assert.assertEquals(1, instance.getDefinition().getAnchorSpec().partitionColumnNames.size());
            Assert.assertEquals("sym", instance.getDefinition().getAnchorSpec().partitionColumnNames.get(0));

            // Drive a refresh so ensureAnchorFunction runs. The anchor function should
            // now be compiled and cached on the instance.
            execute("INSERT INTO base (ts, x, sym) VALUES ('2026-07-01T00:00:00.000000Z', 1, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            Assert.assertNotNull("anchor function must be lazily compiled on first refresh",
                    instance.getAnchorFunction());

            // Simulate restart and verify anchor spec round-trips via _lv.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertNotNull("anchor spec must round-trip via _lv",
                    reloaded.getDefinition().getAnchorSpec());
            Assert.assertEquals("w", reloaded.getDefinition().getAnchorSpec().windowName);
            Assert.assertEquals(1, reloaded.getDefinition().getAnchorSpec().partitionColumnNames.size());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsRunningSumAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Two days for sym='a': day 1 has values 10 and 20 (running sum: 10, 30);
            // day 2 has values 5 and 15 (running sum should restart from 0: 5, 20).
            // Without anchor reset, day 2's sums would continue from day 1 (35, 50).
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t20.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsOnIntBucketChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // hour(ts) returns INT; the anchor's LONG slot must absorb the
            // INT-to-LONG widening cleanly. Two rows in hour 10 accumulate; the
            // hour-11 row resets the running sum.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION hour(ts))");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T10:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T10:30:00.000000Z', 20, 'a'), " +
                    "('2026-08-01T11:00:00.000000Z', 5, 'a'), " +
                    "('2026-08-01T11:30:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T10:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T10:30:00.000000Z\ta\t30.0\n" +
                            "2026-08-01T11:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-01T11:30:00.000000Z\ta\t20.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsAvgAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, avg(x) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // day 1: values 10, 20 -> running avg 10.0, 15.0
            // day 2 (after reset): values 100, 200 -> running avg 100.0, 150.0
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 100, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 200, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, a FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ta\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t15.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t150.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorAvgDecimalRestoresRunningStateAcrossRestart() throws Exception {
        // The migrated avg(DECIMAL) unbounded-partition-rows variant must write
        // its [acc, count] accumulator into the head .cp via
        // snapshotPartitionState and rehydrate it via restorePartitionState on
        // restart, so the running average continues across a restart instead of
        // restarting from the post-restart rows. Uses an INT partition key to
        // side-step the per-WAL-segment SYMBOL index collision.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10.000000m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 20.000000m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertNotEquals(
                        "head .cp written before restart",
                        Numbers.LONG_NULL,
                        instance.getHeadCheckpointLvSeqTxn()
                );
            }

            // Simulate restart: drop the in-memory registry, rebuild from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            // Pure-restore tick (no new commits) fires tryRestoreFromHead, which
            // must rehydrate the function accumulator through restorePartitionState.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertTrue(
                    "restore attempted on the first post-restart cycle",
                    reloaded.isCheckpointRestoreAttempted()
            );

            // New row in the same day-1 bucket and partition. The running avg
            // must include the two pre-restart rows (acc=30.000000, count=2), so
            // avg over {10, 20, 60} = 30.000000, not 60.000000.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 60.000000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, a FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ta\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t10.000000\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t15.000000\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t30.000000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsAvgDecimal128AcrossDayBoundary() throws Exception {
        // avg over a DECIMAL(38,6) column (DECIMAL128 storage) in an anchored
        // live view. Exercises the migrated
        // Decimal128AvgOverUnboundedPartitionRowsFrameFunction: the running avg
        // resets to the new bucket's first row at the day boundary.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // day 1: 10, 20 -> running avg 10, 15
            // day 2 (after reset): 100, 200 -> running avg 100, 150
            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 100.000000m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 200.000000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, a FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ta\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t15.000000\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100.000000\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t150.000000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsAvgDecimal256AcrossDayBoundary() throws Exception {
        // avg over a DECIMAL(60,0) column (DECIMAL256 storage) in an anchored
        // live view. Exercises the migrated
        // Decimal256AvgOverUnboundedPartitionRowsFrameFunction.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(60, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 100m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 200m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, a FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ta\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t15\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t150\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsAvgDecimal32AcrossDayBoundary() throws Exception {
        // avg over a DECIMAL(9,3) column - "narrow" storage with a raw LONG
        // accumulator, the third snapshot payload variant. Represents the
        // Decimal8/16/32 widths (identical migration code).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(9, 3)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 2.000m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 6.000m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 10.000m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 30.000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, a FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ta\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t2.000\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t4.000\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t10.000\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t20.000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsAvgDecimal64AcrossDayBoundary() throws Exception {
        // avg over a DECIMAL(18,2) column - DECIMAL64 storage with a DECIMAL128
        // accumulator, the second snapshot payload variant.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 2.00m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 6.00m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 10.00m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 30.00m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, a FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ta\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t2.00\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t4.00\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t10.00\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t20.00\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorAvgDecimal32RestoresRunningStateAcrossRestart() throws Exception {
        // Restart-restore for the narrow (raw LONG acc) snapshot payload.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(9, 3)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 2.000m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 6.000m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            // avg over {2, 6, 16} = 8.000 if the [acc, count] restored.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 16.000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, a FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ta\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t2.000\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t4.000\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t8.000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorAvgDecimal64RestoresRunningStateAcrossRestart() throws Exception {
        // Restart-restore for the DECIMAL64 (DECIMAL128 acc) snapshot payload.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 2.00m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 6.00m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 16.00m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, a FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ta\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t2.00\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t4.00\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t8.00\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorCountDecimalRestoresRunningStateAcrossRestart() throws Exception {
        // count(DECIMAL) routes through the shared CountFunctionFactoryHelper -
        // the same snapshot-capable window function count(DOUBLE) uses - so the
        // running count of non-null decimal values must survive a restart.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, count(d) OVER w AS c FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 2.00m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 6.00m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            // Running count continues from 2 (the two pre-restart rows), so the
            // third row in the same day-1 bucket is count 3, not 1.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 16.00m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, c FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tc\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t1\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t2\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorMaxDecimal128RestoresRunningStateAcrossRestart() throws Exception {
        // Restart-restore for the DECIMAL128 snapshot payload (16-byte value).
        // The running max must survive a checkpoint and rehydrate via
        // restorePartitionState so a later, smaller value does not lower it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, max(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10.000000m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 30.000000m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            // Running max stays 30.000000: max(30, 20) is 30, not 20.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 20.000000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t10.000000\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t30.000000\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t30.000000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorMaxDecimal16RestoresRunningStateAcrossRestart() throws Exception {
        // Restart-restore for the narrow (raw LONG value slot) snapshot payload,
        // representing the Decimal8/16/32/64 widths.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(4, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, max(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 30m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            // Running max stays 30: max(30, 20) is 30 if the LONG slot restored.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 20m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t10\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t30\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t30\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorMaxDecimal256RestoresRunningStateAcrossRestart() throws Exception {
        // Restart-restore for the DECIMAL256 snapshot payload (32-byte value).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(60, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, max(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 30m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            // Running max stays 30: max(30, 20) is 30 if the DECIMAL256 slot restored.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 20m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t10\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t30\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t30\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorMinDecimal64RestoresRunningStateAcrossRestart() throws Exception {
        // Restart-restore for min (DECIMAL64, LONG value slot). Proves the min
        // path's snapshot/restore: a later, larger value does not raise the min.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, min(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 30.00m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 10.00m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            // Running min stays 10.00: min(10, 20) is 10 if the LONG slot restored.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 20.00m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t30.00\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t10.00\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t10.00\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsCountDecimalAcrossDayBoundary() throws Exception {
        // count(DECIMAL) in an anchored live view: the running count of non-null
        // decimal values resets at the day boundary, and a NULL value does not
        // advance the count (exercises the decimal not-null predicate).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, count(d) OVER w AS c FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // day 1: 5.00, NULL, 7.00 -> running count 1, 1, 2 (NULL skipped)
            // day 2 (after reset): 9.00, 11.00 -> running count 1, 2
            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 5.00m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', 7.00m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 9.00m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 11.00m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, c FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tc\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t1\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t2\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMaxDecimal128AcrossDayBoundary() throws Exception {
        // max over a DECIMAL(38,6) column (DECIMAL128 storage) in an anchored
        // live view. Exercises the migrated
        // Decimal128MaxMinOverUnboundedPartitionRowsFrameFunction: the running
        // max resets to the new bucket's first row at the day boundary, and a
        // smaller value inside a bucket does not lower the running max.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 20.000000m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 10.000000m), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 15.000000m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 5.000000m), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', 25.000000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t20.000000\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t20.000000\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t30.000000\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t15.000000\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t15.000000\n" +
                            "2026-08-02T02:00:00.000000Z\ta\t25.000000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMaxDecimal16AcrossDayBoundary() throws Exception {
        // max over a DECIMAL(4,0) column - "narrow" storage with a raw LONG
        // value slot. Represents the Decimal8/16/32/64 widths (shared layout).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(4, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 20m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 10m), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 15m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 5m), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', 25m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t20\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t30\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t15\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t15\n" +
                            "2026-08-02T02:00:00.000000Z\ta\t25\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMaxDecimal256AcrossDayBoundary() throws Exception {
        // max over a DECIMAL(60,0) column (DECIMAL256 storage).
        // Exercises Decimal256MaxMinOverUnboundedPartitionRowsFrameFunction.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(60, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 20m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 10m), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 15m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 5m), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', 25m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t20\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t30\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t15\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t15\n" +
                            "2026-08-02T02:00:00.000000Z\ta\t25\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMaxDecimal32AcrossDayBoundary() throws Exception {
        // max over a DECIMAL(9,3) column (DECIMAL32 storage, LONG value slot).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(9, 3)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 20.000m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 10.000m), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 15.000m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 5.000m), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', 25.000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t20.000\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t20.000\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t30.000\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t15.000\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t15.000\n" +
                            "2026-08-02T02:00:00.000000Z\ta\t25.000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMaxDecimal64AcrossDayBoundary() throws Exception {
        // max over a DECIMAL(18,2) column (DECIMAL64 storage, LONG value slot).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 20.00m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 10.00m), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 15.00m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 5.00m), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', 25.00m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t20.00\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t20.00\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t30.00\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t15.00\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t15.00\n" +
                            "2026-08-02T02:00:00.000000Z\ta\t25.00\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMaxDecimal8AcrossDayBoundary() throws Exception {
        // max over a DECIMAL(2,0) column (DECIMAL8 storage, LONG value slot).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(2, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 20m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 10m), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 15m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 5m), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', 25m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t20\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t30\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t15\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t15\n" +
                            "2026-08-02T02:00:00.000000Z\ta\t25\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMinDecimal64AcrossDayBoundary() throws Exception {
        // min over a DECIMAL(18,2) column. min reuses Max's MaxMin* classes with
        // the LESS_THAN comparators, so this proves the min wiring and that a
        // larger value inside a bucket does not raise the running min.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, min(d) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 20.00m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 10.00m), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 15.00m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 5.00m), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', 25.00m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t20.00\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t10.00\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t15.00\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t5.00\n" +
                            "2026-08-02T02:00:00.000000Z\ta\t5.00\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsSumDecimal128AcrossDayBoundary() throws Exception {
        // sum over a DECIMAL(38,6) column (DECIMAL128 storage, DECIMAL256 acc) in
        // an anchored live view. Exercises the migrated
        // Decimal128SumOverUnboundedPartitionRowsFrameFunction: the running sum
        // resets to the new bucket's first row at the day boundary.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(d) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 100.000000m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 200.000000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30.000000\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100.000000\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t300.000000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsSumDecimal16AcrossDayBoundary() throws Exception {
        // sum over a DECIMAL(4,0) column - "narrow" storage with a raw LONG
        // accumulator. Represents the Decimal8/16 widths (identical migration).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(4, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(d) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 100m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 200m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t300\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsSumDecimal256AcrossDayBoundary() throws Exception {
        // sum over a DECIMAL(60,0) column (DECIMAL256 storage, DECIMAL256 acc).
        // Exercises Decimal256SumOverUnboundedPartitionRowsFrameFunction.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(60, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(d) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 100m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 200m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t300\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsSumDecimal32AcrossDayBoundary() throws Exception {
        // sum over a DECIMAL(9,3) column (DECIMAL32 storage, DECIMAL128 acc).
        // Exercises Decimal32SumOverUnboundedPartitionRowsFrameFunction.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(9, 3)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(d) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 2.000m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 6.000m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 10.000m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 30.000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t2.000\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t8.000\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t10.000\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t40.000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsSumDecimal64AcrossDayBoundary() throws Exception {
        // sum over a DECIMAL(18,2) column (DECIMAL64 storage, DECIMAL128 acc).
        // Exercises Decimal64SumOverUnboundedPartitionRowsFrameFunction.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(d) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 2.00m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 6.00m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 10.00m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 30.00m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t2.00\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t8.00\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t10.00\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t40.00\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsSumDecimal8AcrossDayBoundary() throws Exception {
        // sum over a DECIMAL(2,0) column (DECIMAL8 storage, raw LONG acc).
        // Exercises Decimal8SumOverUnboundedPartitionRowsFrameFunction.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(2, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(d) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, d) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', 40m), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', 50m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t40\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t90\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorSumDecimal128RestoresRunningStateAcrossRestart() throws Exception {
        // Restart-restore for the DECIMAL256-acc snapshot payload (DECIMAL128
        // input). The running sum must survive a checkpoint and rehydrate via
        // restorePartitionState so the sum continues across a restart.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(d) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10.000000m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 20.000000m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            // Running sum continues from 30.000000, so {10, 20, 60} sums to
            // 90.000000, not 60.000000.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 60.000000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t10.000000\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t30.000000\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t90.000000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorSumDecimal16RestoresRunningStateAcrossRestart() throws Exception {
        // Restart-restore for the narrow (raw LONG acc) snapshot payload.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(4, 0)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(d) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 20m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            // {10, 20, 30} sums to 60 if the LONG acc restored.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 30m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t10\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t30\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t60\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorSumDecimal64RestoresRunningStateAcrossRestart() throws Exception {
        // Restart-restore for the DECIMAL128-acc snapshot payload (DECIMAL64 input).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(18, 2)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(d) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 2.00m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 6.00m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 16.00m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t2.00\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t8.00\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t24.00\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAvgDecimal128OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(38,6)) over a bounded RANGE frame (DECIMAL128 storage,
        // DECIMAL256 acc). Exercises Decimal128AvgOverPartitionRangeFrameFunction:
        // CREATE-accept in a live view, the correct windowed average, and a
        // byte-exact snapshot/restore round-trip of the
        // [acc, frameSize, startOffset, size, capacity, firstIdx] slots plus the
        // variable-size native [ts, value] ring.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 7.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 11.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 13.000000m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // RANGE within 2 HOUR preceding (hourly rows, so all three are in frame at row 3):
                // a: 10, (10+20)/2=15, (10+20+30)/3=20
                // b: 7, (7+11)/2=9, (7+11+13)/3=10.333333
                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns("ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t7.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t10.333333\n");

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAvgDecimal128OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(38,6)) over a bounded ROWS frame (DECIMAL128 storage,
        // DECIMAL256 acc). Exercises Decimal128AvgOverPartitionRowsFrameFunction:
        // CREATE-accept in a live view, the correct rolling average, and a
        // byte-exact snapshot/restore round-trip of the
        // [acc, count, loIdx, startOffset] slots plus the fixed-size native ring.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, avg(d) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 7.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 11.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 13.000000m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // rolling avg over {current and up to 2 preceding rows}:
                // a: 10, (10+20)/2=15, (10+20+30)/3=20
                // b: 7, (7+11)/2=9, (7+11+13)/3=10.333333
                assertQuery("SELECT ts, sym, a FROM lv ORDER BY sym, ts").noLeakCheck().expectSize().returns("ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t7.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t10.333333\n");

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fn = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(fn.supportsSnapshot());
                Map fnMap = fn.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW s1 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                     MemoryCARW s2 = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(s1, fn);
                    final long len = s1.getAppendOffset();
                    fn.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(s1, 0L, fn, 1);
                    Assert.assertEquals(2L, fnMap.size());
                    LiveViewFunctionSnapshot.write(s2, fn);
                    Assert.assertEquals(len, s2.getAppendOffset());
                    for (long i = 0; i < len; i++) {
                        Assert.assertEquals("snapshot byte mismatch at " + i, s1.getByte(i), s2.getByte(i));
                    }
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAvgDecimal16OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(4,1)) over a bounded ROWS frame - narrow LONG acc, SHORT
        // ring element.
        assertAvgDecimalFrameRoundTrip(
                "DECIMAL(4, 1)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.0\n"
        );
    }

    @Test
    public void testAvgDecimal256OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(60,0)) over a bounded RANGE frame - DECIMAL256 acc, 32-byte
        // ring element.
        assertAvgDecimalFrameRoundTrip(
                "DECIMAL(60, 0)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testAvgDecimal256OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(60,0)) over a bounded ROWS frame - DECIMAL256 acc, 32-byte
        // ring element.
        assertAvgDecimalFrameRoundTrip(
                "DECIMAL(60, 0)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testAvgDecimal32OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(9,3)) over a bounded RANGE frame - narrow LONG acc, INT
        // ring element.
        assertAvgDecimalFrameRoundTrip(
                "DECIMAL(9, 3)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000\n"
        );
    }

    @Test
    public void testAvgDecimal32OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(9,3)) over a bounded ROWS frame - narrow LONG acc, INT
        // ring element.
        assertAvgDecimalFrameRoundTrip(
                "DECIMAL(9, 3)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000\n"
        );
    }

    @Test
    public void testAvgDecimal64OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(18,2)) over a bounded RANGE frame - DECIMAL128 acc, 8-byte
        // ring element.
        assertAvgDecimalFrameRoundTrip(
                "DECIMAL(18, 2)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testAvgDecimal64OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(18,2)) over a bounded ROWS frame - DECIMAL128 acc, 8-byte
        // ring element.
        assertAvgDecimalFrameRoundTrip(
                "DECIMAL(18, 2)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testAvgDecimal8OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // avg(DECIMAL(2,0)) over a bounded ROWS frame - narrow LONG acc, BYTE
        // ring element.
        assertAvgDecimalFrameRoundTrip(
                "DECIMAL(2, 0)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t15\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t9\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testMaxDecimal128OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(38,6)) over a bounded RANGE frame - DECIMAL128 ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(38, 6)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.000000\n"
        );
    }

    @Test
    public void testMaxDecimal128OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(38,6)) over a bounded ROWS frame - DECIMAL128 ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(38, 6)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.000000\n"
        );
    }

    @Test
    public void testMaxDecimal16OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(4,1)) over a bounded RANGE frame - SHORT ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(4, 1)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.0\n"
        );
    }

    @Test
    public void testMaxDecimal16OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(4,1)) over a bounded ROWS frame - SHORT ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(4, 1)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.0\n"
        );
    }

    @Test
    public void testMaxDecimal256OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(60,0)) over a bounded RANGE frame - DECIMAL256 ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(60, 0)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18\n"
        );
    }

    @Test
    public void testMaxDecimal256OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(60,0)) over a bounded ROWS frame - DECIMAL256 ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(60, 0)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18\n"
        );
    }

    @Test
    public void testMaxDecimal32OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(9,3)) over a bounded RANGE frame - INT ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(9, 3)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.000\n"
        );
    }

    @Test
    public void testMaxDecimal32OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(9,3)) over a bounded ROWS frame - INT ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(9, 3)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.000\n"
        );
    }

    @Test
    public void testMaxDecimal64OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(18,2)) over a bounded RANGE frame - LONG ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(18, 2)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.00\n"
        );
    }

    @Test
    public void testMaxDecimal64OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(18,2)) over a bounded ROWS frame - LONG ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(18, 2)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.00\n"
        );
    }

    @Test
    public void testMaxDecimal8OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(2,0)) over a bounded RANGE frame - BYTE ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(2, 0)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18\n"
        );
    }

    @Test
    public void testMaxDecimal8OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // max(DECIMAL(2,0)) over a bounded ROWS frame - BYTE ring element +
        // monotonic deque.
        assertMaxMinDecimalFrameRoundTrip(
                "max",
                "DECIMAL(2, 0)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18\n"
        );
    }

    @Test
    public void testMinDecimal64OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // min(DECIMAL(18,2)) over a bounded RANGE frame - min reuses Max's classes;
        // the LESS_THAN comparator yields the running minimum. Increasing values keep
        // a 3-element deque live at snapshot time.
        assertMaxMinDecimalFrameRoundTrip(
                "min",
                "DECIMAL(18, 2)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.00\n"
        );
    }

    @Test
    public void testMinDecimal64OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // min(DECIMAL(18,2)) over a bounded ROWS frame - min reuses Max's classes.
        assertMaxMinDecimalFrameRoundTrip(
                "min",
                "DECIMAL(18, 2)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.00\n"
        );
    }

    @Test
    public void testAnchorResetsMaxDateAcrossDayBoundary() throws Exception {
        // max over a DATE column, re-anchored each day via the null sentinel.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v DATE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(v) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, v) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-20T00:00:00.000Z'::date), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-10T00:00:00.000Z'::date), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000Z'::date), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', '2026-01-15T00:00:00.000Z'::date), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', '2026-01-05T00:00:00.000Z'::date), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', '2026-01-25T00:00:00.000Z'::date)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                    "2026-08-01T00:00:00.000000Z\ta\t2026-01-20T00:00:00.000Z\n" +
                    "2026-08-01T01:00:00.000000Z\ta\t2026-01-20T00:00:00.000Z\n" +
                    "2026-08-01T02:00:00.000000Z\ta\t2026-01-30T00:00:00.000Z\n" +
                    "2026-08-02T00:00:00.000000Z\ta\t2026-01-15T00:00:00.000Z\n" +
                    "2026-08-02T01:00:00.000000Z\ta\t2026-01-15T00:00:00.000Z\n" +
                    "2026-08-02T02:00:00.000000Z\ta\t2026-01-25T00:00:00.000Z\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsFirstValueIgnoreNullsTimestampAcrossDayBoundary() throws Exception {
        // first_value IGNORE NULLS over a TIMESTAMP column: the FirstNotNull path skips
        // leading NULLs and, on each anchor reset, recaptures the first non-null of the
        // new day. Without the reset, day 2 would keep day 1's 2026-01-20.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, first_value(v) IGNORE NULLS OVER w AS f FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, v) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', NULL), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', NULL), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', '2026-01-05T00:00:00.000000Z')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, f FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tf\n" +
                    "2026-08-01T00:00:00.000000Z\ta\t\n" +
                    "2026-08-01T01:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                    "2026-08-01T02:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                    "2026-08-02T00:00:00.000000Z\ta\t\n" +
                    "2026-08-02T01:00:00.000000Z\ta\t2026-01-05T00:00:00.000000Z\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsFirstValueTimestampAcrossDayBoundary() throws Exception {
        // first_value over a TIMESTAMP column, re-anchored each day via the initialized flag.
        // Without the reset, day 2 would keep day 1's first value 2026-01-20.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, first_value(v) OVER w AS f FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, v) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', '2026-01-15T00:00:00.000000Z'), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', '2026-01-05T00:00:00.000000Z'), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', '2026-01-25T00:00:00.000000Z')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, f FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tf\n" +
                    "2026-08-01T00:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                    "2026-08-01T01:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                    "2026-08-01T02:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                    "2026-08-02T00:00:00.000000Z\ta\t2026-01-15T00:00:00.000000Z\n" +
                    "2026-08-02T01:00:00.000000Z\ta\t2026-01-15T00:00:00.000000Z\n" +
                    "2026-08-02T02:00:00.000000Z\ta\t2026-01-15T00:00:00.000000Z\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMaxTimestampAcrossDayBoundary() throws Exception {
        // max over a TIMESTAMP column, re-anchored each day via the null sentinel.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(v) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, v) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', '2026-01-15T00:00:00.000000Z'), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', '2026-01-05T00:00:00.000000Z'), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', '2026-01-25T00:00:00.000000Z')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                    "2026-08-01T00:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                    "2026-08-01T01:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                    "2026-08-01T02:00:00.000000Z\ta\t2026-01-30T00:00:00.000000Z\n" +
                    "2026-08-02T00:00:00.000000Z\ta\t2026-01-15T00:00:00.000000Z\n" +
                    "2026-08-02T01:00:00.000000Z\ta\t2026-01-15T00:00:00.000000Z\n" +
                    "2026-08-02T02:00:00.000000Z\ta\t2026-01-25T00:00:00.000000Z\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMinTimestampAcrossDayBoundary() throws Exception {
        // min over a TIMESTAMP column (min reuses Max's classes), re-anchored each day.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, min(v) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, v) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                    "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                    "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                    "('2026-08-02T00:00:00.000000Z', 'a', '2026-01-15T00:00:00.000000Z'), " +
                    "('2026-08-02T01:00:00.000000Z', 'a', '2026-01-05T00:00:00.000000Z'), " +
                    "('2026-08-02T02:00:00.000000Z', 'a', '2026-01-25T00:00:00.000000Z')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                    "2026-08-01T00:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                    "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                    "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                    "2026-08-02T00:00:00.000000Z\ta\t2026-01-15T00:00:00.000000Z\n" +
                    "2026-08-02T01:00:00.000000Z\ta\t2026-01-05T00:00:00.000000Z\n" +
                    "2026-08-02T02:00:00.000000Z\ta\t2026-01-05T00:00:00.000000Z\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFirstValueDateOverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // first_value(DATE) over a bounded ROWS frame - RESPECT NULLS, fixed-size ring.
        assertFirstValueTimestampDateFrameRoundTrip(
                "DATE",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                false,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000Z'::date), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000Z'::date)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n"
        );
    }

    @Test
    public void testFirstValueIgnoreNullsTimestampOverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // first_value(TIMESTAMP) IGNORE NULLS over a bounded RANGE frame - exercises the
        // FirstNotNull subclass (full physical-ring serialization) and skipping a NULL inside the frame.
        assertFirstValueTimestampDateFrameRoundTrip(
                "TIMESTAMP",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                true,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testFirstValueIgnoreNullsTimestampOverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // first_value(TIMESTAMP) IGNORE NULLS over a bounded ROWS frame - skips a NULL inside the frame.
        assertFirstValueTimestampDateFrameRoundTrip(
                "TIMESTAMP",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                true,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testFirstValueIgnoreNullsTimestampOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // first_value(TIMESTAMP) IGNORE NULLS over the unbounded-preceding (anchored) shape -
        // accumulator only, the captured first non-null survives a NULL row.
        assertFirstValueTimestampDateUnboundedRoundTrip(
                "TIMESTAMP",
                true,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testFirstValueTimestampOverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // first_value(TIMESTAMP) over a bounded RANGE frame - RESPECT NULLS, variable-size ring.
        assertFirstValueTimestampDateFrameRoundTrip(
                "TIMESTAMP",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                false,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testFirstValueTimestampOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // first_value(TIMESTAMP) over the unbounded-preceding (anchored) shape - accumulator only.
        assertFirstValueTimestampDateUnboundedRoundTrip(
                "TIMESTAMP",
                false,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testLastValueDateOverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // last_value(DATE) over a bounded RANGE frame ending before the current row -
        // RESPECT NULLS, variable-size ring.
        assertLastValueTimestampDateFrameRoundTrip(
                "DATE",
                "RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING",
                false,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000Z'::date), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000Z'::date)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-20T00:00:00.000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-12T00:00:00.000Z\n"
        );
    }

    @Test
    public void testLastValueDateOverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // last_value(DATE) over a bounded ROWS frame ending before the current row -
        // RESPECT NULLS, fixed-size ring.
        assertLastValueTimestampDateFrameRoundTrip(
                "DATE",
                "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
                false,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000Z'::date), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000Z'::date)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-20T00:00:00.000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-12T00:00:00.000Z\n"
        );
    }

    @Test
    public void testLastValueDecimal128OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(38, 6)",
                "RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000000\n"
        );
    }

    @Test
    public void testLastValueDecimal128OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(38, 6)",
                "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000000\n"
        );
    }

    @Test
    public void testLastValueDecimal16OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(4, 1)",
                "RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.0\n"
        );
    }

    @Test
    public void testLastValueDecimal16OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(4, 1)",
                "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.0\n"
        );
    }

    @Test
    public void testLastValueDecimal256OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(60, 0)",
                "RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testLastValueDecimal256OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(60, 0)",
                "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testLastValueDecimal32OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(9, 3)",
                "RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000\n"
        );
    }

    @Test
    public void testLastValueDecimal32OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(9, 3)",
                "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000\n"
        );
    }

    @Test
    public void testLastValueDecimal8OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(2, 0)",
                "RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testLastValueDecimal8OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(2, 0)",
                "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testLastValueIgnoreNullsDecimal128OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                true,
                "DECIMAL(38, 6)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.000000\n"
        );
    }

    @Test
    public void testLastValueIgnoreNullsDecimal256OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertLastValueDecimalFrameRoundTrip(
                true,
                "DECIMAL(60, 0)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18\n"
        );
    }

    @Test
    public void testLastValueDecimal64OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // last_value(DECIMAL(18,2)) over a bounded RANGE frame ending before the
        // current row - RESPECT NULLS, 8-byte (LONG) ring element.
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(18, 2)",
                "RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testLastValueDecimal64OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // last_value(DECIMAL(18,2)) over a bounded ROWS frame ending before the
        // current row - RESPECT NULLS, fixed-size ring.
        assertLastValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(18, 2)",
                "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testLastValueIgnoreNullsDecimal64OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // last_value(DECIMAL(18,2)) IGNORE NULLS over a bounded RANGE frame ending at
        // the current row - the FirstNotNull-style IGNORE base, skipping a NULL.
        assertLastValueDecimalFrameRoundTrip(
                true,
                "DECIMAL(18, 2)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.00\n"
        );
    }

    @Test
    public void testLastValueIgnoreNullsDecimal64OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // last_value(DECIMAL(18,2)) IGNORE NULLS over a bounded ROWS frame ending at
        // the current row - skips a NULL inside the frame.
        assertLastValueDecimalFrameRoundTrip(
                true,
                "DECIMAL(18, 2)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t30.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t18.00\n"
        );
    }

    @Test
    public void testLastValueIgnoreNullsTimestampOverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // last_value(TIMESTAMP) IGNORE NULLS over a bounded RANGE frame ending at the current
        // row - the FirstNotNull-style IGNORE base, skipping a NULL inside the frame.
        assertLastValueTimestampDateFrameRoundTrip(
                "TIMESTAMP",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                true,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-30T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-18T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testLastValueIgnoreNullsTimestampOverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // last_value(TIMESTAMP) IGNORE NULLS over a bounded ROWS frame ending at the current
        // row - skips a NULL inside the frame.
        assertLastValueTimestampDateFrameRoundTrip(
                "TIMESTAMP",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                true,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-30T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-18T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testLastValueTimestampOverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // last_value(TIMESTAMP) over a bounded RANGE frame ending before the current row -
        // RESPECT NULLS, variable-size ring.
        assertLastValueTimestampDateFrameRoundTrip(
                "TIMESTAMP",
                "RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING",
                false,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-12T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testLastValueTimestampOverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // last_value(TIMESTAMP) over a bounded ROWS frame ending before the current row -
        // RESPECT NULLS, fixed-size ring.
        assertLastValueTimestampDateFrameRoundTrip(
                "TIMESTAMP",
                "ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING",
                false,
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-12T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testMaxDateOverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // max(DATE) over a bounded RANGE frame - LONG ring element + monotonic deque.
        assertMaxMinTimestampDateFrameRoundTrip(
                "max",
                "DATE",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000Z'::date), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000Z'::date)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-20T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-30T00:00:00.000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-12T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-18T00:00:00.000Z\n"
        );
    }

    @Test
    public void testMaxTimestampOverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // max(TIMESTAMP) over a bounded RANGE frame - LONG ring element + monotonic deque.
        assertMaxMinTimestampDateFrameRoundTrip(
                "max",
                "TIMESTAMP",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-30T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-12T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-18T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testMaxTimestampOverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // max(TIMESTAMP) over a bounded ROWS frame - LONG ring element + monotonic deque.
        assertMaxMinTimestampDateFrameRoundTrip(
                "max",
                "TIMESTAMP",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-30T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-12T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-18T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testMaxTimestampOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // max(TIMESTAMP) over the unbounded-preceding (anchored) shape - accumulator only.
        assertMaxMinTimestampDateUnboundedRoundTrip(
                "max",
                "TIMESTAMP",
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-20T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-30T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-12T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-18T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testMinDateOverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // min(DATE) over a bounded ROWS frame - min reuses Max's classes.
        assertMaxMinTimestampDateFrameRoundTrip(
                "min",
                "DATE",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000Z'::date), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000Z'::date)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n"
        );
    }

    @Test
    public void testMinDateOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // min(DATE) over the unbounded-preceding (anchored) shape - accumulator only.
        assertMaxMinTimestampDateUnboundedRoundTrip(
                "min",
                "DATE",
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000Z'::date), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000Z'::date), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000Z'::date), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000Z'::date)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-06T00:00:00.000Z\n"
        );
    }

    @Test
    public void testMinTimestampOverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // min(TIMESTAMP) over a bounded ROWS frame - min reuses Max's classes.
        assertMaxMinTimestampDateFrameRoundTrip(
                "min",
                "TIMESTAMP",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', '2026-01-10T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', '2026-01-20T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', '2026-01-30T00:00:00.000000Z'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', '2026-03-06T00:00:00.000000Z'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', '2026-03-12T00:00:00.000000Z'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', '2026-03-18T00:00:00.000000Z')",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t2026-01-10T00:00:00.000000Z\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t2026-03-06T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testSumDecimal128OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(38,6)) over a bounded RANGE frame - DECIMAL256 acc, 16-byte
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(38, 6)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36.000000\n"
        );
    }

    @Test
    public void testSumDecimal128OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(38,6)) over a bounded ROWS frame - DECIMAL256 acc, 16-byte
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(38, 6)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36.000000\n"
        );
    }

    @Test
    public void testSumDecimal16OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(4,1)) over a bounded RANGE frame - narrow LONG acc, SHORT
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(4, 1)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36.0\n"
        );
    }

    @Test
    public void testSumDecimal16OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(4,1)) over a bounded ROWS frame - narrow LONG acc, SHORT
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(4, 1)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36.0\n"
        );
    }

    @Test
    public void testSumDecimal256OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(60,0)) over a bounded RANGE frame - DECIMAL256 acc, 32-byte
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(60, 0)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36\n"
        );
    }

    @Test
    public void testSumDecimal256OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(60,0)) over a bounded ROWS frame - DECIMAL256 acc, 32-byte
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(60, 0)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36\n"
        );
    }

    @Test
    public void testSumDecimal32OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(9,3)) over a bounded RANGE frame - DECIMAL128 acc, INT ring
        // element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(9, 3)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36.000\n"
        );
    }

    @Test
    public void testSumDecimal32OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(9,3)) over a bounded ROWS frame - DECIMAL128 acc, INT ring
        // element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(9, 3)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36.000\n"
        );
    }

    @Test
    public void testSumDecimal64OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(18,2)) over a bounded RANGE frame - DECIMAL128 acc, 8-byte
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(18, 2)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36.00\n"
        );
    }

    @Test
    public void testSumDecimal64OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(18,2)) over a bounded ROWS frame - DECIMAL128 acc, 8-byte
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(18, 2)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36.00\n"
        );
    }

    @Test
    public void testSumDecimal8OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(2,0)) over a bounded RANGE frame - narrow LONG acc, BYTE
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(2, 0)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36\n"
        );
    }

    @Test
    public void testSumDecimal8OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // sum(DECIMAL(2,0)) over a bounded ROWS frame - narrow LONG acc, BYTE
        // ring element.
        assertSumDecimalFrameRoundTrip(
                "DECIMAL(2, 0)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t30\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t60\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t18\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t36\n"
        );
    }

    @Test
    public void testAnchorResetsCountAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, count(x) OVER w AS c FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // 3 rows on day 1, 2 rows on day 2; count restarts at 1 each day.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 1, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 2, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 3, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 4, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 5, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, c FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tc\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t2\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t3\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsRowNumberAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 1, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 2, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 3, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 4, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\trn\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t2\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMaxAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(x) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1 max climbs 5 -> 50; day 2 starts fresh at 3 (lower than day 1's
            // 50). Without anchor reset, day 2's first row would carry forward
            // max=50 instead of 3.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 50.0, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 25.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 3.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 7.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t50.0\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t50.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t3.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t7.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsFirstValueAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, first_value(x) OVER w AS f FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1 first_value sticks to the first row (10.0); day 2 first_value
            // should reset to the new first row (100.0). Without anchor reset,
            // day 2 would keep returning 10.0 (the original first row).
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 30.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 100.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 200.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, f FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tf\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t100.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorFirstValueDecimal128RestoresAcrossRestart() throws Exception {
        // Restart-restore for the Decimal128 (16-byte value slot) snapshot payload.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, d DECIMAL(38, 6)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, first_value(d) OVER w AS f FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, d) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10.000000m), " +
                        "('2026-10-01T01:00:00.000000Z', 1, 20.000000m)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(engine.getLiveViewRegistry().getViewInstance("lv").isCheckpointRestoreAttempted());

            // first_value stays 10.000000: the captured value survived the restart.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, d) VALUES ('2026-10-01T02:00:00.000000Z', 1, 30.000000m)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, f FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tf\n" +
                            "2026-10-01T00:00:00.000000Z\t1\t10.000000\n" +
                            "2026-10-01T01:00:00.000000Z\t1\t10.000000\n" +
                            "2026-10-01T02:00:00.000000Z\t1\t10.000000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsFirstValueDecimal128AcrossDayBoundary() throws Exception {
        // first_value over a DECIMAL(38,6) column (DECIMAL128 storage).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DECIMAL(38, 6), sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, first_value(x) OVER w AS f FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.000000m, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.000000m, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 100.000000m, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 200.000000m, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, f FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tf\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t10.000000\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100.000000\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t100.000000\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsFirstValueDecimal64AcrossDayBoundary() throws Exception {
        // first_value over a DECIMAL(18,2) column (DECIMAL64 storage, LONG value slot).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DECIMAL(18, 2), sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, first_value(x) OVER w AS f FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1 first_value sticks to 10.00; day 2 resets to the new first
            // row (100.00). Without anchor reset day 2 would keep 10.00.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.00m, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.00m, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 30.00m, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 100.00m, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 200.00m, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, f FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tf\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t10.00\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100.00\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t100.00\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsFirstValueIgnoreNullsDecimal64AcrossDayBoundary() throws Exception {
        // first_value IGNORE NULLS over DECIMAL(18,2): the FirstNotNull path skips
        // leading NULLs and, on each anchor reset, recaptures the first non-null of
        // the new day. Without the reset, day 2 would keep day 1's 20.00.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DECIMAL(18, 2), sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, first_value(x) IGNORE NULLS OVER w AS f FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', NULL, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.00m, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 30.00m, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', NULL, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 100.00m, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, f FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tf\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t20.00\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t100.00\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFirstValueDecimal128OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(38,6)) over a bounded RANGE frame - 16-byte ring
        // element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(38, 6)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.000000\n"
        );
    }

    @Test
    public void testFirstValueDecimal128OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(38,6)) over a bounded ROWS frame - 16-byte ring
        // element; the sliding 2-preceding window makes the first value change.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(38, 6)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12.000000\n"
        );
    }

    @Test
    public void testFirstValueDecimal128OverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        assertFirstValueDecimalUnboundedRoundTrip(
                false,
                "DECIMAL(38, 6)",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.000000\n"
        );
    }

    @Test
    public void testFirstValueDecimal16OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(4,1)) over a bounded RANGE frame - SHORT ring
        // element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(4, 1)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.0\n"
        );
    }

    @Test
    public void testFirstValueDecimal16OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(4,1)) over a bounded ROWS frame - SHORT ring element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(4, 1)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12.0\n"
        );
    }

    @Test
    public void testFirstValueDecimal16OverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        assertFirstValueDecimalUnboundedRoundTrip(
                false,
                "DECIMAL(4, 1)",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.0\n"
        );
    }

    @Test
    public void testFirstValueDecimal256OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(60,0)) over a bounded RANGE frame - 32-byte ring
        // element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(60, 0)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6\n"
        );
    }

    @Test
    public void testFirstValueDecimal256OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(60,0)) over a bounded ROWS frame - 32-byte ring
        // element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(60, 0)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testFirstValueDecimal256OverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        assertFirstValueDecimalUnboundedRoundTrip(
                false,
                "DECIMAL(60, 0)",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6\n"
        );
    }

    @Test
    public void testFirstValueDecimal32OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(9,3)) over a bounded RANGE frame - INT ring element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(9, 3)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.000\n"
        );
    }

    @Test
    public void testFirstValueDecimal32OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(9,3)) over a bounded ROWS frame - INT ring element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(9, 3)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12.000\n"
        );
    }

    @Test
    public void testFirstValueDecimal32OverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        assertFirstValueDecimalUnboundedRoundTrip(
                false,
                "DECIMAL(9, 3)",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.000\n"
        );
    }

    @Test
    public void testFirstValueDecimal64OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(18,2)) over a bounded RANGE frame - 8-byte (LONG)
        // ring element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(18, 2)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.00\n"
        );
    }

    @Test
    public void testFirstValueDecimal64OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(18,2)) over a bounded ROWS frame - 8-byte (LONG) ring
        // element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(18, 2)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testFirstValueDecimal64OverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        assertFirstValueDecimalUnboundedRoundTrip(
                false,
                "DECIMAL(18, 2)",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.00\n"
        );
    }

    @Test
    public void testFirstValueDecimal8OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(2,0)) over a bounded RANGE frame - BYTE ring element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(2, 0)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6\n"
        );
    }

    @Test
    public void testFirstValueDecimal8OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(2,0)) over a bounded ROWS frame - BYTE ring element.
        assertFirstValueDecimalFrameRoundTrip(
                false,
                "DECIMAL(2, 0)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testFirstValueDecimal8OverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        assertFirstValueDecimalUnboundedRoundTrip(
                false,
                "DECIMAL(2, 0)",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6\n"
        );
    }

    @Test
    public void testFirstValueIgnoreNullsDecimal64OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(18,2)) IGNORE NULLS over a bounded RANGE frame -
        // exercises the FirstNotNull subclass (full physical-ring serialization)
        // and skipping a NULL inside the frame.
        assertFirstValueDecimalFrameRoundTrip(
                true,
                "DECIMAL(18, 2)",
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.00\n"
        );
    }

    @Test
    public void testFirstValueIgnoreNullsDecimal64OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // first_value(DECIMAL(18,2)) IGNORE NULLS over a bounded ROWS frame -
        // exercises the FirstNotNull ROWS subclass (firstNotNullIdx tracking plus
        // full physical-ring serialization) and skipping a NULL inside the frame.
        // 'a' has a NULL at row 2; the 2-preceding window keeps the first non-null
        // at 10.00 until that value falls out of the frame (row 4 -> 30.00).
        assertFirstValueDecimalFrameRoundTrip(
                true,
                "DECIMAL(18, 2)",
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', null), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t10.00\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t30.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testFirstValueIgnoreNullsDecimal64OverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // IGNORE NULLS variant: 'a' has a leading NULL (first non-null is 20.00),
        // 'b' is null-free (first is 6.00). Exercises the FirstNotNull LV path and
        // its snapshot payload.
        assertFirstValueDecimalUnboundedRoundTrip(
                true,
                "DECIMAL(18, 2)",
                "('2026-08-01T00:00:00.000000Z', 'a', NULL), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t6.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t6.00\n"
        );
    }

    @Test
    public void testAnchorResetsRankAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, rank() OVER w AS r FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1: 3 distinct ts values -> rank=1,2,3.
            // Day 2 (anchor reset): rank restarts at 1.
            // Without reset, day 2 would continue counting from 4.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 30, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 6, 'a'), " +
                    "('2026-08-02T02:00:00.000000Z', 7, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, r FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tr\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t2\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t3\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t2\n" +
                            "2026-08-02T02:00:00.000000Z\ta\t3\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorIsolatesPartitionsIndependently() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Two partitions ('a' and 'b') interleaved across two days. Each partition
            // owns its anchor-state independently: when sym='a' rolls over at the
            // 2026-08-02 boundary, sym='b''s running state must NOT also reset
            // (and vice versa). The test interleaves the partitions in time so
            // that any cross-partition state leak would corrupt the running sum.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T00:30:00.000000Z', 100, 'b'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-01T01:30:00.000000Z', 200, 'b'), " +
                    "('2026-08-02T00:00:00.000000Z', 30, 'a'), " +
                    "('2026-08-02T00:30:00.000000Z', 300, 'b')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Day 1: a = 10, then 30 (10+20); b = 100, then 300 (100+200).
            // Day 2: both a and b reset to first-row value (a=30, b=300).
            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T00:30:00.000000Z\tb\t100.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-01T01:30:00.000000Z\tb\t300.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T00:30:00.000000Z\tb\t300.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testManualCompactDoesNotDropLiveState() throws Exception {
        // A manual compact() must never drop a partition that still holds live
        // current-bucket accumulator state. Reset-driven tombstoning is disabled
        // (markPartitionAlive cancels the bit resetPartition sets on the same
        // anchor-cross row), so a just-crossed partition is never a compaction
        // candidate. After compact(), a later same-bucket row must continue the
        // accumulator rather than restart it from identity.
        //
        // INT partition keys side-step the per-WAL-segment SYMBOL index collision
        // that confuses cross-segment partition lookups.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // Day 1 seeds sym 1, 2. Day 2 crosses both (first row of the new
                // bucket: sym 1 -> 11, sym 2 -> 22). Neither is tombstoned.
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10, 1), " +
                        "('2026-08-01T01:00:00.000000Z', 20, 2), " +
                        "('2026-08-02T00:00:00.000000Z', 11, 1), " +
                        "('2026-08-02T01:00:00.000000Z', 22, 2)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull("anchor window must be built after refresh", window);
                Assert.assertEquals("reset-driven tombstones are disabled", 0L, window.getTombstoneCount());
                Assert.assertEquals(2L, window.getAnchorMapSize());

                // Force a manual compaction. It must keep both live partitions.
                window.compact();
                Assert.assertEquals(
                        "compact() keeps live partitions (no reset-driven tombstones to drop)",
                        2L,
                        window.getAnchorMapSize()
                );
                Assert.assertEquals(0L, window.getTombstoneCount());

                // A later same-bucket (day 2) row for sym 1 must continue the
                // day-2 accumulator: 11 + 5 = 16, not restart at 5.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES ('2026-08-02T03:00:00.000000Z', 5, 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, s FROM lv WHERE sym = 1 AND ts >= '2026-08-02' ORDER BY ts").noLeakCheck().timestamp("ts").returns("ts\tsym\ts\n" +
                                "2026-08-02T00:00:00.000000Z\t1\t11.0\n" +
                                "2026-08-02T03:00:00.000000Z\t1\t16.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testResetDrivenTombstonesDisabled() throws Exception {
        // Anchor crossings must not accumulate tombstones: the reset-driven
        // tombstone (which a just-crossed, still-live partition would otherwise
        // carry) is cancelled on the same row, so the anchor map keeps every
        // partition and the per-window tombstone count stays zero. End-to-end
        // output stays correct across the day boundary.
        //
        // INT partition keys side-step the per-WAL-segment SYMBOL index collision.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // Day 1 seeds sym 1, 2, 3. Day 2 crosses all three.
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10, 1), " +
                        "('2026-08-01T01:00:00.000000Z', 20, 2), " +
                        "('2026-08-01T02:00:00.000000Z', 30, 3), " +
                        "('2026-08-02T00:00:00.000000Z', 11, 1), " +
                        "('2026-08-02T01:00:00.000000Z', 22, 2), " +
                        "('2026-08-02T02:00:00.000000Z', 33, 3)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull("anchor window must be built after refresh", window);
                Assert.assertEquals(
                        "no reset-driven tombstones accumulate across anchor crossings",
                        0L,
                        window.getTombstoneCount()
                );
                Assert.assertEquals(
                        "all three partitions stay in the anchor map",
                        3L,
                        window.getAnchorMapSize()
                );

                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts, sym").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-08-01T00:00:00.000000Z\t1\t10.0\n" +
                                "2026-08-01T01:00:00.000000Z\t2\t20.0\n" +
                                "2026-08-01T02:00:00.000000Z\t3\t30.0\n" +
                                "2026-08-02T00:00:00.000000Z\t1\t11.0\n" +
                                "2026-08-02T01:00:00.000000Z\t2\t22.0\n" +
                                "2026-08-02T02:00:00.000000Z\t3\t33.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testHighChurnSameBucketFollowupNoDataLoss() throws Exception {
        // Regression: a low compaction threshold once let many simultaneous
        // anchor crossings drop partitions whose accumulator had just been
        // repopulated by the crossing row's computeNext, so a later same-bucket
        // row restarted the accumulator from identity and produced wrong output.
        // With reset-driven tombstoning disabled, a same-bucket follow-up must
        // continue the day-2 accumulator regardless of the threshold.
        //
        // INT partition keys side-step the per-WAL-segment SYMBOL index collision.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // Day 1 seeds sym 1, 2, 3. Day 2 crosses all three (the old
                // buggy auto-trigger would have fired here and dropped the live
                // day-2 state of sym 1 and sym 2).
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10, 1), " +
                        "('2026-08-01T01:00:00.000000Z', 20, 2), " +
                        "('2026-08-01T02:00:00.000000Z', 30, 3), " +
                        "('2026-08-02T00:00:00.000000Z', 11, 1), " +
                        "('2026-08-02T01:00:00.000000Z', 22, 2), " +
                        "('2026-08-02T02:00:00.000000Z', 33, 3)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull("anchor window must be built after refresh", window);
                Assert.assertEquals(
                        "no reset-driven tombstones accumulate even under churn",
                        0L,
                        window.getTombstoneCount()
                );
                Assert.assertEquals(
                        "all three partitions retained (compaction never drops live state)",
                        3L,
                        window.getAnchorMapSize()
                );

                // Same-bucket (day 2) follow-ups for every partition must
                // continue, not restart: sym 1 -> 11+5=16, sym 2 -> 22+6=28,
                // sym 3 -> 33+7=40.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-02T03:00:00.000000Z', 5, 1), " +
                        "('2026-08-02T04:00:00.000000Z', 6, 2), " +
                        "('2026-08-02T05:00:00.000000Z', 7, 3)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, s FROM lv WHERE ts >= '2026-08-02' ORDER BY ts, sym").noLeakCheck().timestamp("ts").returns("ts\tsym\ts\n" +
                                "2026-08-02T00:00:00.000000Z\t1\t11.0\n" +
                                "2026-08-02T01:00:00.000000Z\t2\t22.0\n" +
                                "2026-08-02T02:00:00.000000Z\t3\t33.0\n" +
                                "2026-08-02T03:00:00.000000Z\t1\t16.0\n" +
                                "2026-08-02T04:00:00.000000Z\t2\t28.0\n" +
                                "2026-08-02T05:00:00.000000Z\t3\t40.0\n");

                // Day-3 row for sym 1 starts a fresh bucket (correct reset).
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES ('2026-08-03T00:00:00.000000Z', 9, 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                assertQuery("SELECT ts, sym, s FROM lv WHERE ts >= '2026-08-03' ORDER BY ts").noLeakCheck().timestamp("ts").returns("ts\tsym\ts\n" +
                                "2026-08-03T00:00:00.000000Z\t1\t9.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFrontierSweepDropsStalePartitionsAndRevivesCorrectly() throws Exception {
        // The frontier sweep reclaims partitions whose bucket has fallen two
        // buckets behind the current one, shrinking both the anchor map and each
        // function's partition map (via the reused ping-pong scratch -- no
        // per-sweep allocation). A dropped partition that later revives does so in
        // a new bucket and starts fresh, which is the correct anchored-window reset.
        //
        // INT partition keys side-step the per-WAL-segment SYMBOL index collision.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // Day 1 seeds sym 1, 2, 3. sym 2 and 3 never trade again (they
                // will fall behind the frontier). sym 1 trades on day 2 and day 3.
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10, 1), " +
                        "('2026-08-01T01:00:00.000000Z', 20, 2), " +
                        "('2026-08-01T02:00:00.000000Z', 30, 3)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES ('2026-08-02T00:00:00.000000Z', 11, 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull(window);
                // Frontier advanced day1 -> day2 (prevFrontier=day1). The sweep keeps
                // current+previous bucket, so sym 2 and 3 (still on day 1 = prev) stay.
                Assert.assertEquals("all partitions retained at the first advance", 3L, window.getAnchorMapSize());

                // Day 3 for sym 1 advances the frontier day2 -> day3 (prevFrontier=day2).
                // sym 2 and 3 are now on day 1, which is below the previous bucket,
                // so the sweep drops them from the anchor map AND the sum's map.
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES ('2026-08-03T00:00:00.000000Z', 12, 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals("stale sym 2 and 3 swept; only sym 1 remains", 1L, window.getAnchorMapSize());
                Assert.assertEquals(
                        "the sum function map shrank in lockstep (reused scratch, no leak)",
                        1L,
                        window.getFunctions().getQuick(0).getPartitionMap().size()
                );

                // sym 2 revives on day 3 -- a brand new bucket, so it starts fresh
                // at 99 (not 20 + 99). sym 1's day-3 running sum is just 12.
                setCurrentMicros(600_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES ('2026-08-03T01:00:00.000000Z', 99, 2)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts, sym").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-08-01T00:00:00.000000Z\t1\t10.0\n" +
                                "2026-08-01T01:00:00.000000Z\t2\t20.0\n" +
                                "2026-08-01T02:00:00.000000Z\t3\t30.0\n" +
                                "2026-08-02T00:00:00.000000Z\t1\t11.0\n" +
                                "2026-08-03T00:00:00.000000Z\t1\t12.0\n" +
                                "2026-08-03T01:00:00.000000Z\t2\t99.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCrossCycleAnchorMapPreserved() throws Exception {
        // A second refresh cycle that hits no anchor crossings
        // must not wipe the anchor map populated by the first cycle. Before
        // 2c.4, the cursor-reopen chain (AnchorDispatchingCursor.toTop ->
        // LiveViewWindow.toTop -> anchorMap.clear()) discarded the in-memory
        // map at the head of every tick, so any partition not visited in
        // the current segment's row range silently lost its lastAnchorValue
        // record.
        //
        // Uses INT partition keys (sym=1, 2, 3) rather than SYMBOL because
        // the WAL writes SYMBOL columns as local-segment indices and the
        // LV's per-partition RecordSink reads them straight through; SYMBOL
        // keys for the same string value collide across separate WAL
        // segments and confuse the multi-cycle anchor-map preservation
        // assertion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10.0), " +
                        "('2026-10-01T01:00:00.000000Z', 2, 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals(
                        "two partitions seeded on cycle 1",
                        2L,
                        lv.getAnchorWindow().getAnchorMapSize()
                );

                // Cycle 2 commits one row for a new partition sym=3. Without
                // the 2c.4 fix the cursor-reopen chain would clear the
                // anchor map first, leaving only {3}. With 2c.4 in place
                // {1, 2} survive and 3 joins them.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-10-01T02:00:00.000000Z', 3, 30.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "anchor map carries cycle-1 entries alongside cycle-2's new partition",
                        3L,
                        lv.getAnchorWindow().getAnchorMapSize()
                );
                Assert.assertEquals(
                        "no tombstones - the new row hit a fresh partition, the existing ones weren't anchor-crossed",
                        0L,
                        lv.getAnchorWindow().getTombstoneCount()
                );
                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-10-01T00:00:00.000000Z\t1\t10.0\n" +
                                "2026-10-01T01:00:00.000000Z\t2\t20.0\n" +
                                "2026-10-01T02:00:00.000000Z\t3\t30.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testPostRestartCommitPreservesAnchorMap() throws Exception {
        // The first post-restart refresh cycle must NOT wipe
        // the anchor map that tryRestoreFromHead just rehydrated. Without
        // the 2c.4 fix, getIncrementalCursor would drive
        // AnchorDispatchingCursor.toTop -> LiveViewWindow.toTop and clear
        // the map before the first new row arrived. The test seeds two
        // partitions, simulates a restart, then commits one new row for a
        // third partition; only with the fix in place does the map carry
        // all three entries after the post-restart cycle.
        //
        // Uses INT partition keys to side-step the per-WAL-segment SYMBOL
        // index collision that confuses cross-segment partition lookups.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            final long preHeadLvSeqTxn;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10.0), " +
                        "('2026-10-01T01:00:00.000000Z', 2, 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(
                        "head .cp was written before restart",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertEquals(
                        "two partitions seeded pre-restart",
                        2L,
                        instance.getAnchorWindow().getAnchorMapSize()
                );
            }

            // Simulate restart: clear in-memory registry, rebuild from
            // on-disk state. The startup sweep restamps the head .cp's
            // lvSeqTxn on the reloaded instance.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            // Drive a single refresh tick with no new commits: this fires
            // tryRestoreFromHead alone (mirroring testRestartRestoresFrom
            // HeadCheckpoint) and validates the rehydrate side of the
            // contract.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(
                    "restore attempted on the first post-restart cycle",
                    reloaded.isCheckpointRestoreAttempted()
            );
            Assert.assertEquals(
                    "rehydrated anchor map carries 'a' and 'b' after restore",
                    2L,
                    reloaded.getAnchorWindow().getAnchorMapSize()
            );

            // Commit a new row for sym=3. Advance the clock past FLUSH EVERY
            // 100ms so the next refresh tick is not rate-limited. Without
            // the 2c.4 fix, getIncrementalCursor's cursor-open chain would
            // clear the rehydrated map before processRow saw the new row,
            // leaving the post-cycle map at {3}. With 2c.4 in place, the
            // map preserves {1, 2} and adds 3.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, x) VALUES ('2026-10-01T02:00:00.000000Z', 3, 30.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            Assert.assertEquals(
                    "anchor map keeps the rehydrated 1 and 2 alongside the new partition 3",
                    3L,
                    reloaded.getAnchorWindow().getAnchorMapSize()
            );
            Assert.assertEquals(
                    "no tombstones - the new row hit a fresh partition, the rehydrated ones stayed alive",
                    0L,
                    reloaded.getAnchorWindow().getTombstoneCount()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testMixedAnchoredBoundedWindowAcrossAnchorBoundary() throws Exception {
        // Regression: in a view that mixes an anchored WINDOW with a bounded
        // ROWS/RANGE WINDOW, the anchor reset must NOT touch the bounded window.
        // The LV anchor runtime previously dispatched resetPartition to every
        // window function in the SELECT, so a bounded sum/avg was zeroed at each
        // anchor crossing and lost the rows that straddled the boundary. The
        // anchored result (row_number) must still reset per bucket while the
        // bounded result (ROWS 1 PRECEDING) must keep spanning the boundary.
        //
        // INT partition keys side-step the per-WAL-segment SYMBOL index collision.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  row_number() OVER w_anchor AS rn, " +
                    "  sum(x) OVER w_bounded AS s " +
                    "FROM base " +
                    "WINDOW " +
                    "  w_anchor AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts)), " +
                    "  w_bounded AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 10, 1), " +
                        "('2026-09-01T01:00:00.000000Z', 100, 2), " +
                        "('2026-09-02T00:00:00.000000Z', 11, 1), " +
                        "('2026-09-02T01:00:00.000000Z', 110, 2), " +
                        "('2026-09-02T02:00:00.000000Z', 12, 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull("anchor window must be built after refresh", window);
                // The anchor window owns only the anchored (unbounded) function;
                // the bounded sum is not in its reset-dispatch set.
                Assert.assertEquals(
                        "anchor window resets only its own (unbounded) functions",
                        1L,
                        (long) window.getFunctions().size()
                );
                Assert.assertTrue(
                        "the bounded ROWS sum must not be in the anchor reset set",
                        window.getFunctions().getQuick(0).getClass().getSimpleName().contains("RowNumber")
                );

                // rn resets at the day boundary; s (ROWS 1 PRECEDING) spans it:
                //   sym 1: 10 -> 21 (10+11) -> 23 (11+12)
                //   sym 2: 100 -> 210 (100+110)
                assertQuery("SELECT ts, sym, rn, s FROM lv ORDER BY ts, sym").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\trn\ts\n" +
                                "2026-09-01T00:00:00.000000Z\t1\t1\t10.0\n" +
                                "2026-09-01T01:00:00.000000Z\t2\t1\t100.0\n" +
                                "2026-09-02T00:00:00.000000Z\t1\t1\t21.0\n" +
                                "2026-09-02T01:00:00.000000Z\t2\t1\t210.0\n" +
                                "2026-09-02T02:00:00.000000Z\t1\t2\t23.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSymbolPartitionKeyStableAcrossWalSegments() throws Exception {
        // Anchored LV with PARTITION BY <SYMBOL col> against a base table that
        // receives multiple INSERT statements. Each INSERT emits its own WAL
        // segment whose local symbol indices start at 0 and grow independently
        // of prior segments, so the same partition string ('a') resolves to a
        // different segment-local int in cycle 2 than in cycle 1.
        // The fix routes SYMBOL partition columns through their resolved
        // string in both the anchor map sink and the per-function partition
        // map sink, so multi-segment cycles converge on a single set of
        // partition entries instead of growing per-segment.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-11-01T00:00:00.000000Z', 10.0, 'a'), " +
                        "('2026-11-01T01:00:00.000000Z', 20.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals(
                        "two partitions seeded on cycle 1",
                        2L,
                        lv.getAnchorWindow().getAnchorMapSize()
                );

                // Cycle 2 lands in a separate WAL segment. The local index for
                // 'a' here is again 0 (the segment starts fresh), but the fix
                // routes the resolved string into the map key so the same
                // partition does not split into two entries.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-11-01T02:00:00.000000Z', 30.0, 'a'), " +
                        "('2026-11-01T03:00:00.000000Z', 40.0, 'c')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "anchor map carries 'a' (from cycle 1), 'b', and the new 'c'",
                        3L,
                        lv.getAnchorWindow().getAnchorMapSize()
                );

                // Per-function map check: 'a' must accumulate 10 + 30 = 40 in
                // a single entry, not split into a cycle-1 and cycle-2 entry
                // by segment-local index collision.
                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:00.000000Z\ta\t10.0\n" +
                                "2026-11-01T01:00:00.000000Z\tb\t20.0\n" +
                                "2026-11-01T02:00:00.000000Z\ta\t40.0\n" +
                                "2026-11-01T03:00:00.000000Z\tc\t40.0\n");

                // Day boundary in cycle 3 anchor-crosses 'a' so its running
                // sum resets. The anchor reset must reach the per-function
                // map entry keyed by the resolved string, not by the
                // segment-local index.
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-11-02T00:00:00.000000Z', 7.0, 'a')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, s FROM lv WHERE ts >= '2026-11-02' ORDER BY ts").noLeakCheck().timestamp("ts").returns("ts\tsym\ts\n" +
                                "2026-11-02T00:00:00.000000Z\ta\t7.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsEmaAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, avg(x, 'alpha', 0.5) OVER w AS e FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1 (10, 20): EMA seeds at 10 then 0.5*20 + 0.5*10 = 15.
            // Day 2 (5, 15) after anchor reset: re-seeds at 5 then 0.5*15 + 0.5*5 = 10.
            // Without the EMA migration, day 2 would carry the day-1 EMA forward.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, e FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\te\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t15.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t10.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsKsumAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, ksum(x) OVER w AS k FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Day 1 cumulative: 10, 30. Day 2 reset cumulative: 5, 20.
            assertQuery("SELECT ts, sym, k FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tk\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t20.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsLagAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, lag(x) OVER w AS l FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Day 1: row 1 lag is null (no prior row in partition), row 2 lag is 10.
            // Day 2 anchor reset: row 1 lag is null again (state cleared); row 2 lag is 5.
            // Without the lag migration, day 2 row 1 would lag the last day-1 value (20).
            assertQuery("SELECT ts, sym, l FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tl\n" +
                            "2026-08-01T00:00:00.000000Z\ta\tnull\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                            "2026-08-02T00:00:00.000000Z\ta\tnull\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t5\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsStddevAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, stddev_pop(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // stddev_pop with a single value is 0; with two values v1, v2 it is |v2-v1|/2.
            // Day 1 (10, 20): 0, then 5. Day 2 reset (5, 25): 0, then 10.
            // Without the Welford migration, day 2 would continue the running stddev.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 25.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t0.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t0.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t10.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsDenseRankAcrossDayBoundary() throws Exception {
        // dense_rank reuses the migrated RankOverPartitionFunction (dense=true);
        // resetPartition restarts the counter at each anchor crossing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, dense_rank() OVER w AS r FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Distinct ts per row, so dense_rank climbs 1,2 within a day and
            // restarts at 1 on day 2. Without reset day 2 would be 3,4.
            execute("INSERT INTO base (ts, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, r FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tr\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t2\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t2\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMinAcrossDayBoundary() throws Exception {
        // min reuses the migrated MaxMin partition function; resetPartition
        // clears the initialized flag so day 2 starts from its own first value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, min(x) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1 min falls 50 -> 5; day 2 starts fresh at 30 (higher than
            // day 1's 5). Without anchor reset, day 2's first row would carry
            // forward min=5 instead of 30.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 50.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 25.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 30.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 7.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, m FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t50.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t7.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsVarPopAcrossDayBoundary() throws Exception {
        // var_pop shares the Welford state with stddev/var_samp; resetPartition
        // zeroes count/mean/m2 so day 2's variance covers day 2 rows only.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, var_pop(x) OVER w AS v FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // var_pop of one value is 0; of {a,b} it is ((a-b)/2)^2 * 2 / 2 =
            // (a-b)^2 / 4. Day 1 (10,20): 0, 25. Day 2 reset (5,25): 0, 100.
            // Without reset, day 2's first row would be the variance over all
            // three prior values (non-zero).
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 25.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, v FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tv\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t0.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t25.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t0.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t100.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsCovarPopAcrossDayBoundary() throws Exception {
        // covar_pop exercises the bivariate Welford state shared by corr /
        // covar_samp / covar_pop; resetPartition clears it at each anchor cross.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, y DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, covar_pop(x, y) OVER w AS c FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // covar_pop of one point is 0; of {(a,b),(c,d)} it is (a-c)(b-d)/4.
            // Day 1 (10,10),(20,30): 0, then (-10)(-20)/4=50. Day 2 reset
            // (5,5),(25,45): 0, then (-20)(-40)/4=200. Without reset day 2's
            // first row would mix day 1 points and be non-zero.
            execute("INSERT INTO base (ts, x, y, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 30.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5.0, 5.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 25.0, 45.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, c FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tc\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t0.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t50.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t0.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t200.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsNthValueAcrossDayBoundary() throws Exception {
        // nth_value tracks N within the anchor bucket; resetPartition restarts
        // the per-partition count so day 2's nth_value(x, 2) sees only day 2.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, nth_value(x, 2) OVER w AS n FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // nth_value(x, 2) is null until the bucket holds two rows, then
            // pins the 2nd value. Day 1 (10,20,30): null, 20, 20. Day 2 reset
            // (100,200): null, 200. Without reset day 2's first row would be
            // the overall 2nd value (20), not null.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 30.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 100.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 200.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, n FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\tn\n" +
                            "2026-08-01T00:00:00.000000Z\ta\tnull\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t20.0\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t20.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\tnull\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t200.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorDailyResetsAcrossDstWithTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Europe/London: clocks jump forward at 2026-03-29T01:00Z to 02:00 BST.
            // ANCHOR DAILY '00:00' Europe/London buckets at local-midnight, which in
            // UTC means 2026-03-28T00:00 (GMT) and 2026-03-29T00:00 (still GMT, since
            // the DST spring-forward happens at 01:00 UTC). The two rows on either
            // side of the boundary live in different anchor buckets even though they
            // are only an hour apart in wall-clock UTC.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00' 'Europe/London')");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-03-28T23:00:00.000000Z', 10, 'a'), " +
                    "('2026-03-28T23:30:00.000000Z', 20, 'a'), " +
                    "('2026-03-29T00:30:00.000000Z', 5, 'a'), " +
                    "('2026-03-29T01:30:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Bucket 2026-03-28 (London local): rows at 23:00Z, 23:30Z -> sums 10, 30.
            // Bucket 2026-03-29 (London local): rows at 00:30Z, 01:30Z -> sums 5, 20.
            // (The DST spring-forward at 01:00 UTC does not split a London local day.)
            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-03-28T23:00:00.000000Z\ta\t10.0\n" +
                            "2026-03-28T23:30:00.000000Z\ta\t30.0\n" +
                            "2026-03-29T00:30:00.000000Z\ta\t5.0\n" +
                            "2026-03-29T01:30:00.000000Z\ta\t20.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorDailyResetsAtMidnightUtc() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");

            // Same shape as testAnchorResetsRunningSumAcrossDayBoundary but exercising
            // the DAILY desugar path. For UTC midnight the two should be equivalent.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t20.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorDailyResetsAtNonZeroTimeUtc() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // ANCHOR DAILY '09:30' (UTC) buckets at 09:30:00.000000Z each day.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '09:30')");

            // Bucket 1 (2026-08-01T09:30Z .. 2026-08-02T09:29:59...Z): 09:30 and 18:00 -> 10, 30.
            // Bucket 2 (2026-08-02T09:30Z .. ): 09:30 and 18:00 -> 5, 20.
            // The 08:00 row on day 2 still belongs to bucket 1 (before 09:30 cutover).
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T09:30:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T18:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T08:00:00.000000Z', 7, 'a'), " +
                    "('2026-08-02T09:30:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T18:00:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-08-01T09:30:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T18:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T08:00:00.000000Z\ta\t37.0\n" +
                            "2026-08-02T09:30:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T18:00:00.000000Z\ta\t20.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInvalidationSurvivesRestart() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Drop the base table — this should invalidate the live view AND persist
            // the invalidation to _lv.s so restart sees the invalid state.
            execute("DROP TABLE base");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertTrue("live view must be invalid after base drop", instance.isInvalid());
            Assert.assertEquals(
                    "invalidation reason must record the trigger",
                    "base table drop",
                    instance.getInvalidationReason().toString()
            );

            // Simulate restart: clear registry, re-load from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertTrue(
                    "invalidation must round-trip via _lv.s",
                    reloaded.isInvalid()
            );
            Assert.assertEquals(
                    "invalidation reason must round-trip via _lv.s",
                    "base table drop",
                    reloaded.getInvalidationReason().toString()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsCatalogueColumnOrderMatchesRfc() throws Exception {
        // Columns appear in the documented order so clients binding by
        // ordinal see a stable shape. The documented columns come first;
        // the three head_checkpoint_* columns trail as debug
        // surface.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            try {
                assertQuery("SELECT * FROM live_views() WHERE 1 = 0").noLeakCheck().returns("view_name\tview_table_dir_name\tbase_table_name\tview_sql\tview_status\t"
                                + "invalidation_reason\tflush_every_interval\tflush_every_interval_unit\t"
                                + "in_memory_interval\tin_memory_interval_unit\tin_mem_bytes\t"
                                + "symbol_translation_size\to3_rejected_count\tlag_seqtxn\tlag_micros\t"
                                + "last_processed_seqtxn\tapplied_watermark\tlv_consumed_seqtxn\t"
                                + "view_lower_bound_timestamp\twriter_stall_micros\tbackfill_target_seqtxn\t"
                                + "head_checkpoint_lv_seqtxn\thead_checkpoint_max_ts\thead_checkpoint_state_bytes\n");
            } finally {
                execute("DROP LIVE VIEW lv");
            }
        });
    }

    @Test
    public void testLiveViewsCatalogueExposesView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 5s IN MEMORY 30s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            assertQuery("SELECT view_name, base_table_name, view_status, flush_every_interval, flush_every_interval_unit, " +
                            "in_memory_interval, in_memory_interval_unit FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\tbase_table_name\tview_status\tflush_every_interval\tflush_every_interval_unit\tin_memory_interval\tin_memory_interval_unit\n" +
                            "lv\tbase\tactive\t5\tSECOND\t30\tSECOND\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsCatalogueExposesMillisecondUnit() throws Exception {
        // Regression: getIntervalUnit had no arm for the internal 'T' (millisecond)
        // unit char, so flush_every_interval_unit / in_memory_interval_unit returned
        // NULL for any LV created with a ms-granularity FLUSH EVERY or IN MEMORY.
        // Since FLUSH EVERY's minimum is 100ms, this hit the most common LV shape.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms IN MEMORY 500ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            assertQuery("SELECT view_name, flush_every_interval, flush_every_interval_unit, in_memory_interval, in_memory_interval_unit FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\tflush_every_interval\tflush_every_interval_unit\tin_memory_interval\tin_memory_interval_unit\n" +
                            "lv\t200\tMILLISECOND\t500\tMILLISECOND\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsCatalogueExposesViewLowerBoundTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Pin the microsecond clock so view_lower_bound_timestamp captures the
            // wall-clock at CREATE deterministically.
            setCurrentMicros(1_700_000_000_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            assertQuery("SELECT view_name, view_lower_bound_timestamp FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\tview_lower_bound_timestamp\n" +
                            "lv\t2023-11-14T22:13:20.000000Z\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testViewLowerBoundTimestampPersistsInBaseUnitsForMicroBase() throws Exception {
        // Pins the identity path: for MICRO bases, the persisted value equals the
        // wall-clock micros at CREATE because the driver's fromMicros is the identity.
        // Acts as a guard against future refactors to the conversion shape.
        assertMemoryLeak(() -> {
            setCurrentMicros(1_700_000_000_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals(
                    "MICRO base persists wall-clock micros as-is",
                    1_700_000_000_000_000L,
                    instance.getDefinition().getViewLowerBoundTimestamp()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testViewLowerBoundTimestampPersistsInBaseUnitsForNanoBase() throws Exception {
        // Regression: viewLowerBoundTimestamp used to be persisted in wall-clock
        // micros regardless of the base's timestamp unit, so a TIMESTAMP_NS base
        // ended up with a value 1000x smaller than any base-table ts. The persisted
        // value is now scaled to base units so the eventual O3 reject
        // can compare it against late_row.ts directly. The catalogue column stays
        // TIMESTAMP_MICRO and rounds
        // NS values back to the MICRO grid at display time.
        assertMemoryLeak(() -> {
            setCurrentMicros(1_700_000_000_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP_NS, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals(
                    "NS base persists wall-clock value scaled to nanoseconds",
                    1_700_000_000_000_000_000L,
                    instance.getDefinition().getViewLowerBoundTimestamp()
            );

            // Catalogue column commits to TIMESTAMP_MICRO; toMicros rounds NS back
            // to the MICRO grid (lossless here since the source is wall-clock micros).
            assertQuery("SELECT view_name, view_lower_bound_timestamp FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\tview_lower_bound_timestamp\n" +
                            "lv\t2023-11-14T22:13:20.000000Z\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillCapturesEarliestBaseRowAsLowerBound() throws Exception {
        // For a BACKFILL view, viewLowerBoundTimestamp is the earliest visible
        // base-table row at CREATE, not the wall-clock CREATE moment. Operators
        // inspecting view_lower_bound_timestamp then see the real retention floor.
        assertMemoryLeak(() -> {
            // Pin the clock well after the data so a CREATE-moment floor would read
            // visibly different from the earliest base row.
            setCurrentMicros(1_800_000_000_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2023-01-01T00:00:00.000000Z', 1)," +
                    "('2024-06-15T12:00:00.000000Z', 2)," +
                    "('2026-01-01T00:00:00.000000Z', 3)");
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            assertQuery("SELECT view_name, view_lower_bound_timestamp FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\tview_lower_bound_timestamp\n" +
                            "lv\t2023-01-01T00:00:00.000000Z\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillOnEmptyBaseUsesCreateMomentLowerBound() throws Exception {
        // BACKFILL on an empty base has no earliest row to anchor to, so
        // viewLowerBoundTimestamp falls back to the wall-clock CREATE moment,
        // matching the non-BACKFILL default.
        assertMemoryLeak(() -> {
            setCurrentMicros(1_700_000_000_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            assertQuery("SELECT view_name, view_lower_bound_timestamp FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\tview_lower_bound_timestamp\n" +
                            "lv\t2023-11-14T22:13:20.000000Z\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3LateRowBelowLowerBoundIsRejectedAndCounted() throws Exception {
        // A late O3 row whose timestamp falls below viewLowerBoundTimestamp is
        // rejected: it never reaches the on-disk tier, and the rejection is
        // recorded in live_views().o3_rejected_count.
        assertMemoryLeak(() -> {
            // Pin the clock so the non-BACKFILL view's lower bound is a known
            // wall-clock moment (2023-11-14T22:13:20Z).
            setCurrentMicros(1_700_000_000_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            // A row above the lower bound advances latestSeenTs through a refresh.
            execute("INSERT INTO base (ts, x) VALUES ('2023-11-15T00:00:00.000000Z', 1)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            Assert.assertEquals("no rejections yet", 0L, instance.getO3RejectedCount());

            // A late row below the lower bound: it is O3 (ts < latestSeenTs) and
            // sits entirely below viewLowerBoundTimestamp, so the refresh rejects it.
            execute("INSERT INTO base (ts, x) VALUES ('2023-11-14T00:00:00.000000Z', 2)");
            drainWalQueue();
            // FLUSH EVERY 1s would otherwise rate-limit the back-to-back cycle.
            instance.setLastFlushTimeUs(Numbers.LONG_NULL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // The rejected row never reached the tier; the counter recorded it.
            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            Assert.assertEquals(
                    "one O3 row rejected below the lower bound",
                    1L,
                    instance.getO3RejectedCount()
            );
            assertQuery("SELECT view_name, o3_rejected_count FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("view_name\to3_rejected_count\n" +
                            "lv\t1\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testUnsupportedDefinitionVersionSurfacesAndDrops() throws Exception {
        // An _lv whose CORE format version is newer than this build supports must
        // not vanish silently: the load path surfaces the view in live_views()
        // with view_status='version_unsupported' and other columns NULL, and DROP
        // LIVE VIEW still removes it best-effort.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            TableToken token = engine.getLiveViewRegistry().getViewInstance("lv").getLiveViewToken();

            overwriteLiveViewFileWithBumpedFormatVersion(
                    token,
                    LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME,
                    LiveViewDefinition.LIVE_VIEW_DEFINITION_CORE_MSG_TYPE
            );
            // Simulate restart: drop the in-memory registry and rebuild from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance stub = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("version-unsupported view must still be registered", stub);
            Assert.assertTrue("stub must report version-unsupported", stub.isVersionUnsupported());

            // view_name and view_status surface; definition/state columns are NULL
            // (string columns render empty, long columns render "null").
            assertQuery("SELECT view_name, view_status, view_sql, base_table_name, lv_consumed_seqtxn, " +
                            "view_lower_bound_timestamp FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("view_name\tview_status\tview_sql\tbase_table_name\tlv_consumed_seqtxn\tview_lower_bound_timestamp\n" +
                            "lv\tversion_unsupported\t\t\tnull\t\n");

            // DROP works on the stub (best-effort recovery path).
            execute("DROP LIVE VIEW lv");
            assertQuery("SELECT count() FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
        });
    }

    @Test
    public void testUnsupportedStateVersionSurfaces() throws Exception {
        // The version gate also covers _lv.s: a newer state-file format surfaces
        // the view as version_unsupported even when _lv reads cleanly.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            TableToken token = engine.getLiveViewRegistry().getViewInstance("lv").getLiveViewToken();

            overwriteLiveViewFileWithBumpedFormatVersion(
                    token,
                    LiveViewState.LIVE_VIEW_STATE_FILE_NAME,
                    LiveViewState.LIVE_VIEW_STATE_CORE_MSG_TYPE
            );
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance stub = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(stub);
            Assert.assertTrue(stub.isVersionUnsupported());
            assertQuery("SELECT view_name, view_status FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("view_name\tview_status\n" +
                            "lv\tversion_unsupported\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    private void overwriteLiveViewFileWithBumpedFormatVersion(TableToken token, String fileName, int coreBlockType) {
        try (
                BlockFileWriter writer = new BlockFileWriter(
                        engine.getConfiguration().getFilesFacade(),
                        engine.getConfiguration().getCommitMode());
                Path path = new Path()
        ) {
            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(fileName);
            writer.of(path.$());
            AppendableBlock block = writer.append();
            // The format version is the CORE block's first field, so a value above
            // the reader's supported version is rejected before the rest is parsed.
            block.putInt(Integer.MAX_VALUE);
            block.commit(coreBlockType);
            writer.commit();
        }
    }

    @Test
    public void testLiveViewsCatalogueExposesOperationalColumns() throws Exception {
        // Pin the clock so lag_micros is deterministic across runs.
        assertMemoryLeak(() -> {
            setCurrentMicros(1_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000001Z', 1)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Advance the clock so lag_micros reflects time since the last flush.
            setCurrentMicros(3_000_000L);

            // view_table_dir_name must match the live view's actual directory; once mangling
            // is enabled in tests it diverges from the view name, so resolve via the engine.
            TableToken token = engine.verifyTableName("lv");
            String expectedDir = token.getDirName();
            // writer_stall_micros is 0 here: this scenario never stalls the writer.
            // lag_micros = 3_000_000 - lastFlushTimeUs. lastFlushTimeUs was set to 1_000_000
            // (the clock at refresh), so lag_micros = 2_000_000.
            assertQuery("SELECT view_name, view_table_dir_name, lag_micros, writer_stall_micros FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\tview_table_dir_name\tlag_micros\twriter_stall_micros\n" +
                            "lv\t" + expectedDir + "\t2000000\t0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueDoubleOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // nth_value() Step 1 — DOUBLE variant. State per partition is
        // [value: DOUBLE, count: LONG, tombstone: BYTE]. Picks N=2 so the second
        // row per partition locks the value; rows beyond that propagate the
        // locked value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(x, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5.0, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50.0, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20.0, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7.0, 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 11.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // 'a' nth_value(x, 2) is 50.0, 'b' is 11.0. Sum 61.0.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    double total = 0;
                    while (mc.hasNext()) {
                        total += rec.getValue().getDouble(0);
                    }
                    Assert.assertEquals(61.0, total, 0.0);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueLongOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // nth_value() Step 1 — LONG variant. State per partition is
        // [value: LONG, count: LONG, tombstone: BYTE].
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(x, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7, 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 11, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // 'a' nth_value(x, 2) is 50, 'b' is 11. Sum 61.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long total = 0;
                    while (mc.hasNext()) {
                        total += rec.getValue().getLong(0);
                    }
                    Assert.assertEquals(61L, total);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueDecimal128AnchoredSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalAnchoredRoundTrip(
                "DECIMAL(38, 6)",
                2,
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000000\n"
        );
    }

    @Test
    public void testNthValueDecimal128OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(38, 6)",
                2,
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000000\n"
        );
    }

    @Test
    public void testNthValueDecimal128OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(38, 6)",
                2,
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.000000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000000\n"
        );
    }

    @Test
    public void testNthValueDecimal128OverPartitionRowsUnboundedSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(38, 6)",
                2,
                "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000000m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40.000000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000000m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24.000000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t20.000000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000000\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12.000000\n"
        );
    }

    @Test
    public void testNthValueDecimal16OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(4, 1)",
                2,
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.0\n"
        );
    }

    @Test
    public void testNthValueDecimal16OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(4, 1)",
                2,
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.0m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.0m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.0m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.0m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.0\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.0\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.0\n"
        );
    }

    @Test
    public void testNthValueDecimal256AnchoredSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalAnchoredRoundTrip(
                "DECIMAL(60, 0)",
                2,
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testNthValueDecimal256OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(60, 0)",
                2,
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testNthValueDecimal256OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(60, 0)",
                2,
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testNthValueDecimal256OverPartitionRowsUnboundedSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(60, 0)",
                2,
                "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testNthValueDecimal32OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(9, 3)",
                2,
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000\n"
        );
    }

    @Test
    public void testNthValueDecimal32OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(9, 3)",
                2,
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.000m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.000m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.000m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.000m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.000\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.000\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.000\n"
        );
    }

    @Test
    public void testNthValueDecimal8OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(2, 0)",
                2,
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testNthValueDecimal8OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(2, 0)",
                2,
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12\n"
        );
    }

    @Test
    public void testNthValueDecimal64AnchoredSnapshotRoundTrip() throws Exception {
        // nth_value(DECIMAL(18,2), 2) over an anchored partition (UNBOUNDED
        // PRECEDING AND CURRENT ROW) - the [value, count] accumulator shape with
        // newCompactionScratch + count==0 re-anchor.
        assertNthValueDecimalAnchoredRoundTrip(
                "DECIMAL(18, 2)",
                2,
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testNthValueDecimal64OverPartitionRangeFrameSnapshotRoundTrip() throws Exception {
        // nth_value(DECIMAL(18,2), 2) over a bounded RANGE frame - 8-byte (LONG)
        // ring element; [frameSize, startOffset, size, capacity, firstIdx] state.
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(18, 2)",
                2,
                "RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testNthValueDecimal64OverPartitionRowsFrameSnapshotRoundTrip() throws Exception {
        // nth_value(DECIMAL(18,2), 2) over a bounded ROWS frame - fixed-size LONG
        // ring; [loIdx, startOffset, count] state.
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(18, 2)",
                2,
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t12.00\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testNthValueDecimal64OverPartitionRowsUnboundedSnapshotRoundTrip() throws Exception {
        // nth_value(DECIMAL(18,2), 2) over ROWS UNBOUNDED PRECEDING AND 1 PRECEDING
        // - the O(1) [count, lockedValue] shape; the 2nd value locks and is emitted
        // once the frame contains at least n rows.
        assertNthValueDecimalFrameRoundTrip(
                "DECIMAL(18, 2)",
                2,
                "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING",
                "('2026-08-01T00:00:00.000000Z', 'a', 10.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'a', 20.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'a', 30.00m), " +
                        "('2026-08-01T03:00:00.000000Z', 'a', 40.00m), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 6.00m), " +
                        "('2026-08-01T01:00:00.000000Z', 'b', 12.00m), " +
                        "('2026-08-01T02:00:00.000000Z', 'b', 18.00m), " +
                        "('2026-08-01T03:00:00.000000Z', 'b', 24.00m)",
                "ts\tsym\ta\n" +
                        "2026-08-01T00:00:00.000000Z\ta\t\n" +
                        "2026-08-01T01:00:00.000000Z\ta\t\n" +
                        "2026-08-01T02:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T03:00:00.000000Z\ta\t20.00\n" +
                        "2026-08-01T00:00:00.000000Z\tb\t\n" +
                        "2026-08-01T01:00:00.000000Z\tb\t\n" +
                        "2026-08-01T02:00:00.000000Z\tb\t12.00\n" +
                        "2026-08-01T03:00:00.000000Z\tb\t12.00\n"
        );
    }

    @Test
    public void testNthValueTimestampOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // nth_value() Step 1 — TIMESTAMP variant. State per partition is
        // [value: TIMESTAMP, count: LONG, tombstone: BYTE]. The value column
        // happens to use ts itself so the captured value is the timestamp of
        // the Nth row per partition.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(ts, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // 'a' nth_value(ts, 2) is 2026-08-01T01:00:00 (3_600_000_000us),
                    // 'b' nth_value(ts, 2) is 2026-08-01T01:00:00 — same value, sum doubles.
                    final long expected = 2L * MicrosFormatUtils.parseUTCTimestamp("2026-08-01T01:00:00.000000Z");
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long total = 0;
                    while (mc.hasNext()) {
                        total += rec.getValue().getLong(0);
                    }
                    Assert.assertEquals(expected, total);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueDoubleOverPartitionRowsKPrecedingSnapshotRoundTrip() throws Exception {
        // nth_value() Step 2 -- DOUBLE variant, ROWS BETWEEN UNBOUNDED
        // PRECEDING AND K PRECEDING (non-anchored). State per partition is
        // [count: LONG, lockedValue: LONG (double bits), tombstone: BYTE].
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(x, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5.0, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50.0, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20.0, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7.0, 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 11.0, 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 13.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // lockedValue is the value at count == n == 2: 'a' -> 50.0, 'b' -> 11.0. Sum 61.0.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    double total = 0;
                    while (mc.hasNext()) {
                        total += Double.longBitsToDouble(rec.getValue().getLong(1));
                    }
                    Assert.assertEquals(61.0, total, 0.0);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueLongOverPartitionRowsKPrecedingSnapshotRoundTrip() throws Exception {
        // nth_value() Step 2 -- LONG variant, ROWS BETWEEN UNBOUNDED
        // PRECEDING AND K PRECEDING (non-anchored). State per partition is
        // [count: LONG, lockedValue: LONG, tombstone: BYTE].
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(x, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7, 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 11, 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 13, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // lockedValue is the value at count == n == 2: 'a' -> 50, 'b' -> 11. Sum 61.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long total = 0;
                    while (mc.hasNext()) {
                        total += rec.getValue().getLong(1);
                    }
                    Assert.assertEquals(61L, total);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueTimestampOverPartitionRowsKPrecedingSnapshotRoundTrip() throws Exception {
        // nth_value() Step 2 -- TIMESTAMP variant, ROWS BETWEEN UNBOUNDED
        // PRECEDING AND K PRECEDING (non-anchored). State per partition is
        // [count: LONG, lockedValue: TIMESTAMP-as-LONG, tombstone: BYTE].
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(ts, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // lockedValue is ts at count == 2 = '2026-08-01T01:00:00' for both partitions.
                    final long expected = 2L * MicrosFormatUtils.parseUTCTimestamp("2026-08-01T01:00:00.000000Z");
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long total = 0;
                    while (mc.hasNext()) {
                        total += rec.getValue().getLong(1);
                    }
                    Assert.assertEquals(expected, total);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueDoubleOverPartitionRowsBoundedSnapshotRoundTrip() throws Exception {
        // nth_value() Step 3 -- DOUBLE variant, bounded ROWS BETWEEN X PRECEDING
        // AND CURRENT ROW (non-anchored). State per partition is
        // [loIdx: LONG, startOffset: LONG, count: LONG, tombstone: BYTE] plus
        // a bufferSize-long ring of DOUBLE values in MemoryARW.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(x, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5.0, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50.0, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20.0, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7.0, 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 11.0, 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 13.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // After 3 rows per partition, both rings hold (val_0, val_1, val_2) with count=3.
                    // We only assert state round-trip via aggregated counts; ring contents are
                    // off-heap and validated indirectly by the count slot matching.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long totalCount = 0;
                    while (mc.hasNext()) {
                        totalCount += rec.getValue().getLong(2);
                    }
                    Assert.assertEquals(6L, totalCount);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueLongOverPartitionRowsBoundedSnapshotRoundTrip() throws Exception {
        // nth_value() Step 3 -- LONG variant, bounded ROWS BETWEEN X PRECEDING
        // AND CURRENT ROW (non-anchored). State per partition is
        // [loIdx: LONG, startOffset: LONG, count: LONG, tombstone: BYTE] plus
        // a bufferSize-long ring of LONG values in MemoryARW.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(x, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7, 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 11, 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 13, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long totalCount = 0;
                    while (mc.hasNext()) {
                        totalCount += rec.getValue().getLong(2);
                    }
                    Assert.assertEquals(6L, totalCount);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueTimestampOverPartitionRowsBoundedSnapshotRoundTrip() throws Exception {
        // nth_value() Step 3 -- TIMESTAMP variant, bounded ROWS BETWEEN X PRECEDING
        // AND CURRENT ROW (non-anchored). State per partition is
        // [loIdx: LONG, startOffset: LONG, count: LONG, tombstone: BYTE] plus
        // a bufferSize-long ring of TIMESTAMP-as-LONG values in MemoryARW.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(ts, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long totalCount = 0;
                    while (mc.hasNext()) {
                        totalCount += rec.getValue().getLong(2);
                    }
                    Assert.assertEquals(6L, totalCount);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueDoubleOverPartitionRangeBoundedSnapshotRoundTrip() throws Exception {
        // nth_value() Step 4 -- DOUBLE variant, bounded RANGE BETWEEN '<n>' HOUR
        // PRECEDING AND CURRENT ROW (non-anchored). State per partition is
        // [frameSize: LONG, startOffset: LONG, size: LONG, capacity: LONG,
        // firstIdx: LONG, tombstone: BYTE] plus a (ts, value) ring slab in
        // MemoryARW; ts is LONG, value is DOUBLE (RECORD_SIZE = 16).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(x, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '5' HOUR PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5.0, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50.0, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20.0, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7.0, 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 11.0, 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 13.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // All 3 rows per partition land inside the 5-hour window, so
                    // each partition's ring stores size=3 (slot 2). Sum across
                    // partitions verifies the (ts, value) sequence round-trips.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long totalSize = 0;
                    while (mc.hasNext()) {
                        totalSize += rec.getValue().getLong(2);
                    }
                    Assert.assertEquals(6L, totalSize);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueLongOverPartitionRangeBoundedSnapshotRoundTrip() throws Exception {
        // nth_value() Step 4 -- LONG variant, bounded RANGE BETWEEN '<n>' HOUR
        // PRECEDING AND CURRENT ROW (non-anchored). State per partition is
        // [frameSize: LONG, startOffset: LONG, size: LONG, capacity: LONG,
        // firstIdx: LONG, tombstone: BYTE] plus a (ts, value) ring slab in
        // MemoryARW; ts and value both LONG (RECORD_SIZE = 16).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(x, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '5' HOUR PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7, 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 11, 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 13, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long totalSize = 0;
                    while (mc.hasNext()) {
                        totalSize += rec.getValue().getLong(2);
                    }
                    Assert.assertEquals(6L, totalSize);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testNthValueTimestampOverPartitionRangeBoundedSnapshotRoundTrip() throws Exception {
        // nth_value() Step 4 -- TIMESTAMP variant, bounded RANGE BETWEEN '<n>'
        // HOUR PRECEDING AND CURRENT ROW (non-anchored). State per partition is
        // [frameSize: LONG, startOffset: LONG, size: LONG, capacity: LONG,
        // firstIdx: LONG, tombstone: BYTE] plus a (ts, value) ring slab in
        // MemoryARW; ts and value both LONG (RECORD_SIZE = 16), with value
        // carrying the TIMESTAMP encoded as LONG.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, nth_value(ts, 2) OVER w AS nv FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '5' HOUR PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b'), " +
                        "('2026-08-01T01:00:00.000000Z', 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction nvFunc = unwrapWindowFunctions(lv).getQuick(0);
                Assert.assertTrue(nvFunc.supportsSnapshot());
                Map fnMap = nvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    LiveViewFunctionSnapshot.write(sink, nvFunc);
                    nvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    LiveViewFunctionSnapshot.restore(sink, 0L, nvFunc, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long totalSize = 0;
                    while (mc.hasNext()) {
                        totalSize += rec.getValue().getLong(2);
                    }
                    Assert.assertEquals(6L, totalSize);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewWindowSnapshotRoundTrip() throws Exception {
        // LiveViewWindow's anchor map serialises into
        // the WINDOW_ANCHOR block payload via snapshot() and rehydrates via
        // restore(). The format mirrors the codec used by the migrated window
        // functions in 2a.5 - typed key columns + a single LONG anchor value
        // per partition. End-to-end checkpoint integration is gated on the
        // 2a.4 write hook and the 2a.7 restart restore path; this test
        // exercises the codec in isolation.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // One row per partition - no anchor crossings - so the anchor
                // map ends with exactly two live entries, no tombstones.
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull("anchored LV exposes the window driver", window);
                Assert.assertEquals("two partitions seeded", 2L, window.getAnchorMapSize());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    window.snapshot(sink);

                    // Sanity-check the documented payload prefix:
                    //   STR windowName (INT len + len * CHAR), INT keyCount=1,
                    //   INT keyType=STRING, INT anchorValueType=TIMESTAMP,
                    //   LONG partitionCount=2.
                    // The persisted key column type is STRING (not SYMBOL):
                    // LiveViewWindow.build rewrites SYMBOL partition columns as
                    // STRING in the anchor map's key types so cross-WAL-segment
                    // SYMBOL collisions can't corrupt the partition state.
                    long off = 0;
                    final int nameLen = sink.getInt(off);
                    off += Integer.BYTES;
                    Assert.assertEquals("window name 'w' is one char", 1, nameLen);
                    Assert.assertEquals('w', sink.getChar(off));
                    off += (long) nameLen * Character.BYTES;
                    Assert.assertEquals("single key column", 1, sink.getInt(off));
                    off += Integer.BYTES;
                    Assert.assertEquals("key column is STRING (SYMBOL columns route through resolved STRING)", ColumnType.STRING, sink.getInt(off));
                    off += Integer.BYTES;
                    Assert.assertEquals("anchor value type is TIMESTAMP", ColumnType.TIMESTAMP, sink.getInt(off));
                    off += Integer.BYTES;
                    Assert.assertEquals("partition count is 2", 2L, sink.getLong(off));

                    // toTop() wipes the anchor map; the round-trip must rebuild it.
                    window.toTop();
                    Assert.assertEquals(0L, window.getAnchorMapSize());

                    window.restore(sink);
                    Assert.assertEquals("restore brought back both partitions", 2L, window.getAnchorMapSize());
                    Assert.assertEquals("no tombstones post-restore", 0L, window.getTombstoneCount());

                    // The restored anchor map drives processRow's "existing
                    // partition, anchor unchanged" branch on the very next row,
                    // confirming we built valid SLOT_INITIALIZED=1 entries.
                    setCurrentMicros(200_000L);
                    execute("INSERT INTO base (ts, sym, x) VALUES ('2026-08-01T02:00:00.000000Z', 'a', 4.0)");
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testHeadCheckpointSetAndReadAtomicity() throws Exception {
        // The head-checkpoint trio (lvSeqTxn, maxTs, stateBytes) is published
        // via an immutable long[] reference store so the lock-free O3 head-hit
        // reader cannot observe a torn (lvSeqTxn, maxTs) pair across a
        // concurrent setHeadCheckpoint. Stress: writer alternates between two
        // known tuples; reader spins and asserts every snapshot it sees is
        // exactly one of those tuples.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            final long lvSeqA = 10L;
            final long maxTsA = 100L;
            final long lvSeqB = 20L;
            final long maxTsB = 200L;
            // Seed with tuple A so the reader sees a known value on first read.
            instance.setHeadCheckpoint(lvSeqA, maxTsA, 1L, 1L);

            final AtomicBoolean stop = new AtomicBoolean(false);
            final AtomicBoolean tornObserved = new AtomicBoolean(false);

            final Thread writer = new Thread(() -> {
                int n = 0;
                while (!stop.get()) {
                    if ((n & 1) == 0) {
                        instance.setHeadCheckpoint(lvSeqA, maxTsA, 1L, 1L);
                    } else {
                        instance.setHeadCheckpoint(lvSeqB, maxTsB, 2L, 2L);
                    }
                    n++;
                    if ((n & 0xff) == 0) {
                        Thread.yield();
                    }
                }
            }, "lv-head-checkpoint-writer");
            final Thread reader = new Thread(() -> {
                while (!stop.get()) {
                    long[] pair = instance.getHeadCheckpointSeqAndMaxTs();
                    long lvSeq = pair[0];
                    long maxTs = pair[1];
                    boolean isA = lvSeq == lvSeqA && maxTs == maxTsA;
                    boolean isB = lvSeq == lvSeqB && maxTs == maxTsB;
                    if (!isA && !isB) {
                        tornObserved.set(true);
                        return;
                    }
                }
            }, "lv-head-checkpoint-reader");

            writer.start();
            reader.start();
            Thread.sleep(250);
            stop.set(true);
            writer.join(5_000);
            reader.join(5_000);
            Assert.assertFalse("writer thread must have stopped", writer.isAlive());
            Assert.assertFalse("reader thread must have stopped", reader.isAlive());
            Assert.assertFalse(
                    "reader observed a torn (lvSeqTxn, maxTs) pair",
                    tornObserved.get()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testHeadCheckpointWrittenOnFirstCommit() throws Exception {
        // The refresh worker writes a head .cp on the first cycle
        // that lands rows, so subsequent restart / O3 paths have a head to
        // restore from. The cadence triggers (rows / max.duration) gate
        // subsequent writes; this test only proves the first one fires.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);

                // Cap was true (anchor SYMBOL + sum(DOUBLE) is fully migrated),
                // so the first commit wrote a head; subsequent cycles will
                // honor the row / duration cadence.
                Assert.assertTrue("snapshot capability computed and true", lv.isSnapshotCapability());
                Assert.assertNotEquals(
                        "head_checkpoint_lv_seqtxn populated after first commit",
                        Numbers.LONG_NULL,
                        lv.getHeadCheckpointLvSeqTxn()
                );
                Assert.assertTrue(
                        "head_checkpoint_state_bytes is positive",
                        lv.getHeadCheckpointStateBytes() > 0
                );
                Assert.assertEquals(
                        "rows counter reset after head write",
                        0L,
                        lv.getRowsSinceLastCheckpointWritten()
                );

                // A .cp file lives under <lv_dir>/_checkpoints/.
                TableToken token = lv.getLiveViewToken();
                FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path cpDir = new Path()) {
                    cpDir.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
                    Assert.assertTrue("_checkpoints/ exists", ff.exists(cpDir.$()));
                    // Enumerate; expect exactly one .cp file (and zero .cp.tmp).
                    final StringSink nameSink = new StringSink();
                    boolean foundCp = false;
                    long pFind = ff.findFirst(cpDir.$());
                    Assert.assertNotEquals("can iterate _checkpoints/", 0L, pFind);
                    try {
                        do {
                            long namePtr = ff.findName(pFind);
                            if (namePtr == 0) {
                                continue;
                            }
                            nameSink.clear();
                            Utf8s.utf8ToUtf16Z(namePtr, nameSink);
                            if (Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_FILE_EXT)
                                    && !Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_TMP_FILE_EXT)) {
                                foundCp = true;
                            }
                        } while (ff.findNext(pFind) > 0);
                    } finally {
                        ff.findClose(pFind);
                    }
                    Assert.assertTrue("at least one .cp file exists", foundCp);
                }

                // The live_views() catalogue surfaces the same trio the
                // refresh worker stamped via setHeadCheckpoint(). Asserting
                // here closes the end-to-end loop: write hook -> instance
                // setter -> LiveViewsFunctionFactory -> SQL.
                assertQuery("SELECT view_name, head_checkpoint_lv_seqtxn, head_checkpoint_state_bytes FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\thead_checkpoint_lv_seqtxn\thead_checkpoint_state_bytes\n" +
                                "lv\t" + lv.getHeadCheckpointLvSeqTxn() + '\t' + lv.getHeadCheckpointStateBytes() + '\n');
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresFromHeadCheckpoint() throws Exception {
        // A simulated restart should re-discover the head .cp
        // via the startup sweep, then the first refresh-worker tick rehydrates
        // the LV's window-function state from the head and advances
        // lastProcessedSeqTxn to the manifest's baseSeqTxn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            final long preFunctionMapSize;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                preFunctionMapSize = instance.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size();
                Assert.assertNotEquals("head .cp was written before restart", Numbers.LONG_NULL, preHeadLvSeqTxn);
                Assert.assertEquals("two partitions seeded pre-restart", 2L, preFunctionMapSize);
            }

            // Simulate restart: clear the in-memory registry and rebuild
            // from on-disk _lv + _lv.s. The sweep should re-discover the
            // head .cp via the lvSeqTxn embedded in its filename.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(
                    "startup sweep stamped head lvSeqTxn from filename",
                    preHeadLvSeqTxn,
                    reloaded.getHeadCheckpointLvSeqTxn()
            );
            Assert.assertFalse(
                    "restore not attempted yet (no refresh cycle has run)",
                    reloaded.isCheckpointRestoreAttempted()
            );

            // Drive a single refresh cycle. There are no new base commits, but
            // the fallback scan still calls refreshInstance, which runs the
            // restore on the first cycle for an LV with a stamped head.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            Assert.assertTrue(
                    "refresh worker attempted the restore on the first post-restart cycle",
                    reloaded.isCheckpointRestoreAttempted()
            );
            Assert.assertEquals(
                    "lastProcessedSeqTxn matches manifest.baseSeqTxn after restore",
                    preLastProcessed,
                    reloaded.getLastProcessedSeqTxn()
            );
            Assert.assertEquals(
                    "function partition map rehydrated to its pre-restart size",
                    preFunctionMapSize,
                    reloaded.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestoreFileVersionMismatchInvalidatesView() throws Exception {
        // File-level formatVersion mismatch in the head .cp is a real
        // compatibility break (not corruption); the restore path must mark
        // the LV INVALID and leave the .cp on disk. Mirrors
        // testRestoreVersionMismatchInvalidatesView for the per-function
        // snapshot version branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, x, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");

            // Drive a refresh so a real .cp is written.
            execute("INSERT INTO base (ts, sym, x) VALUES ('2026-06-01T00:00:00.000000Z', 'a', 1)");
            drainWalQueue();
            final long headLvSeqTxn;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                drainWalQueue();
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, headLvSeqTxn);
            }

            // Mutate the file-level formatVersion field (4-byte int at
            // offset 4, right after the magic). The reader checks the
            // version before the CRC trailer, so the broken CRC after the
            // overwrite is unreached.
            try (Path cpPath = new Path()) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                cpPath.of(engine.getConfiguration().getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                        .slash();
                LiveViewCheckpointWriter.appendCpFileName(cpPath, headLvSeqTxn);
                try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                    mem.of(
                            engine.getConfiguration().getFilesFacade(),
                            cpPath.$(),
                            engine.getConfiguration().getFilesFacade().getPageSize(),
                            8L,
                            MemoryTag.MMAP_DEFAULT,
                            CairoConfiguration.O_NONE
                    );
                    mem.putInt(4L, LiveViewCheckpointReader.SUPPORTED_VERSION_MAX + 1);
                    mem.sync(false);
                }
            }

            // Restart: clear the registry and rebuild from on-disk. The
            // startup sweep re-stamps the head lvSeqTxn from the filename.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(headLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());
            Assert.assertFalse("LV must still be valid pre-refresh", reloaded.isInvalid());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            Assert.assertTrue(
                    "file-level version mismatch in the head .cp must invalidate the LV",
                    reloaded.isInvalid()
            );
            final CharSequence reason = reloaded.getStateReader().getInvalidationReason();
            Assert.assertNotNull(reason);
            Assert.assertTrue(
                    "invalidation reason mentions format version [reason=" + reason + ']',
                    Chars.contains(reason, "format version")
            );

            // The .cp file must survive: unlike the corruption branch, the
            // version-mismatch route does not unlink derived state, so an
            // operator can inspect it before DROP+CREATE.
            try (Path cpPath = new Path()) {
                cpPath.of(engine.getConfiguration().getDbRoot())
                        .concat(reloaded.getLiveViewToken())
                        .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                        .slash();
                LiveViewCheckpointWriter.appendCpFileName(cpPath, headLvSeqTxn);
                Assert.assertTrue(
                        "head .cp must be preserved across the version-mismatch invalidation",
                        engine.getConfiguration().getFilesFacade().exists(cpPath.$())
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestoreVersionMismatchInvalidatesView() throws Exception {
        // A FUNCTION_SNAPSHOT block whose formatVersion is below the
        // function's current snapshotMinSupportedVersion is a real
        // compatibility break, not structural corruption. The restore path
        // must mark the LV INVALID (operators recover with DROP+CREATE)
        // instead of unlinking the .cp and falling into head-miss replay.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, x, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");

            // Drive a refresh so a real .cp is written and the compiled
            // factory is cached on the instance. Capture the lvSeqTxn and the
            // factory name; the hand-written replacement below must match the
            // factory name so restoreFunctionBlock's dispatch finds it.
            execute("INSERT INTO base (ts, sym, x) VALUES ('2026-06-01T00:00:00.000000Z', 'a', 1)");
            drainWalQueue();
            final String fnFactoryName;
            final long headLvSeqTxn;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                drainWalQueue();
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, headLvSeqTxn);
                Class<?> fnClass = instance.getAnchorWindow().getFunctions().getQuick(0).getClass();
                Class<?> enclosing = fnClass.getEnclosingClass();
                fnFactoryName = (enclosing != null ? enclosing : fnClass).getName();
            }

            // Replace the .cp with one that has the same lvSeqTxn but
            // formatVersion = 0 in the function block. The writer auto-handles
            // CRC. We omit the anchor block since the restore unwinds at the
            // version check before getting to it.
            try (Path lvDir = new Path()) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                lvDir.of(engine.getConfiguration().getDbRoot()).concat(instance.getLiveViewToken()).slash();

                try (Path cpPath = new Path()) {
                    cpPath.of(lvDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, headLvSeqTxn);
                    engine.getConfiguration().getFilesFacade().removeQuiet(cpPath.$());
                }

                try (LiveViewCheckpointWriter w = new LiveViewCheckpointWriter(engine.getConfiguration())) {
                    w.of(lvDir.$(), headLvSeqTxn);
                    w.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(headLvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY)
                            .addWindowName("w"));
                    final MemoryA fnSink = w.beginBlock(LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT);
                    fnSink.putStr("w");
                    fnSink.putStr(fnFactoryName);
                    fnSink.putInt(0); // intentionally below snapshotMinSupportedVersion
                    w.endBlock();
                    w.commit(Numbers.LONG_NULL);
                }
            }

            // Restart: clear the registry and rebuild from on-disk. The
            // startup sweep re-stamps the head lvSeqTxn from the filename.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(headLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());
            Assert.assertFalse("LV must still be valid pre-refresh", reloaded.isInvalid());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            Assert.assertTrue(
                    "version-too-old in the head .cp must invalidate the LV",
                    reloaded.isInvalid()
            );
            final CharSequence reason = reloaded.getStateReader().getInvalidationReason();
            Assert.assertNotNull(reason);
            Assert.assertTrue(
                    "invalidation reason mentions version-too-old [reason=" + reason + ']',
                    Chars.contains(reason, "version too old")
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestoreKeyShapeMismatchReplaysFromHeadMiss() throws Exception {
        // A FUNCTION_SNAPSHOT block whose key-shape header (the partition key
        // column count) disagrees with the running function is structural
        // corruption that slipped past the CRC, not a version break. The
        // restore must treat it like any other corrupt head .cp: unlink it,
        // clear the head metadata, and fall through to head-miss replay -
        // WITHOUT invalidating the view. This is the counterpart to
        // testRestoreVersionMismatchInvalidatesView, which takes the other
        // branch (invalidate, keep the .cp) on a real compatibility break.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-06-01T00:00:00.000000Z', 'a', 1.0), " +
                    "('2026-06-01T01:00:00.000000Z', 'a', 2.0)");
            drainWalQueue();

            final String fnFactoryName;
            final int fnFormatVersion;
            final long headLvSeqTxn;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                drainWalQueue();
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, headLvSeqTxn);
                final WindowFunction fn = instance.getAnchorWindow().getFunctions().getQuick(0);
                final Class<?> fnClass = fn.getClass();
                final Class<?> enclosing = fnClass.getEnclosingClass();
                fnFactoryName = (enclosing != null ? enclosing : fnClass).getName();
                // Stamp the function's CURRENT (valid) snapshot version so the
                // restore clears the version gate and reaches the key-shape
                // header check below.
                fnFormatVersion = fn.snapshotFormatVersion();
            }

            // The LV has already materialised the cumulative sums to its
            // on-disk tier before any corruption.
            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-06-01T00:00:00.000000Z\ta\t1.0\n" +
                            "2026-06-01T01:00:00.000000Z\ta\t3.0\n");

            // Replace the head .cp: same lvSeqTxn, valid prelude, but a
            // FUNCTION_SNAPSHOT payload whose partitionKeyColumnCount is bogus
            // (the running sum() partitions by exactly one column). The restore
            // unwinds at the key-shape check before decoding any partition
            // state, so we need no anchor block.
            try (Path lvDir = new Path()) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                lvDir.of(engine.getConfiguration().getDbRoot()).concat(instance.getLiveViewToken()).slash();

                try (Path cpPath = new Path()) {
                    cpPath.of(lvDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, headLvSeqTxn);
                    engine.getConfiguration().getFilesFacade().removeQuiet(cpPath.$());
                }

                try (LiveViewCheckpointWriter w = new LiveViewCheckpointWriter(engine.getConfiguration())) {
                    w.of(lvDir.$(), headLvSeqTxn);
                    w.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(headLvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY)
                            .addWindowName("w"));
                    final MemoryA fnSink = w.beginBlock(LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT);
                    fnSink.putStr("w");
                    fnSink.putStr(fnFactoryName);
                    fnSink.putInt(fnFormatVersion); // valid version - clears the version gate
                    fnSink.putInt(99); // bogus partitionKeyColumnCount - key-shape mismatch
                    w.endBlock();
                    w.commit(Numbers.LONG_NULL);
                }
            }

            // Restart: clear the registry and rebuild from on-disk. The
            // startup sweep re-stamps the head lvSeqTxn from the filename.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(headLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());
            Assert.assertFalse("LV must still be valid pre-refresh", reloaded.isInvalid());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            Assert.assertTrue("the restore was attempted on the first post-restart cycle", reloaded.isCheckpointRestoreAttempted());
            Assert.assertFalse(
                    "a key-shape mismatch is corruption, not a version break - the LV must NOT be invalidated",
                    reloaded.isInvalid()
            );
            Assert.assertEquals(
                    "the corrupt head .cp was unlinked and head metadata cleared (head-miss routing)",
                    Numbers.LONG_NULL,
                    reloaded.getHeadCheckpointLvSeqTxn()
            );

            // The failed restore left the on-disk tier untouched: the LV still
            // reads the same cumulative sums it computed before the corruption.
            assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                            "2026-06-01T00:00:00.000000Z\ta\t1.0\n" +
                            "2026-06-01T01:00:00.000000Z\ta\t3.0\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresRankFromHeadCheckpoint() throws Exception {
        // rank() is now snapshot-capable. End-to-end check that a
        // refresh cycle writes a head .cp, a simulated restart re-discovers
        // it, and the first post-restart refresh tick rehydrates the rank
        // function's partition map. Mirrors testRestartRestoresFromHeadCheckpoint
        // for sum() but covers the rank chain-prefix path the 2b.1a codec
        // extension and 2b.1b migration introduce.
        //
        // The post-restart-then-new-commit scenario is intentionally NOT
        // covered: getIncrementalCursor's pre-existing toTop chain wipes the
        // anchor map and function maps at the start of each refresh cycle,
        // so a new commit immediately after restore would discard the
        // rehydrated state. That cross-cycle wipe is the broader limitation
        // tracked under 2a.8's "known limitations" - addressing it sits
        // outside this test's scope.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, rank() OVER w AS r FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            final long preFunctionMapSize;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                preFunctionMapSize = instance.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with rank() now that 2b.1b makes it snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertEquals("two partition keys seeded pre-restart", 2L, preFunctionMapSize);
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());
            Assert.assertEquals(
                    "rank's partition map rehydrates to its pre-restart partition count",
                    preFunctionMapSize,
                    reloaded.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRankSnapshotRestoreRoundTripsState() throws Exception {
        // snapshot() / restore() round-trip the rank function's
        // per-partition rank, count, and chain-prefix bytes through a MemoryCARW
        // buffer. The end-to-end LV head .cp path is exercised by
        // testRestartRestoresRankFromHeadCheckpoint; this case isolates the codec
        // round-trip on the function level so a regression in the chain-prefix
        // serializer surfaces here before the integration test sees it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, rank() OVER w AS r FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 3.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'b', 4.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            WindowFunction rankFn = lv.getAnchorWindow().getFunctions().getQuick(0);
            Assert.assertTrue("rank reports snapshot capability after 2b.1b", rankFn.supportsSnapshot());
            Assert.assertEquals(2L, rankFn.getPartitionMap().size());

            try (MemoryCARW buf = Vm.getCARWInstance(64 * 1024L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                LiveViewFunctionSnapshot.write(buf, rankFn);
                final long snapshotBytes = buf.getAppendOffset();
                Assert.assertTrue("snapshot wrote some bytes", snapshotBytes > 0);
                // Clear the function's map and restore from the captured bytes.
                rankFn.getPartitionMap().clear();
                Assert.assertEquals(0L, rankFn.getPartitionMap().size());
                LiveViewFunctionSnapshot.restore(buf, 0L, rankFn, rankFn.snapshotFormatVersion());
                Assert.assertEquals(
                        "restore rehydrates the same partition count snapshot captured",
                        2L,
                        rankFn.getPartitionMap().size()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLagSnapshotRestoreRoundTripsState() throws Exception {
        // snapshot() / restore() round-trip the lag function's
        // per-partition firstIdx + count and the raw ring buffer contents
        // through a MemoryCARW buffer. Isolates the codec round-trip from the
        // end-to-end .cp path so a regression in the ring-blob serializer
        // surfaces here before the integration test sees it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, lag(x, 2) OVER w AS prev FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            WindowFunction lagFn = lv.getAnchorWindow().getFunctions().getQuick(0);
            Assert.assertTrue("lag reports snapshot capability after 2b.2a", lagFn.supportsSnapshot());
            Assert.assertEquals(2L, lagFn.getPartitionMap().size());

            try (MemoryCARW buf = Vm.getCARWInstance(64 * 1024L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                LiveViewFunctionSnapshot.write(buf, lagFn);
                final long snapshotBytes = buf.getAppendOffset();
                Assert.assertTrue("snapshot wrote some bytes", snapshotBytes > 0);
                lagFn.getPartitionMap().clear();
                Assert.assertEquals(0L, lagFn.getPartitionMap().size());
                LiveViewFunctionSnapshot.restore(buf, 0L, lagFn, lagFn.snapshotFormatVersion());
                Assert.assertEquals(
                        "restore rehydrates the same partition count snapshot captured",
                        2L,
                        lagFn.getPartitionMap().size()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRowsAggregatesFromHeadCheckpoint() throws Exception {
        // avg, sum, count(*), count(arg), and ksum over
        // (PARTITION BY ... ROWS N PRECEDING ...) are now snapshot-capable.
        // End-to-end check that an LV combining all of them writes a head .cp
        // and the first post-restart refresh tick rehydrates the partition
        // maps. No ANCHOR is involved - the validator rejects ANCHOR over
        // bounded frames, so this exercises the no-anchor snapshot path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  avg(x) OVER w AS a, " +
                    "  sum(x) OVER w AS s, " +
                    "  count(*) OVER w AS cs, " +
                    "  count(x) OVER w AS cx, " +
                    "  ksum(x) OVER w AS k " +
                    "FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS 2 PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with bounded ROWS aggregates now that 2b.3a/b/c makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresStatefulEmaFamilyFromHeadCheckpoint() throws Exception {
        // The stateful + EMA family's UNBOUNDED +
        // ANCHOR variants now ship snapshot/restore. Six classes migrated:
        //   StdDevOverUnboundedPartitionRowsFrameFunction (covers stddev_*,
        //     var_*; 3 slots, Welford mean/m2/count)
        //   BivarStatOverUnboundedPartitionRowsFrameFunction (covers corr,
        //     covar_*; 6 slots, paired Welford)
        //   EmaOverPartitionFunction + EmaTimeWeightedOverPartitionFunction
        //     (3 slots: ema, prevTimestamp, hasValue)
        //   VwemaOverPartitionFunction + VwemaTimeWeightedOverPartitionFunction
        //     (4 slots: numerator, denominator, prevTimestamp, hasValue)
        // All six are fixed-shape with no ring buffer, so snapshot/restore
        // is a straight slot-by-slot round-trip.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, y DOUBLE, vol DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  stddev_samp(x) OVER w AS sd, " +
                    "  corr(x, y) OVER w AS cr, " +
                    "  avg(x, 'period', 5) OVER w AS ep, " +
                    "  avg(x, 'minute', 5) OVER w AS et, " +
                    "  avg(x, 'period', 5, vol) OVER w AS vp, " +
                    "  avg(x, 'minute', 5, vol) OVER w AS vt " +
                    "FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x, y, vol) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0, 10.0, 100.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0, 20.0, 150.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0, 30.0, 200.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 5.0, 50.0, 500.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 6.0, 60.0, 600.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with stddev/corr/ema/vwema now that 2b.5 makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRowsMinMaxFromHeadCheckpoint() throws Exception {
        // min/max over (PARTITION BY ... ROWS N PRECEDING ...) are
        // now snapshot-capable. The single MaxMinOverPartitionRowsFrameFunction
        // class carries two state shapes:
        //   - frameLoBounded == true:  ring + monotonic deque (5 LONG slots)
        //   - frameLoBounded == false: ring + scalar max/min   (3 slots, last
        //                              one typed DOUBLE/LONG/TIMESTAMP)
        // Both flavours need to round-trip through .cp. The LV below exercises
        // both: w1 uses ROWS 2 PRECEDING (bounded lower) and w2 uses ROWS
        // BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING (unbounded lower).
        // min() and max() share the implementation class via a comparator
        // parameter, so covering both functions also covers Min* and Max*
        // factories at once.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  min(x) OVER w1 AS mn, " +
                    "  max(x) OVER w1 AS mx, " +
                    "  min(x) OVER w2 AS mnu, " +
                    "  max(x) OVER w2 AS mxu " +
                    "FROM base " +
                    "WINDOW " +
                    "  w1 AS (PARTITION BY sym ORDER BY ts ROWS 2 PRECEDING), " +
                    "  w2 AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with min/max bounded ROWS now that 2b.4 makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRangeAggregatesFromHeadCheckpoint() throws Exception {
        // avg/sum/count(*)/count(arg) over (PARTITION BY ...
        // RANGE BETWEEN '<n>' <unit> PRECEDING AND ...) are now snapshot-
        // capable. End-to-end check that an LV combining all of them writes a
        // head .cp and the first post-restart refresh tick rehydrates the
        // partition maps. Variable-length deque serialisation: snapshot writes
        // size + size * (LONG ts, DOUBLE/LONG value); restore re-allocates the
        // ring at capacity = max(size, initialBufferSize).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  avg(x) OVER w AS a, " +
                    "  sum(x) OVER w AS s, " +
                    "  count(*) OVER w AS cs, " +
                    "  count(x) OVER w AS cx " +
                    "FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with bounded RANGE aggregates now that 2b.6a/b makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRangeFirstLastValueFromHeadCheckpoint() throws Exception {
        // first_value() and last_value() over (PARTITION BY ...
        // RANGE BETWEEN ...) are now snapshot-capable across Double / Long /
        // Timestamp factories and respect-nulls vs IGNORE NULLS variants.
        // First and last share the same per-partition slot count in their LV
        // value-types static; snapshot/restore are overridden per class to
        // handle their distinct slot orderings.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  first_value(x) OVER w1 AS fv, " +
                    "  first_value(x) IGNORE NULLS OVER w1 AS fvn, " +
                    "  last_value(x) IGNORE NULLS OVER w1 AS lvn, " +
                    "  last_value(x) OVER w2 AS lv " +
                    "FROM base " +
                    "WINDOW " +
                    "  w1 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW), " +
                    "  w2 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with first_value/last_value bounded RANGE now that 2b.6d/e makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRangeMinMaxFromHeadCheckpoint() throws Exception {
        // min/max over (PARTITION BY ... RANGE BETWEEN ...) are
        // now snapshot-capable. The MaxMinOverPartitionRangeFrameFunction
        // class carries two state shapes:
        //   - frameLoBounded == true:  ring + monotonic deque (9 LONG slots)
        //   - frameLoBounded == false: ring + scalar max/min (5 LONGs + 1
        //                              typed DOUBLE/LONG/TIMESTAMP)
        // Both flavours need to round-trip through .cp. The LV below exercises
        // both: w1 uses RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW
        // (bounded lower) and w2 uses RANGE BETWEEN UNBOUNDED PRECEDING AND
        // '1' HOUR PRECEDING (unbounded lower). min() and max() share the
        // implementation class via a comparator parameter, so covering both
        // functions also covers Min* and Max* factories at once.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  min(x) OVER w1 AS mn, " +
                    "  max(x) OVER w1 AS mx, " +
                    "  min(x) OVER w2 AS mnu, " +
                    "  max(x) OVER w2 AS mxu " +
                    "FROM base " +
                    "WINDOW " +
                    "  w1 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW), " +
                    "  w2 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND '1' HOUR PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with min/max bounded RANGE now that 2b.6c makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresFirstLastValueFromHeadCheckpoint() throws Exception {
        // first_value() and last_value() over (PARTITION BY ...
        // ROWS N PRECEDING ...) are now snapshot-capable, in both the parent
        // ("respect nulls", default) and IGNORE NULLS subclass variants. The
        // four migrated classes have distinct slot layouts:
        //   - FirstValue parent:   3 LONG slots
        //   - FirstNotNull:        4 LONG slots
        //   - LastValue parent:    2 LONG slots (3rd reserved)
        //   - LastNotNull:         1 TYPED + 2 LONG slots
        // The LV below routes through all four code paths; w1 fires when
        // rowsHi == 0 (frame includes current row) for first_value variants
        // and the IGNORE NULLS last_value path; w2 fires when rowsHi < 0 for
        // the respect-nulls last_value parent (rowsHi == 0 would route to
        // LastValueIncludeCurrent, which has no snapshot support).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  first_value(x) OVER w1 AS fv, " +
                    "  first_value(x) IGNORE NULLS OVER w1 AS fvn, " +
                    "  last_value(x) IGNORE NULLS OVER w1 AS lvn, " +
                    "  last_value(x) OVER w2 AS lv " +
                    "FROM base " +
                    "WINDOW " +
                    "  w1 AS (PARTITION BY sym ORDER BY ts ROWS 2 PRECEDING), " +
                    "  w2 AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with first_value/last_value bounded ROWS now that 2b.3d/e makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresLagFromHeadCheckpoint() throws Exception {
        // lag() is now snapshot-capable. End-to-end check that a
        // refresh cycle writes a head .cp, a simulated restart re-discovers
        // it, and the first post-restart refresh tick rehydrates the lag
        // function's partition map. Mirrors testRestartRestoresRankFromHead
        // Checkpoint for the lag ring-blob path.
        //
        // The post-restart-then-new-commit scenario is intentionally NOT
        // covered: getIncrementalCursor's pre-existing toTop chain wipes the
        // anchor map and function maps at the start of each refresh cycle,
        // so a new commit immediately after restore would discard the
        // rehydrated state. That cross-cycle wipe is tracked under 2a.8's
        // "known limitations".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, lag(x, 1) OVER w AS prev FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            final long preFunctionMapSize;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                preFunctionMapSize = instance.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with lag() now that 2b.2a makes it snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertEquals("two partition keys seeded pre-restart", 2L, preFunctionMapSize);
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());
            Assert.assertEquals(
                    "lag's partition map rehydrates to its pre-restart partition count",
                    preFunctionMapSize,
                    reloaded.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLatestSeenTsAdvancesAcrossRows() throws Exception {
        // The anchor-dispatch cursor stamps the per-LV latestSeenTs
        // watermark on every base row consumed by the refresh worker. This is
        // the input the O3 detection path will read in a later commit; for now
        // we just verify the cursor feeds the setter with the max ts in the
        // batch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                Assert.assertEquals(
                        "latestSeenTs starts at LONG_NULL on a fresh LV",
                        Numbers.LONG_NULL,
                        lv.getLatestSeenTs()
                );

                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-10-01T00:00:05.000000Z', 'b', 3.0), " +
                        "('2026-10-01T00:00:10.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "latestSeenTs equals max(ts) of the batch consumed by the refresh worker",
                        MicrosFormatUtils.parseUTCTimestamp("2026-10-01T00:00:10.000000Z"),
                        lv.getLatestSeenTs()
                );

                // A subsequent in-order batch advances the watermark; advance
                // microtime past the FLUSH EVERY gate so the second refresh
                // actually fires rather than no-oping on the rate limiter.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-10-01T00:01:00.000000Z', 'a', 4.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "latestSeenTs advances on the next in-order batch",
                        MicrosFormatUtils.parseUTCTimestamp("2026-10-01T00:01:00.000000Z"),
                        lv.getLatestSeenTs()
                );

                // The setter's monotonic clamp is asserted directly so the
                // contract is pinned independently of the cursor wiring.
                long beforeClampAttempt = lv.getLatestSeenTs();
                lv.setLatestSeenTs(beforeClampAttempt - 1_000_000L);
                Assert.assertEquals(
                        "setLatestSeenTs is monotonic; a lower value is ignored",
                        beforeClampAttempt,
                        lv.getLatestSeenTs()
                );

                // forceSetLatestSeenTs bypasses the monotonic clamp; the
                // refresh worker uses it on O3 detect + rollback to revert
                // any in-cycle bumps the discarded rows applied.
                long rollbackTarget = beforeClampAttempt - 10_000_000L;
                lv.forceSetLatestSeenTs(rollbackTarget);
                Assert.assertEquals(
                        "forceSetLatestSeenTs writes the value verbatim",
                        rollbackTarget,
                        lv.getLatestSeenTs()
                );
                lv.forceSetLatestSeenTs(beforeClampAttempt);
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3InvalidatesHeadCheckpoint() throws Exception {
        // An O3 base commit (min ts strictly below the LV's
        // latestSeenTs watermark) cannot be replayed in WAL order without
        // corrupting per-partition window state, so the refresh worker
        // rolls back the in-flight WAL writer, branches to o3Replay, and
        // re-feeds base data in ts order from a TableReader. The prior
        // head .cp is retired by the replay path; a fresh head reflecting
        // the post-replay state is written before the cycle returns so
        // restart can short-circuit to head-hit for any subsequent O3.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                final long preO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(
                        "head .cp was written before the O3 commit",
                        Numbers.LONG_NULL,
                        preO3HeadLvSeqTxn
                );
                Assert.assertEquals(
                        "latestSeenTs equals max(ts) of the first batch",
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getLatestSeenTs()
                );

                // The head .cp lives at <root>/<lv_dir>/_checkpoints/<lvSeqTxn>.cp.
                TableToken token = lv.getLiveViewToken();
                FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path cpPath = new Path()) {
                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, preO3HeadLvSeqTxn);
                    Assert.assertTrue("head .cp exists on disk before O3", ff.exists(cpPath.$()));

                    // Insert an out-of-order row: ts (00:00:05) is strictly
                    // below the watermark (00:00:20). Advance microtime past
                    // the FLUSH EVERY gate so the refresh worker actually
                    // ticks rather than no-oping on the rate limiter.
                    setCurrentMicros(200_000L);
                    execute("INSERT INTO base (ts, sym, x) VALUES " +
                            "('2026-11-01T00:00:05.000000Z', 'a', 2.0)");
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();

                    final long postO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                    Assert.assertNotEquals(
                            "head metadata refreshed post O3 replay",
                            Numbers.LONG_NULL,
                            postO3HeadLvSeqTxn
                    );
                    Assert.assertNotEquals(
                            "post-replay head lvSeqTxn differs from the pre-O3 head",
                            preO3HeadLvSeqTxn,
                            postO3HeadLvSeqTxn
                    );
                    Assert.assertNotEquals(
                            "head_checkpoint_max_ts populated post O3 replay",
                            Numbers.LONG_NULL,
                            lv.getHeadCheckpointMaxTs()
                    );
                    Assert.assertNotEquals(
                            "head_checkpoint_state_bytes populated post O3 replay",
                            0L,
                            lv.getHeadCheckpointStateBytes()
                    );

                    // The pre-O3 .cp is gone (retired by the replay path).
                    // Re-derive the path because the helper mutates it in
                    // place.
                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, preO3HeadLvSeqTxn);
                    Assert.assertFalse("pre-O3 head .cp unlinked", ff.exists(cpPath.$()));

                    // The fresh post-replay .cp is on disk at the new lvSeqTxn.
                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, postO3HeadLvSeqTxn);
                    Assert.assertTrue("post-replay head .cp on disk", ff.exists(cpPath.$()));
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3HeadMissReplaysFromLowerBound() throws Exception {
        // An O3 row with ts strictly below the head's maxTimestamp
        // forces a head-miss replay. The path resets every window-function map,
        // wipes the anchor map, scans the base TableReader from
        // viewLowerBoundTimestamp through advanceTo, emits a single REPLACE_RANGE
        // commit, and writes a fresh head reflecting the post-replay state.
        // After the dust settles the LV output reads the cumulative sum across
        // all rows in ts order - matching what the non-incremental SELECT
        // against the base would produce.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                final long preO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, preO3HeadLvSeqTxn);
                Assert.assertEquals(
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getHeadCheckpointMaxTs()
                );

                // Drop an O3 row at ts strictly below headMaxTs. lateRowTs=05
                // < headMaxTs=20 - head-miss eligibility is the only path.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:05.000000Z', 'a', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Post-replay LV output is the cumulative sum across all three
                // rows in ts-ascending order: 3 -> 3+1=4 -> 4+2=6.
                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:05.000000Z\ta\t3.0\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t4.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t6.0\n");

                // A fresh head has landed at a new lvSeqTxn, the prior one is
                // gone on disk.
                Assert.assertNotEquals(preO3HeadLvSeqTxn, lv.getHeadCheckpointLvSeqTxn());
                Assert.assertNotEquals(Numbers.LONG_NULL, lv.getHeadCheckpointLvSeqTxn());

                // lvRowsTotal (-> MANIFEST.lvRowPosition) must match the on-disk
                // count: the intact-base head-miss is a full rebuild of 3 rows,
                // counted once. The double-count bug returned 6 (3 on disk + 3
                // appended).
                Assert.assertEquals(3L, lv.getLvRowsTotal());
                assertQuery("SELECT count(*) AS count FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3WithinSingleCommitIsReordered() throws Exception {
        // A single base commit whose rows are NOT in ts-ascending order must
        // not corrupt window state. The refresh worker reads raw (unsorted) WAL
        // segments, so detection cannot rely on the commit's min ts alone:
        // here the whole commit sits above the LV watermark (the LV is empty),
        // yet the rows are internally shuffled. The commit carries
        // WalEventCursor.DataInfo.isOutOfOrder()==true, which must route the
        // batch through the ts-sorted O3 replay path. All rows share one day
        // bucket so no anchor reset fires - the bug is purely row order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // One INSERT statement == one WAL commit; the two rows are out
                // of ts order within it (ts=20 appended before ts=10).
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0), " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Correct cumulative-in-ts-order output:
                //   ts=10 -> sum=1.0, rn=1
                //   ts=20 -> sum=1+2=3.0, rn=2
                // The buggy in-WAL-order path computes the reverse (ts=10 carries
                // sum=3.0/rn=2 because it was processed second).
                assertQuery("SELECT ts, sym, s, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\trn\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t1.0\t1\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t3.0\t2\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3HeadMissWithFullyFilteredReplayPreservesState() throws Exception {
        // The head-miss path replays from viewLowerBoundTimestamp and
        // rebuilds state. The probe-then-wipe ordering ensures a replay that
        // produces zero output rows does not clobber pre-O3 accumulator
        // state. This test covers the closely related scenario: an O3 commit
        // whose row is filtered out by the LV's WHERE; the replay still reads
        // two surviving base rows so the probe passes, the wipe + replay run
        // as usual, and the LV's output matches the pre-O3 state.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base WHERE x > 100 " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 200.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 300.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t200.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t500.0\n");

                // O3 row at ts=05 that fails the filter (x <= 100). The WAL
                // path still detects O3 by min(ts) < latestSeenTs; head-miss
                // replay reads (05,50), (10,200), (20,300) from the
                // TableReader, the filter drops (05,50), and the post-filter
                // probe sees the remaining two rows. State wipes and
                // rebuilds; LV output is unchanged.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:05.000000Z', 'a', 50.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t200.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t500.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3HeadMissAfterBaseDropPartitionPreservesUnrefreshablePrefix() throws Exception {
        // Head-miss O3 replay re-reads the BASE table (it is not base-
        // independent). When the base has dropped an old partition, the replay
        // can no longer produce rows for that range. Without the replayMinTs
        // clamp the REPLACE_RANGE spanned [viewLowerBoundTimestamp, +inf) and
        // DELETED the LV's already-computed rows for the dropped range. The
        // clamp scopes the rewrite to the rows the base still produces, so the
        // unrefreshable prefix is preserved (frozen) instead of dropped. The
        // daily anchor makes each day its own bucket, so dropping a whole day
        // leaves the surviving days on bucket boundaries (no straddling-bucket
        // accumulator artifact in this scenario).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0), " +
                        "('2026-11-02T00:00:10.000000Z', 'a', 10.0), " +
                        "('2026-11-02T00:00:20.000000Z', 'a', 20.0), " +
                        "('2026-11-03T00:00:10.000000Z', 'a', 100.0), " +
                        "('2026-11-03T00:00:20.000000Z', 'a', 200.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                // A head checkpoint must exist so the upcoming deep-past O3 is a
                // genuine head-miss decision rather than a no-head fallback.
                Assert.assertNotEquals(Numbers.LONG_NULL, lv.getHeadCheckpointLvSeqTxn());
                // Per-day cumulative sum (the anchor resets each day).
                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t1.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t3.0\n" +
                                "2026-11-02T00:00:10.000000Z\ta\t10.0\n" +
                                "2026-11-02T00:00:20.000000Z\ta\t30.0\n" +
                                "2026-11-03T00:00:10.000000Z\ta\t100.0\n" +
                                "2026-11-03T00:00:20.000000Z\ta\t300.0\n");

                // Base drops its oldest partition; the LV walks past it
                // (transparent) and stays ACTIVE with its derived rows intact.
                execute("ALTER TABLE base DROP PARTITION LIST '2026-11-01'");
                drainWalQueue();
                lv.setLastFlushTimeUs(Numbers.LONG_NULL);
                drainJob(job);
                drainWalQueue();
                Assert.assertFalse(lv.isInvalid());

                // Deep-past O3 into the surviving day-2 bucket (ts < headMaxTs)
                // forces a head-miss replay. The base no longer holds day-1, so
                // the replay produces nothing below 2026-11-02; the clamp leaves
                // the day-1 LV rows in place rather than deleting them.
                lv.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-11-02T00:00:05.000000Z', 'a', 5.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertFalse(lv.isInvalid());
                assertQuery("SELECT view_status FROM live_views() WHERE view_name = 'lv'").noLeakCheck().noRandomAccess().returns("view_status\nactive\n");
                // Day-1 rows survive with their ORIGINAL sums (frozen prefix);
                // day-2 is rewritten with the merged O3 row; day-3 is unchanged.
                // Without the clamp, the day-1 rows would be gone.
                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t1.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t3.0\n" +
                                "2026-11-02T00:00:05.000000Z\ta\t5.0\n" +
                                "2026-11-02T00:00:10.000000Z\ta\t15.0\n" +
                                "2026-11-02T00:00:20.000000Z\ta\t35.0\n" +
                                "2026-11-03T00:00:10.000000Z\ta\t100.0\n" +
                                "2026-11-03T00:00:20.000000Z\ta\t300.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3HeadHitReplaysFromHead() throws Exception {
        // An O3 row with ts > head.maxTimestamp drops into the
        // head-hit branch. State rolls back to the head's snapshot moment
        // (restoreFromHead populates anchor + function maps), the replay
        // scans only the rows past head.maxTimestamp, and emits a single
        // REPLACE_RANGE commit covering (head.maxTimestamp, +inf). LV
        // output afterwards is the cumulative sum across all rows in
        // ts-ascending order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: two rows. Drain writes a head at maxTs=20.
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                final long preO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, preO3HeadLvSeqTxn);
                Assert.assertEquals(
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getHeadCheckpointMaxTs()
                );

                // Batch 2: two more rows in WAL order. Cadence triggers
                // (default 1M rows / 5 min) do not fire, so the head
                // metadata still points at the batch-1 head.
                setCurrentMicros(150_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:30.000000Z', 'a', 3.0), " +
                        "('2026-11-01T00:00:40.000000Z', 'a', 4.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "head metadata unchanged after batch 2 (cadence did not fire)",
                        preO3HeadLvSeqTxn,
                        lv.getHeadCheckpointLvSeqTxn()
                );
                Assert.assertEquals(
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:40.000000Z"),
                        lv.getLatestSeenTs()
                );

                // O3 row at ts=25 sits strictly between headMaxTs=20 and
                // latestSeenTs=40, so head-hit eligibility applies
                // (headMaxTs <= lateRowTs).
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:25.000000Z', 'a', 5.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Post-replay output: cumulative sum across all five rows
                // ordered by ts.
                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t1.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t3.0\n" +
                                "2026-11-01T00:00:25.000000Z\ta\t8.0\n" +
                                "2026-11-01T00:00:30.000000Z\ta\t11.0\n" +
                                "2026-11-01T00:00:40.000000Z\ta\t15.0\n");

                Assert.assertNotEquals(preO3HeadLvSeqTxn, lv.getHeadCheckpointLvSeqTxn());
                Assert.assertNotEquals(Numbers.LONG_NULL, lv.getHeadCheckpointLvSeqTxn());
                Assert.assertEquals(
                        "post-replay head maxTs reflects max row in replay output",
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:40.000000Z"),
                        lv.getHeadCheckpointMaxTs()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3ResetsInMemoryTier() throws Exception {
        // After an O3 replay rewrites the on-disk tier, the in-mem tier still
        // holds the pre-replay output rows for the rewritten range. The refresh
        // worker resets the in-mem tier so a post-O3 cursor falls through to the
        // now-correct on-disk tier; the next normal cycle republishes it. Under
        // the current max-disk-ts routing the stale rows would be masked anyway,
        // so this guards the latent gap that activates once the cursor adopts
        // seam_ts routing (Phase 3a completion) or the Phase 4 hand-off ring lets
        // the in-mem tier outrun disk.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // IN MEMORY 1h keeps every test row resident in the in-mem tier.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1h AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Two in-order batches: four rows land in the in-mem tier; the
                // batch-1 head sits at maxTs=20.
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(150_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:30.000000Z', 'a', 3.0), " +
                        "('2026-11-01T00:00:40.000000Z', 'a', 4.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                LiveViewInMemoryTier tier = lv.getInMemoryTier();
                Assert.assertNotNull("in-mem tier must be allocated after refresh", tier);
                Assert.assertEquals(
                        "in-mem tier holds all four in-order rows before O3",
                        4,
                        tier.getSlot(tier.getPublishedIdx()).rowCount()
                );

                // O3 row at ts=25 sits inside the in-mem-resident range and
                // strictly above headMaxTs=20, so it drives a head-hit replay
                // that rewrites the on-disk tier.
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:25.000000Z', 'a', 5.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // The replay reset the in-mem tier: the published slot is empty.
                Assert.assertEquals(
                        "O3 replay must reset the in-mem tier",
                        0,
                        tier.getSlot(tier.getPublishedIdx()).rowCount()
                );

                // Reads are correct post-O3 (served from the rewritten on-disk
                // tier): cumulative sum across all five rows in ts order.
                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t1.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t3.0\n" +
                                "2026-11-01T00:00:25.000000Z\ta\t8.0\n" +
                                "2026-11-01T00:00:30.000000Z\ta\t11.0\n" +
                                "2026-11-01T00:00:40.000000Z\ta\t15.0\n");

                // A subsequent in-order commit republishes the in-mem tier.
                setCurrentMicros(600_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:50.000000Z', 'a', 6.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertTrue(
                        "next normal cycle republishes the in-mem tier",
                        tier.getSlot(tier.getPublishedIdx()).rowCount() > 0
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3AtHeadMaxTimestampBoundaryIsNotDropped() throws Exception {
        // Boundary case: an O3 row whose ts is EXACTLY the head's maxTimestamp.
        // Head-hit eligibility must be strict (headMaxTs < lateRowTs): the head
        // state already covers every row up to and including headMaxTs, and a
        // head-hit replay starts at headMaxTs + 1, so a late row at headMaxTs
        // would be excluded from the replay and silently lost. The exact
        // boundary must route to head-miss instead, which re-reads from
        // viewLowerBoundTimestamp and merges the late row in ts order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: two rows. Drain writes a head at maxTs=20.
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                final long preO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, preO3HeadLvSeqTxn);
                Assert.assertEquals(
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getHeadCheckpointMaxTs()
                );

                // Batch 2: advance latestSeenTs to 40 without rewriting the head
                // (cadence triggers do not fire), so headMaxTs stays at 20 while
                // the watermark moves past it.
                setCurrentMicros(150_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:30.000000Z', 'a', 4.0), " +
                        "('2026-11-01T00:00:40.000000Z', 'a', 8.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "head metadata unchanged after batch 2 (cadence did not fire)",
                        preO3HeadLvSeqTxn,
                        lv.getHeadCheckpointLvSeqTxn()
                );
                Assert.assertEquals(
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:40.000000Z"),
                        lv.getLatestSeenTs()
                );

                // O3 row at ts=20 == headMaxTs. latestSeenTs=40, so it is O3
                // (20 < 40). Under strict head-hit eligibility it routes to
                // head-miss; the late row tie-sorts after the original ts=20 row
                // and merges into the cumulative sum.
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 100.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Correct cumulative sum across all rows in ts order, ties by
                // WAL order (original ts=20 before the O3 ts=20):
                //   10 -> 1, 20 -> 3, 20 -> 103, 30 -> 107, 40 -> 115.
                // The pre-fix head-hit path replays from 21 and drops the O3 row
                // entirely, yielding 10->1, 20->3, 30->7, 40->15 (four rows).
                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts, s").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t1.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t3.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t103.0\n" +
                                "2026-11-01T00:00:30.000000Z\ta\t107.0\n" +
                                "2026-11-01T00:00:40.000000Z\ta\t115.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3StormAtFixedHorizon() throws Exception {
        // Repeated O3 rows at the same historical horizon (well
        // below headMaxTs) fall into the head-miss path each time. Every event triggers a
        // full replay from viewLowerBoundTimestamp and writes one fresh head.
        // After three such events the LV output reflects all rows in ts order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:50.000000Z', 'a', 1.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                long lastHeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, lastHeadLvSeqTxn);

                String[] o3Inserts = new String[]{
                        "('2026-11-01T00:00:05.000000Z', 'a', 2.0)",
                        "('2026-11-01T00:00:06.000000Z', 'a', 3.0)",
                        "('2026-11-01T00:00:07.000000Z', 'a', 4.0)",
                };
                long microtime = 200_000L;
                for (int i = 0; i < o3Inserts.length; i++) {
                    setCurrentMicros(microtime);
                    execute("INSERT INTO base (ts, sym, x) VALUES " + o3Inserts[i]);
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                    microtime += 200_000L;

                    // Each storm event writes a fresh head with a new lvSeqTxn.
                    long head = lv.getHeadCheckpointLvSeqTxn();
                    Assert.assertNotEquals("storm event #" + i + " did not refresh the head", lastHeadLvSeqTxn, head);
                    Assert.assertNotEquals(Numbers.LONG_NULL, head);
                    lastHeadLvSeqTxn = head;
                }

                // Final LV output covers all four rows in ts order with the
                // cumulative sum across the day-anchored bucket.
                assertQuery("SELECT ts, sym, s FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tsym\ts\n" +
                                "2026-11-01T00:00:05.000000Z\ta\t2.0\n" +
                                "2026-11-01T00:00:06.000000Z\ta\t5.0\n" +
                                "2026-11-01T00:00:07.000000Z\ta\t9.0\n" +
                                "2026-11-01T00:00:50.000000Z\ta\t10.0\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3WritesFreshCheckpointPostReplay() throws Exception {
        // After the O3 replay path drives an apply commit, the
        // head metadata trio (lvSeqTxn, maxTs, stateBytes) reflects the
        // post-replay state - not the pre-O3 state, and not LONG_NULL.
        // The .cp file exists on disk at the new lvSeqTxn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                final long preO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                final long preO3StateBytes = lv.getHeadCheckpointStateBytes();
                Assert.assertNotEquals(Numbers.LONG_NULL, preO3HeadLvSeqTxn);
                Assert.assertTrue(preO3StateBytes > 0L);

                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:05.000000Z', 'a', 5.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                final long postReplayLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, postReplayLvSeqTxn);
                Assert.assertNotEquals(preO3HeadLvSeqTxn, postReplayLvSeqTxn);
                Assert.assertNotEquals(Numbers.LONG_NULL, lv.getHeadCheckpointMaxTs());
                Assert.assertTrue(
                        "post-replay state_bytes populated",
                        lv.getHeadCheckpointStateBytes() > 0L
                );

                TableToken token = lv.getLiveViewToken();
                FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path cpPath = new Path()) {
                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, postReplayLvSeqTxn);
                    Assert.assertTrue("post-replay head .cp exists", ff.exists(cpPath.$()));

                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, preO3HeadLvSeqTxn);
                    Assert.assertFalse("pre-O3 head .cp unlinked", ff.exists(cpPath.$()));
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3DetectionFiresForNonAnchoredLv() throws Exception {
        // Non-anchored stamp: the row-loop stamp updates
        // latestSeenTs for LVs without an anchored named window. Pre-fix,
        // only AnchorDispatchingCursor stamped, so non-anchored LVs never
        // drove O3 detection. The test uses row_number() OVER (), which the
        // factory specialises into SequenceRowNumberFunction (no per-partition
        // map). That puts the LV on the not-snapshot-capable branch of the
        // replay path: the O3 cycle invalidates the head, advances the
        // watermarks, and trails for the O3 batch. Detection itself - what
        // the row-loop stamp enables - is the surface this test pins.
        // Head-miss / head-hit content correctness is covered by
        // testO3HeadMissReplaysFromLowerBound and testO3HeadHitReplaysFromHead
        // against anchored snapshot-capable LVs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                Assert.assertNull("LV has no anchored window", lv.getAnchorWindow());
                Assert.assertEquals(
                        "row-loop stamp updates latestSeenTs without an anchor cursor",
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getLatestSeenTs()
                );
                final long lastProcessedAfterBatch1 = lv.getLastProcessedSeqTxn();

                // Drop an O3 row. The detect block fires because latestSeenTs
                // is populated; the not-snapshot-capable LV then takes the
                // skip branch in o3Replay.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-11-01T00:00:05.000000Z', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // The watermark stays at 20 thanks to the monotonic clamp,
                // and lastProcessedSeqTxn advances so the next cycle does
                // not re-iterate the O3 batch in WAL order.
                Assert.assertEquals(
                        "monotonic clamp keeps latestSeenTs at the pre-O3 high water",
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getLatestSeenTs()
                );
                Assert.assertTrue(
                        "lastProcessedSeqTxn advanced past the O3 base seqTxn",
                        lv.getLastProcessedSeqTxn() > lastProcessedAfterBatch1
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFreezeBlocksInFlightRefreshCycle() throws Exception {
        // startCheckpoint must force a happens-before edge with any in-flight
        // refresh turn before its file copy begins, so the agent never reads
        // _lv.s mid-rewrite. The fix takes and releases the refresh latch
        // inside startCheckpoint. Here the test stands in for the worker by
        // holding the latch manually and asserts startCheckpoint blocks
        // until the latch is released.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(lv);

            // Simulate the worker mid-turn by holding the latch on this thread.
            Assert.assertTrue(
                    "test setup must take the refresh latch",
                    lv.tryLockForRefresh()
            );

            final AtomicBoolean returned = new AtomicBoolean(false);
            final Thread agent = new Thread(() -> {
                lv.startCheckpoint(lv.getStateReader().getAppliedWatermark());
                returned.set(true);
            }, "lv-freeze-handshake-test");
            try {
                agent.start();
                // Give the agent time to publish the flag and start spinning on
                // the latch. startCheckpoint must not return while we hold it.
                Thread.sleep(50);
                Assert.assertTrue(
                        "freeze flag must be published before the agent blocks on the latch",
                        lv.isFreezeInProgress()
                );
                Assert.assertFalse(
                        "startCheckpoint must block while the refresh latch is held",
                        returned.get()
                );

                // Release the latch. startCheckpoint should return promptly.
                lv.unlockAfterRefresh();
                agent.join(5_000);
                Assert.assertFalse(
                        "startCheckpoint thread must have completed",
                        agent.isAlive()
                );
                Assert.assertTrue(
                        "startCheckpoint must return after the latch is released",
                        returned.get()
                );
            } finally {
                if (lv.isFreezeInProgress()) {
                    lv.endCheckpoint();
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFreezeQueuesInvalidationUntilEnd() throws Exception {
        // markInvalid blocks on the freeze. If a base-table schema change
        // happens mid-snapshot, invalidation is queued and applied right
        // after endCheckpoint. The snapshot reflects the pre-invalidation
        // state.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(lv);

            // Take the freeze. Now an out-of-band invalidate must wait.
            lv.startCheckpoint(lv.getStateReader().getAppliedWatermark());
            Assert.assertTrue(lv.isFreezeInProgress());
            Assert.assertFalse("LV must still be valid pre-freeze", lv.isInvalid());

            final AtomicBoolean returned = new AtomicBoolean(false);
            final Thread invalidator = new Thread(() -> {
                engine.invalidateLiveView(lv, "test queued behind freeze");
                returned.set(true);
            }, "lv-invalidate-freeze-test");
            try {
                invalidator.start();
                Thread.sleep(50);
                Assert.assertFalse(
                        "invalidateLiveView must wait until endCheckpoint",
                        returned.get()
                );
                Assert.assertFalse(
                        "LV must still be valid while frozen",
                        lv.isInvalid()
                );

                lv.endCheckpoint();
                invalidator.join(5_000);
                Assert.assertFalse(
                        "invalidate thread must have returned",
                        invalidator.isAlive()
                );
                Assert.assertTrue(
                        "invalidate must complete after endCheckpoint",
                        returned.get()
                );
                Assert.assertTrue(
                        "LV is invalid after endCheckpoint",
                        lv.isInvalid()
                );
                Assert.assertTrue(
                        "invalidation reason persisted",
                        Chars.equals("test queued behind freeze", lv.getStateReader().getInvalidationReason())
                );
            } finally {
                if (lv.isFreezeInProgress()) {
                    lv.endCheckpoint();
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFreezeGateSkipsRefreshTurn() throws Exception {
        // DatabaseCheckpointAgent toggles freezeInProgress around
        // its per-LV file copy so the refresh worker does not advance _lv.s /
        // the on-disk tier mid-snapshot. This test exercises the gate without
        // running the full agent: set startCheckpoint manually, drive a
        // refresh - the worker must skip and lastProcessedSeqTxn must not
        // advance. After endCheckpoint(), a subsequent refresh processes the
        // pending commit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                long beforeProcessed = lv.getLastProcessedSeqTxn();

                // Insert a row + ingest the WAL; refresh has NOT run yet.
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 1)");
                drainWalQueue();

                // Freeze the LV. The refresh worker's next turn must see the
                // flag, skip, and leave lastProcessedSeqTxn unchanged.
                lv.startCheckpoint(lv.getStateReader().getAppliedWatermark());
                Assert.assertTrue("freeze in progress after startCheckpoint", lv.isFreezeInProgress());
                drainJob(job);
                Assert.assertEquals(
                        "frozen refresh did not advance lastProcessedSeqTxn",
                        beforeProcessed,
                        lv.getLastProcessedSeqTxn()
                );

                // Unfreeze, drive a fresh refresh. The pending commit should
                // land in the LV.
                lv.endCheckpoint();
                Assert.assertFalse("freeze cleared after endCheckpoint", lv.isFreezeInProgress());
                setCurrentMicros(200_000L);
                drainJob(job);
                drainWalQueue();

                Assert.assertTrue(
                        "post-unfreeze refresh advanced lastProcessedSeqTxn",
                        lv.getLastProcessedSeqTxn() > beforeProcessed
                );
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testWorkerYieldsAtTurnBudget() throws Exception {
        // A single refresh turn is bounded by max commits and max duration
        // so a long backlog cannot monopolise the worker. With budget = 3
        // commits per turn and a five-commit gap to close in
        // scanForLaggingViews, the worker processes exactly three commits
        // then yields; the next FLUSH-EVERY pass drains the remainder.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_REFRESH_TURN_MAX_COMMITS, 3);
        assertMemoryLeak(() -> {
            setCurrentMicros(0L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            // Six separate base WAL commits - one INSERT per row so each
            // becomes its own sequencer txn for the refresh worker to walk.
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:01.000000Z', 1)");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:02.000000Z', 2)");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:03.000000Z', 3)");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:04.000000Z', 4)");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:05.000000Z', 5)");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:06.000000Z', 6)");
            drainWalQueue();

            LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(lv);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // First drainJob at clock = 0 processes the first queued
                // notification (seqTxn = 1) and then the FLUSH EVERY gate
                // freezes further work until the clock advances.
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "initial notification turn processes one commit",
                        1L,
                        lv.getLastProcessedSeqTxn()
                );
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");

                // Advance past FLUSH EVERY. scanForLaggingViews picks up the
                // five remaining commits in one refreshInstance call; budget = 3
                // forces a yield after the first three.
                setCurrentMicros(200_000L);
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "turn budget yields after processing exactly three more commits",
                        4L,
                        lv.getLastProcessedSeqTxn()
                );
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n4\n");

                // Advance again; the residual two commits fit inside the next turn.
                setCurrentMicros(400_000L);
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "final turn drains the residual two commits",
                        6L,
                        lv.getLastProcessedSeqTxn()
                );
                assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n6\n");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsCatalogueExposesHeadCheckpointColumns() throws Exception {
        // head_checkpoint_* columns are preallocated; values stay
        // at LONG_NULL / 0 until the flush-cycle write hook starts
        // populating them.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            try {
                assertQuery("SELECT view_name, head_checkpoint_lv_seqtxn, head_checkpoint_max_ts, head_checkpoint_state_bytes FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\thead_checkpoint_lv_seqtxn\thead_checkpoint_max_ts\thead_checkpoint_state_bytes\n" +
                                "lv\tnull\t\t0\n");

                // Direct mutation via the setter (the 2a.4 write hook will call this).
                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                lv.setHeadCheckpoint(42L, 1_700_000_000_000_000L, 4096L, 0L);

                assertQuery("SELECT view_name, head_checkpoint_lv_seqtxn, head_checkpoint_max_ts, head_checkpoint_state_bytes FROM live_views()").noLeakCheck().noRandomAccess().returns("view_name\thead_checkpoint_lv_seqtxn\thead_checkpoint_max_ts\thead_checkpoint_state_bytes\n" +
                                "lv\t42\t2023-11-14T22:13:20.000000Z\t4096\n");
            } finally {
                execute("DROP LIVE VIEW lv");
            }
        });
    }

    @Test
    public void testTablesIntegrationReportsLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            // tables() emits 'L' in the table_type column for live views.
            assertQuery("SELECT table_name, table_type FROM tables() WHERE table_name = 'lv'").noLeakCheck().noRandomAccess().returns("table_name\ttable_type\n" +
                            "lv\tL\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testShowCreateLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms IN MEMORY 5s PARTITION BY DAY AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            assertQuery("SHOW CREATE LIVE VIEW lv").noLeakCheck().noRandomAccess().returns("ddl\n" +
                            "CREATE LIVE VIEW 'lv' FLUSH EVERY 200ms IN MEMORY 5s PARTITION BY DAY AS (\n" +
                            "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0\n" +
                            ");\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectExplicitPartitionByNone() throws Exception {
        // Regression: the parser sentinel for "PARTITION BY omitted" used to be
        // PartitionBy.NONE — the same value the user-facing grammar produces for
        // explicit PARTITION BY NONE. LiveViewTableStructure.resolvePartitionBy
        // collapsed both into the base table's scheme, so a user asking for "no
        // partitioning on the LV" silently got the base's scheme instead.
        // Honouring the user's choice instead would fail downstream with the
        // generic "WAL is only supported for partitioned tables"; the LV's
        // WAL-backed on-disk tier requires a partition scheme. Reject up front
        // with an LV-specific message at parse time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY NONE AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
                Assert.fail("expected SqlException rejecting PARTITION BY NONE");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(),
                                "live view PARTITION BY NONE is not supported")
                );
            }
            // Confirm no partial-CREATE residue: re-creating with a valid scheme works.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectDuplicateBackfillClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s BACKFILL BACKFILL AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0";
            final int secondClausePos = sql.indexOf("BACKFILL", sql.indexOf("BACKFILL") + 1);
            try {
                execute(sql);
                Assert.fail("expected SqlException rejecting duplicate BACKFILL");
            } catch (SqlException e) {
                Assert.assertEquals(secondClausePos, e.getPosition());
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(),
                                "live view BACKFILL clause specified more than once")
                );
            }
        });
    }

    @Test
    public void testRejectDuplicateInMemoryClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 1s IN MEMORY 2s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0";
            final int secondClausePos = sql.indexOf("IN MEMORY", sql.indexOf("IN MEMORY") + 1);
            try {
                execute(sql);
                Assert.fail("expected SqlException rejecting duplicate IN MEMORY");
            } catch (SqlException e) {
                Assert.assertEquals(secondClausePos, e.getPosition());
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(),
                                "live view IN MEMORY clause specified more than once")
                );
            }
        });
    }

    @Test
    public void testRejectDuplicatePartitionByClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY DAY PARTITION BY HOUR AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0";
            final int secondClausePos = sql.indexOf("PARTITION BY", sql.indexOf("PARTITION BY") + 1);
            try {
                execute(sql);
                Assert.fail("expected SqlException rejecting duplicate PARTITION BY");
            } catch (SqlException e) {
                Assert.assertEquals(secondClausePos, e.getPosition());
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(),
                                "live view PARTITION BY clause specified more than once")
                );
            }
        });
    }

    @Test
    public void testShowCreateEmitsResolvedPartitionByForDefault() throws Exception {
        // When PARTITION BY is omitted, the LV inherits the base's scheme. SHOW
        // CREATE emits the resolved value (not a missing clause), so re-executing
        // the output produces an LV with the same partition scheme regardless of
        // any later change to the base.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals(
                    "omitted PARTITION BY must inherit base's DAY scheme",
                    io.questdb.cairo.PartitionBy.DAY,
                    instance.getDefinition().getPartitionBy()
            );

            assertQuery("SHOW CREATE LIVE VIEW lv").noLeakCheck().noRandomAccess().returns("ddl\n" +
                            "CREATE LIVE VIEW 'lv' FLUSH EVERY 1s IN MEMORY 1s PARTITION BY DAY AS (\n" +
                            "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0\n" +
                            ");\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAcceptAnchorExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // ANCHOR EXPRESSION on a default-frame WINDOW must parse without error.
            // The runtime that drives resetPartition lands with the window-function
            // migration; here we only verify CREATE accepts the syntax.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAcceptAnchorDaily() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '00:00')");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAcceptAnchorDailyWithTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '09:30' 'America/New_York')");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectAnchorWithBoundedFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ROWS 5 PRECEDING ANCHOR EXPRESSION timestamp_floor('1d', ts))");
                Assert.fail("expected ANCHOR + bounded frame reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("ANCHOR is incompatible with bounded frames"));
            }
        });
    }

    @Test
    public void testRejectConstantAnchorExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION 1)");
                Assert.fail("expected constant anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not be a constant"));
            }
        });
    }

    @Test
    public void testRejectInlineAnchorDaily() throws Exception {
        // Inline OVER (... ANCHOR DAILY ...) parses but the runtime AnchorSpec
        // is captured only from named WINDOW clauses, so an inline anchor would
        // silently never reset. The parser rejects it up front and points the
        // user at the named-window form.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, sym, row_number() OVER (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00') AS rn FROM base");
                Assert.fail("expected inline ANCHOR reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("ANCHOR is only supported on named WINDOW clauses")
                );
            }
        });
    }

    @Test
    public void testRejectInlineAnchorExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts)) AS s FROM base");
                Assert.fail("expected inline ANCHOR EXPRESSION reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("ANCHOR is only supported on named WINDOW clauses")
                );
            }
        });
    }

    @Test
    public void testRejectInlineAnchorNestedInArithmetic() throws Exception {
        // sum(x) OVER (... ANCHOR ...) + 1 — the inline OVER lives inside an
        // arithmetic tree rather than at the QueryColumn top level. The
        // recursive walk must reach it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00') + 1 AS s FROM base");
                Assert.fail("expected nested inline ANCHOR reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("ANCHOR is only supported on named WINDOW clauses")
                );
            }
        });
    }

    @Test
    public void testRejectMultipleAnchoredWindows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, sum(x) OVER w1 AS sx, sum(y) OVER w2 AS sy FROM base " +
                        "WINDOW w1 AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts)), " +
                        "       w2 AS (PARTITION BY y ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1h', ts))");
                Assert.fail("expected multi-anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("at most one anchored WINDOW"));
            }
        });
    }

    @Test
    public void testRejectAnchoredWindowWithoutPartitionBy() throws Exception {
        // An anchored named WINDOW with no PARTITION BY must be rejected at
        // CREATE. resetPartition is keyed on the partition; with no partition
        // column the anchor machinery (LiveViewWindow) cannot be built. Pre-fix
        // CREATE accepted the view and the build failure was swallowed at first
        // refresh, leaving the anchor reset permanently un-dispatched - e.g.
        // row_number() numbered for the view's lifetime instead of resetting
        // each day, silently and with no error surfaced to the user.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, row_number() OVER w AS rn FROM base " +
                        "WINDOW w AS (ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");
                Assert.fail("expected reject for anchored WINDOW without PARTITION BY");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("anchored WINDOW requires PARTITION BY"));
            }
        });
    }

    @Test
    public void testRejectAnchorWithSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION (SELECT 1))");
                Assert.fail("expected subquery anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not contain subqueries"));
            }
        });
    }

    @Test
    public void testRejectGeohashAnchorExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, lon DOUBLE, lat DOUBLE, sym SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, sym, lon, lat, sum(lon) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION make_geohash(lon, lat, 10))");
                Assert.fail("expected GEOHASH anchor return type reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("must return TIMESTAMP, LONG, or INT")
                );
            }
        });
    }

    @Test
    public void testRejectArrayAnchorExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, xs DOUBLE[], sym SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, sym, xs, count() OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION xs)");
                Assert.fail("expected ARRAY anchor return type reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("must return TIMESTAMP, LONG, or INT")
                );
            }
        });
    }

    @Test
    public void testRejectAnchorWithRandomFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION rnd_long())");
                Assert.fail("expected random anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("ANCHOR EXPRESSION must be deterministic; rnd_long() is not allowed")
                );
            }
        });
    }

    @Test
    public void testRejectAnchorWithBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION ts + $1)");
                Assert.fail("expected bind-variable anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("ANCHOR EXPRESSION must not reference bind variables")
                );
            }
        });
    }

    @Test
    public void testRejectAnchorWithNow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION now())");
                Assert.fail("expected now() anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must be deterministic"));
            }
        });
    }

    @Test
    public void testRejectAnchorWithNonLiteralPartitionBy() throws Exception {
        // V1 anchored windows require PARTITION BY to reference base columns
        // directly (literals). An expression in PARTITION BY (here x + 1)
        // cannot be reconstructed at refresh time from the persisted column
        // names, so CREATE rejects it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x + 1 ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");
                Assert.fail("expected non-literal PARTITION BY reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("ANCHOR currently requires PARTITION BY to reference base columns directly")
                );
            }
        });
    }

    @Test
    public void testRejectFoldToConstantAnchorExpression() throws Exception {
        // Pass 2 of the ANCHOR EXPRESSION validator: function calls whose arguments
        // are all constants fold to a constant at the top level. Pass 1 (parser AST)
        // only catches direct CONSTANT nodes (e.g. ANCHOR EXPRESSION 1); the fold case
        // needs the post-constant-fold Function tree.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION 1 + 2 + 3)");
                Assert.fail("expected fold-to-constant anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not be a constant"));
            }
        });
    }

    @Test
    public void testRejectAggregationAnchorExpression() throws Exception {
        // Pass 2 of the ANCHOR EXPRESSION validator: aggregates can't appear in an
        // anchor expression because anchor evaluation is scalar per-row. The compiled
        // Function is a GroupByFunction; the validator surfaces it with the asserted
        // "must not contain aggregation" wording.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION sum(x))");
                Assert.fail("expected aggregation anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not contain aggregation"));
            }
        });
    }

    @Test
    public void testRejectLeadWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, lead(x) OVER (ORDER BY ts) AS nxt FROM base");
                Assert.fail("expected lead() reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("lead() is not supported"));
            }
        });
    }

    @Test
    public void testRejectWindowOrderByNonTimestampColumn() throws Exception {
        // Each named WINDOW must ORDER BY the base's designated timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY x)");
                Assert.fail("expected non-timestamp ORDER BY reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must ORDER BY ts"));
            }
        });
    }

    @Test
    public void testRejectWindowOrderByDescending() throws Exception {
        // ORDER BY direction must be ascending; DESC violates the WAL-row-order
        // processing model.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts DESC)");
                Assert.fail("expected ORDER BY DESC reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must ORDER BY ts ASC"));
            }
        });
    }

    @Test
    public void testRejectWindowOrderByMissing() throws Exception {
        // A named WINDOW without any ORDER BY can't be ordered by the
        // designated ts and must be rejected.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x)");
                Assert.fail("expected missing ORDER BY reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must ORDER BY ts"));
            }
        });
    }

    @Test
    public void testRejectWindowOrderByMultipleColumns() throws Exception {
        // ORDER BY must be a single column (the designated timestamp);
        // multi-column ordering doesn't have a meaningful WAL-stream semantics.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts, x)");
                Assert.fail("expected multi-column ORDER BY reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must ORDER BY a single column"));
            }
        });
    }

    @Test
    public void testRequireFlushEvery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv AS SELECT ts, x FROM base");
                Assert.fail("expected FLUSH EVERY required");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("flush every"));
            }
        });
    }

    @Test
    public void testFastPathAppendsToSamePublishedSlot() throws Exception {
        // When no reader pins the published slot and the
        // slot's footprint is under the growth budget, the refresh worker
        // appends staging rows in place via the {@code 0 -> -1} CAS protocol
        // and releases without flipping {@code publishedIdx}. Three
        // back-to-back cycles must therefore land on the same slot index and
        // accumulate rows linearly.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1h AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000001Z', 1)");
                drainWalQueue();
                drainJob(job);
                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                int initialIdx = tier.getPublishedIdx();

                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000002Z', 2)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("fast-path must not flip publishedIdx (cycle 2)",
                        initialIdx, tier.getPublishedIdx());

                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000003Z', 3)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("fast-path must not flip publishedIdx (cycle 3)",
                        initialIdx, tier.getPublishedIdx());

                LiveViewInMemoryBuffer published = tier.getSlot(initialIdx);
                Assert.assertEquals("fast-path accumulated three rows in the same slot",
                        3, published.rowCount());
                Assert.assertEquals(1, published.getInt(0, 1));
                Assert.assertEquals(2, published.getInt(1, 1));
                Assert.assertEquals(3, published.getInt(2, 1));
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFastPathFallsBackToSlowPathWhenReaderPinned() throws Exception {
        // A reader pin on the published slot makes the
        // fast-path CAS {@code 0 -> -1} fail (rc > 0). The refresh worker
        // falls through to the slow-path swap, taking the non-published
        // slot, copying retained rows, appending staging, and flipping
        // {@code publishedIdx}. The reader's snapshot on the old slot
        // remains intact.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1h AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000001Z', 1)");
                drainWalQueue();
                drainJob(job);
                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                int initialIdx = tier.getPublishedIdx();

                // Pin the published slot — the next cycle's fast-path CAS
                // must fail and slow-path must engage.
                int pin = tier.acquireRead();
                Assert.assertEquals("pin lands on the currently-published slot",
                        initialIdx, pin);
                try {
                    setCurrentMicros(200_000L);
                    execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000002Z', 2)");
                    drainWalQueue();
                    drainJob(job);

                    Assert.assertNotEquals(
                            "slow-path must flip publishedIdx when fast-path is blocked",
                            initialIdx, tier.getPublishedIdx()
                    );
                    LiveViewInMemoryBuffer pinned = tier.getSlot(pin);
                    Assert.assertEquals("reader's pinned slot retains its single row",
                            1, pinned.rowCount());
                    LiveViewInMemoryBuffer published = tier.getSlot(tier.getPublishedIdx());
                    Assert.assertEquals("new published slot holds retained + staging",
                            2, published.rowCount());
                } finally {
                    tier.releaseRead(pin);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testGrowthThresholdForcesSwap() throws Exception {
        // When the published slot's footprint already
        // meets or exceeds {@code cairo.live.view.in.memory.buffer.growth.bytes},
        // the refresh worker skips the fast-path acquire and goes directly to
        // the slow-path swap. The growth budget acts as a backstop against
        // unbounded in-place growth between slow-path edges.
        // Setting the threshold to 0 forces the very first fast-path check
        // to fail (footprint 0 < 0 is false), so cycle 2 takes the slow path
        // and publishedIdx flips. (Cycle 1 also takes slow path under
        // growth=0; the assertion is that cycle 2's publishedIdx differs
        // from cycle 1's.)
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1h AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000001Z', 1)");
                drainWalQueue();
                drainJob(job);
                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                int firstIdx = tier.getPublishedIdx();

                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000002Z', 2)");
                drainWalQueue();
                drainJob(job);
                Assert.assertNotEquals(
                        "growth=0 forces slow-path swap on every cycle",
                        firstIdx, tier.getPublishedIdx()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInMemTierSkipsAllocationForUnsupportedColumnTypes() throws Exception {
        // An LV whose output schema contains a column type
        // the fixed-width in-mem tier cannot store (STRING / VARCHAR /
        // BINARY / ARRAY) skips tier allocation entirely. The cursor stays
        // disk-only with no pin, and reads pass through unchanged. The
        // seam_ts routing scaffolding in LiveViewRecordCursor exits via the
        // {@code pinnedSlot == null} branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, s, row_number() OVER () AS rn FROM base WHERE x > 0");

            execute("INSERT INTO base (ts, x, s) VALUES ('2026-05-12T00:00:00.000001Z', 1, 'a')");
            drainWalQueue();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            Assert.assertNull(
                    "in-mem tier must not be allocated for an LV with a STRING output column",
                    instance.getInMemoryTier()
            );
            // Disk-only cursor must return the inserted row through SELECT.
            assertQuery("SELECT x, s, rn FROM lv ORDER BY ts").noLeakCheck().expectSize().returns("x\ts\trn\n1\ta\t1\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectDateTruncFoldedConstantAnchor() throws Exception {
        // A deeper constant fold than the 1+2+3 arithmetic case: every arg of
        // the top-level call is itself a constant, so the function parser
        // collapses the whole tree to a single ConstantFunction and the
        // existing top-level isConstant() check fires.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts " +
                        "ANCHOR EXPRESSION date_trunc('day', '2025-01-01'::timestamp))");
                Assert.fail("expected folded-constant anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not be a constant"));
            }
        });
    }

    @Test
    public void testRejectFlushEveryZero() throws Exception {
        // FLUSH EVERY 0 is rejected
        // alongside any value below 100ms - no row would ever durably reach
        // disk. The implementation collapses both branches into one message;
        // this test pins that the zero case also fires the same wording so a
        // future refactor that splits the branches keeps both visible.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 0s AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected FLUSH EVERY 0 reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("FLUSH EVERY must be at least 100ms"));
            }
        });
    }

    @Test
    public void testRejectLengthFoldedConstantAnchor() throws Exception {
        // Pass-2 fold case explicitly handled alongside 1+2+3 and
        // date_trunc-of-literal: length('hello') folds to the constant 5
        // before the validator walks it, so the top-level isConstant() check
        // is the surface that fires the reject. Position must land on the
        // ANCHOR keyword in the user's CREATE SQL - validateAnchorPurity walks
        // a re-parsed expression tree, so the rootPosition has to come from
        // the captured LvAnchorSpec rather than the re-parse.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION length('hello'))";
            try {
                execute(sql);
                Assert.fail("expected length-folded-constant anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not be a constant"));
                Assert.assertEquals(
                        "position must point at the ANCHOR keyword in CREATE SQL",
                        sql.indexOf("ANCHOR"),
                        e.getPosition()
                );
            }
        });
    }

    @Test
    public void testRejectAnchorWithSystimestamp() throws Exception {
        // systimestamp() is the second runtime-state function named
        // alongside now() / current_timestamp; the validator surfaces it via
        // the same isRuntimeConstant branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION systimestamp())");
                Assert.fail("expected systimestamp() anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must be deterministic"));
            }
        });
    }

    @Test
    public void testRejectSymbolAnchorReturnType() throws Exception {
        // ANCHOR EXPRESSION must return
        // TIMESTAMP, LONG, or INT. SYMBOL is rejected because base_id vs.
        // lv_id translation lands after window evaluation - equality
        // semantics on the raw base symbol id don't compose with the runtime
        // symbol-table maintenance. The return-type check is strictly
        // Pass-2 (parser-side Pass-1 walks tokens and AST shapes only), so
        // this case is the cleanest place to pin the CREATE-SQL position.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION sym)";
            try {
                execute(sql);
                Assert.fail("expected SYMBOL anchor return type reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("must return TIMESTAMP, LONG, or INT")
                );
                Assert.assertEquals(
                        "position must point at the ANCHOR keyword in CREATE SQL",
                        sql.indexOf("ANCHOR"),
                        e.getPosition()
                );
            }
        });
    }

    @Test
    public void testRejectStringAnchorReturnType() throws Exception {
        // STRING anchor would route through the var-length codec on snapshot
        // / restore; deferred to a future revision once the codec lands.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, s STRING, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, s, x, sum(x) OVER w AS sm FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION s)");
                Assert.fail("expected STRING anchor return type reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("must return TIMESTAMP, LONG, or INT")
                );
            }
        });
    }

    @Test
    public void testRejectBooleanAnchorReturnType() throws Exception {
        // BOOLEAN anchors collapse to a two-bucket reset cadence; deliberately
        // out of scope for V1 since the equality semantics on the encoded
        // anchor value need an explicit choice.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, flag BOOLEAN, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, flag, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION flag)");
                Assert.fail("expected BOOLEAN anchor return type reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("must return TIMESTAMP, LONG, or INT")
                );
            }
        });
    }

    @Test
    public void testRejectDoubleAnchorReturnType() throws Exception {
        // DOUBLE anchors carry NaN-zero equivalence hazards that did not
        // clear the cost / value bar for V1 inclusion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, d DOUBLE, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, d, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION d)");
                Assert.fail("expected DOUBLE anchor return type reject");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("must return TIMESTAMP, LONG, or INT")
                );
            }
        });
    }

    @Test
    public void testDailyUtcMidnightDesugarsToTimestampFloor() throws Exception {
        // ANCHOR DAILY '00:00' 'UTC' produces the same buckets as the
        // unqualified ANCHOR DAILY '00:00' since a UTC tz at zero offset
        // contributes nothing. The desugarer must collapse the two forms
        // into the same timestamp_floor('1d', ts) expression so neither
        // pins a needless timestamp_floor_utc call on the hot anchor path
        // or persists a heavier expression than necessary.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv_utc FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '00:00' 'UTC')");
            execute("CREATE LIVE VIEW lv_no_tz FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '00:00')");
            execute("CREATE LIVE VIEW lv_london FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '00:00' 'Europe/London')");

            LiveViewInstance utc = engine.getLiveViewRegistry().getViewInstance("lv_utc");
            LiveViewInstance noTz = engine.getLiveViewRegistry().getViewInstance("lv_no_tz");
            LiveViewInstance london = engine.getLiveViewRegistry().getViewInstance("lv_london");
            Assert.assertNotNull(utc);
            Assert.assertNotNull(noTz);
            Assert.assertNotNull(london);
            Assert.assertEquals(
                    "DAILY '00:00' 'UTC' must collapse to the no-tz form",
                    "timestamp_floor('1d', ts)",
                    utc.getDefinition().getAnchorSpec().anchorExpressionSql
            );
            Assert.assertEquals(
                    "DAILY '00:00' (no tz) must also produce the no-tz form",
                    "timestamp_floor('1d', ts)",
                    noTz.getDefinition().getAnchorSpec().anchorExpressionSql
            );
            // Non-UTC tz keeps the timestamp_floor_utc form. The collapse
            // applies only to the UTC special case where the tz argument
            // contributes no offset.
            Assert.assertEquals(
                    "DAILY '00:00' 'Europe/London' must keep the tz-aware form",
                    "timestamp_floor_utc('1d', ts, '1970-01-01T00:00:00.000000Z'::timestamp, '+00:00', 'Europe/London')",
                    london.getDefinition().getAnchorSpec().anchorExpressionSql
            );
            execute("DROP LIVE VIEW lv_utc");
            execute("DROP LIVE VIEW lv_no_tz");
            execute("DROP LIVE VIEW lv_london");
        });
    }

    @Test
    public void testDailyNonMidnightNoTzDesugarsWithEpochOffset() throws Exception {
        // ANCHOR DAILY '09:30' without a tz desugars to a timestamp_floor whose
        // epoch-reference argument carries the 09:30 wall-clock offset - NOT a
        // timestamp_floor_utc (there is no tz to resolve). The midnight no-tz
        // case collapses the offset away; this pins the non-midnight branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '09:30')");
            LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(lv);
            Assert.assertEquals(
                    "DAILY '09:30' (no tz) must desugar to a timestamp_floor with a 09:30 epoch offset",
                    "timestamp_floor('1d', ts, '1970-01-01T09:30:00.000000Z'::timestamp)",
                    lv.getDefinition().getAnchorSpec().anchorExpressionSql
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testMaterializedViewAcceptedAsBase() throws Exception {
        // A materialized view is a valid live-view base (WAL-backed,
        // designated timestamp). The live-on-live reject must NOT fire for it.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mv AS (" +
                    "SELECT ts, sym, avg(px) AS ap FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalQueue();

            // Live view over the mat view - must be accepted.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, row_number() OVER w AS rn FROM mv " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            Assert.assertNotNull(
                    "live view over a materialized view must be created",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );

            execute("DROP LIVE VIEW lv");
            execute("DROP MATERIALIZED VIEW mv");
        });
    }

    @Test
    public void testShowCreateRoundTripsDailyUtc() throws Exception {
        // ANCHOR DAILY '00:00' 'UTC' and the
        // unqualified ANCHOR DAILY '00:00' desugar to the same expression
        // (timestamp_floor('1d', ts)). SHOW CREATE must round-trip the
        // user-typed clause faithfully so re-executing the emitted DDL
        // reproduces the original definition.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '00:00' 'UTC')");
            assertQuery("SHOW CREATE LIVE VIEW lv").noLeakCheck().noRandomAccess().returns("ddl\n" +
                            "CREATE LIVE VIEW 'lv' FLUSH EVERY 1s IN MEMORY 1s PARTITION BY DAY AS (\n" +
                            "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                            "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '00:00' 'UTC')\n" +
                            ");\n");
            execute("DROP LIVE VIEW lv");
        });
    }
}
