/*******************************************************************************
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Pins the contract of {@link CairoEngine#notifyLiveViewAlterApplying}: the WAL apply hands every
 * non-structural {@code ALTER LIVE VIEW} to the engine right before it runs, keyed by the view's own
 * seqTxn, with its partition selector already resolved against this node's reader. Enterprise relays
 * the call to replicas, which compute their live views locally and never see this WAL; a
 * {@code WHERE} clause must therefore arrive as the concrete partition set it matched here, not as
 * the predicate text.
 */
public class LiveViewAlterApplyHookTest extends AbstractLiveViewTest {
    private static final ObjList<CapturedAlter> CAPTURED = new ObjList<>();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = configuration -> new HookedEngine(configuration);
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
        CAPTURED.clear();
    }

    @Test
    public void testConvertPartitionCarriesResolvedListAndParquetOptions() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1), ('1970-01-02T00:00:00.000000Z', 2)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO PARQUET LIST '1970-01-01' WITH (bloom_filter_columns = 'x', fpp = '0.05')");
                driveLiveViewWalApply(job);

                Assert.assertEquals(1, CAPTURED.size());
                final CapturedAlter captured = CAPTURED.getQuick(0);
                Assert.assertEquals(AlterOperation.CONVERT_PARTITION_TO_PARQUET, captured.command);
                Assert.assertEquals("x", captured.extraStrInfo.getQuick(0));
                // (ts, position) for the one partition, then the fpp bits.
                Assert.assertEquals(3, captured.extraInfo.size());
                Assert.assertEquals(day("1970-01-01"), captured.extraInfo.getQuick(0));
                Assert.assertEquals(0.05, Double.longBitsToDouble(captured.extraInfo.getQuick(2)), 0.0);

                execute("ALTER LIVE VIEW lv CONVERT PARTITION TO NATIVE WHERE ts < '1970-01-02'");
                driveLiveViewWalApply(job);
                Assert.assertEquals(2, CAPTURED.size());
                final CapturedAlter back = CAPTURED.getQuick(1);
                Assert.assertEquals(AlterOperation.CONVERT_PARTITION_TO_NATIVE, back.command);
                Assert.assertEquals(0, back.extraStrInfo.size());
                Assert.assertEquals(2, back.extraInfo.size());
                Assert.assertEquals(day("1970-01-01"), back.extraInfo.getQuick(0));
                Assert.assertTrue("seqTxns must follow WAL order", back.seqTxn > captured.seqTxn);
            }
        });
    }

    @Test
    public void testDropPartitionWhereArrivesAsResolvedPartitionTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("INSERT INTO base VALUES " +
                    "('1970-01-01T00:00:00.000000Z', 1), " +
                    "('1970-01-02T00:00:00.000000Z', 2), " +
                    "('1970-01-03T00:00:00.000000Z', 3)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                Assert.assertEquals("a flush is not an ALTER", 0, CAPTURED.size());

                execute("ALTER LIVE VIEW lv DROP PARTITION WHERE ts < '1970-01-03'");
                driveLiveViewWalApply(job);

                Assert.assertEquals(1, CAPTURED.size());
                final CapturedAlter captured = CAPTURED.getQuick(0);
                Assert.assertEquals("lv", captured.viewName);
                Assert.assertEquals(AlterOperation.DROP_PARTITION, captured.command);
                Assert.assertTrue(captured.seqTxn > 0);
                // Two (timestamp, position) pairs: the predicate resolved against the reader. The
                // compiler visits interior partitions before the first and last, so compare as a set.
                Assert.assertEquals(4, captured.extraInfo.size());
                final LongList resolved = new LongList();
                resolved.add(captured.extraInfo.getQuick(0));
                resolved.add(captured.extraInfo.getQuick(2));
                resolved.sort();
                Assert.assertEquals(day("1970-01-01"), resolved.getQuick(0));
                Assert.assertEquals(day("1970-01-02"), resolved.getQuick(1));
                Assert.assertEquals("the hook fires before the apply", 3, captured.partitionCountAtHook);

                assertQuery("SELECT name FROM table_partitions('lv') ORDER BY name")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                name
                                1970-01-03
                                """);
            }
        });
    }

    @Test
    public void testSetTtlCarriesEncodedTtl() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("INSERT INTO base VALUES ('1970-01-01T00:00:00.000000Z', 1)");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                execute("ALTER LIVE VIEW lv SET TTL 2 DAYS");
                driveLiveViewWalApply(job);
                execute("ALTER LIVE VIEW lv SET TTL 3 MONTHS");
                driveLiveViewWalApply(job);
                execute("ALTER LIVE VIEW lv SET TTL 0 HOURS");
                driveLiveViewWalApply(job);

                Assert.assertEquals(3, CAPTURED.size());
                Assert.assertEquals(AlterOperation.SET_TTL, CAPTURED.getQuick(0).command);
                Assert.assertEquals(48, CAPTURED.getQuick(0).extraInfo.getQuick(0));
                Assert.assertEquals(-3, CAPTURED.getQuick(1).extraInfo.getQuick(0));
                Assert.assertEquals(0, CAPTURED.getQuick(2).extraInfo.getQuick(0));
                Assert.assertTrue(CAPTURED.getQuick(0).seqTxn < CAPTURED.getQuick(1).seqTxn);
                Assert.assertTrue(CAPTURED.getQuick(1).seqTxn < CAPTURED.getQuick(2).seqTxn);
            }
        });
    }

    private static long day(String date) throws Exception {
        return MicrosFormatUtils.parseUTCTimestamp(date + "T00:00:00.000000Z");
    }

    private void createBaseAndView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY DAY START FROM NOW AS " +
                "(SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW) AS rn FROM base)");
    }

    private void driveLiveViewWalApply(LiveViewRefreshJob job) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        for (int i = 0; i < REFRESH_QUIESCENCE_PASSES; i++) {
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(instance.getLiveViewToken());
            if (tracker.isInitialised() && tracker.getWriterTxn() >= tracker.getSeqTxn()) {
                return;
            }
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            drainJob(job);
            drainWalQueue();
        }
        Assert.fail("live view WAL was not fully applied within " + REFRESH_QUIESCENCE_PASSES + " passes");
    }

    private static final class CapturedAlter {
        final short command;
        final LongList extraInfo = new LongList();
        final ObjList<String> extraStrInfo = new ObjList<>();
        final int partitionCountAtHook;
        final long seqTxn;
        final String viewName;

        CapturedAlter(TableWriter writer, long seqTxn, AlterOperation alterOp) {
            this.viewName = writer.getTableToken().getTableName();
            this.seqTxn = seqTxn;
            this.command = alterOp.getCommand();
            this.extraInfo.addAll(alterOp.getExtraInfo());
            for (int i = 0, n = alterOp.getExtraStrInfoSize(); i < n; i++) {
                final CharSequence str = alterOp.getExtraStrInfo(i);
                this.extraStrInfo.add(str == null ? null : str.toString());
            }
            this.partitionCountAtHook = writer.getPartitionCount();
        }
    }

    private static final class HookedEngine extends CairoEngine {
        HookedEngine(CairoConfiguration configuration) {
            super(configuration);
        }

        @Override
        public void notifyLiveViewAlterApplying(TableWriter writer, long seqTxn, AlterOperation alterOp) {
            final TableToken token = writer.getTableToken();
            Assert.assertTrue("the hook is a live view hook", token.isLiveView());
            CAPTURED.add(new CapturedAlter(writer, seqTxn, alterOp));
        }
    }
}
