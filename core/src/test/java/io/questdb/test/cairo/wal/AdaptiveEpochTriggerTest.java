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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultDdlListener;
import io.questdb.cairo.DefaultLifecycleManager;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cutlass.line.udp.DefaultLineUdpReceiverConfiguration;
import io.questdb.cutlass.line.udp.LineUdpLexer;
import io.questdb.cutlass.line.udp.LineUdpParserImpl;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class AdaptiveEpochTriggerTest extends AbstractCairoTest {
    // Every epoch writes a manifest; counting its opens counts the attempts.
    private static final FilesFacade FF = new TestFilesFacadeImpl() {
        @Override
        public long openRW(LPSZ name, int opts) {
            if (Utf8s.containsAscii(name, DurableEpochManifest.FILE_NAME)) {
                epochAttempts.incrementAndGet();
                if (failEpochs) {
                    return -1;
                }
            }
            return super.openRW(name, opts);
        }
    };
    private static final long UDP_INTERVAL_MS = 60_000L;
    // 2024-01-01T00:00:00Z
    private static final long UDP_T0_MICROS = 1_704_067_200_000_000L;
    private static final AtomicInteger epochAttempts = new AtomicInteger();
    private static volatile boolean failEpochs;

    @Test
    public void testAdaptiveEpochConfigDefaults() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(60000L, engine.getConfiguration().getAdaptiveEpochIntervalMs());
            Assert.assertEquals(5_000_000L, engine.getConfiguration().getAdaptiveEpochMaxRows());
        });
    }

    // Long interval so, after the mandatory first-batch epoch, ONLY the row cap can fire within a
    // sub-second test — independent of wall-clock advancement.
    private static final int LONG_INTERVAL_MS = 3_600_000;

    @Test
    public void testBacklogCapForcesEpoch() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 500);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floorAfterFirst = durableEpoch("x");
            Assert.assertTrue("first-batch epoch should advance the floor", floorAfterFirst > 0);

            // 600 rows (> cap 500) with an effectively infinite interval -> only the cap can fire.
            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(600)");
            drainWalQueue();
            long floorAfterCap = durableEpoch("x");
            Assert.assertTrue(
                    "backlog cap should force an epoch: floorAfterCap=" + floorAfterCap
                            + " must exceed floorAfterFirst=" + floorAfterFirst,
                    floorAfterCap > floorAfterFirst
            );
        });
    }

    @Test
    public void testCapDisabledNoEpochOnBacklog() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0); // cap disabled
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floorAfterFirst = durableEpoch("x");

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(600)");
            drainWalQueue();
            long floorAfterBacklog = durableEpoch("x");
            Assert.assertEquals(
                    "cap disabled + long interval must not fire a second epoch",
                    floorAfterFirst, floorAfterBacklog
            );
        });
    }

    @Test
    public void testBacklogCounterResetsAfterEpoch() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 500);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floor1 = durableEpoch("x");

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(600)");
            drainWalQueue();
            long floor2 = durableEpoch("x");
            Assert.assertTrue("cap should fire epoch #2", floor2 > floor1);

            // 300 < cap, counter reset after epoch #2 -> no epoch (else cumulative 600+300 would fire).
            execute("insert into x select timestamp_sequence(200000000000L, 1000000L), x from long_sequence(300)");
            drainWalQueue();
            long floor3 = durableEpoch("x");
            Assert.assertEquals("counter reset: 300 < cap must not fire", floor2, floor3);

            // 300 more -> 600 since epoch #2 >= cap -> epoch #3.
            execute("insert into x select timestamp_sequence(300000000000L, 1000000L), x from long_sequence(300)");
            drainWalQueue();
            long floor4 = durableEpoch("x");
            Assert.assertTrue("crossing the cap again should fire epoch #3", floor4 > floor2);
        });
    }

    @Test
    public void testTimePathStillFiresWhenCapHuge() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0); // every batch (time path)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 1_000_000_000L); // cap never hit
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floor1 = durableEpoch("x");

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            long floor2 = durableEpoch("x");
            Assert.assertTrue("interval=0 must still epoch every batch (time path intact)", floor2 > floor1);
        });
    }

    // A graceful writer close must fold any committed-but-un-epoched tail into a final durable epoch,
    // so a clean restart lands on the committed frontier with nothing to roll forward. Long interval +
    // disabled cap means only the mandatory first-batch epoch fires; a second applied batch is left as
    // an un-epoched tail, and closing the writer must catch the durable epoch up to seqTxn.
    @Test
    public void testGracefulCloseFlushesUnEpochedTail() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS); // no time-based re-fire
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);                // no cap-based re-fire
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            Assert.assertTrue("first-batch epoch should advance the floor", durableEpoch("x") > 0);

            // A second applied batch with no time/cap trigger leaves a committed-but-un-epoched tail.
            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            final long frontier = committedSeqTxn("x");
            Assert.assertTrue(
                    "precondition: an un-epoched tail must exist (durableEpoch " + durableEpoch("x")
                            + " < seqTxn " + frontier + ")",
                    durableEpoch("x") < frontier
            );

            // Clean writer close -> doClose flushes a final durable epoch over the tail.
            engine.releaseInactive();

            Assert.assertEquals(
                    "graceful close must advance the durable epoch to the committed frontier",
                    frontier, durableEpoch("x")
            );
        });
    }

    // cairo.adaptive.epoch.flush.on.close=false is the operator kill-switch: a clean close must NOT
    // flush, leaving the tail for the next boot's WAL replay (durableEpoch stays behind the frontier).
    @Test
    public void testEpochFlushOnCloseDisabledLeavesTail() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, LONG_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x select timestamp_sequence(0, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            final long floorAfterFirst = durableEpoch("x");
            Assert.assertTrue("first-batch epoch should advance the floor", floorAfterFirst > 0);

            execute("insert into x select timestamp_sequence(100000000000L, 1000000L), x from long_sequence(10)");
            drainWalQueue();
            final long frontier = committedSeqTxn("x");
            Assert.assertTrue("precondition: an un-epoched tail must exist", durableEpoch("x") < frontier);

            engine.releaseInactive();

            Assert.assertEquals(
                    "flush-on-close disabled: a clean close must NOT advance the durable epoch",
                    floorAfterFirst, durableEpoch("x")
            );
        });
    }

    // The UDP commit loop over the cached writers serves only enrolled adaptive tables: outside ADAPTIVE no tick,
    // batch or idle, visits a writer, WAL table or not; in ADAPTIVE every tick visits each one, a quiet one included.
    @Test
    public void testUdpLoopVisitsCachedWritersOnlyInAdaptive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (ts timestamp, v long) timestamp(ts) partition by day bypass wal");
            execute("create table b (ts timestamp, v long) timestamp(ts) partition by day wal");
            engine.releaseAllWriters();
            engine.releaseInactive();
            final AtomicInteger visits = new AtomicInteger();
            try (CairoEngine e = new CairoEngine(configuration) {
                @Override
                public TableWriter getWriter(TableToken tableToken, @NotNull String lockReason) {
                    final CairoConfiguration conf = getConfiguration();
                    return new TableWriter(
                            conf,
                            tableToken,
                            getMessageBus(),
                            null,
                            true,
                            DefaultLifecycleManager.INSTANCE,
                            conf.getDbRoot(),
                            DefaultDdlListener.INSTANCE,
                            this
                    ) {
                        @Override
                        public void advanceDurableEpochAtInterval(CharSequence operation) {
                            visits.incrementAndGet();
                            super.advanceDurableEpochAtInterval(operation);
                        }
                    };
                }
            }) {
                final long nanos = UDP_T0_MICROS * 1000L;
                for (String mode : new String[]{"nosync", "async", "sync", "adaptive"}) {
                    setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
                    visits.set(0);
                    try (LineUdpParserImpl parser = new LineUdpParserImpl(e, new DefaultLineUdpReceiverConfiguration())) {
                        udpParse(parser, "a v=1i " + nanos + "\nb v=1i " + nanos + "\n");
                        // One tick commits the batch, three idle ticks follow.
                        for (int tick = 0; tick < 4; tick++) {
                            parser.commitAll();
                        }
                    }
                    Assert.assertEquals(mode, "adaptive".equals(mode) ? 8 : 0, visits.get());
                }
            }
        });
    }

    // A receiver that writes several tables commits only the tables a tick wrote, yet every table it holds takes the
    // epoch its UDP rows owe within one interval: x and z go quiet while y stays busy, z writes again in between, and
    // y, the last table written until x writes again, goes quiet while the receiver idles. No table takes more than
    // one epoch per interval, and a crash at the end loses no UDP row.
    @Test
    public void testUdpQuietTablesTakeEpochWithinInterval() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, UDP_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);
        setCurrentMicros(UDP_T0_MICROS);
        assertMemoryLeak(() -> {
            final String[] tables = {"x", "y", "z"};
            final SeqTxnTracker[] trackers = new SeqTxnTracker[tables.length];
            final long[] lastEpochTs = new long[tables.length];
            final LongList[] epochSeconds = new LongList[tables.length];
            final int[] owedSince = new int[tables.length];
            final String[] beforeCrash = new String[tables.length];
            for (String table : tables) {
                execute("create table " + table + " (ts timestamp, s symbol, v long) timestamp(ts) partition by day wal");
                execute("insert into " + table + " values ('2024-01-01', 'w', 0)");
            }
            drainWalQueue();
            for (int t = 0; t < tables.length; t++) {
                trackers[t] = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName(tables[t]));
                lastEpochTs[t] = trackers[t].getLastEpochTs();
                epochSeconds[t] = new LongList();
            }
            try (LineUdpParserImpl parser = new LineUdpParserImpl(engine, new DefaultLineUdpReceiverConfiguration())) {
                for (int s = 1; s <= 300; s++) {
                    final long micros = UDP_T0_MICROS + s * 1_000_000L;
                    setCurrentMicros(micros);
                    final boolean[] writes = {s == 1 || s == 150, s <= 130, s == 1 || s == 100};
                    final StringBuilder lines = new StringBuilder();
                    for (int t = 0; t < tables.length; t++) {
                        if (writes[t]) {
                            lines.append(tables[t]).append(",s=u v=").append(s).append("i ").append(micros * 1000L).append('\n');
                        }
                    }
                    if (!lines.isEmpty()) {
                        udpParse(parser, lines.toString());
                    }
                    parser.commitAll();
                    for (int t = 0; t < tables.length; t++) {
                        if (!trackers[t].isDurableEpochOwed()) {
                            owedSince[t] = 0;
                        } else if (owedSince[t] == 0) {
                            owedSince[t] = s;
                        }
                        Assert.assertTrue(
                                tables[t] + " still owes an epoch " + (s - owedSince[t]) + " s after its UDP rows",
                                owedSince[t] == 0 || (s - owedSince[t]) * 1000L < UDP_INTERVAL_MS
                        );
                        if (trackers[t].getLastEpochTs() != lastEpochTs[t]) {
                            lastEpochTs[t] = trackers[t].getLastEpochTs();
                            epochSeconds[t].add(s);
                        }
                    }
                }
                for (int t = 0; t < tables.length; t++) {
                    beforeCrash[t] = rows(engine, tables[t]);
                }
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
            }
            Assert.assertEquals("[60,150]", epochSeconds[0].toString());
            Assert.assertEquals("[60,120,180]", epochSeconds[1].toString());
            Assert.assertEquals("[60,120]", epochSeconds[2].toString());
            try (CairoEngine restarted = restart(engine)) {
                TestUtils.drainWalQueue(restarted);
                for (int t = 0; t < tables.length; t++) {
                    Assert.assertEquals(tables[t] + " lost UDP rows in the crash", beforeCrash[t], rows(restarted, tables[t]));
                }
            }
        });
    }

    // UDP rows bypass the WAL, so only a durable epoch covers them. A clean restart keeps every one of them; a
    // crash loses at most the rows since the last epoch, and none older. Each batch ends with a line that adds a
    // column and is then rejected, so its rows are committed by the column add, not by commitAll().
    @Test
    public void testUdpRowsCleanRestartAndCrash() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, UDP_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);
        setCurrentMicros(UDP_T0_MICROS);
        assertMemoryLeak(FF, () -> {
            CairoEngine e = engine;
            try {
                e.execute("create table x (ts timestamp, s symbol, v long) timestamp(ts) partition by day wal");
                e.execute("insert into x values ('2024-01-01', 'w', 0)");
                TestUtils.drainWalQueue(e);
                // Inside the interval of the apply batch's epoch: the batch owes one, which the clean close takes.
                setCurrentMicros(UDP_T0_MICROS + 1_000_000L);
                udpBatch(e, "u1", "c1");
                final String firstBatch = rows(e, "x");
                e = restart(e);
                Assert.assertEquals(firstBatch, rows(e, "x"));

                // The interval has passed, so the batch takes an epoch.
                setCurrentMicros(UDP_T0_MICROS + UDP_INTERVAL_MS * 1000L + 2_000_000L);
                udpBatch(e, "u2", "c2");
                final String covered = rows(e, "x");
                // Inside the interval again: a crash loses this batch and nothing older.
                setCurrentMicros(UDP_T0_MICROS + UDP_INTERVAL_MS * 1000L + 3_000_000L);
                udpBatch(e, "u3", "c3");
                Assert.assertNotEquals(covered, rows(e, "x"));
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
                e = restart(e);
                TestUtils.drainWalQueue(e);
                Assert.assertEquals(covered, rows(e, "x"));
            } finally {
                if (e != engine) {
                    e.close();
                }
            }
        });
    }

    // A trickle of UDP batches, one a second, takes at most one epoch per interval; a failed epoch is retried an
    // interval later, not on every batch, and an idle tick takes the epoch the last batches owe once the interval
    // has passed.
    @Test
    public void testUdpTrickleTakesOneEpochPerInterval() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, UDP_INTERVAL_MS);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, 0);
        setCurrentMicros(UDP_T0_MICROS);
        assertMemoryLeak(FF, () -> {
            execute("create table x (ts timestamp, s symbol, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01', 'w', 0)");
            drainWalQueue();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("x"));
            final LongList epochSeconds = new LongList();
            epochAttempts.set(0);
            try (LineUdpParserImpl parser = new LineUdpParserImpl(engine, new DefaultLineUdpReceiverConfiguration())) {
                for (int s = 1; s <= 200; s++) {
                    final long micros = UDP_T0_MICROS + s * 1_000_000L;
                    setCurrentMicros(micros);
                    if (s <= 150) {
                        udpParse(parser, "x,s=u v=" + s + "i " + micros * 1000L + "\n");
                    }
                    final long lastEpochTs = tracker.getLastEpochTs();
                    failEpochs = s == 60;
                    try {
                        parser.commitAll();
                    } finally {
                        failEpochs = false;
                    }
                    if (tracker.getLastEpochTs() != lastEpochTs) {
                        epochSeconds.add(s);
                    }
                }
                Assert.assertFalse("the idle tick must take the owed epoch", tracker.isDurableEpochOwed());
            }
            // 60 s fails, 120 s retries it, 180 s is an idle tick after the trickle ended at 150 s.
            Assert.assertEquals("[120,180]", epochSeconds.toString());
            Assert.assertEquals(3, epochAttempts.get());
        });
    }

    private static long committedSeqTxn(String tableName) {
        TableToken token = engine.verifyTableName(tableName);
        return engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn();
    }

    private static long durableEpoch(String tableName) {
        TableToken token = engine.verifyTableName(tableName);
        return engine.getTableSequencerAPI().getTxnTracker(token).getDurableEpochSeqTxn();
    }

    // Startup in a fresh engine; the writers close with an epoch only if flush on close is on.
    private static CairoEngine restart(CairoEngine e) {
        e.releaseAllReaders();
        e.releaseAllWriters();
        e.releaseInactive();
        if (e != engine) {
            e.close();
        }
        return new CairoEngine(configuration);
    }

    private static String rows(CairoEngine e, String table) throws SqlException {
        final StringSink sink = new StringSink();
        e.print("select * from " + table, sink);
        return sink.toString();
    }

    // One UDP batch at the current time: a row, then a line that adds a column, which commits the row, and is
    // rejected because v is a LONG.
    private static void udpBatch(CairoEngine e, String row, String column) {
        final long nanos = e.getConfiguration().getMicrosecondClock().getTicks() * 1000L;
        try (LineUdpParserImpl parser = new LineUdpParserImpl(e, new DefaultLineUdpReceiverConfiguration())) {
            udpParse(parser, "x,s=" + row + " v=1i " + nanos + "\nx,s=rejected " + column + "=5i,v=\"x\" " + nanos + "\n");
            parser.commitAll();
        }
    }

    private static void udpParse(LineUdpParserImpl parser, String lines) {
        final byte[] bytes = lines.getBytes(StandardCharsets.UTF_8);
        final long mem = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try (LineUdpLexer lexer = new LineUdpLexer(4096)) {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.putByte(mem + i, bytes[i]);
            }
            lexer.withParser(parser);
            lexer.parse(mem, mem + bytes.length);
            lexer.parseLast();
        } finally {
            Unsafe.free(mem, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
