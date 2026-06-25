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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TDD tests for Plan 2 Task A — CommitMode.ADAPTIVE + durable WAL commit.
 *
 * <p>Asserts that under ADAPTIVE mode, every WAL commit issues {@code fdatasync} on the
 * segment column data file(s), the WAL-e events file ({@code _event}), AND the sequencer
 * part file ({@code _txn_parts/…}) and header ({@code _txnlog}), IN THAT ORDER
 * (data → events → sequencer), before the commit returns.
 *
 * <p>Under NOSYNC mode, zero fdatasync calls must be issued to these files.
 *
 * <p>(a) ordering test — RED before implementation, GREEN after.
 * <p>(b) NOSYNC zero-fdatasync test — GREEN before and after (regression guard).
 * <p>(c) round-trip data-integrity test — ingest → drainWalQueue → select returns data.
 */
public class AdaptiveWalDurabilityTest extends AbstractCairoTest {

    /**
     * (a) ADAPTIVE: a WAL commit must fdatasync segment column data BEFORE events BEFORE sequencer.
     * Fails until WalWriter.syncIfRequired(), WalEventWriter.sync(), and
     * TableTransactionLogV2.sync0() all call fdatasync after their msync under ADAPTIVE mode.
     *
     * <p>We do a "warmup" insert first (to pre-allocate all file pages so that no page-extension
     * fdatasyncs occur during the measurement commit). Then we reset the fdatasync log and do
     * the measurement insert, capturing only the per-commit durability fdatasyncs.
     */
    @Test
    public void testAdaptiveFdatasyncOrderDataBeforeEventsBeforeSequencer() throws Exception {
        // Use a small sequencer part size so the _txn_parts file is exercised.
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");

        final FdatasyncOrderFacade trackFf = new FdatasyncOrderFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");

            // Warmup insert: allocates all file pages (segment columns, event file, sequencer).
            // The extension fdatasyncs from this insert are intentionally discarded.
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            trackFf.resetFdatasyncOrder(); // discard all fdatasyncs from warmup + page allocation

            // Measurement insert: pages already allocated, so only the per-commit durability
            // fdatasyncs (from our new ADAPTIVE code) appear in the log.
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 42)");

            // The commit should have issued fdatasync on column data, events, and sequencer.
            List<String> order = trackFf.getFdatasyncOrder();

            // Assert all three categories are present.
            boolean hasColumnData = order.stream().anyMatch(p ->
                    isWalColumnDataFile(p));
            boolean hasEvents = order.stream().anyMatch(p ->
                    p.endsWith(WalUtils.EVENT_FILE_NAME) || p.endsWith(WalUtils.EVENT_FILE_NAME + "."));
            boolean hasSeqPart = order.stream().anyMatch(p ->
                    p.contains(WalUtils.TXNLOG_PARTS_DIR));
            boolean hasSeqHeader = order.stream().anyMatch(p ->
                    p.endsWith(WalUtils.TXNLOG_FILE_NAME) || p.endsWith(WalUtils.TXNLOG_FILE_NAME + "."));

            if (!hasColumnData || !hasEvents || !hasSeqPart || !hasSeqHeader) {
                StringBuilder sb = buildMissingReport(order, hasColumnData, hasEvents, hasSeqPart, hasSeqHeader);
                Assert.fail(sb.toString());
            }

            // Assert ordering: first column-data fdatasync before first events fdatasync
            // before first sequencer (part or header) fdatasync.
            int firstColumnIdx = -1;
            int firstEventsIdx = -1;
            int firstSeqIdx = -1;
            for (int i = 0; i < order.size(); i++) {
                String p = order.get(i);
                if (firstColumnIdx < 0 && isWalColumnDataFile(p)) {
                    firstColumnIdx = i;
                }
                if (firstEventsIdx < 0 && (p.endsWith(WalUtils.EVENT_FILE_NAME)
                        || p.endsWith(WalUtils.EVENT_FILE_NAME + "."))) {
                    firstEventsIdx = i;
                }
                if (firstSeqIdx < 0 && (p.contains(WalUtils.TXNLOG_PARTS_DIR)
                        || p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                        || p.endsWith(WalUtils.TXNLOG_FILE_NAME + "."))) {
                    firstSeqIdx = i;
                }
            }

            Assert.assertTrue(
                    "ADAPTIVE: column data fdatasync (" + firstColumnIdx + ") must come before events ("
                            + firstEventsIdx + "). Order: " + order,
                    firstColumnIdx < firstEventsIdx
            );
            Assert.assertTrue(
                    "ADAPTIVE: events fdatasync (" + firstEventsIdx + ") must come before sequencer ("
                            + firstSeqIdx + "). Order: " + order,
                    firstEventsIdx < firstSeqIdx
            );
        });
    }

    /**
     * (b) NOSYNC: zero fdatasync calls must be issued on WAL commit.
     */
    @Test
    public void testNosyncIssuesZeroFdatasync() throws Exception {
        // NOSYNC is the default — no need to set the property, but we set it explicitly for clarity.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");

        final FdatasyncOrderFacade trackFf = new FdatasyncOrderFacade();
        assertMemoryLeak(trackFf, () -> {
            execute("create table y (ts timestamp, v long) timestamp(ts) partition by day wal");

            trackFf.resetFdatasyncOrder();
            execute("insert into y values ('2024-01-02T00:00:00.000000Z', 99)");

            List<String> order = trackFf.getFdatasyncOrder();
            // Filter to only WAL-related paths (segment column data, _event, _txnlog, _txn_parts).
            long walFdatasyncs = order.stream().filter(p ->
                    isWalColumnDataFile(p)
                            || p.endsWith(WalUtils.EVENT_FILE_NAME)
                            || p.endsWith(WalUtils.EVENT_FILE_NAME + ".")
                            || p.contains(WalUtils.TXNLOG_PARTS_DIR)
                            || p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                            || p.endsWith(WalUtils.TXNLOG_FILE_NAME + ".")
            ).count();
            Assert.assertEquals(
                    "NOSYNC must issue zero fdatasync on WAL commit paths, but got: " + order,
                    0, walFdatasyncs
            );
        });
    }

    /**
     * (c) Round-trip: adaptive ingest → drainWalQueue → select returns the inserted data.
     * Uses the default FilesFacade (not the tracking one) because we only need data correctness here.
     */
    @Test
    public void testAdaptiveRoundTrip() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 16);
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");

        assertMemoryLeak(() -> {
            execute("create table z (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into z values ('2024-03-01T00:00:00.000000Z', 7)");
            execute("insert into z values ('2024-03-01T01:00:00.000000Z', 13)");
            drainWalQueue();
            assertQuery("select * from z order by ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tv\n" +
                            "2024-03-01T00:00:00.000000Z\t7\n" +
                            "2024-03-01T01:00:00.000000Z\t13\n");
        });
    }
    // Note: testAdaptiveRoundTrip uses the plain assertMemoryLeak() (no custom ff) so that
    // the BlockFileWriter and other engine components operate normally.

    // ---------- helpers ----------

    /**
     * Returns true if the given path is a WAL segment column data file (.d file or fixed-size
     * column file in a wal/ segment directory — i.e. NOT the event or sequencer files).
     */
    private static boolean isWalColumnDataFile(String p) {
        if (p == null) {
            return false;
        }
        // WAL segment column files live inside wal<N>/<segmentId>/ and end with .d
        // (variable-length data: varchar, binary) or have no extension (fixed-size columns).
        // The key distinguishing feature is the path containing "/wal" and a segment integer
        // directory, AND not being an event or sequencer file.
        boolean inWalDir = p.contains("/wal") || p.contains("\\wal");
        boolean isEventFile = p.endsWith(WalUtils.EVENT_FILE_NAME)
                || p.endsWith(WalUtils.EVENT_FILE_NAME + ".")
                || p.endsWith(WalUtils.EVENT_INDEX_FILE_NAME)
                || p.endsWith(WalUtils.EVENT_INDEX_FILE_NAME + ".");
        boolean isSeqFile = p.contains(WalUtils.TXNLOG_PARTS_DIR)
                || p.endsWith(WalUtils.TXNLOG_FILE_NAME)
                || p.endsWith(WalUtils.TXNLOG_FILE_NAME + ".");
        boolean isMetaFile = p.endsWith("_meta") || p.endsWith("_meta.")
                || p.endsWith("_walmeta") || p.endsWith("_walmeta.");
        return inWalDir && !isEventFile && !isSeqFile && !isMetaFile;
    }

    private static StringBuilder buildMissingReport(
            List<String> order, boolean hasColumnData, boolean hasEvents,
            boolean hasSeqPart, boolean hasSeqHeader
    ) {
        StringBuilder sb = new StringBuilder("ADAPTIVE commit missing fdatasync on:\n");
        if (!hasColumnData) {
            sb.append("  - segment column data\n");
        }
        if (!hasEvents) {
            sb.append("  - WAL-e events file (_event)\n");
        }
        if (!hasSeqPart) {
            sb.append("  - sequencer part file (_txn_parts/…)\n");
        }
        if (!hasSeqHeader) {
            sb.append("  - sequencer header (_txnlog)\n");
        }
        sb.append("Recorded fdatasync paths:\n");
        for (int i = 0; i < order.size(); i++) {
            sb.append("  [").append(i).append("] ").append(order.get(i)).append('\n');
        }
        return sb;
    }

    /**
     * A FilesFacade that records every {@code fdatasync(fd)} call, resolving the fd to its
     * file path via open-time tracking. Inherits from {@link TestFilesFacadeImpl} so that the
     * standard open-file leak detection still applies.
     */
    static class FdatasyncOrderFacade extends TestFilesFacadeImpl {
        private final Map<Long, String> fdToPath = new HashMap<>();
        private final List<String> fdatasyncOrder = new ArrayList<>();

        public List<String> getFdatasyncOrder() {
            return fdatasyncOrder;
        }

        public void resetFdatasyncOrder() {
            fdatasyncOrder.clear();
        }

        @Override
        public boolean close(long fd) {
            fdToPath.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fdatasync(long fd) {
            super.fdatasync(fd);
            String p = fdToPath.get(fd);
            if (p != null) {
                fdatasyncOrder.add(p);
            }
        }

        @Override
        public long openAppend(LPSZ name) {
            long fd = super.openAppend(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            long fd = super.openCleanRW(name, size);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRO(LPSZ name) {
            long fd = super.openRO(name);
            trackFd(fd, name);
            return fd;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            long fd = super.openRW(name, opts);
            trackFd(fd, name);
            return fd;
        }

        private void trackFd(long fd, LPSZ name) {
            if (fd > -1) {
                fdToPath.put(fd, Utf8String.newInstance(name).toString());
            }
        }
    }
}
