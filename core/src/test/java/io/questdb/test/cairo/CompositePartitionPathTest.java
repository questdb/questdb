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

package io.questdb.test.cairo;

import io.questdb.cairo.CompositeDimensionTransform;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Plan 4a (composite partitioning write routing), Task 3: pure path-rendering assertions for the
 * per-cell on-disk PARTITION PATH. This is deliberately NOT wired into {@code processO3Block}/the
 * write path (Task 4) -- these tests drive {@link TableWriter#renderCellSegment(io.questdb.std.str.CharSink, int)}
 * and the new cell-aware {@link TableUtils#setSinkForNativePartition(io.questdb.std.str.CharSink, int, int, long, long, CharSequence)}
 * overload directly, exactly mirroring how {@link CompositeRoutingTest} drives
 * {@code resolveCellKey}/{@code resolveDimensionOrdinal} directly rather than through a real
 * O3/WAL-driven {@code INSERT}.
 * <p>
 * Every scenario is exercised in both {@code LAYOUT HIVE} (default) and {@code LAYOUT PLAIN} to lock
 * in the two on-disk naming schemes {@code PartitionSpec.MODE_HIVE}/{@code MODE_PLAIN} already commit
 * to (Plan 1).
 */
public class CompositePartitionPathTest extends AbstractCairoTest {

    private static final long NAME_TXN = 3L;
    private static final String PARTITION_DATE = "2026-07-15";

    /**
     * The existing 5-arg {@link TableUtils#setSinkForNativePartition(io.questdb.std.str.CharSink, int, int, long, long)}
     * must remain byte-identical: the new 6-arg cell-aware overload, called with a {@code null}
     * cellSegment, must render the exact same string. Also locks in today's literal plain-table
     * shape ({@code "2026-07-15.3"}) so a future change to either overload's date/txn placement is
     * caught even if the two overloads accidentally drift in lockstep.
     */
    @Test
    public void testPlainTableByteIdenticalToExistingSignature() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day");
            try (TableWriter w = getWriter("p")) {
                long ts = parseFloorPartialTimestamp(PARTITION_DATE);

                StringSink oldSink = new StringSink();
                TableUtils.setSinkForNativePartition(oldSink, w.getTimestampType(), PartitionBy.DAY, ts, NAME_TXN);

                StringSink newSinkNullSegment = new StringSink();
                TableUtils.setSinkForNativePartition(newSinkNullSegment, w.getTimestampType(), PartitionBy.DAY, ts, NAME_TXN, null);

                Assert.assertEquals("2026-07-15.3", oldSink.toString());
                Assert.assertEquals(
                        "6-arg overload with a null cellSegment must be byte-identical to the pre-existing 5-arg signature",
                        oldSink.toString(), newSinkNullSegment.toString()
                );
            }
        });
    }

    /**
     * IDENTITY dimension, both naming modes: HIVE renders {@code <sourceColName>=<value>}, PLAIN
     * renders the bare {@code <value>}.
     */
    @Test
    public void testIdentityHiveAndPlain() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, x double) " +
                    "timestamp(ts) partition by day, exch wal");   // default LAYOUT HIVE
            try (TableWriter w = getWriter("c")) {
                int keyBtc = w.getSymbolMapWriter(1).put("BTC");
                int cellKey = w.resolveCellKey(new int[]{keyBtc});

                StringSink cellSink = new StringSink();
                w.renderCellSegment(cellSink, cellKey);
                Assert.assertEquals("exch=BTC", cellSink.toString());

                long ts = parseFloorPartialTimestamp(PARTITION_DATE);
                StringSink pathSink = new StringSink();
                TableUtils.setSinkForNativePartition(pathSink, w.getTimestampType(), PartitionBy.DAY, ts, NAME_TXN, cellSink);
                Assert.assertEquals("2026-07-15/exch=BTC.3", pathSink.toString());
            }
        });

        assertMemoryLeak(() -> {
            execute("create table cp (ts timestamp, exch symbol, x double) " +
                    "timestamp(ts) partition by day, exch layout plain wal");
            try (TableWriter w = getWriter("cp")) {
                int keyBtc = w.getSymbolMapWriter(1).put("BTC");
                int cellKey = w.resolveCellKey(new int[]{keyBtc});

                StringSink cellSink = new StringSink();
                w.renderCellSegment(cellSink, cellKey);
                Assert.assertEquals("BTC", cellSink.toString());

                long ts = parseFloorPartialTimestamp(PARTITION_DATE);
                StringSink pathSink = new StringSink();
                TableUtils.setSinkForNativePartition(pathSink, w.getTimestampType(), PartitionBy.DAY, ts, NAME_TXN, cellSink);
                Assert.assertEquals("2026-07-15/BTC.3", pathSink.toString());
            }
        });
    }

    /**
     * HASH dimension: the ordinal IS the bucket number, rendered directly as an integer (no reverse
     * lookup -- a bucket cannot be un-hashed). The expected bucket is derived independently via
     * {@link CompositeDimensionTransform#hashBucket}, not just echoed back from the value under
     * test (mirrors {@code CompositeRoutingTest}'s own precedent for testing HASH).
     */
    @Test
    public void testHashBucketRendersIntegerBothModes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table h (ts timestamp, exch symbol, x double) " +
                    "timestamp(ts) partition by day, hash(exch, 4) wal");
            try (TableWriter w = getWriter("h")) {
                int expectedBucket = CompositeDimensionTransform.hashBucket("SYM0", 4);
                int key = w.getSymbolMapWriter(1).put("SYM0");
                int ordinal = w.resolveDimensionOrdinal(0, key, "SYM0");
                Assert.assertEquals(expectedBucket, ordinal);
                int cellKey = w.resolveCellKey(new int[]{ordinal});

                StringSink sink = new StringSink();
                w.renderCellSegment(sink, cellKey);
                Assert.assertEquals("exch=" + expectedBucket, sink.toString());
            }
        });

        assertMemoryLeak(() -> {
            execute("create table hp (ts timestamp, exch symbol, x double) " +
                    "timestamp(ts) partition by day, hash(exch, 4) layout plain wal");
            try (TableWriter w = getWriter("hp")) {
                int expectedBucket = CompositeDimensionTransform.hashBucket("SYM0", 4);
                int key = w.getSymbolMapWriter(1).put("SYM0");
                int ordinal = w.resolveDimensionOrdinal(0, key, "SYM0");
                int cellKey = w.resolveCellKey(new int[]{ordinal});

                StringSink sink = new StringSink();
                w.renderCellSegment(sink, cellKey);
                Assert.assertEquals(Integer.toString(expectedBucket), sink.toString());
            }
        });
    }

    /**
     * TRUNCATE dimension: the ordinal reverse-looks-up the interned N-char prefix from the
     * dedicated dictionary.
     */
    @Test
    public void testTruncatePrefixReverseLookupBothModes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tc (ts timestamp, sku symbol, x double) " +
                    "timestamp(ts) partition by day, truncate(sku, 3) wal");
            try (TableWriter w = getWriter("tc")) {
                int key = w.getSymbolMapWriter(1).put("BTCUSDT");
                int ordinal = w.resolveDimensionOrdinal(0, key, "BTCUSDT");
                int cellKey = w.resolveCellKey(new int[]{ordinal});

                StringSink sink = new StringSink();
                w.renderCellSegment(sink, cellKey);
                Assert.assertEquals("sku=BTC", sink.toString());
            }
        });

        assertMemoryLeak(() -> {
            execute("create table tcp (ts timestamp, sku symbol, x double) " +
                    "timestamp(ts) partition by day, truncate(sku, 3) layout plain wal");
            try (TableWriter w = getWriter("tcp")) {
                int key = w.getSymbolMapWriter(1).put("BTCUSDT");
                int ordinal = w.resolveDimensionOrdinal(0, key, "BTCUSDT");
                int cellKey = w.resolveCellKey(new int[]{ordinal});

                StringSink sink = new StringSink();
                w.renderCellSegment(sink, cellKey);
                Assert.assertEquals("BTC", sink.toString());
            }
        });
    }

    /**
     * Arity must not be hardcoded to 1 (same concern {@code CompositeRoutingTest#testResolveCellKeyArityTwoPacking}
     * already raises for {@code resolveCellKey}): a 2-dimension spec renders as two nested,
     * {@code '/'}-joined segments (Hive-style multi-column partitioning), not one segment with both
     * values crammed in.
     */
    @Test
    public void testArityTwoJoinsSegmentsAsNestedDirs() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t2 (ts timestamp, exchange symbol, symbol symbol) " +
                    "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
            try (TableWriter w = getWriter("t2")) {
                int dim0 = w.getSymbolMapWriter(1).put("NYSE");
                int dim1 = w.internDimensionValue(1, "BTCUSDT"); // truncated prefix "BTC"
                int cellKey = w.resolveCellKey(new int[]{dim0, dim1});

                StringSink sink = new StringSink();
                w.renderCellSegment(sink, cellKey);
                Assert.assertEquals("exchange=NYSE/symbol=BTC", sink.toString());
            }
        });
    }

    /**
     * The {@code .nameTxn} suffix must attach to the cell segment, not the shared day directory
     * (composite on-disk versioning is per-cell, see {@code TxReader.getPartitionNameTxn}).
     */
    @Test
    public void testNameTxnAttachesToCellSegmentNotDayDir() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, x double) " +
                    "timestamp(ts) partition by day, exch wal");
            try (TableWriter w = getWriter("c")) {
                int keyBtc = w.getSymbolMapWriter(1).put("BTC");
                int cellKey = w.resolveCellKey(new int[]{keyBtc});
                StringSink cellSink = new StringSink();
                w.renderCellSegment(cellSink, cellKey);

                long ts = parseFloorPartialTimestamp(PARTITION_DATE);
                StringSink pathSink = new StringSink();
                TableUtils.setSinkForNativePartition(pathSink, w.getTimestampType(), PartitionBy.DAY, ts, NAME_TXN, cellSink);

                String rendered = pathSink.toString();
                Assert.assertEquals("2026-07-15/exch=BTC.3", rendered);
                Assert.assertFalse(
                        "nameTxn must not attach to the shared day dir once a cell segment is present",
                        rendered.contains("2026-07-15.3")
                );
            }
        });
    }

    /**
     * A rendered cell segment becomes an on-disk directory name: a SYMBOL value is arbitrary user
     * data with no restriction against path-unsafe characters (unlike a table/column identifier),
     * so {@code '/'} (would silently break the path structure), {@code '.'} (ambiguous with the
     * {@code .nameTxn} suffix) and {@code '%'} (keeps the escaping unambiguous) must be escaped
     * rather than passed through raw. This test is self-discriminating: an unescaped implementation
     * would render {@code "exch=A/../B"}, which fails the exact-match assertion below.
     */
    @Test
    public void testPathUnsafeCharacterEscaped() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table u (ts timestamp, exch symbol, x double) " +
                    "timestamp(ts) partition by day, exch wal");
            try (TableWriter w = getWriter("u")) {
                int key = w.getSymbolMapWriter(1).put("A/../B");
                int cellKey = w.resolveCellKey(new int[]{key});

                StringSink sink = new StringSink();
                w.renderCellSegment(sink, cellKey);
                Assert.assertEquals("exch=A%2f%2e%2e%2fB", sink.toString());
            }
        });
    }
}
