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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests lazy column type conversion on parquet partitions.
 * <p>
 * When a column type changes via ALTER TABLE ALTER COLUMN TYPE while a partition is
 * stored in parquet, the Rust parquet decoder converts data on-the-fly during queries.
 * This test verifies that the parquet (Rust) conversion path produces the same results
 * as the native (JNI) conversion path for all supported type pairs.
 * <p>
 * Rust-handled conversions tested here:
 * <ul>
 *     <li>Fixed-to-Fixed: pairs among BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP,
 *         FLOAT, DOUBLE</li>
 *     <li>Var-to-Var: STRING to VARCHAR (UTF-8 kept), VARCHAR to STRING (UTF-8 to UTF-16)</li>
 * </ul>
 * <p>
 * Each test inserts data (including nulls and boundary values) into two identical WAL
 * tables, converts one to parquet, alters the column type on both, and asserts that
 * both produce identical query results.
 */
public class ParquetColumnTypeConversionTest extends AbstractCairoTest {

    @Test
    public void testAddColumnAfterParquetConversionReadsNullThenAltersType() throws Exception {
        // ADD COLUMN fires AFTER the partition is parquet, so the parquet file
        // never carries the new column. The lazy decoder must surface NULL for
        // all existing rows (column-top covers the whole partition), and a
        // subsequent ALTER COLUMN TYPE on that column must keep the NULLs
        // matching the native control table.
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE nt (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE pt (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                String values = """
                        INSERT INTO %s(id, ts) VALUES
                        (1, '2024-01-01T00:00:01.000000Z'),
                        (2, '2024-01-01T00:00:02.000000Z'),
                        (3, '2024-01-01T00:00:03.000000Z')""";
                execute(values.formatted("nt"));
                execute(values.formatted("pt"));
                drainWalQueue();

                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                // Add the column after parquet conversion. The new column is
                // absent from the parquet file; the reader must materialise
                // NULL for every row through the column-top path. INT carries
                // a null sentinel (Integer.MIN_VALUE), so column-top rows must
                // surface as NULL rather than 0.
                execute("ALTER TABLE nt ADD COLUMN extra INT");
                execute("ALTER TABLE pt ADD COLUMN extra INT");
                drainWalQueue();

                // Lazy read: the parquet file has no 'extra' column, so all
                // rows must surface as NULL through the column-top fallback.
                assertQuery("SELECT * FROM pt ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns(
                        "id\tts\textra\n" +
                                "1\t2024-01-01T00:00:01.000000Z\tnull\n" +
                                "2\t2024-01-01T00:00:02.000000Z\tnull\n" +
                                "3\t2024-01-01T00:00:03.000000Z\tnull\n");

                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");

                // ALTER the absent column's type. The parquet partition has
                // no data for 'extra', so the converter must still report
                // NULL after the type change.
                execute("ALTER TABLE nt ALTER COLUMN extra TYPE LONG");
                execute("ALTER TABLE pt ALTER COLUMN extra TYPE LONG");
                drainWalQueue();

                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");

                // Eager rewrite to native must preserve the NULLs.
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();
                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
            } finally {
                tryDrop("nt");
                tryDrop("pt");
            }
        });
    }

    @Test
    public void testAllNullColumnInParquetConvertsToTargetNulls() throws Exception {
        // A partition where one column is entirely NULL must round-trip NULLs
        // through both the lazy parquet read and the eager rewrite, regardless
        // of source and target type. Covers fixed-source, var-source, and
        // var-target paths.
        assertMemoryLeak(() -> {
            String[][] pairs = new String[][]{
                    {"INT", "LONG"},
                    {"INT", "STRING"},
                    {"STRING", "INT"},
                    {"STRING", "VARCHAR"},
                    {"DOUBLE", "FLOAT"},
                    {"DOUBLE", "VARCHAR"}
            };
            for (String[] pair : pairs) {
                String values = """
                        (NULL, '2024-01-01T00:00:01.000000Z'),
                        (NULL, '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""";
                assertConversion(pair[0], pair[1], values);
            }
        });
    }

    /**
     * Regression for the {@code getVarcharSize} override gap. {@code length_bytes(val)}
     * routes through {@code LengthBytesVarcharFunctionFactory.getInt} which calls
     * {@code arg.getVarcharSize(rec)}.
     * {@link io.questdb.cairo.sql.PageFrameFilteredMemoryRecord#getVarcharSize(int)}
     * reads the aux page without consulting {@code needsLazyConversion}, mirroring the
     * {@code getStrLen} gap.
     */
    @Test
    public void testAsyncGroupByByLengthBytesOfFixedToVarcharParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryFixedToVarParity(
                "VARCHAR",
                "SELECT length_bytes(val) len, sym, count() c FROM $T WHERE other > 700 GROUP BY len, sym ORDER BY len, sym"
        ));
    }

    /**
     * Regression for the {@code getStrLen} override gap. {@code length(val)} routes
     * through {@code LengthStrFunctionFactory.getInt} which calls
     * {@code arg.getStrLen(rec)}. With {@code val} on the fixed-to-STRING lazy conversion
     * path,
     * {@link io.questdb.cairo.sql.PageFrameFilteredMemoryRecord#getStrLen(int)} fires
     * instead of the parent. The filtered override reads the aux page directly and
     * does NOT check {@code needsLazyConversion}, so for a fixed-source column where
     * no STRING aux page exists, it either returns {@code TableUtils.NULL_LEN} or
     * reads garbage. The GROUP BY result diverges from the native control table.
     */
    @Test
    public void testAsyncGroupByByLengthOfFixedToStrParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryFixedToVarParity(
                "STRING",
                "SELECT length(val) len, sym, count() c FROM $T WHERE other > 700 GROUP BY len, sym ORDER BY len, sym"
        ));
    }

    /**
     * Regression for the fixed-to-var typecast read in {@link io.questdb.cairo.sql.PageFrameFilteredMemoryRecord}.
     * <p>
     * {@code val} starts as INT, the partition is converted to parquet, then {@code val}
     * is ALTERed to STRING. Reads on the parquet partition take the fixed-to-var lazy
     * conversion path ({@code sourceColumnTypes[val] >= 0}, {@code hasTypeCasts=true}).
     * <p>
     * The query groups by {@code val} and filters on a separate non-typecast column
     * {@code other}. This sequences the code path under test:
     * <ol>
     *   <li>{@code AsyncGroupByRecordCursorFactory.run} sees
     *       {@code frameMemory.hasColumnTypeCasts()} and falls back to the scalar filter,
     *       calling {@code populateRemainingColumns(filterColumnIndexes, rows, false)}
     *       which decodes non-filter columns in compacted layout.</li>
     *   <li>{@code aggregateFilteredNonSharded} wraps the frame in a
     *       {@link io.questdb.cairo.sql.PageFrameFilteredMemoryRecord}, where
     *       {@code filteredColumns[val]=false} since {@code val} is the GROUP BY key,
     *       not the filter target.</li>
     *   <li>The map sink calls {@code getStrA(val)}. {@code PageFrameFilteredMemoryRecord}
     *       does NOT override {@code getStrA} (or {@code getStrB}), so the call falls
     *       through to the parent {@link io.questdb.cairo.sql.PageFrameMemoryRecord},
     *       which routes to {@code convertFixedToStr} and indexes by the parent's
     *       absolute {@code rowIndex} field rather than {@code getRowIndex(columnIndex)}
     *       (which would return the compacted index).</li>
     *   <li>The non-filter column buffer holds only {@code rows.size()} entries in
     *       compacted layout, so the absolute-index read goes out of bounds.</li>
     * </ol>
     * A miss on this path surfaces as wrong GROUP BY keys (silent data corruption)
     * or a JVM crash on native OOB. The control table {@code nt} is native; the
     * parquet+ALTER table {@code pt} must produce an identical cursor.
     */
    @Test
    public void testAsyncGroupByKeyedByFixedToStrParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryFixedToVarParity(
                "STRING",
                "SELECT val, count() c FROM $T WHERE other > 700 GROUP BY val ORDER BY val"
        ));
    }

    /**
     * Same regression as {@link #testAsyncGroupByKeyedByFixedToStrParquetColumn} but for
     * fixed-to-VARCHAR. {@code PageFrameFilteredMemoryRecord} does not override
     * {@code getVarcharA} / {@code getVarcharB}, so the call falls through to the parent
     * which routes to {@code convertFixedToVarchar} and indexes by the absolute rowIndex
     * into the compacted buffer.
     */
    @Test
    public void testAsyncGroupByKeyedByFixedToVarcharParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryFixedToVarParity(
                "VARCHAR",
                "SELECT val, count() c FROM $T WHERE other > 700 GROUP BY val ORDER BY val"
        ));
    }

    /**
     * Mirror of {@link #testAsyncGroupByKeyedByFixedToStrParquetColumn} in the reverse
     * direction: var-to-fixed. {@code val} starts as STRING, the partition is converted to
     * parquet, then {@code val} is ALTERed to INT. On {@code pt} the parquet keeps STRING
     * storage so reads of {@code val} take the var-to-fixed lazy conversion path
     * ({@code sourceColumnTypes[val] < -1}, {@code hasTypeCasts=true}).
     * <p>
     * The query groups by {@code val} and filters on a separate non-typecast column
     * {@code other}, so {@code val} is a non-filter column in compacted layout under
     * {@link io.questdb.cairo.sql.PageFrameFilteredMemoryRecord}. The map sink calls
     * {@code getInt(val)} on the filtered record. The override delegates to
     * {@code super.getInt}, which routes through {@code convertVarToInt} ->
     * {@code readVarValueForConversion} -> {@code getStr0} / {@code getVarchar}. The
     * latter pair is overridden on the filtered record to use {@code getRowIndex},
     * so the compacted index reaches the read. This pins the routing: a future
     * regression that adds a fall-through path inside a fixed-width lazy getter
     * which reads {@code this.rowIndex} directly (mirroring the
     * {@code convertFixedToStr}/{@code convertFixedToVarchar} path that
     * {@code getStrA}/{@code getVarcharA} already wrap with save/restore of
     * {@code rowIndex}) would break this query.
     */
    @Test
    public void testAsyncGroupByKeyedByStrToIntParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryStrToFixedParity(
                "INT",
                "SELECT val, count() c FROM $T WHERE other > 700 GROUP BY val ORDER BY val"
        ));
    }

    /**
     * The async keyed group-by factory. Same {@code hasColumnTypeCasts()} gate. The
     * difference vs the not-keyed variant is that a {@code GROUP BY} key is materialized
     * per row and the per-key aggregation update path runs.
     */
    @Test
    public void testAsyncGroupByKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryParity(
                "SELECT sym, min(val) mn, max(val) mx, sum(val) s, count() c FROM $T WHERE val > 50 GROUP BY sym ORDER BY sym"
        ));
    }

    /**
     * The async not-keyed group-by factory runs an aggregation without a {@code GROUP BY}
     * clause. Its inner loop checks {@code frameMemory.hasColumnTypeCasts()} and falls back
     * to a row-by-row update path when {@code true}; the batched / vectorized aggregation
     * path reads raw page addresses and would silently produce wrong values if the parquet
     * column needs lazy conversion. The control table {@code nt} is native; {@code pt} is a
     * parquet partition with {@code val} ALTER'd from STRING to INT, which sets
     * {@code hasTypeCasts=true} (var to fixed). Results across both tables must match.
     */
    @Test
    public void testAsyncGroupByNotKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryParity(
                "SELECT min(val) mn, max(val) mx, sum(val) s, avg(val) a, count() c FROM $T WHERE val > 50"
        ));
    }

    /**
     * The async horizon-join factory with {@code GROUP BY} keys. Same gate, plus the keyed
     * grouping path is exercised.
     */
    @Test
    public void testAsyncHorizonJoinKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncJoinFactoryParity(
                """
                        SELECT t.sym, h.offset, count() n, sum(t.val) sum_val, avg(p.price) avg_price
                        FROM $T t
                        HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0s, 5s, 30s) AS h
                        WHERE t.val > 50
                        GROUP BY t.sym, h.offset
                        ORDER BY t.sym, h.offset"""
        ));
    }

    /**
     * The async horizon-join factory with no {@code GROUP BY} keys -- a single aggregated
     * output row. The left side is the parquet+ALTER'd table; the right side is a native
     * shared {@code prices} table. The aggregation references the lazy-converted {@code val}.
     */
    @Test
    public void testAsyncHorizonJoinNotKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncJoinFactoryParity(
                """
                        SELECT count() n, sum(t.val) sum_val, avg(p.price) avg_price
                        FROM $T t
                        HORIZON JOIN prices p ON (t.sym = p.sym) LIST (0s, 5s, 30s) AS h
                        WHERE t.val > 50"""
        ));
    }

    /**
     * The async JIT-filtered factory. When parquet needs a lazy conversion, the JIT'ed
     * filter cannot be applied directly to the parquet page bytes -- the factory must
     * fall back to scalar filter evaluation through {@code PageFrameMemoryRecord}.
     */
    @Test
    public void testAsyncJitFilteredOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryParity(
                "SELECT val, sym FROM $T WHERE val > 50 AND val < 150 ORDER BY ts"
        ));
    }

    /**
     * The async multi-horizon-join factory with {@code GROUP BY} keys. Same gate as the
     * not-keyed variant; selects {@code AsyncMultiHorizonJoinRecordCursorFactory}.
     */
    @Test
    public void testAsyncMultiHorizonJoinKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncMultiJoinFactoryParity(
                """
                        SELECT t.sym, h.offset, count() n, sum(t.val) sum_val,
                               avg(b.price) avg_bid, avg(a.price) avg_ask
                        FROM $T t
                        HORIZON JOIN bids b ON (t.sym = b.sym)
                        HORIZON JOIN asks a ON (t.sym = a.sym)
                            LIST (0s, 5s, 30s) AS h
                        WHERE t.val > 50
                        GROUP BY t.sym, h.offset
                        ORDER BY t.sym, h.offset"""
        ));
    }

    /**
     * The async multi-horizon-join factory with no {@code GROUP BY} keys -- a single
     * aggregated output row. Two HORIZON JOIN clauses share a single offset {@code LIST},
     * which routes the query through {@code AsyncMultiHorizonJoinNotKeyedRecordCursorFactory}.
     * The {@code WHERE t.val > 50} predicate is JIT-compiled against the current INT
     * metadata, but the parquet frame still stores {@code val} as STRING. The factory
     * must consult {@code hasColumnTypeCasts()} and fall back to the scalar filter;
     * applying the compiled filter directly would read VARCHAR aux bytes as INT and
     * select wrong rows. The parity check against the native control pins this gate.
     */
    @Test
    public void testAsyncMultiHorizonJoinNotKeyedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncMultiJoinFactoryParity(
                """
                        SELECT count() n, sum(t.val) sum_val, avg(b.price) avg_bid, avg(a.price) avg_ask
                        FROM $T t
                        HORIZON JOIN bids b ON (t.sym = b.sym)
                        HORIZON JOIN asks a ON (t.sym = a.sym)
                            LIST (0s, 5s, 30s) AS h
                        WHERE t.val > 50"""
        ));
    }

    /**
     * The async top-K factory orders by {@code val} with a {@code LIMIT}. Its vectorized
     * comparator path reads {@code val} via the column page address; with a lazy var to
     * fixed conversion in parquet, only the scalar fallback materializes the converted
     * integer.
     */
    @Test
    public void testAsyncTopKOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryParity(
                "SELECT val, sym FROM $T WHERE val > 50 ORDER BY val DESC, sym LIMIT 10"
        ));
    }

    /**
     * Sibling of {@link #testAsyncTopKSingleKeyOverAlteredParquetColumn()} for the STRING key
     * shape: a single-column ORDER BY ... LIMIT over a lazily fixed-to-STRING converted parquet
     * column routes through {@code SortKeyEncoder.encodeStringBatch}. For a natively-stored STRING
     * that batch reads {@code frameMemory.getPageAddress(...)} plus the aux page straight, but with
     * an INT-to-STRING lazy cast the parquet still stores INT, so the data page holds raw INT bytes
     * rather than a native STRING layout. The batch must fall back to the per-row converting path.
     */
    @Test
    public void testAsyncTopKSingleKeyFixedToStrParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryFixedToVarParity(
                "STRING",
                "SELECT val FROM $T WHERE other > 50 ORDER BY val DESC LIMIT 10"
        ));
    }

    /**
     * Sibling of {@link #testAsyncTopKSingleKeyOverAlteredParquetColumn()} for the VARCHAR key
     * shape: a single-column ORDER BY ... LIMIT over a lazily fixed-to-VARCHAR converted parquet
     * column routes through {@code SortKeyEncoder.encodeVarcharBatch}. Unlike the fixed/string/wide
     * siblings, that batch already special-cases parquet, and for an INT-to-VARCHAR cast the parquet
     * keeps the INT storage so the column has no aux page ({@code auxAddr == 0}); the batch declines
     * and the per-row converting path runs. This pins that contract for the one batch shape the fix
     * leaves unchanged.
     */
    @Test
    public void testAsyncTopKSingleKeyFixedToVarcharParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryFixedToVarParity(
                "VARCHAR",
                "SELECT val FROM $T WHERE other > 50 ORDER BY val DESC LIMIT 10"
        ));
    }

    /**
     * No-filter variant of {@link #testAsyncTopKSingleKeyOverAlteredParquetColumn()}: with no
     * WHERE predicate the async top-K routes through {@code findTopK} rather than
     * {@code filterAndFindTopK}, so {@code SortKeyEncoder.encodeFrame} runs with a null
     * {@code rows} list (the whole-frame scan). This pins the {@code rows == null} branch of the
     * lazy-cast fallback in the fixed-width batch path.
     */
    @Test
    public void testAsyncTopKSingleKeyNoFilterOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryStrToFixedParity(
                "LONG",
                "SELECT val FROM $T ORDER BY val DESC LIMIT 10"
        ));
    }

    /**
     * Single-column ORDER BY ... LIMIT over a lazily var-to-fixed converted parquet column.
     * Unlike {@link #testAsyncTopKOverAlteredParquetColumn()} (a two-column key that routes to
     * {@code SortKeyEncoder.encodeGeneric}, i.e. the per-row converting record path), a
     * single-column fixed-width sort key takes the batch path
     * {@code SortKeyEncoder.encodeFixed8Batch}/{@code encodeFixedWideBatch}/{@code encodeStringBatch},
     * which read {@code frameMemory.getPageAddress(...)} raw and only fall back on a column top
     * ({@code colAddr == 0}). {@code AsyncTopKRecordCursorFactory} gates only its filter on
     * {@code frameMemory.hasColumnTypeCasts()}, not the {@code encoder.encodeFrame(...)} call, so
     * the batch reads the VARCHAR_SLICE source buffer as raw longs and the top-K diverges from the
     * native control. Only {@code encodeVarcharBatch} guards on {@code PartitionFormat.PARQUET}; the
     * fixed/wide/string batch siblings do not.
     */
    @Test
    public void testAsyncTopKSingleKeyOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryStrToFixedParity(
                "LONG",
                "SELECT val FROM $T WHERE other > 50 ORDER BY val DESC LIMIT 10"
        ));
    }

    /**
     * Sibling of {@link #testAsyncTopKSingleKeyOverAlteredParquetColumn()} for the wide-fixed key
     * shape: a single-column ORDER BY ... LIMIT over a lazily STRING-to-UUID converted parquet
     * column routes through {@code SortKeyEncoder.encodeFixedWideBatch}. The parquet keeps the
     * STRING storage, so the column page is a VARCHAR_SLICE buffer; reading it as raw 16-byte UUID
     * values yields garbage. The batch must fall back to the per-row converting path.
     */
    @Test
    public void testAsyncTopKSingleKeyStrToUuidParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncFactoryStrToUuidParity(
                "SELECT val FROM $T WHERE other > 50 ORDER BY val DESC LIMIT 10"
        ));
    }

    /**
     * The async window-join FAST factory ({@code WINDOW JOIN ... ON (key)}). The left
     * frame is the parquet+ALTER'd table; the join key is the symbol column. Aggregation
     * references both the lazy-converted left column {@code t.val} and the right table's
     * {@code p.price}.
     */
    @Test
    public void testAsyncWindowJoinFastOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncJoinFactoryParity(
                """
                        SELECT t.sym, t.val, t.ts, sum(p.price) p_sum
                        FROM $T t
                        WINDOW JOIN prices p ON (t.sym = p.sym)
                        RANGE BETWEEN 30 SECONDS PRECEDING AND 30 SECONDS FOLLOWING EXCLUDE PREVAILING
                        WHERE t.val > 50
                        ORDER BY t.ts, t.sym"""
        ));
    }

    /**
     * The async window-join SLOW factory ({@code WINDOW JOIN} without {@code ON}). Same
     * fallback gate as the fast variant.
     */
    @Test
    public void testAsyncWindowJoinOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertAsyncJoinFactoryParity(
                """
                        SELECT t.sym, t.val, t.ts, sum(p.price) p_sum
                        FROM $T t
                        WINDOW JOIN prices p
                        RANGE BETWEEN 30 SECONDS PRECEDING AND 30 SECONDS FOLLOWING EXCLUDE PREVAILING
                        WHERE t.val > 50
                        ORDER BY t.ts, t.sym"""
        ));
    }

    @Test
    public void testBooleanToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    (true, '2024-01-01T00:00:01.000000Z'),
                    (false, '2024-01-01T00:00:02.000000Z')""";
            for (String target : new String[]{"BYTE", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("BOOLEAN", target, values);
            }
        });
    }

    @Test
    public void testByteToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (127, '2024-01-01T00:00:04.000000Z'),
                    (-128, '2024-01-01T00:00:05.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("BYTE", target, values);
            }
        });
    }

    @Test
    public void testCharToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('a', '2024-01-01T00:00:01.000000Z'),
                    ('z', '2024-01-01T00:00:02.000000Z'),
                    ('1', '2024-01-01T00:00:03.000000Z')""";
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("CHAR", target, values);
            }
        });
    }

    /**
     * Reproducer for the claimed C1 defect: a column is type-converted BEFORE the
     * partition is encoded to parquet, then converted again AFTER. The claim was that
     * the encoder stamps each parquet column's field_id with the column's CURRENT
     * (intermediate) writer index, so a later reader -- which only probes the chain
     * head (getOriginalWriterIndex) or the current index -- would miss the intermediate
     * id and silently read every row as NULL.
     * <p>
     * In this codebase every encode path stamps field_id = getOriginalWriterIndex()
     * (the chain head), see TableUtils.java and O3PartitionJob.java. So the column is
     * always findable by the head, regardless of how deep the conversion chain grows.
     * This test pins that behaviour: a parquet partition encoded mid-chain must keep
     * reading the real values, matching the native control table.
     */
    @Test
    public void testColumnEncodedToParquetMidChainThenConvertedAgain() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE nt (c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE pt (c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                String values = """
                        INSERT INTO %s(c, ts) VALUES
                        (10, '2024-01-01T00:00:01.000000Z'),
                        (NULL, '2024-01-01T00:00:02.000000Z'),
                        (30, '2024-01-01T00:00:03.000000Z')""";
                execute(values.formatted("nt"));
                execute(values.formatted("pt"));
                drainWalQueue();

                // Step 1: re-key c via a type conversion BEFORE parquet. The column now
                // sits at a fresh writer index with a replacing chain back to index 0.
                execute("ALTER TABLE nt ALTER COLUMN c TYPE LONG");
                execute("ALTER TABLE pt ALTER COLUMN c TYPE LONG");
                drainWalQueue();

                // Step 2: encode to parquet while c is mid-chain. The claim says field_id
                // gets the intermediate index; the code stamps getOriginalWriterIndex().
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                // Reads after the mid-chain encode must still see the real values.
                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");

                // Step 3: grow the chain again, on top of the mid-chain parquet file.
                execute("ALTER TABLE nt ALTER COLUMN c TYPE STRING");
                execute("ALTER TABLE pt ALTER COLUMN c TYPE STRING");
                drainWalQueue();

                // C1 claim: probing head (idx 0) and current idx both miss the parquet
                // field_id, so every row reads NULL. If the bug were real, pt would be
                // all-NULL for c while nt keeps the values.
                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
                assertQuery("SELECT c, ts FROM pt ORDER BY ts").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                        """
                                c\tts
                                10\t2024-01-01T00:00:01.000000Z
                                \t2024-01-01T00:00:02.000000Z
                                30\t2024-01-01T00:00:03.000000Z
                                """);

                // Eager rewrite to native must also preserve the values (exercises the
                // ConvertOperatorImpl pre-pass / getParquetColumnType path the claim
                // says returns UNDEFINED for a mid-chain field_id).
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();
                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
            } finally {
                tryDrop("nt");
                tryDrop("pt");
            }
        });
    }

    @Test
    public void testDateToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // DATE stores milliseconds since epoch.
            // '2020-06-15T12:00:00.000Z' = 1_592_222_400_000 ms
            // DATE -> TIMESTAMP scales x1000 (ms -> us), null preserved.
            // DATE -> TIMESTAMP_NS scales x1_000_000 (ms -> ns) via the post-decode
            // nano-scaling branch in PageFrameMemoryPool.openParquet.
            // Sub-second millisecond precision exercised with .999Z.
            // '9999-12-31T23:59:59.999Z' = 253_402_300_799_999 ms; multiplied by
            // 1_000_000 for TIMESTAMP_NS this overflows i64. Both paths must wrap
            // identically (native uses unchecked C++ multiply in convert_ms_to_ns,
            // parquet uses wrapping_mul in scale_i64_in_place). The parquet decoder
            // must produce the wrapped i64, not LONG NULL, to match the value the
            // native ALTER materializes.
            String values = """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                    ('2020-06-15T12:00:00.999Z', '2024-01-01T00:00:03.000000Z'),
                    ('9999-12-31T23:59:59.999Z', '2024-01-01T00:00:04.000000Z'),
                    (NULL, '2024-01-01T00:00:05.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "TIMESTAMP", "TIMESTAMP_NS", "FLOAT", "DOUBLE"}) {
                assertConversion("DATE", target, values);
            }
        });
    }

    /**
     * Exercises Decimal sources across the six physical backing widths (Decimal8 / Decimal16
     * / Decimal32 / Decimal64 / Decimal128 / Decimal256) which map respectively to parquet
     * Int32, Int32, Int32, Int64, FixedLenByteArray(16), FixedLenByteArray(32). Each row
     * lands on a different dispatch arm in {@code decode_int32_dispatch},
     * {@code decode_int64_dispatch} or {@code decode_fixed_len_dispatch}. The Fixed-to-Var
     * conversion path passes the source type to Rust (Java post-converts), so the matched
     * arm is the source {@code ColumnTypeTag::DecimalN} arm at the column's natural width.
     */
    @Test
    public void testDecimalSourceWidthsToString() throws Exception {
        assertMemoryLeak(() -> {
            // Decimal8 (Int32 physical, fits in i8 after scaling)
            assertConversion("DECIMAL(2, 0)", "STRING", """
                    (12m, '2024-01-01T00:00:01.000000Z'),
                    (-99m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
            assertConversion("DECIMAL(2, 1)", "VARCHAR", """
                    (1.2m, '2024-01-01T00:00:01.000000Z'),
                    (-9.9m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal16 (Int32 physical, fits in i16 after scaling)
            assertConversion("DECIMAL(4, 0)", "STRING", """
                    (1234m, '2024-01-01T00:00:01.000000Z'),
                    (-9999m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
            assertConversion("DECIMAL(4, 2)", "VARCHAR", """
                    (12.34m, '2024-01-01T00:00:01.000000Z'),
                    (-99.99m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal32 (Int32 physical)
            assertConversion("DECIMAL(9, 0)", "STRING", """
                    (123456789m, '2024-01-01T00:00:01.000000Z'),
                    (-987654321m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
            assertConversion("DECIMAL(9, 3)", "VARCHAR", """
                    (123456.789m, '2024-01-01T00:00:01.000000Z'),
                    (-987654.321m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal64 (Int64 physical)
            assertConversion("DECIMAL(18, 0)", "STRING", """
                    (123456789012345678m, '2024-01-01T00:00:01.000000Z'),
                    (-999999999999999999m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal128 (FixedLenByteArray(16))
            assertConversion("DECIMAL(38, 0)", "STRING", """
                    (12345678901234567890123456789012345678m, '2024-01-01T00:00:01.000000Z'),
                    (-99999999999999999999999999999999999999m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
            assertConversion("DECIMAL(38, 4)", "VARCHAR", """
                    (1234567890123456789012345678901234.5678m, '2024-01-01T00:00:01.000000Z'),
                    (-9999999999999999999999999999999999.9999m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Decimal256 (FixedLenByteArray(32))
            assertConversion("DECIMAL(76, 0)", "STRING", """
                    (1234567890123456789012345678901234567890123456789012345678901234567890123456m, '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");
            assertConversion("DECIMAL(76, 1)", "VARCHAR", """
                    (123456789012345678901234567890123456789012345678901234567890123456789012345.6m, '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");
        });
    }

    @Test
    public void testDecimalToDecimal() throws Exception {
        assertMemoryLeak(() -> {
            // Decimal64 -> Decimal128: cross-physical widening (Int64 -> FLBA(16)),
            // same-scale and scale-up. Scale-up hits rescale_i128 with small magnitudes.
            String d64Values = """
                    (12345.6789m, '2024-01-01T00:00:01.000000Z'),
                    (0.0000m, '2024-01-01T00:00:02.000000Z'),
                    (-99.9999m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(18, 4)", "DECIMAL(38, 4)", d64Values);
            assertConversion("DECIMAL(18, 4)", "DECIMAL(38, 8)", d64Values);

            // Decimal8 <-> Decimal256: extreme width gap. Widening sign-extends 1 byte
            // to 32 bytes; narrowing keeps the bottom byte. Narrowing keeps the same
            // scale so no rescale runs: raw values stay inside i8 and the bottom-byte
            // truncation is lossless. Scale-change-with-narrowing (where a rescale runs at
            // the source width before the narrow) is covered separately in
            // testDecimalToDecimalNarrowingWithScaleChange.
            String tinyValues = """
                    (1.2m, '2024-01-01T00:00:01.000000Z'),
                    (0.0m, '2024-01-01T00:00:02.000000Z'),
                    (-9.9m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(2, 1)", "DECIMAL(76, 1)", tinyValues);
            assertConversion("DECIMAL(2, 1)", "DECIMAL(76, 5)", tinyValues);
            assertConversion("DECIMAL(76, 1)", "DECIMAL(2, 1)", tinyValues);

            // Decimal16 <-> Decimal128: rescale_i128 widening and narrowing.
            String d16Values = """
                    (12.34m, '2024-01-01T00:00:01.000000Z'),
                    (0.00m, '2024-01-01T00:00:02.000000Z'),
                    (-99.99m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(4, 2)", "DECIMAL(38, 2)", d16Values);
            assertConversion("DECIMAL(4, 2)", "DECIMAL(38, 6)", d16Values);
            assertConversion("DECIMAL(38, 2)", "DECIMAL(4, 2)", d16Values);

            // Decimal32 <-> Decimal64: cross-physical Int32 <-> Int64 boundary.
            String d32Values = """
                    (123456.789m, '2024-01-01T00:00:01.000000Z'),
                    (0.000m, '2024-01-01T00:00:02.000000Z'),
                    (-987654.321m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(9, 3)", "DECIMAL(18, 3)", d32Values);
            assertConversion("DECIMAL(9, 3)", "DECIMAL(18, 6)", d32Values);
            assertConversion("DECIMAL(18, 3)", "DECIMAL(9, 3)", d32Values);

            // Decimal128 -> Decimal128 same-width rescale: exercises rescale_i128 in place
            // with magnitudes past i64 range (so the i128 multiply genuinely matters) for
            // both the multiply (scale-up) and divide (scale-down) branches. Trailing zero
            // fractional digits keep the divide branch exact so the expected value is obvious; a
            // lossy scale-down would instead round half away from zero on both paths (never NULL).
            String d128Values = """
                    (12345678901234567890.5600m, '2024-01-01T00:00:01.000000Z'),
                    (0.0000m, '2024-01-01T00:00:02.000000Z'),
                    (-99999999999999999999.9900m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(38, 4)", "DECIMAL(38, 8)", d128Values);
            assertConversion("DECIMAL(38, 4)", "DECIMAL(38, 2)", d128Values);

            // Decimal256 -> Decimal256 same-width rescale: rescale_i256 path. Negative
            // values exercise the negate_i256 branch in div_i256_pow10. Values use trailing
            // zero fractional digits for the same reason as the Decimal128 divide above.
            String d256Values = """
                    (1234567890123456789012345678901234567890123456789012345678.0000m, '2024-01-01T00:00:01.000000Z'),
                    (0.0000m, '2024-01-01T00:00:02.000000Z'),
                    (-9999999999999999999999999999999999999999999999999999999999.0000m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(76, 4)", "DECIMAL(76, 8)", d256Values);
            assertConversion("DECIMAL(76, 4)", "DECIMAL(76, 0)", d256Values);
        });
    }

    @Test
    public void testDecimalToDecimalNarrowingSameScaleAllWidths() throws Exception {
        assertMemoryLeak(() -> {
            // Narrowing physical width while KEEPING the scale. No rescale runs, so the
            // decode-time byte truncation is lossless as long as the value fits the target
            // precision. Every value here is representable in the narrower target, so the
            // lazy parquet read must equal the native conversion. Covers each wider->narrower
            // backing-width step.
            String v = """
                    (1.2m, '2024-01-01T00:00:01.000000Z'),
                    (-3.4m, '2024-01-01T00:00:02.000000Z'),
                    (0.0m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            // Decimal256 -> Decimal128 -> Decimal64 -> Decimal32 -> Decimal16 -> Decimal8, scale fixed at 1.
            assertConversion("DECIMAL(76, 1)", "DECIMAL(38, 1)", v);
            assertConversion("DECIMAL(38, 1)", "DECIMAL(18, 1)", v);
            assertConversion("DECIMAL(18, 1)", "DECIMAL(9, 1)", v);
            assertConversion("DECIMAL(9, 1)", "DECIMAL(4, 1)", v);
            assertConversion("DECIMAL(4, 1)", "DECIMAL(2, 1)", v);
            // Extreme gaps, scale fixed.
            assertConversion("DECIMAL(76, 1)", "DECIMAL(2, 1)", v);
            assertConversion("DECIMAL(38, 1)", "DECIMAL(2, 1)", v);
            assertConversion("DECIMAL(76, 1)", "DECIMAL(9, 1)", v);
        });
    }

    @Test
    public void testDecimalToDecimalNarrowingWithScaleChange() throws Exception {
        assertMemoryLeak(() -> {
            // Decimal->decimal conversions that narrow the physical width AND change the scale.
            //
            // The native converter (DecimalColumnTypeConverter.convertToDecimal) loads each value
            // into a full Decimal256, rescales at full width, range-checks against the target
            // precision, then narrows to the target (widen -> rescale -> range-check -> narrow), so
            // it preserves any value that fits the target precision. The lazy parquet decoder mirrors
            // this via convert_decimal_narrowing (row_groups.rs): plan_decode_conversion keeps the
            // SOURCE width during decode, then convert_decimal_narrowing rescales, range-checks, and
            // only then narrows. So a value whose raw form (logical * 10^srcScale) exceeds the
            // target's physical byte width still survives when the logical value fits the target
            // precision -- it is the rescaled, not the raw, magnitude that is range-checked.
            //
            // Every value below is representable in the narrower target, so the lazy parquet read
            // (pt) must equal the native conversion (nt). A naive narrow-before-rescale decoder
            // would truncate 1.2 to 0.0 here; this test pins that it does not.

            // Decimal128(scale 4) -> Decimal8(scale 1): raw 1.2 -> 12000 overflows 1 byte.
            assertConversion("DECIMAL(38, 4)", "DECIMAL(2, 1)", """
                    (1.2m, '2024-01-01T00:00:01.000000Z'),
                    (-3.4m, '2024-01-01T00:00:02.000000Z'),
                    (0.0m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");

            // Decimal256(scale 5) -> Decimal16(scale 1): raw 1.2 -> 120000 overflows 2 bytes.
            assertConversion("DECIMAL(76, 5)", "DECIMAL(4, 1)", """
                    (1.2m, '2024-01-01T00:00:01.000000Z'),
                    (-7.8m, '2024-01-01T00:00:02.000000Z'),
                    (0.0m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");

            // Decimal128(scale 10) -> Decimal32(scale 2): raw 1.00 -> 10_000_000_000 wraps i32.
            assertConversion("DECIMAL(38, 10)", "DECIMAL(9, 2)", """
                    (1.00m, '2024-01-01T00:00:01.000000Z'),
                    (-2.50m, '2024-01-01T00:00:02.000000Z'),
                    (0.00m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");

            // Decimal256(scale 12) -> Decimal64(scale 2): raw 10_000_000 -> 1e19 wraps i64.
            assertConversion("DECIMAL(76, 12)", "DECIMAL(18, 2)", """
                    (10000000.00m, '2024-01-01T00:00:01.000000Z'),
                    (-20000000.00m, '2024-01-01T00:00:02.000000Z'),
                    (0.00m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");

            // Decimal256(scale 20) -> Decimal128(scale 2): raw 2e18 -> 2e38 wraps i128.
            assertConversion("DECIMAL(76, 20)", "DECIMAL(38, 2)", """
                    (2000000000000000000.00m, '2024-01-01T00:00:01.000000Z'),
                    (-3000000000000000000.00m, '2024-01-01T00:00:02.000000Z'),
                    (0.00m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");
        });
    }

    @Test
    public void testDecimalToDecimalScaleDownRounds() throws Exception {
        assertMemoryLeak(() -> {
            // A scale reduction rounds half away from zero on BOTH the native ALTER and the lazy
            // parquet decode; a dropped fraction never NULLs. assertConversion pins that nt (native)
            // and pt (parquet) agree across the lazy read and the eager parquet->native rewrite.
            // Values carry non-zero dropped digits, including exact-half ties and negatives, across
            // all backings.
            // Decimal16 -> Decimal16 (i64), scale 2 -> 1; 99.99 rounds up to 100.0 (carry).
            assertConversion("DECIMAL(4, 2)", "DECIMAL(4, 1)", """
                    (12.34m, '2024-01-01T00:00:01.000000Z'),
                    (12.35m, '2024-01-01T00:00:02.000000Z'),
                    (-12.35m, '2024-01-01T00:00:03.000000Z'),
                    (99.99m, '2024-01-01T00:00:04.000000Z'),
                    (NULL, '2024-01-01T00:00:05.000000Z')""");
            // Decimal128 -> Decimal128 (i128), scale 4 -> 2, magnitudes past i64 range.
            assertConversion("DECIMAL(38, 4)", "DECIMAL(38, 2)", """
                    (12345678901234567890.5678m, '2024-01-01T00:00:01.000000Z'),
                    (-12345678901234567890.5650m, '2024-01-01T00:00:02.000000Z'),
                    (0.0001m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");
            // Decimal256 -> Decimal256 (i256), scale 5 -> 2; 0.005 ties and rounds away to 0.01.
            assertConversion("DECIMAL(76, 5)", "DECIMAL(76, 2)", """
                    (12345.67891m, '2024-01-01T00:00:01.000000Z'),
                    (-12345.67850m, '2024-01-01T00:00:02.000000Z'),
                    (0.00500m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");
            // Narrowing width AND scale: Decimal128 -> Decimal32 (i128 source), scale 4 -> 2.
            assertConversion("DECIMAL(38, 4)", "DECIMAL(9, 2)", """
                    (1234.5678m, '2024-01-01T00:00:01.000000Z'),
                    (-1234.5650m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
        });
    }

    @Test
    public void testDecimalToDecimalTwoStep() throws Exception {
        assertMemoryLeak(() -> {
            // Chained (two-step) decimal->decimal conversions over a parquet partition. The
            // first ALTER reads lazily; the second ALTER sees a prior conversion, so the
            // ConvertOperatorImpl pre-pass eagerly materialises the parquet partition to native
            // (isParquetStorageCompatible is false for differing decimal types) and the second
            // step runs through the native DecimalColumnTypeConverter. Both intermediate and
            // final reads must equal native.
            String widen = """
                    (12.34m, '2024-01-01T00:00:01.000000Z'),
                    (-56.78m, '2024-01-01T00:00:02.000000Z'),
                    (0.00m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            // Widen then widen, with scale changes at each step.
            assertTwoStepConversion("DECIMAL(4, 2)", "DECIMAL(18, 4)", "DECIMAL(38, 6)", widen);
            // Widen then narrow back (the narrowing step is the eager native path).
            assertTwoStepConversion("DECIMAL(9, 2)", "DECIMAL(38, 4)", "DECIMAL(4, 2)", widen);
            // Cross every backing width up then back down.
            assertTwoStepConversion("DECIMAL(2, 1)", "DECIMAL(76, 5)", "DECIMAL(9, 1)", """
                    (1.2m, '2024-01-01T00:00:01.000000Z'),
                    (-3.4m, '2024-01-01T00:00:02.000000Z'),
                    (0.0m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");
            // Two same-width rescales in a row (Decimal128).
            assertTwoStepConversion("DECIMAL(38, 4)", "DECIMAL(38, 8)", "DECIMAL(38, 2)", """
                    (12345678901234567890.5600m, '2024-01-01T00:00:01.000000Z'),
                    (0.0000m, '2024-01-01T00:00:02.000000Z'),
                    (-99999999999999999999.9900m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");
        });
    }

    @Test
    public void testDecimalToDecimalWideningAllWidths() throws Exception {
        assertMemoryLeak(() -> {
            // Widening physical width (with same / scale-up / scale-down). Widening preserves
            // the full source value before any rescale, so every case must match native.
            String tiny = """
                    (1.2m, '2024-01-01T00:00:01.000000Z'),
                    (-3.4m, '2024-01-01T00:00:02.000000Z'),
                    (0.0m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            // Decimal8 -> every wider width, scale up.
            assertConversion("DECIMAL(2, 1)", "DECIMAL(4, 2)", tiny);
            assertConversion("DECIMAL(2, 1)", "DECIMAL(9, 3)", tiny);
            assertConversion("DECIMAL(2, 1)", "DECIMAL(18, 5)", tiny);
            assertConversion("DECIMAL(2, 1)", "DECIMAL(38, 6)", tiny);
            assertConversion("DECIMAL(2, 1)", "DECIMAL(76, 10)", tiny);

            String d16 = """
                    (12.34m, '2024-01-01T00:00:01.000000Z'),
                    (-56.78m, '2024-01-01T00:00:02.000000Z'),
                    (0.00m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(4, 2)", "DECIMAL(9, 4)", d16);
            assertConversion("DECIMAL(4, 2)", "DECIMAL(38, 2)", d16); // same scale, widen
            assertConversion("DECIMAL(4, 2)", "DECIMAL(76, 8)", d16);

            String d32 = """
                    (123456.789m, '2024-01-01T00:00:01.000000Z'),
                    (-987654.321m, '2024-01-01T00:00:02.000000Z'),
                    (0.000m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(9, 3)", "DECIMAL(18, 6)", d32);
            assertConversion("DECIMAL(9, 3)", "DECIMAL(76, 3)", d32);
            // Scale down (3 -> 1) uses trailing zeros so the result is exact; a lossy scale-down
            // would round half away from zero on both paths (it never NULLs on a dropped fraction).
            assertConversion("DECIMAL(9, 3)", "DECIMAL(38, 1)", """
                    (123456.700m, '2024-01-01T00:00:01.000000Z'),
                    (-987654.300m, '2024-01-01T00:00:02.000000Z'),
                    (0.000m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");

            String d64 = """
                    (123456789012.3456m, '2024-01-01T00:00:01.000000Z'),
                    (-987654321098.7654m, '2024-01-01T00:00:02.000000Z'),
                    (0.0000m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(18, 4)", "DECIMAL(38, 8)", d64);
            assertConversion("DECIMAL(18, 4)", "DECIMAL(76, 4)", d64);

            String d128 = """
                    (12345678901234567890.5600m, '2024-01-01T00:00:01.000000Z'),
                    (-99999999999999999999.9900m, '2024-01-01T00:00:02.000000Z'),
                    (0.0000m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("DECIMAL(38, 4)", "DECIMAL(76, 8)", d128);
            assertConversion("DECIMAL(38, 4)", "DECIMAL(76, 2)", d128); // scale down, widen
        });
    }

    @Test
    public void testDecimalToDecimalLazyUnrepresentableProducesNull() throws Exception {
        assertMemoryLeak(() -> {
            // Decimal->decimal conversions that stay lazy (same width, or widening) must read a value
            // that does not fit the target as NULL on the lazy parquet path, matching the native
            // converter (DecimalColumnTypeConverter). These all keep the same or a wider physical width,
            // so the eager pre-pass does not materialise them - the Rust decoder must do the NULL clamp.
            // Each case mixes an unrepresentable value (-> NULL) with one that fits (-> survives).

            // Same width (Decimal128), precision reduced 38 -> 20, same scale. 18 integer digits exceed
            // DECIMAL(20,4) (16 integer digits); 12.3400 fits.
            assertConversion("DECIMAL(38, 4)", "DECIMAL(20, 4)", """
                    (123456789012345678.9012m, '2024-01-01T00:00:01.000000Z'),
                    (12.3400m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Widening width (Decimal32 -> Decimal128) with a lossy scale-down 3 -> 1: 1.234 drops the
            // .034 (information loss) -> NULL; 5.600 is exact at scale 1 -> 5.6.
            assertConversion("DECIMAL(9, 3)", "DECIMAL(38, 1)", """
                    (1.234m, '2024-01-01T00:00:01.000000Z'),
                    (5.600m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Same width (Decimal128), scale up 2 -> 30. 1234567890.12 * 10^28 overflows the target
            // precision/width -> NULL; 0.00 stays representable.
            assertConversion("DECIMAL(38, 2)", "DECIMAL(38, 30)", """
                    (1234567890.12m, '2024-01-01T00:00:01.000000Z'),
                    (0.00m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            // Same width (Decimal256), precision reduced 76 -> 40, same scale: a 50-digit value exceeds
            // DECIMAL(40,0) but fits Decimal256 width -> must be NULL (exercises the i256 precision clamp).
            assertConversion("DECIMAL(76, 0)", "DECIMAL(40, 0)", """
                    (12345678901234567890123456789012345678901234567890m, '2024-01-01T00:00:01.000000Z'),
                    (1234567890m, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
        });
    }

    @Test
    public void testDecimalToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    (12345.6789m, '2024-01-01T00:00:01.000000Z'),
                    (0.0000m, '2024-01-01T00:00:02.000000Z'),
                    (-99.9999m, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("DECIMAL(18, 4)", target, values);
            }
        });
    }

    @Test
    public void testDoubleToLongBoundary() throws Exception {
        assertMemoryLeak(() -> {
            // 9.223372036854775807E18 parses to the nearest f64, 2^63 (one ULP
            // above Long.MAX_VALUE). Contract: the parquet path must emit NULL
            // rather than letting `as i64` saturate to Long.MAX_VALUE.
            // Asserted via absolute oracle because the native (JNI) path has
            // an analogous precision-loss bug and would diverge from parquet.
            assertParquetFloatOutOfRangeNull("DOUBLE", "LONG", "9.223372036854775807E18");
        });
    }

    /**
     * Pins lazy parquet behavior for DOUBLE-&gt;LONG/DATE/TIMESTAMP/INT at the
     * float-to-integer boundaries. Two distinct concerns share the same test:
     * <ul>
     *     <li>Upper bound precision loss (LONG/DATE/TIMESTAMP only): the Rust
     *         converter in {@code core/rust/qdbr/src/parquet_read/decode.rs} cannot
     *         use {@code i64::MAX as f64} as the bound because {@code i64::MAX = 2^63 - 1}
     *         requires 63 mantissa bits and f64 only has 53, so the cast rounds up
     *         to {@code 2^63}. An f64 value equal to {@code 2^63} is strictly greater
     *         than {@code i64::MAX} but would pass a {@code v <= max} guard, then
     *         saturate to {@code i64::MAX} under the {@code as} cast - silently
     *         producing wrong data instead of the documented NULL sentinel. The
     *         converter uses {@code F64_MAX_SAFE_FOR_I64} to defend against this.</li>
     *     <li>Lower bound NULL sentinel collision (LONG/DATE/TIMESTAMP/INT):
     *         {@code i64::MIN as f64} and {@code i32::MIN as f64} are exactly
     *         representable, but those integers are also the destination NULL
     *         sentinels. Without a strict lower bound check the value passes
     *         {@code v >= min} and the cast lands on the sentinel itself, so a
     *         legitimate float value at the boundary reads back as NULL via the
     *         cast path rather than via the explicit null branch. The converter
     *         uses {@code LOWER_STRICT = true} for these cases so the strict bound
     *         routes the value through the explicit null branch.</li>
     * </ul>
     * Differential assertion against the native (JNI) path does not catch either
     * because the C++ kernel has analogous behavior, so this test asserts the
     * contract directly: out-of-range or sentinel-colliding floats read back as NULL.
     */
    @Test
    public void testDoubleToLongBoundaryPrecisionLoss() throws Exception {
        assertMemoryLeak(() -> {
            // 9.223372036854776e18 == 2^63 exactly in f64. 2^63 is strictly
            // greater than i64::MAX = 2^63 - 1, so the contract is NULL.
            // -9.223372036854776e18 == -2^63 exactly in f64. -2^63 is i64::MIN,
            // which is the LONG NULL sentinel; the strict lower bound check
            // routes this through the explicit null branch.
            for (String targetType : new String[]{"LONG", "DATE", "TIMESTAMP"}) {
                assertParquetFloatOutOfRangeNull("DOUBLE", targetType, "9.223372036854776e18");
                assertParquetFloatOutOfRangeNull("DOUBLE", targetType, "-9.223372036854776e18");
            }
            // For DOUBLE -> INT both bounds are exactly representable in f64,
            // but the lower bound i32::MIN as f64 = -2147483648.0 lands on
            // i32::MIN (the INT NULL sentinel), so the strict lower bound check
            // applies. 2147483648.0 is i32::MAX + 1 and exercises the standard
            // upper out-of-range path.
            assertParquetFloatOutOfRangeNull("DOUBLE", "INT", "-2147483648.0");
            assertParquetFloatOutOfRangeNull("DOUBLE", "INT", "2147483648.0");
        });
    }

    @Test
    public void testDoubleToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // 1e300 overflows FLOAT range (~3.4e38) and all integer types.
            // Infinity/-Infinity are distinct from NaN (null sentinel).
            String values = """
                    (1.5, '2024-01-01T00:00:01.000000Z'),
                    (0.0, '2024-01-01T00:00:02.000000Z'),
                    (-1.5, '2024-01-01T00:00:03.000000Z'),
                    (1e300, '2024-01-01T00:00:04.000000Z'),
                    (1.0/0.0, '2024-01-01T00:00:05.000000Z'),
                    (-1.0/0.0, '2024-01-01T00:00:06.000000Z'),
                    (NULL, '2024-01-01T00:00:07.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT"}) {
                assertConversion("DOUBLE", target, values);
            }
        });
    }

    /**
     * All-null fast path (parquet_meta_decode.rs: chunk.null_count == chunk.num_values): a
     * no-sentinel column that is entirely column-top in the parquet partition (added, then
     * the partition converted with no values for it), then ALTER'd to a sentinel target and
     * read under a filter. The fast path skips post_convert; the filtered read must still
     * surface NULL for every selected row, matching native.
     */
    @Test
    public void testFilteredAllNullNoSentinelColumnConverted() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE nt (sel INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE pt (sel INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                String seed = """
                        INSERT INTO %s(sel, ts)
                        SELECT x::INT, timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_000_000)
                        FROM long_sequence(50)""";
                execute(seed.formatted("nt"));
                execute(seed.formatted("pt"));
                drainWalQueue();

                // v is added but never populated, so the whole partition is column-top for v.
                execute("ALTER TABLE nt ADD COLUMN v SHORT");
                execute("ALTER TABLE pt ADD COLUMN v SHORT");
                drainWalQueue();

                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                execute("ALTER TABLE nt ALTER COLUMN v TYPE LONG");
                execute("ALTER TABLE pt ALTER COLUMN v TYPE LONG");
                drainWalQueue();

                assertSqlCursors(
                        "SELECT v, count() c FROM nt WHERE sel < 10 GROUP BY v ORDER BY v",
                        "SELECT v, count() c FROM pt WHERE sel < 10 GROUP BY v ORDER BY v"
                );
            } finally {
                tryDrop("nt");
                tryDrop("pt");
            }
        });
    }

    @Test
    public void testFilteredColumnTopNullBooleanToInt() throws Exception {
        assertMemoryLeak(() -> assertFilteredColumnTopNull("BOOLEAN", "INT", "(x % 2 = 0)"));
    }

    @Test
    public void testFilteredColumnTopNullByteToDecimal() throws Exception {
        assertMemoryLeak(() -> assertFilteredColumnTopNull("BYTE", "DECIMAL(18,2)", "x::BYTE"));
    }

    @Test
    public void testFilteredColumnTopNullByteToDouble() throws Exception {
        assertMemoryLeak(() -> assertFilteredColumnTopNull("BYTE", "DOUBLE", "x::BYTE"));
    }

    @Test
    public void testFilteredColumnTopNullOverConvertedNoSentinelColumn() throws Exception {
        // SHORT -> LONG: the documented M1 reproducer path (FILL_NULLS=false GROUP BY).
        assertMemoryLeak(() -> assertFilteredColumnTopNull("SHORT", "LONG", "x::SHORT"));
    }

    @Test
    public void testFixedToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Fixed→Var path: Rust decodes the source fixed type;
            // Java formats to string after decode.
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("BOOLEAN", target, """
                        (true, '2024-01-01T00:00:01.000000Z'),
                        (false, '2024-01-01T00:00:02.000000Z')""");

                assertConversion("BYTE", target, """
                        (1, '2024-01-01T00:00:01.000000Z'),
                        (0, '2024-01-01T00:00:02.000000Z'),
                        (-1, '2024-01-01T00:00:03.000000Z')""");

                assertConversion("SHORT", target, """
                        (1, '2024-01-01T00:00:01.000000Z'),
                        (0, '2024-01-01T00:00:02.000000Z'),
                        (-1, '2024-01-01T00:00:03.000000Z')""");

                assertConversion("INT", target, """
                        (42, '2024-01-01T00:00:01.000000Z'),
                        (0, '2024-01-01T00:00:02.000000Z'),
                        (-1, '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion("LONG", target, """
                        (42, '2024-01-01T00:00:01.000000Z'),
                        (0, '2024-01-01T00:00:02.000000Z'),
                        (-1, '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion("FLOAT", target, """
                        (1.5, '2024-01-01T00:00:01.000000Z'),
                        (0.0, '2024-01-01T00:00:02.000000Z'),
                        (-1.5, '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion("DOUBLE", target, """
                        (1.5, '2024-01-01T00:00:01.000000Z'),
                        (0.0, '2024-01-01T00:00:02.000000Z'),
                        (-1.5, '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion("DATE", target, """
                        ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");

                assertConversion("TIMESTAMP", target, """
                        ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");
            }
        });
    }

    @Test
    public void testFixedToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            // →Symbol requires the pre-pass to convert parquet→native
            // before building the symbol map.
            assertConversion("BOOLEAN", "SYMBOL", """
                    (true, '2024-01-01T00:00:01.000000Z'),
                    (false, '2024-01-01T00:00:02.000000Z')""");

            assertConversion("INT", "SYMBOL", """
                    (42, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");

            assertConversion("LONG", "SYMBOL", """
                    (42, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("DOUBLE", "SYMBOL", """
                    (1.5, '2024-01-01T00:00:01.000000Z'),
                    (0.0, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("DATE", "SYMBOL", """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");

            assertConversion("TIMESTAMP", "SYMBOL", """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");
        });
    }

    @Test
    public void testFixedWithAllEncodings() throws Exception {
        assertMemoryLeak(() -> {
            // Encoding-specific decoders (e.g. delta_binary_packed) cannot produce
            // cross-family output. The dispatch must keep the source type during decode
            // and convert afterward via post_convert.
            String intValues = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (42, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            String floatValues = """
                    (1.5, '2024-01-01T00:00:01.000000Z'),
                    (0.0, '2024-01-01T00:00:02.000000Z'),
                    (-1.5, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            String dateValues = """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String tsValues = """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";

            // Integer sources support default, plain, rle_dictionary, delta_binary_packed.
            String[] intEncodings = {"default", "plain", "rle_dictionary", "delta_binary_packed"};
            // BOOLEAN target exercises the new Int32/Int64-to-Boolean dispatch arms in
            // decode_int32_dispatch / decode_int64_dispatch for every encoding.
            String[] intTargets = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN"};
            for (String encoding : intEncodings) {
                for (String source : new String[]{"BYTE", "SHORT", "INT", "LONG"}) {
                    for (String target : intTargets) {
                        if (source.equals(target)) continue;
                        assertConversionWithEncoding(source, target, intValues, encoding);
                    }
                }
                for (String target : intTargets) {
                    if (!"DATE".equals(target)) {
                        assertConversionWithEncoding("DATE", target, dateValues, encoding);
                    }
                    if (!"TIMESTAMP".equals(target)) {
                        assertConversionWithEncoding("TIMESTAMP", target, tsValues, encoding);
                    }
                }
            }

            // Float sources support default, plain, rle_dictionary (delta_binary_packed is integer-only).
            String[] floatEncodings = {"default", "plain", "rle_dictionary"};
            // BOOLEAN target exercises the range-checked Double-to-Byte/Boolean and Float-to-Byte/Boolean
            // dispatch arms (decode_double_dispatch, decode_other_fixed_dispatch) for every encoding.
            String[] floatTargets = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN"};
            for (String encoding : floatEncodings) {
                for (String source : new String[]{"FLOAT", "DOUBLE"}) {
                    for (String target : floatTargets) {
                        if (source.equals(target)) continue;
                        assertConversionWithEncoding(source, target, floatValues, encoding);
                    }
                }
            }
        });
    }

    @Test
    public void testFloatToIntBoundary() throws Exception {
        assertMemoryLeak(() -> {
            // 2.147483647E9 parses to the nearest f32, 2^31 (one ULP above INT_MAX).
            // Contract: the parquet path must emit NULL rather than letting
            // `as i32` saturate to Integer.MAX_VALUE.
            assertParquetFloatOutOfRangeNull("FLOAT", "INT", "cast(2.147483647E9 as float)");
        });
    }

    /**
     * Pins lazy parquet behavior for FLOAT-&gt;LONG/DATE/TIMESTAMP/INT at the
     * float-to-integer boundaries. Two distinct concerns share the same test:
     * <ul>
     *     <li>Upper bound precision loss (LONG/DATE/TIMESTAMP and INT): the Rust
     *         converter cannot use {@code i64::MAX as f32} as the bound because
     *         {@code i64::MAX = 2^63 - 1} is not representable in f32 (23 mantissa
     *         bits available, 63 needed), so the cast rounds up to {@code 2^63}.
     *         The same applies to {@code i32::MAX} in f32 (only 23 mantissa bits;
     *         {@code i32::MAX} rounds up to {@code 2^31}). The converter uses
     *         {@code F32_MAX_SAFE_FOR_I64} / {@code F32_MAX_SAFE_FOR_I32} to defend
     *         against the saturating-cast hazard.</li>
     *     <li>Lower bound NULL sentinel collision (LONG/DATE/TIMESTAMP/INT):
     *         {@code i64::MIN as f32} and {@code i32::MIN as f32} are exactly
     *         representable (both are powers of two), but those integers are
     *         also the destination NULL sentinels. Without a strict lower bound
     *         check the value passes {@code v >= min} and the cast lands on the
     *         sentinel itself. The converter uses {@code LOWER_STRICT = true}
     *         for these cases so the strict bound routes the value through the
     *         explicit null branch.</li>
     * </ul>
     * The differential helper {@link #assertConversion} does not catch either
     * because the native (JNI) {@code convert_from_type_to_type} kernel has
     * analogous behavior. This test asserts the contract directly: out-of-range
     * or sentinel-colliding floats read back as NULL.
     * <p>
     * At the f32 magnitude of {@code 2^63}, adjacent representable values are
     * spaced by {@code 2^40}, so {@code 2^63} is the only f32 in the open
     * interval {@code (i64::MAX, 2^63]}.
     */
    @Test
    public void testFloatToLongBoundaryPrecisionLoss() throws Exception {
        assertMemoryLeak(() -> {
            // 9.223372036854776e18 == 2^63 exactly when stored as f32.
            // 2^63 is strictly greater than i64::MAX = 2^63 - 1, so the contract is NULL.
            // -9.223372036854776e18 == -2^63 exactly when stored as f32.
            // -2^63 is i64::MIN, which is the LONG NULL sentinel; the strict
            // lower bound check routes this through the explicit null branch.
            for (String targetType : new String[]{"LONG", "DATE", "TIMESTAMP"}) {
                assertParquetFloatOutOfRangeNull("FLOAT", targetType, "cast(9.223372036854776e18 as float)");
                assertParquetFloatOutOfRangeNull("FLOAT", targetType, "cast(-9.223372036854776e18 as float)");
            }
            // For FLOAT -> INT both bounds suffer from f32 precision loss /
            // sentinel collision: i32::MAX rounds up to 2^31 in f32, and
            // i32::MIN as f32 = -2^31 is the INT NULL sentinel. 2147483648.0
            // exercises the upper safe-bound path; -2147483648.0 (= -2^31
            // exactly in f32) exercises the strict lower bound path.
            assertParquetFloatOutOfRangeNull("FLOAT", "INT", "cast(2147483648.0 as float)");
            assertParquetFloatOutOfRangeNull("FLOAT", "INT", "cast(-2147483648.0 as float)");
        });
    }

    @Test
    public void testFloatToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Float-to-integer conversions truncate the fractional part.
            // NaN (null) maps to the target type's null sentinel.
            // Infinity/-Infinity are distinct from NaN (null sentinel).
            String values = """
                    (1.5, '2024-01-01T00:00:01.000000Z'),
                    (0.0, '2024-01-01T00:00:02.000000Z'),
                    (-1.5, '2024-01-01T00:00:03.000000Z'),
                    (cast(1.0/0.0 as float), '2024-01-01T00:00:04.000000Z'),
                    (cast(-1.0/0.0 as float), '2024-01-01T00:00:05.000000Z'),
                    (NULL, '2024-01-01T00:00:06.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP", "DOUBLE"}) {
                assertConversion("FLOAT", target, values);
            }
        });
    }

    /**
     * Pins the shared overflow contract for narrow-int -&gt; narrow-decimal scaling: a value that
     * does not fit the target DECIMAL becomes the target NULL sentinel on BOTH the native ALTER and
     * the lazy parquet decode, and neither path throws or suspends the WAL table.
     * <p>
     * The native (JNI) {@code DecimalColumnTypeConverter.convertToDecimal} scales through Decimal256,
     * maps a rescale that loses information or a magnitude beyond the target precision to NULL, and
     * never throws. The lazy parquet decoder ({@code convert_fixed_to_decimal} in
     * core/rust/qdbr/src/parquet_read/row_groups.rs) must match it: it scales each value by
     * {@code 10^scale} and NULLs the result when it exceeds the target <b>precision</b>, not merely
     * the destination byte width. Two overflow shapes are covered:
     * <ul>
     *     <li><b>Width overflow</b> -- the scaled value exceeds the target's physical byte width.
     *         INT {@code 2_000_000} -&gt; {@code DECIMAL(2, 2)} (Decimal8, 1 byte) computes
     *         {@code 200_000_000}; INT {@code 1_000_000} -&gt; {@code DECIMAL(9, 4)} (Decimal32,
     *         4 bytes) computes {@code 10^10}. A naive low-byte write would truncate these to a
     *         wrong value; both paths produce NULL instead.</li>
     *     <li><b>Precision overflow only</b> -- the scaled value fits the byte width but exceeds the
     *         target precision. INT {@code 100} -&gt; {@code DECIMAL(2, 0)} fits a Decimal8 byte
     *         (&lt;= 127) yet precision 2 admits only 99; INT {@code 1_500_000_000} -&gt;
     *         {@code DECIMAL(9, 0)} fits i32 yet precision 9 admits only 999_999_999; INT {@code 1}
     *         -&gt; {@code DECIMAL(18, 18)} computes {@code 10^18}, which fits i64 but is 19 digits.
     *         A byte-width-only guard stored these verbatim, diverging from the native NULL.</li>
     * </ul>
     * The helper asserts NULL on the native control, the lazy parquet read, and after a
     * CONVERT PARTITION TO NATIVE rewrite, so all three materialization paths agree.
     */
    @Test
    public void testIntToNarrowDecimalScalingOverflow() throws Exception {
        assertMemoryLeak(() -> {
            // Width overflow: the scaled value exceeds the target's physical byte width.
            assertIntToDecimalOverflowNull("DECIMAL(2, 2)", "2_000_000");
            assertIntToDecimalOverflowNull("DECIMAL(4, 2)", "1_000_000");
            assertIntToDecimalOverflowNull("DECIMAL(9, 4)", "1_000_000");
            // Precision overflow without width overflow: fits the byte width, exceeds the precision.
            assertIntToDecimalOverflowNull("DECIMAL(2, 0)", "100");
            assertIntToDecimalOverflowNull("DECIMAL(9, 0)", "1_500_000_000");
            assertIntToDecimalOverflowNull("DECIMAL(18, 18)", "1");
        });
    }

    @Test
    public void testIntToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // 256 wraps to 0 in BYTE (256 & 0xFF = 0).
            // 2_147_483_647 (MAX_INT) wraps to -1 in SHORT (0x7FFFFFFF & 0xFFFF).
            // -2_147_483_647 is i32::MIN+1, the smallest legal non-NULL INT (one above
            // INT_NULL = i32::MIN). It must survive widening to LONG/DATE/TIMESTAMP
            // as -2_147_483_647 rather than be mistaken for the null sentinel.
            // NULL (INT_MIN) maps to the target type's null sentinel.
            String values = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (256, '2024-01-01T00:00:04.000000Z'),
                    (2_147_483_647, '2024-01-01T00:00:05.000000Z'),
                    (-2_147_483_647, '2024-01-01T00:00:06.000000Z'),
                    (NULL, '2024-01-01T00:00:07.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("INT", target, values);
            }
        });
    }

    /**
     * Pins the multi-row-group contract of {@code TableWriter.produceNativeFromParquet}
     * on the fixed-to-var arm. Two invariants must hold simultaneously:
     * <ol>
     *     <li>{@code appendBuffer(dstDataFd, dataBuf, dataSize)} must write only
     *         the bytes {@code convertFixedColumnToString /
     *         convertFixedColumnToVarchar} actually populated, not the full
     *         {@code estimateStringDataSize / estimateVarcharDataSize} buffer.
     *         Writing the trailing {@code dataSize - actualBytes} bytes would
     *         leak uninitialized memory from {@code Unsafe.malloc} into the
     *         column data file.</li>
     *     <li>The fixed-to-var arm must track {@code dataVecBytesWritten} across
     *         row groups, matching the var-to-var arm at the same call site.
     *         Without that running offset, aux entries produced for row groups
     *         beyond the first carry offsets that are relative to the start of
     *         their own local data buffer (i.e. zero for the first entry of
     *         each row group), and subsequent row groups read back the bytes
     *         of row group 0.</li>
     * </ol>
     * <p>
     * The default test row group size is 1_000 rows; the existing fixed-to-var
     * coverage uses fewer rows so the entire partition fits in one row group and
     * neither invariant is exercised. This test forces a tiny row group size and
     * uses 12 rows with distinct integer values, so the partition spans three row
     * groups. After {@code ALTER COLUMN ... TYPE STRING/VARCHAR} and
     * {@code CONVERT PARTITION TO NATIVE}, every row must read back as its
     * original integer formatted as a string.
     */
    @Test
    public void testIntToStringConvertToNativeMultiRowGroup() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                try {
                    execute("CREATE TABLE pt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    // 12 rows in one partition, row group size 4 => three row groups.
                    execute("""
                            INSERT INTO pt VALUES
                            (1,    '2024-01-01T00:00:01.000000Z'),
                            (2,    '2024-01-01T00:00:02.000000Z'),
                            (3,    '2024-01-01T00:00:03.000000Z'),
                            (4,    '2024-01-01T00:00:04.000000Z'),
                            (5,    '2024-01-01T00:00:05.000000Z'),
                            (6,    '2024-01-01T00:00:06.000000Z'),
                            (7,    '2024-01-01T00:00:07.000000Z'),
                            (8,    '2024-01-01T00:00:08.000000Z'),
                            (9,    '2024-01-01T00:00:09.000000Z'),
                            (10,   '2024-01-01T00:00:10.000000Z'),
                            (11,   '2024-01-01T00:00:11.000000Z'),
                            (NULL, '2024-01-01T00:00:12.000000Z')""");
                    drainWalQueue();

                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();

                    execute("ALTER TABLE pt ALTER COLUMN val TYPE " + target);
                    drainWalQueue();

                    // Materializes the fixed->var conversion through produceNativeFromParquet,
                    // so subsequent reads hit the native files, not the lazy parquet path.
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    drainWalQueue();

                    assertQuery("SELECT val, ts FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                            """
                                    val\tts
                                    1\t2024-01-01T00:00:01.000000Z
                                    2\t2024-01-01T00:00:02.000000Z
                                    3\t2024-01-01T00:00:03.000000Z
                                    4\t2024-01-01T00:00:04.000000Z
                                    5\t2024-01-01T00:00:05.000000Z
                                    6\t2024-01-01T00:00:06.000000Z
                                    7\t2024-01-01T00:00:07.000000Z
                                    8\t2024-01-01T00:00:08.000000Z
                                    9\t2024-01-01T00:00:09.000000Z
                                    10\t2024-01-01T00:00:10.000000Z
                                    11\t2024-01-01T00:00:11.000000Z
                                    \t2024-01-01T00:00:12.000000Z
                                    """);
                } finally {
                    tryDrop("pt");
                }
            }
        });
    }

    @Test
    public void testIntegerToDecimal() throws Exception {
        assertMemoryLeak(() -> {
            assertConversion("BYTE", "DECIMAL(4, 1)", """
                    (127, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-128, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SHORT", "DECIMAL(8, 2)", """
                    (32767, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-32768, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("INT", "DECIMAL(18, 2)", """
                    (2_147_483_647, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""");

            assertConversion("LONG", "DECIMAL(38, 2)", """
                    (1_000_000_000, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
        });
    }

    @Test
    public void testIpv4ToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('192.168.1.1', '2024-01-01T00:00:01.000000Z'),
                    ('10.0.0.1', '2024-01-01T00:00:02.000000Z'),
                    ('255.255.255.255', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("IPv4", target, values);
            }
        });
    }

    /**
     * ASOF JOIN whose ON key is a lazily converted parquet column. The existing JOIN coverage
     * ({@link #testAsyncHorizonJoinKeyedOverAlteredParquetColumn} and the WINDOW/MULTI-HORIZON
     * siblings) always joins on the unconverted {@code sym} and only aggregates the converted
     * {@code val}, so the converted column is never read as a join equality key. Here {@code val}
     * is the ASOF key, so the join driver must pull each master row's key through the lazy
     * var-to-fixed conversion before probing the slave's key map; reading the raw parquet STRING
     * bytes as INT would mis-key the lookup. Parity against the native control {@code nt} pins it.
     * Master rows live in 2024-01-02 (the parquet partition); the slave {@code quotes} carries
     * keys 1..50 timestamped in 2024-01-01, so master {@code val} 1..50 match a preceding quote
     * and 51..100 fall through to a NULL price.
     */
    @Test
    public void testJoinOnConvertedKeyAsof() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE nt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE pt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE quotes (k INT, px DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                String insertLeft = """
                        INSERT INTO $T
                        SELECT x::STRING AS val,
                               ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                               timestamp_sequence('2024-01-02T00:00:00.000000Z', 60_000_000) AS ts
                        FROM long_sequence(100)""";
                execute(insertLeft.replace("$T", "nt"));
                execute(insertLeft.replace("$T", "pt"));
                execute("""
                        INSERT INTO quotes
                        SELECT x::INT AS k, (x * 1.5) AS px,
                               timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_000_000) AS ts
                        FROM long_sequence(50)""");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
                drainWalQueue();
                execute("ALTER TABLE nt ALTER COLUMN val TYPE INT");
                execute("ALTER TABLE pt ALTER COLUMN val TYPE INT");
                drainWalQueue();
                String query = """
                        SELECT t.ts, t.val, t.sym, q.px
                        FROM $T t
                        ASOF JOIN quotes q ON (t.val = q.k)
                        ORDER BY t.ts""";
                assertSqlCursors(query.replace("$T", "nt"), query.replace("$T", "pt"));
            } finally {
                tryDrop("nt");
                tryDrop("pt");
                tryDrop("quotes");
            }
        });
    }

    /**
     * INNER hash JOIN whose ON key is a lazily converted parquet column, covering both a
     * fixed converted key (STRING-&gt;INT) and a var converted key (INT-&gt;STRING). See
     * {@link #assertEquiJoinOnConvertedKeyParity} for the shared setup and why the converted
     * column being the equality key (rather than an aggregated/filtered column) is the point.
     */
    @Test
    public void testJoinOnConvertedKeyInner() throws Exception {
        assertMemoryLeak(() -> {
            assertEquiJoinOnConvertedKeyParity("STRING", "INT", "JOIN");
            assertEquiJoinOnConvertedKeyParity("INT", "STRING", "JOIN");
        });
    }

    /**
     * LEFT OUTER hash JOIN whose ON key is a lazily converted parquet column, covering both a
     * fixed converted key (STRING-&gt;INT) and a var converted key (INT-&gt;STRING). The dim
     * table holds only even keys, so the odd-keyed master rows exercise the unmatched-row path
     * (NULL on the slave side) in addition to the matched path. See
     * {@link #assertEquiJoinOnConvertedKeyParity}.
     */
    @Test
    public void testJoinOnConvertedKeyLeftOuter() throws Exception {
        assertMemoryLeak(() -> {
            assertEquiJoinOnConvertedKeyParity("STRING", "INT", "LEFT JOIN");
            assertEquiJoinOnConvertedKeyParity("INT", "STRING", "LEFT JOIN");
        });
    }

    @Test
    public void testLongToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // 256 wraps to 0 in BYTE, stays 256 in SHORT.
            // 2_147_483_648 overflows INT (becomes -2_147_483_648 via truncation).
            // -9_223_372_036_854_775_807 is i64::MIN+1, the smallest legal non-NULL LONG
            // (one above LONG_NULL = i64::MIN). It must survive pass-through to
            // DATE/TIMESTAMP as the same value rather than be mistaken for null.
            // NULL (LONG_MIN) maps to the target type's null sentinel.
            String values = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (256, '2024-01-01T00:00:04.000000Z'),
                    (2_147_483_648, '2024-01-01T00:00:05.000000Z'),
                    (-9_223_372_036_854_775_807, '2024-01-01T00:00:06.000000Z'),
                    (NULL, '2024-01-01T00:00:07.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("LONG", target, values);
            }
        });
    }

    /**
     * C1 repro (fixed->var dedup key, ALTER before DEDUP): like
     * {@link #testO3DedupKeyConversionFixedToVar} but the column becomes a dedup key only AFTER the
     * crossing ALTER, so no pre-pass materialises the parquet partition. The O3 merge then SIGSEGVs
     * on the var-path getChunkAuxPtr over a fixed INT decode. See
     * {@link #assertO3DedupKeyConversionDedupAfterAlter}.
     */
    @Test
    public void testO3DedupAfterAlterKeyConversionFixedToVar() throws Exception {
        assertMemoryLeak(() -> assertO3DedupKeyConversionDedupAfterAlter(
                "INT", "VARCHAR",
                "10", "30", "50",   // seed keys: fixed INT literals
                "'30'", "'31'"      // O3 keys: var literals, '30' duplicates seed 30, '31' is new
        ));
    }

    /**
     * C1 repro (symbol->fixed dedup key, ALTER before DEDUP): like
     * {@link #testO3DedupKeyConversionSymbolToFixed} but the column becomes a dedup key only AFTER
     * the crossing ALTER, so no pre-pass materialises the parquet partition (and resolves the symbol
     * map). The O3 merge then misreads the VARCHAR_SLICE bytes as fixed LONG keys, making the wrong
     * dedup decision. See {@link #assertO3DedupKeyConversionDedupAfterAlter}.
     */
    @Test
    public void testO3DedupAfterAlterKeyConversionSymbolToFixed() throws Exception {
        assertMemoryLeak(() -> assertO3DedupKeyConversionDedupAfterAlter(
                "SYMBOL", "LONG",
                "'10'", "'30'", "'50'",   // seed keys: symbol string literals
                "30", "31"                // O3 keys: fixed literals, 30 duplicates seed '30', 31 is new
        ));
    }

    /**
     * C1 repro (var->fixed dedup key, ALTER before DEDUP): like
     * {@link #testO3DedupKeyConversionVarToFixed} but the column becomes a dedup key only AFTER the
     * crossing ALTER, so no pre-pass materialises the parquet partition. The O3 merge then misreads
     * the VARCHAR_SLICE bytes as fixed LONG keys, making the wrong dedup decision. See
     * {@link #assertO3DedupKeyConversionDedupAfterAlter}.
     */
    @Test
    public void testO3DedupAfterAlterKeyConversionVarToFixed() throws Exception {
        assertMemoryLeak(() -> assertO3DedupKeyConversionDedupAfterAlter(
                "VARCHAR", "LONG",
                "'10'", "'30'", "'50'",   // seed keys: var VARCHAR literals
                "30", "31"                // O3 keys: fixed literals, 30 duplicates seed '30', 31 is new
        ));
    }

    /**
     * fixed->var dedup key: symmetric to {@link #testO3DedupKeyConversionVarToFixed} but
     * converting a fixed dedup key (INT) to a var-size target (VARCHAR). The parquet partition
     * decodes the key as a fixed INT array with no aux buffer. Before approach B, the O3 dedup
     * compare in O3PartitionJob.mergeRowGroup treated the target VARCHAR column as var-size and
     * read getChunkAuxPtr -- a dangling/zero aux pointer for a fixed decode -- producing a garbage
     * compare or a SIGSEGV. Now mergeRowGroup converts the decode buffer to native VARCHAR (Phase
     * 1a) before building the dedup-compare address, so the partition stays lazy parquet (no
     * eager pre-pass) and the cursors match the native control table (nt).
     */
    @Test
    public void testO3DedupKeyConversionFixedToVar() throws Exception {
        assertMemoryLeak(() -> assertO3DedupKeyConversion(
                "INT", "VARCHAR",
                "10", "30", "50",   // seed keys: fixed INT literals
                "'30'", "'31'"      // O3 keys: var literals, '30' duplicates seed 30, '31' is new
        ));
    }

    /**
     * symbol->fixed dedup key: a SYMBOL dedup key converted to a fixed target (LONG). Like
     * {@link #testO3DedupKeyConversionVarToFixed}, the parquet partition decodes the key as
     * VARCHAR_SLICE (symbol stored as UTF-8 BYTE_ARRAY), which a naive O3 dedup compare in
     * O3PartitionJob.mergeRowGroup would misread as a fixed LONG array. mergeRowGroup instead
     * converts the slice to native LONG (Phase 1a) before building the dedup-compare address, so
     * the partition stays lazy parquet (no eager pre-pass) and the cursors match the native
     * control table (nt).
     */
    @Test
    public void testO3DedupKeyConversionSymbolToFixed() throws Exception {
        assertMemoryLeak(() -> assertO3DedupKeyConversion(
                "SYMBOL", "LONG",
                "'10'", "'30'", "'50'",   // seed keys: symbol string literals
                "30", "31"                // O3 keys: fixed literals, 30 duplicates seed '30', 31 is new
        ));
    }

    /**
     * var->fixed dedup key: a deduplicated WAL table whose non-timestamp dedup key is converted
     * from a var-size source (VARCHAR) to a fixed target (LONG) while its partition is parquet,
     * then takes an O3 insert. The parquet partition's dedup-key column is decoded into the
     * row-group buffer as VARCHAR_SLICE. Before approach B, the O3 dedup compare in
     * O3PartitionJob.mergeRowGroup read it as a fixed LONG array (getChunkDataPtr interpreted with
     * the target column size), comparing var-size slice-layout bytes instead of LONG key values
     * and making the wrong dedup decision. Now mergeRowGroup converts the slice to native LONG
     * (Phase 1a) before building the dedup-compare address, so the partition stays lazy parquet
     * (no eager pre-pass) and the cursors match the native control table (nt).
     */
    @Test
    public void testO3DedupKeyConversionVarToFixed() throws Exception {
        assertMemoryLeak(() -> assertO3DedupKeyConversion(
                "VARCHAR", "LONG",
                "'10'", "'30'", "'50'",   // seed keys: var VARCHAR literals
                "30", "31"                // O3 keys: fixed literals, 30 duplicates seed '30', 31 is new
        ));
    }

    /**
     * O3 insert into a parquet partition that has a pending column type cast must
     * rewrite the parquet with the new type, materializing the conversion. The
     * partition stays in parquet format but the on-disk parquet column type
     * matches the post-ALTER metadata type, so subsequent reads no longer take
     * the lazy decode path for that column.
     */
    @Test
    public void testO3InsertRewritesConvertedParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO pt VALUES
                    (10, '2024-01-01T00:00:01.000000Z'),
                    (20, '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:05.000000Z')""");
            drainWalQueue();

            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("ALTER TABLE pt ALTER COLUMN val TYPE LONG");
            drainWalQueue();

            // Before O3: parquet still stores the column as INT, the lazy decoder
            // converts to LONG on the fly.
            try (TableWriter writer = getWriter("pt")) {
                Assert.assertEquals(PartitionFormat.PARQUET, writer.getPartitionFormat(0));
                int colIdx = writer.getMetadata().getColumnIndex("val");
                Assert.assertEquals(ColumnType.INT, ColumnType.tagOf(writer.getParquetColumnType(0, colIdx)));
            }

            // O3 row lands at 00:00:02, between the existing 00:00:01 and 00:00:03.
            execute("INSERT INTO pt VALUES (99, '2024-01-01T00:00:02.000000Z')");
            drainWalQueue();

            // After the merge: partition is still parquet, but the parquet file
            // has been rewritten with the new LONG type for val. hasSchemaChange
            // in O3PartitionJob forces this rewrite when the partition has
            // type-converted columns and new rows land on it.
            try (TableWriter writer = getWriter("pt")) {
                Assert.assertEquals(PartitionFormat.PARQUET, writer.getPartitionFormat(0));
                int colIdx = writer.getMetadata().getColumnIndex("val");
                Assert.assertEquals(ColumnType.LONG, ColumnType.tagOf(writer.getParquetColumnType(0, colIdx)));
            }

            assertQuery("SELECT val, ts FROM pt ORDER BY ts").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            val\tts
                            10\t2024-01-01T00:00:01.000000Z
                            99\t2024-01-01T00:00:02.000000Z
                            20\t2024-01-01T00:00:03.000000Z
                            null\t2024-01-01T00:00:05.000000Z
                            """);
        });
    }

    /**
     * Verifies snapshot isolation across {@code ALTER TABLE ... ALTER COLUMN TYPE} on a
     * parquet-backed partition. A cursor opened against the pre-ALTER transaction must
     * continue to see the original column type and values for its entire lifetime, while
     * a fresh cursor opened after the ALTER has been applied via the WAL must see the
     * converted type. This pins the contract between the page frame pool's
     * {@code sourceColumnTypes} snapshot, the parquet frame cache, and cursor state
     * across a schema change.
     */
    @Test
    public void testReaderOpenedBeforeAlterSeesOldSchema() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO pt VALUES
                        (1, '2024-01-01T00:00:01.000000Z'),
                        (2, '2024-01-01T00:00:02.000000Z'),
                        (3, '2024-01-01T00:00:03.000000Z')""");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                try (
                        RecordCursorFactory oldFactory = select("SELECT val FROM pt ORDER BY ts");
                        RecordCursor oldCursor = oldFactory.getCursor(sqlExecutionContext)
                ) {
                    Assert.assertEquals(
                            ColumnType.INT,
                            ColumnType.tagOf(oldFactory.getMetadata().getColumnType(0))
                    );

                    execute("ALTER TABLE pt ALTER COLUMN val TYPE STRING");
                    drainWalQueue();

                    // The pre-ALTER cursor still sees INT values from its snapshot.
                    Record oldRecord = oldCursor.getRecord();
                    int expected = 1;
                    while (oldCursor.hasNext()) {
                        Assert.assertEquals("row " + expected, expected, oldRecord.getInt(0));
                        expected++;
                    }
                    Assert.assertEquals(4, expected);

                    // A fresh cursor opened after the ALTER sees the converted STRING values.
                    try (
                            RecordCursorFactory newFactory = select("SELECT val FROM pt ORDER BY ts");
                            RecordCursor newCursor = newFactory.getCursor(sqlExecutionContext)
                    ) {
                        Assert.assertEquals(
                                ColumnType.STRING,
                                ColumnType.tagOf(newFactory.getMetadata().getColumnType(0))
                        );
                        Record newRecord = newCursor.getRecord();
                        StringSink sink = new StringSink();
                        int newExpected = 1;
                        while (newCursor.hasNext()) {
                            sink.clear();
                            sink.put(newRecord.getStrA(0));
                            TestUtils.assertEquals(Integer.toString(newExpected), sink);
                            newExpected++;
                        }
                        Assert.assertEquals(4, newExpected);
                    }

                    // Re-read the held cursor: still INT, still the original values.
                    oldCursor.toTop();
                    expected = 1;
                    while (oldCursor.hasNext()) {
                        Assert.assertEquals("re-read row " + expected, expected, oldRecord.getInt(0));
                        expected++;
                    }
                    Assert.assertEquals(4, expected);
                }
            } finally {
                tryDrop("pt");
            }
        });
    }

    /**
     * Pins the contract that the pool's {@code sourceColumnTypes}
     * {@link io.questdb.std.IntList} must not be aliased between Record A and Record B.
     * The package-private {@code init(...)} overload in
     * {@link io.questdb.cairo.sql.PageFrameMemoryRecord} (around line 1499) takes the
     * list by reference; navigating Record B via {@code recordAt} to a partition whose
     * parquet schema does NOT need conversion rebuilds {@code sourceColumnTypes} in
     * place (see {@code PageFrameMemoryPool.openParquet} at the
     * {@code setAll(readParquetColumnCount, -1)} call). If Record A keeps the same
     * reference, its view of the conversion mapping is clobbered while it is still
     * anchored at a partition that needs INT-&gt;STRING lazy conversion, and reads
     * return raw INT bytes interpreted as native STRING storage -- garbage.
     * <p>
     * Setup:
     * <ul>
     *   <li>Partition 2024-01-01: inserted while column {@code val} is INT, converted to
     *       parquet (parquet physical type = INT32). After the ALTER, the table schema
     *       says STRING, so this partition needs lazy INT-&gt;STRING conversion.</li>
     *   <li>Partition 2024-01-02: inserted AFTER the ALTER, so {@code val} is already
     *       stored natively as STRING; then converted to parquet (parquet stores STRING).
     *       This partition does NOT need any conversion -- {@code sourceColumnTypes[val]==-1}.</li>
     * </ul>
     * Trigger: iterate the cursor to land Record A on partition 2024-01-01, then call
     * {@code recordAt(recordB, rowIdInPartition_2024_01_02)} to navigate Record B to the
     * other partition. Re-reading Record A's {@code getStrA(val)} must still return the
     * INT value as a string; a shared-reference implementation would return garbage
     * or throw.
     */
    @Test
    public void testRecordABMixedConversionStatesAcrossPartitions() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Partition 2024-01-01: val stored as INT in parquet (conversion needed after ALTER).
                execute("INSERT INTO pt VALUES (42, '2024-01-01T00:00:00.000000Z')");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                // Schema change: val is now STRING. Partition 2024-01-01 still stores INT
                // in its parquet file; reads must lazy-convert INT->STRING.
                execute("ALTER TABLE pt ALTER COLUMN val TYPE STRING");
                drainWalQueue();

                // Partition 2024-01-02: val inserted as STRING natively, then parquet-encoded
                // as STRING. This partition does NOT need any conversion at read time.
                execute("INSERT INTO pt VALUES ('hello-from-p2', '2024-01-02T00:00:00.000000Z')");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
                drainWalQueue();

                // Baseline: a straight SELECT must see the correctly-converted strings on both partitions.
                assertQuery("SELECT * FROM pt ORDER BY ts").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                        "val\tts\n42\t2024-01-01T00:00:00.000000Z\nhello-from-p2\t2024-01-02T00:00:00.000000Z\n");

                // Now manually drive the cursor so we can interleave Record A iteration with
                // a Record B recordAt() on a different frame. This exercises the case where
                // the pool's sourceColumnTypes is rebuilt for a non-converting partition
                // while Record A is still pointing at the converting one; each record must
                // retain its own conversion mapping.
                try (
                        RecordCursorFactory factory = select("SELECT val FROM pt ORDER BY ts");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final Record recordA = cursor.getRecord();
                    final Record recordB = cursor.getRecordB();

                    Assert.assertTrue("expected at least one row", cursor.hasNext());

                    // Capture the rowId while recordA is positioned on partition 1 (INT->STRING).
                    final long rowIdPartition1 = recordA.getRowId();

                    // Read Record A before any Record B traversal -- must be the converted INT.
                    final StringSink beforeBSink = new StringSink();
                    beforeBSink.put(recordA.getStrA(0));
                    TestUtils.assertEquals("42", beforeBSink);

                    // Advance recordA to row in partition 2 to grab its rowId, then rewind
                    // recordA back to the partition-1 row via recordAt.
                    Assert.assertTrue("expected second row", cursor.hasNext());
                    final long rowIdPartition2 = recordA.getRowId();
                    cursor.recordAt(recordA, rowIdPartition1);
                    TestUtils.assertEquals("42", recordA.getStrA(0));

                    // Exercise the aliasing path: navigate recordB to partition 2.
                    // This rebuilds the pool's sourceColumnTypes in place with partition 2's
                    // mapping (no conversion). A correct implementation must not let this
                    // rebuild leak into Record A's view; an aliased reference would silently
                    // overwrite the state Record A relies on.
                    cursor.recordAt(recordB, rowIdPartition2);
                    TestUtils.assertEquals("hello-from-p2", recordB.getStrA(0));

                    // Re-read Record A: it is still anchored at partition 1 (rowIdPartition1)
                    // which needs INT->STRING conversion. The pool must give each record a
                    // private sourceColumnTypes snapshot; sharing a single array between
                    // Record A and Record B would let partition 2's "no conversion" mapping
                    // clobber Record A's view and the read would return garbage.
                    final CharSequence afterB = recordA.getStrA(0);
                    final String afterBStr = afterB == null ? "<null>" : afterB.toString();
                    TestUtils.assertEquals(
                            "Record A must still see partition 1's INT->STRING conversion "
                                    + "after Record B navigated to a non-converting partition. "
                                    + "Got: '" + afterBStr + "'",
                            "42",
                            afterBStr
                    );
                }
            } finally {
                tryDrop("pt");
            }
        });
    }

    /**
     * Control: fixed-to-fixed (INT-&gt;LONG) over a converted parquet column must still produce
     * correct SAMPLE BY first()/last() results (matching the native control nt).
     */
    @Test
    public void testSampleByFirstLastFixedToFixedOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertSampleByFirstLastParity(
                "INT", "(x * 3)::INT", "LONG",
                "SELECT ts, sym, first(val) fv, last(val) lv FROM $T WHERE sym = 's1' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION"
        ));
    }

    /**
     * No-projected-symbol variant: the projection omits {@code sym}, so the factory is chosen via
     * the {@code symbolColIndex == -1} branch.
     */
    @Test
    public void testSampleByFirstLastNoProjectedSymbolStrToIntOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertSampleByFirstLastParity(
                "STRING", "(x * 3)::STRING", "INT",
                "SELECT ts, first(val) fv, last(val) lv FROM $T WHERE sym = 's1' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION"
        ));
    }

    /**
     * STRING-to-DOUBLE direction.
     */
    @Test
    public void testSampleByFirstLastStrToDoubleOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertSampleByFirstLastParity(
                "STRING", "(x * 3)::STRING", "DOUBLE",
                "SELECT ts, sym, first(val) fv, last(val) lv FROM $T WHERE sym = 's1' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION"
        ));
    }

    /**
     * SampleByFirstLastRecordCursorFactory reads aggregate columns by raw page address, so a
     * parquet column ALTERed STRING-&gt;INT (kept in its source var layout and converted lazily on
     * read) must still produce correct first()/last() values. The factory is chosen for
     * {@code SAMPLE BY ... ALIGN TO FIRST OBSERVATION} with a single indexed-symbol filter;
     * {@code ALIGN TO CALENDAR} uses the keyed GroupBy instead. Differential against native control
     * {@code nt}.
     */
    @Test
    public void testSampleByFirstLastStrToIntOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertSampleByFirstLastParity(
                "STRING", "(x * 3)::STRING", "INT",
                "SELECT ts, sym, first(val) fv, last(val) lv FROM $T WHERE sym = 's1' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION"
        ));
    }

    /**
     * STRING-to-LONG direction (8-byte raw read).
     */
    @Test
    public void testSampleByFirstLastStrToLongOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertSampleByFirstLastParity(
                "STRING", "(x * 3)::STRING", "LONG",
                "SELECT ts, sym, first(val) fv, last(val) lv FROM $T WHERE sym = 's1' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION"
        ));
    }

    /**
     * SYMBOL-to-INT direction: parquet stores the symbol as a UTF-8 BYTE_ARRAY decoded lazily as
     * VARCHAR_SLICE, so the raw fixed read must not be taken over the var-slice page.
     */
    @Test
    public void testSampleByFirstLastSymbolToIntOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertSampleByFirstLastParity(
                "SYMBOL", "(x * 3)::STRING", "INT",
                "SELECT ts, sym, first(val) fv, last(val) lv FROM $T WHERE sym = 's1' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION"
        ));
    }

    /**
     * VARCHAR-to-INT direction: VARCHAR decodes to an aux+data split layout, so the raw fixed read
     * must not be taken over the aux page.
     */
    @Test
    public void testSampleByFirstLastVarcharToIntOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> assertSampleByFirstLastParity(
                "VARCHAR", "(x * 3)::STRING", "INT",
                "SELECT ts, sym, first(val) fv, last(val) lv FROM $T WHERE sym = 's1' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION"
        ));
    }

    @Test
    public void testShortToIntColumnTopAndNull() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE nt (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE pt (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Pre-ADD-COLUMN rows -- column_top region for column 'v'.
                String preAdd = """
                        INSERT INTO %s(ts) VALUES
                        ('2020-01-01T00:00:00.000Z'),
                        ('2020-01-01T04:00:00.000Z'),
                        ('2020-01-02T00:00:00.000Z')""";
                execute(preAdd.formatted("nt"));
                execute(preAdd.formatted("pt"));
                drainWalQueue();

                execute("ALTER TABLE nt ADD COLUMN v SHORT");
                execute("ALTER TABLE pt ADD COLUMN v SHORT");
                drainWalQueue();

                String postAdd = """
                        INSERT INTO %s(v, ts) VALUES
                        (5, '2020-01-01T08:00:00.000Z'),
                        (NULL, '2020-01-01T12:00:00.000Z'),
                        (7, '2020-01-01T16:00:00.000Z'),
                        (8, '2020-01-01T20:00:00.000Z'),
                        (9, '2020-01-02T12:00:00.000Z')""";
                execute(postAdd.formatted("nt"));
                execute(postAdd.formatted("pt"));
                drainWalQueue();

                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
                drainWalQueue();

                execute("ALTER TABLE nt ALTER COLUMN v TYPE INT");
                execute("ALTER TABLE pt ALTER COLUMN v TYPE INT");
                drainWalQueue();

                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
            } finally {
                tryDrop("nt");
                tryDrop("pt");
            }
        });
    }

    @Test
    public void testShortToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    (1, '2024-01-01T00:00:01.000000Z'),
                    (0, '2024-01-01T00:00:02.000000Z'),
                    (-1, '2024-01-01T00:00:03.000000Z'),
                    (32767, '2024-01-01T00:00:04.000000Z'),
                    (-32768, '2024-01-01T00:00:05.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "INT", "LONG", "DATE", "TIMESTAMP", "FLOAT", "DOUBLE"}) {
                assertConversion("SHORT", target, values);
            }
        });
    }

    @Test
    public void testStringToFixed() throws Exception {
        assertMemoryLeak(() -> {
            // Var→Fixed path: Rust decodes the source var type;
            // Java parses to fixed after decode.
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                // Both paths must agree on the ASCII case-fold of "true" and on
                // returning false for any other input (numbers, empty, NULL).
                assertConversion(source, "BOOLEAN", """
                        ('true', '2024-01-01T00:00:01.000000Z'),
                        ('True', '2024-01-01T00:00:02.000000Z'),
                        ('TRUE', '2024-01-01T00:00:03.000000Z'),
                        ('TrUe', '2024-01-01T00:00:04.000000Z'),
                        ('false', '2024-01-01T00:00:05.000000Z'),
                        ('False', '2024-01-01T00:00:06.000000Z'),
                        ('FALSE', '2024-01-01T00:00:07.000000Z'),
                        ('1', '2024-01-01T00:00:08.000000Z'),
                        ('0', '2024-01-01T00:00:09.000000Z'),
                        ('', '2024-01-01T00:00:10.000000Z'),
                        (NULL, '2024-01-01T00:00:11.000000Z')""");

                assertConversion(source, "BYTE", """
                        ('42', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-1', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "SHORT", """
                        ('42', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-1', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "INT", """
                        ('42', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-1', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "LONG", """
                        ('42', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-1', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "FLOAT", """
                        ('1.5', '2024-01-01T00:00:01.000000Z'),
                        ('0.0', '2024-01-01T00:00:02.000000Z'),
                        ('-1.5', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "DOUBLE", """
                        ('1.5', '2024-01-01T00:00:01.000000Z'),
                        ('0.0', '2024-01-01T00:00:02.000000Z'),
                        ('-1.5', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                assertConversion(source, "DATE", """
                        ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");

                assertConversion(source, "TIMESTAMP", """
                        ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");

                assertConversion(source, "TIMESTAMP_NS", """
                        ('2020-06-15T12:30:00.123456789Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000000001Z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");

                assertConversion(source, "CHAR", """
                        ('a', '2024-01-01T00:00:01.000000Z'),
                        ('z', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");

                // Sweep all six decimal widths (DECIMAL8/16/32/64/128/256, precisions
                // 2/4/9/18/38/76). Each list includes an overflow input whose magnitude
                // exceeds the target precision; both the lazy parquet read path and the
                // CONVERT-PARTITION-TO-NATIVE materialized path must agree on NULL for
                // that row (NumericException in decimal.ofString -> DECIMAL NULL).
                assertConversion(source, "DECIMAL(2, 1)", """
                        ('1.2', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-1.5', '2024-01-01T00:00:03.000000Z'),
                        ('999', '2024-01-01T00:00:04.000000Z'),
                        (NULL, '2024-01-01T00:00:05.000000Z')""");

                assertConversion(source, "DECIMAL(4, 2)", """
                        ('12.34', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-9.99', '2024-01-01T00:00:03.000000Z'),
                        ('99999', '2024-01-01T00:00:04.000000Z'),
                        (NULL, '2024-01-01T00:00:05.000000Z')""");

                assertConversion(source, "DECIMAL(9, 3)", """
                        ('123456.789', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-999.999', '2024-01-01T00:00:03.000000Z'),
                        ('9999999999', '2024-01-01T00:00:04.000000Z'),
                        (NULL, '2024-01-01T00:00:05.000000Z')""");

                assertConversion(source, "DECIMAL(18, 4)", """
                        ('12345.6789', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-99.9999', '2024-01-01T00:00:03.000000Z'),
                        ('99999999999999999999', '2024-01-01T00:00:04.000000Z'),
                        (NULL, '2024-01-01T00:00:05.000000Z')""");

                assertConversion(source, "DECIMAL(38, 4)", """
                        ('1234567890123456789012345678901234.5678', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-99.9999', '2024-01-01T00:00:03.000000Z'),
                        ('999999999999999999999999999999999999999', '2024-01-01T00:00:04.000000Z'),
                        (NULL, '2024-01-01T00:00:05.000000Z')""");

                assertConversion(source, "DECIMAL(76, 4)", """
                        ('123456789012345678901234567890123456789012345678901234567890123456789012.5678', '2024-01-01T00:00:01.000000Z'),
                        ('0', '2024-01-01T00:00:02.000000Z'),
                        ('-99.9999', '2024-01-01T00:00:03.000000Z'),
                        ('99999999999999999999999999999999999999999999999999999999999999999999999999999', '2024-01-01T00:00:04.000000Z'),
                        (NULL, '2024-01-01T00:00:05.000000Z')""");

                assertConversion(source, "IPv4", """
                        ('192.168.1.1', '2024-01-01T00:00:01.000000Z'),
                        ('10.0.0.1', '2024-01-01T00:00:02.000000Z'),
                        ('255.255.255.255', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");

                // →Symbol requires the pre-pass to convert parquet→native
                // before building the symbol map.
                assertConversion(source, "SYMBOL", """
                        ('hello', '2024-01-01T00:00:01.000000Z'),
                        ('world', '2024-01-01T00:00:02.000000Z'),
                        (NULL, '2024-01-01T00:00:03.000000Z')""");

                // UUID cases include malformed inputs: non-UUID strings must produce NULL on
                // the lazy parquet path, matching the native str2Uuid converter (which calls
                // Uuid.checkDashesAndLength first and treats failure as null). Without the
                // length/dash pre-check, Uuid.parseHi/parseLo index past the end of short
                // strings and raise IndexOutOfBoundsException, which the NumericException-only
                // catch in convertVarToUuidHi/Lo does not handle, propagating out of the
                // Record API.
                assertConversion(source, "UUID", """
                        ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                        ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                        ('not-a-uuid', '2024-01-01T00:00:03.000000Z'),
                        ('', '2024-01-01T00:00:04.000000Z'),
                        ('short', '2024-01-01T00:00:05.000000Z'),
                        ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11-extra', '2024-01-01T00:00:06.000000Z'),
                        ('zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz', '2024-01-01T00:00:07.000000Z'),
                        (NULL, '2024-01-01T00:00:08.000000Z')""");
            }
        });
    }

    @Test
    public void testStringToUuidHalfCorrupt() throws Exception {
        assertMemoryLeak(() -> {
            for (String source : new String[]{"STRING", "VARCHAR", "SYMBOL"}) {
                assertConversion(source, "UUID", """
                        ('a0eebc99-9c0b-4ef8-zzzz-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                        ('zzzzzzzz-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:02.000000Z'),
                        ('a0eebc99-9c0b-4ef8-bb6d-zzzzzzzzzzzz', '2024-01-01T00:00:03.000000Z'),
                        ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:04.000000Z')""");
            }
        });
    }

    @Test
    public void testStringToUuidLazyScanAcrossParquetPartitions() throws Exception {
        // Two parquet partitions form two frames whose in-frame row indexes both start at 0, so the
        // lazy scan reuses the same (rowIndex, columnIndex) for different data across the frame
        // switch. PageFrameMemoryRecord caches the per-row var->UUID parse keyed by (frameIndex,
        // rowIndex, columnIndex) -- frameIndex is what keeps a partition-2 read from serving a
        // partition-1 cached value. The half-corrupt value at partition 2 row 0 must read NULL
        // (matching native), and the distinct valid UUIDs across partitions must each round-trip.
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE nt (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE pt (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                String rows = """
                        ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                        ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                        ('a0eebc99-9c0b-4ef8-zzzz-6bb9bd380a11', '2024-01-02T00:00:01.000000Z'),
                        ('22222222-2222-2222-2222-222222222222', '2024-01-02T00:00:02.000000Z')""";
                execute("INSERT INTO nt VALUES " + rows);
                execute("INSERT INTO pt VALUES " + rows);
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
                drainWalQueue();
                execute("ALTER TABLE nt ALTER COLUMN val TYPE UUID");
                execute("ALTER TABLE pt ALTER COLUMN val TYPE UUID");
                drainWalQueue();
                // Lazy read: both partitions are still parquet, scanned frame by frame.
                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
                // Eager rewrite, then re-read against native files.
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-02'");
                drainWalQueue();
                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
            } finally {
                tryDrop("nt");
                tryDrop("pt");
            }
        });
    }

    /**
     * Pins parity between the lazy per-row parquet read path
     * ({@code PageFrameMemoryRecord.convertVarToXxx}) and the eager native rewrite path
     * ({@code ColumnTypeConverter}) for adversarial inputs that one path might silently
     * normalize while the other rejects. Whitespace, empty strings, and non-numeric text
     * must produce the same sentinel on both paths; date strings with timezone offsets
     * or no time component must either both succeed (same UTC value) or both fail to NULL.
     * If a future change adds a {@code .trim()} or pre-normalization on only one side,
     * this test catches the drift.
     */
    @Test
    public void testStringToFixedAdversarialInputs() throws Exception {
        assertMemoryLeak(() -> {
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                String numericValues = """
                        (' 42 ', '2024-01-01T00:00:01.000000Z'),
                        ('',     '2024-01-01T00:00:02.000000Z'),
                        ('abc',  '2024-01-01T00:00:03.000000Z'),
                        (NULL,   '2024-01-01T00:00:04.000000Z')""";
                assertConversion(source, "BYTE", numericValues);
                assertConversion(source, "SHORT", numericValues);
                assertConversion(source, "INT", numericValues);
                assertConversion(source, "LONG", numericValues);
                assertConversion(source, "FLOAT", numericValues);
                assertConversion(source, "DOUBLE", numericValues);

                String dateValues = """
                        ('2020-06-15T12:00:00+01:00', '2024-01-01T00:00:01.000000Z'),
                        ('2020-06-15',                '2024-01-01T00:00:02.000000Z'),
                        ('',                          '2024-01-01T00:00:03.000000Z'),
                        ('abc',                       '2024-01-01T00:00:04.000000Z'),
                        (NULL,                        '2024-01-01T00:00:05.000000Z')""";
                assertConversion(source, "DATE", dateValues);
                assertConversion(source, "TIMESTAMP", dateValues);
                assertConversion(source, "TIMESTAMP_NS", dateValues);
            }
        });
    }

    @Test
    public void testStringToIpv4NullThroughConvertToNative() throws Exception {
        assertMemoryLeak(() -> {
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                try {
                    execute("CREATE TABLE pt (val " + source + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("""
                            INSERT INTO pt VALUES
                            ('192.168.1.1', '2024-01-01T00:00:01.000000Z'),
                            ('10.0.0.1',    '2024-01-01T00:00:02.000000Z'),
                            (NULL,          '2024-01-01T00:00:03.000000Z')""");
                    drainWalQueue();
                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();
                    execute("ALTER TABLE pt ALTER COLUMN val TYPE IPv4");
                    drainWalQueue();
                    // Materializes the var->fixed conversion through O3PartitionJob, so
                    // the bytes on disk are whatever writeFixedNull wrote for the NULL row.
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    drainWalQueue();

                    assertQuery("SELECT val, ts FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                            """
                                    val\tts
                                    192.168.1.1\t2024-01-01T00:00:01.000000Z
                                    10.0.0.1\t2024-01-01T00:00:02.000000Z
                                    \t2024-01-01T00:00:03.000000Z
                                    """);
                    assertQuery("SELECT count() c FROM pt WHERE val IS NULL").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                            "c\n1\n");
                } finally {
                    tryDrop("pt");
                }
            }
        });
    }

    /**
     * Pins lazy VARCHAR/STRING -> TIMESTAMP_NS conversion on a parquet partition.
     * The partition stays in parquet (no CONVERT PARTITION TO NATIVE), so reads
     * go through PageFrameMemoryRecord.convertVarToTimestamp. That path must
     * dispatch to NanosTimestampDriver based on the target column type; the prior
     * implementation hard-coded MicrosTimestampDriver, returning values 1000x
     * too small.
     */
    @Test
    public void testStringToTimestampNanoLazyParquet() throws Exception {
        assertMemoryLeak(() -> {
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                try {
                    execute("CREATE TABLE pt (val " + source + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("""
                            INSERT INTO pt VALUES
                            ('2020-06-15T12:30:00.123456789Z', '2024-01-01T00:00:01.000000Z'),
                            ('1970-01-01T00:00:00.000000001Z', '2024-01-01T00:00:02.000000Z'),
                            (NULL, '2024-01-01T00:00:03.000000Z')""");
                    drainWalQueue();
                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();
                    execute("ALTER TABLE pt ALTER COLUMN val TYPE TIMESTAMP_NS");
                    drainWalQueue();

                    assertQuery("SELECT val, ts FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                            """
                                    val\tts
                                    2020-06-15T12:30:00.123456789Z\t2024-01-01T00:00:01.000000Z
                                    1970-01-01T00:00:00.000000001Z\t2024-01-01T00:00:02.000000Z
                                    \t2024-01-01T00:00:03.000000Z
                                    """);
                } finally {
                    tryDrop("pt");
                }
            }
        });
    }

    @Test
    public void testStringToUuidMalformedThroughConvertToNative() throws Exception {
        assertMemoryLeak(() -> {
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                try {
                    execute("CREATE TABLE pt (val " + source + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("""
                            INSERT INTO pt VALUES
                            ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                            ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                            ('not-a-uuid',                           '2024-01-01T00:00:03.000000Z'),
                            ('',                                     '2024-01-01T00:00:04.000000Z'),
                            ('short',                                '2024-01-01T00:00:05.000000Z'),
                            ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11-extra', '2024-01-01T00:00:06.000000Z'),
                            ('zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz', '2024-01-01T00:00:07.000000Z'),
                            (NULL,                                   '2024-01-01T00:00:08.000000Z')""");
                    drainWalQueue();
                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();
                    execute("ALTER TABLE pt ALTER COLUMN val TYPE UUID");
                    drainWalQueue();
                    // Materializes the var->fixed conversion through
                    // O3PartitionJob.writeFixedParsedValue. parseLo/parseHi only
                    // read positions [0, 36); writeFixedParsedValue must reject
                    // values longer than 36 characters as NULL, otherwise row 6
                    // ('...-extra') would round-trip as the prefix UUID with the
                    // trailing characters silently dropped.
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    drainWalQueue();

                    assertQuery("SELECT val, ts FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                            """
                                    val\tts
                                    a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t2024-01-01T00:00:01.000000Z
                                    11111111-1111-1111-1111-111111111111\t2024-01-01T00:00:02.000000Z
                                    \t2024-01-01T00:00:03.000000Z
                                    \t2024-01-01T00:00:04.000000Z
                                    \t2024-01-01T00:00:05.000000Z
                                    \t2024-01-01T00:00:06.000000Z
                                    \t2024-01-01T00:00:07.000000Z
                                    \t2024-01-01T00:00:08.000000Z
                                    """);
                    assertQuery("SELECT count() c FROM pt WHERE val IS NULL").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                            "c\n6\n");
                } finally {
                    tryDrop("pt");
                }
            }
        });
    }

    @Test
    public void testStringToVarchar() throws Exception {
        // Covers the UTF-8/UTF-16 translation across the conversion boundary. Each width
        // class exercises a distinct encoding path and each is a canonical mojibake source
        // if the lazy path (readVarValueForConversion -> asAsciiCharSequence) is hit:
        //   - 2-byte UTF-8 (e, n, u, a with diacritics): 0xC3 0xXX pairs would render as
        //     two Latin-1 chars (e.g. UTF-8 'e-acute' 0xC3 0xA9 -> U+00C3 U+00A9 'A-tilde,
        //     copyright')
        //   - 3-byte UTF-8 (Cyrillic, CJK): 0xE0-0xEF 0x80-0xBF 0x80-0xBF triples
        //   - 4-byte UTF-8 (supplementary plane, emoji): surrogate pair round-trip
        //   - mixed ASCII + non-ASCII in one value, to catch partial-decode bugs
        assertMemoryLeak(() -> assertConversion("STRING", "VARCHAR", """
                ('hello', '2024-01-01T00:00:01.000000Z'),
                ('', '2024-01-01T00:00:02.000000Z'),
                ('тест', '2024-01-01T00:00:03.000000Z'),
                ('café naïve über', '2024-01-01T00:00:04.000000Z'),
                ('日本語 中文 한글', '2024-01-01T00:00:05.000000Z'),
                ('emoji \ud83e\udd86 \ud83d\ude00 mixed', '2024-01-01T00:00:06.000000Z'),
                ('ascii then é', '2024-01-01T00:00:07.000000Z'),
                (NULL, '2024-01-01T00:00:08.000000Z')"""));
    }

    @Test
    public void testSymbolToOtherTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Parquet stores SYMBOL as UTF-8 BYTE_ARRAY, decoded as VARCHAR by Rust.
            String stringValues = """
                    ('hello', '2024-01-01T00:00:01.000000Z'),
                    ('world', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            assertConversion("SYMBOL", "STRING", stringValues);
            assertConversion("SYMBOL", "VARCHAR", stringValues);

            // SYMBOL→Fixed: the pre-pass converts parquet→native,
            // then normal Symbol→X conversion runs (parses symbol string to target).
            String smallIntValues = """
                    ('42', '2024-01-01T00:00:01.000000Z'),
                    ('0', '2024-01-01T00:00:02.000000Z'),
                    ('-1', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("SYMBOL", "BYTE", smallIntValues);
            assertConversion("SYMBOL", "SHORT", smallIntValues);
            assertConversion("SYMBOL", "INT", smallIntValues);
            assertConversion("SYMBOL", "LONG", smallIntValues);

            String floatValues = """
                    ('3.14', '2024-01-01T00:00:01.000000Z'),
                    ('0.0', '2024-01-01T00:00:02.000000Z'),
                    ('-1.5', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            assertConversion("SYMBOL", "FLOAT", floatValues);
            assertConversion("SYMBOL", "DOUBLE", floatValues);

            assertConversion("SYMBOL", "BOOLEAN", """
                    ('true', '2024-01-01T00:00:01.000000Z'),
                    ('false', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "CHAR", """
                    ('a', '2024-01-01T00:00:01.000000Z'),
                    ('Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "IPV4", """
                    ('192.168.1.1', '2024-01-01T00:00:01.000000Z'),
                    ('10.0.0.1', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "UUID", """
                    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "DATE", """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");

            assertConversion("SYMBOL", "TIMESTAMP", """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    (NULL, '2024-01-01T00:00:02.000000Z')""");

            assertConversion("SYMBOL", "TIMESTAMP_NS", """
                    ('2020-06-15T12:30:00.123456789Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000000001Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""");
        });
    }

    /**
     * Pins the chained-conversion guard in ConvertOperatorImpl.isParquetStorageCompatible:
     * TIMESTAMP -> TIMESTAMP_NS -> TIMESTAMP on a parquet partition where the parquet
     * itself stays TIMESTAMP (us) throughout. After the second hop the metadata tag is
     * TIMESTAMP again, matching the parquet tag, but a naive tag-only equality check
     * would let the lazy decoder return raw us values as if they were ns (off by 1000x).
     * The guard must instead notice the prior conversion on the column metadata and
     * force a parquet -> native rewrite before the second hop, so the round-trip is
     * lossless. The control table nt (no parquet) must agree with pt at every step.
     */
    @Test
    public void testTimestampChainedThroughTimestampNanoOnParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE nt (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE pt (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                String values = """
                        INSERT INTO $T VALUES
                        ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                        ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                        ('2020-06-15T12:30:00.999999Z', '2024-01-01T00:00:03.000000Z'),
                        (NULL,                          '2024-01-01T00:00:04.000000Z')""";
                execute(values.replace("$T", "nt"));
                execute(values.replace("$T", "pt"));
                drainWalQueue();

                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                // Hop 1: TIMESTAMP -> TIMESTAMP_NS on both tables. On pt the parquet
                // stays TIMESTAMP (us); the lazy decoder scales each read by 1000 via
                // post_convert's date/timestamp branch.
                execute("ALTER TABLE nt ALTER COLUMN val TYPE TIMESTAMP_NS");
                execute("ALTER TABLE pt ALTER COLUMN val TYPE TIMESTAMP_NS");
                drainWalQueue();
                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");

                // Hop 2: TIMESTAMP_NS -> TIMESTAMP on both tables. The pt metadata now
                // says TIMESTAMP again while the parquet still stores TIMESTAMP (us),
                // so the metadata tag matches the parquet tag. isParquetStorageCompatible
                // must still detect that the column has a prior conversion recorded
                // (getReplacingIndex() >= 0) and force a parquet -> native rewrite
                // before the second hop; otherwise the lazy path would return raw us
                // values as ns values (off by 1000x) or the table would suspend.
                execute("ALTER TABLE nt ALTER COLUMN val TYPE TIMESTAMP");
                execute("ALTER TABLE pt ALTER COLUMN val TYPE TIMESTAMP");
                drainWalQueue();
                assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");

                // Final state: original microsecond values, round-trip lossless.
                assertQuery("SELECT val, ts FROM pt ORDER BY ts").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                        """
                                val\tts
                                2020-06-15T12:30:00.123456Z\t2024-01-01T00:00:01.000000Z
                                1970-01-01T00:00:00.000000Z\t2024-01-01T00:00:02.000000Z
                                2020-06-15T12:30:00.999999Z\t2024-01-01T00:00:03.000000Z
                                \t2024-01-01T00:00:04.000000Z
                                """);
            } finally {
                tryDrop("nt");
                tryDrop("pt");
            }
        });
    }

    /**
     * Direct fixed-to-fixed conversions from TIMESTAMP_NS source on a parquet partition.
     * Exercises the post-decode nano-scaling branch in PageFrameMemoryPool.openParquet
     * (the bit-24 encoded flag path) for both narrowing into Timestamp/Date (divide by
     * 1000 and 1_000_000) and pass-through into Long. NULL preservation across the nano
     * scale is verified by the LONG_MIN sentinel surviving the divide.
     */
    @Test
    public void testTimestampNanoToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // TIMESTAMP_NS stores nanoseconds since epoch.
            // Sub-microsecond precision is intentionally truncated by the ÷1000 to
            // TIMESTAMP and the ÷1_000_000 to DATE; both lazy and native paths
            // truncate identically.
            String values = """
                    ('2020-06-15T12:30:00.123456789Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000000001Z', '2024-01-01T00:00:02.000000Z'),
                    ('2020-06-15T12:30:00.999999999Z', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            for (String target : new String[]{"LONG", "TIMESTAMP", "DATE"}) {
                assertConversion("TIMESTAMP_NS", target, values);
            }
        });
    }

    @Test
    public void testTimestampToOtherFixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // TIMESTAMP stores microseconds since epoch.
            // '2020-06-15T12:30:00.123456Z' = 1_592_224_200_123_456 us
            // TIMESTAMP -> DATE divides by 1000 (us -> ms), losing sub-ms precision.
            // TIMESTAMP -> TIMESTAMP_NS multiplies by 1000 (us -> ns) via the
            // post-decode nano-scaling branch in PageFrameMemoryPool.openParquet.
            // NULL (LONG_MIN) is preserved across scaling (checked before divide).
            // Sub-millisecond precision: .000001Z = 1 microsecond, .999999Z = max micros.
            String values = """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                    ('2020-06-15T12:30:00.000001Z', '2024-01-01T00:00:03.000000Z'),
                    ('2020-06-15T12:30:00.999999Z', '2024-01-01T00:00:04.000000Z'),
                    (NULL, '2024-01-01T00:00:05.000000Z')""";
            for (String target : new String[]{"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP_NS", "FLOAT", "DOUBLE"}) {
                assertConversion("TIMESTAMP", target, values);
            }
        });
    }

    @Test
    public void testUuidToStringTypes() throws Exception {
        assertMemoryLeak(() -> {
            String values = """
                    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            for (String target : new String[]{"STRING", "VARCHAR"}) {
                assertConversion("UUID", target, values);
            }
        });
    }

    /**
     * Var-to-fixed counterpart of {@link #testIntToStringConvertToNativeMultiRowGroup}.
     * <p>
     * The var-to-fixed arm of {@code TableWriter.produceNativeFromParquet} converts each row
     * group's freshly decoded var buffer to fixed values with
     * {@code O3PartitionJob.convertVarColumnToFixed} and appends the result to the destination
     * fixed file. {@code convertVarColumnToFixed} reads every value through the row group's own
     * aux vector, whose entry offsets are relative to that row group's local data buffer (the
     * first entry of each row group starts at offset 0). If a later row group's decode were
     * keyed off the wrong base -- a stale running offset, or reusing row group 0's data buffer --
     * rows beyond the first row group would parse the bytes of row group 0 and read back the
     * wrong integers.
     * <p>
     * The existing var-to-fixed coverage ({@link #testStringToFixed}) uses few enough rows that
     * the whole partition fits in one row group, so the per-row-group decode is never exercised.
     * This test forces a tiny row group size and uses 12 rows whose source strings have distinct
     * values and varying widths, so the partition spans three row groups. After
     * {@code ALTER COLUMN ... TYPE INT} and {@code CONVERT PARTITION TO NATIVE}, every row must
     * read back as the integer parsed from its own source string.
     */
    @Test
    public void testVarToFixedConvertToNativeMultiRowGroup() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                try {
                    execute("CREATE TABLE pt (val " + source + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    // 12 rows in one partition, row group size 4 => three row groups. Source widths
                    // vary within and across row groups so the per-row-group aux offsets are
                    // non-trivial; values are distinct so cross-row-group contamination would show
                    // up as a wrong integer.
                    execute("""
                            INSERT INTO pt VALUES
                            ('1',      '2024-01-01T00:00:01.000000Z'),
                            ('22',     '2024-01-01T00:00:02.000000Z'),
                            ('333',    '2024-01-01T00:00:03.000000Z'),
                            ('4444',   '2024-01-01T00:00:04.000000Z'),
                            ('50000',  '2024-01-01T00:00:05.000000Z'),
                            ('6',      '2024-01-01T00:00:06.000000Z'),
                            ('77',     '2024-01-01T00:00:07.000000Z'),
                            ('888',    '2024-01-01T00:00:08.000000Z'),
                            ('9999',   '2024-01-01T00:00:09.000000Z'),
                            ('101010', '2024-01-01T00:00:10.000000Z'),
                            ('11',     '2024-01-01T00:00:11.000000Z'),
                            (NULL,     '2024-01-01T00:00:12.000000Z')""");
                    drainWalQueue();

                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();

                    execute("ALTER TABLE pt ALTER COLUMN val TYPE INT");
                    drainWalQueue();

                    String expected = """
                            val\tts
                            1\t2024-01-01T00:00:01.000000Z
                            22\t2024-01-01T00:00:02.000000Z
                            333\t2024-01-01T00:00:03.000000Z
                            4444\t2024-01-01T00:00:04.000000Z
                            50000\t2024-01-01T00:00:05.000000Z
                            6\t2024-01-01T00:00:06.000000Z
                            77\t2024-01-01T00:00:07.000000Z
                            888\t2024-01-01T00:00:08.000000Z
                            9999\t2024-01-01T00:00:09.000000Z
                            101010\t2024-01-01T00:00:10.000000Z
                            11\t2024-01-01T00:00:11.000000Z
                            null\t2024-01-01T00:00:12.000000Z
                            """;

                    // Lazy parquet read: the partition is still parquet, decoded per row group on
                    // the fly. Pins multi-row-group correctness of the lazy var->fixed path.
                    assertQuery("SELECT val, ts FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);

                    // Eager rewrite: materializes the var->fixed conversion through
                    // produceNativeFromParquet, so subsequent reads hit the native files.
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    drainWalQueue();

                    assertQuery("SELECT val, ts FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
                } finally {
                    tryDrop("pt");
                }
            }
        });
    }

    /**
     * Var-to-var counterpart of {@link #testIntToStringConvertToNativeMultiRowGroup}, on the arm
     * of {@code TableWriter.produceNativeFromParquet} that appends Rust's per-row-group decode of
     * the target var format directly (STRING&lt;-&gt;VARCHAR).
     * <p>
     * That arm tracks {@code dataVecBytesWritten} across row groups and rebases each row group's
     * aux vector with {@code shiftCopyAuxVector(-dataVecBytesWritten, ...)} plus the leading-entry
     * trim -- the exact running-offset bookkeeping the fixed-to-var arm in
     * {@link #testIntToStringConvertToNativeMultiRowGroup} pins. Without it, aux entries for row
     * groups beyond the first carry offsets relative to the start of their own local data buffer
     * (zero for the first entry of each row group), so later row groups read back the bytes of
     * row group 0.
     * <p>
     * The existing var-to-var coverage ({@link #testStringToVarchar}, {@link #testVarcharToString})
     * fits the whole partition in one row group, so the running offset is never exercised. This
     * test forces a tiny row group size and uses 12 distinct values -- several wider than the
     * 9-byte VARCHAR inline limit ({@code VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED}) in
     * every row group, so the VARCHAR data vector accumulates a non-zero running offset across all
     * three row groups, and several multi-byte UTF-8 so a byte-vs-char offset confusion would
     * surface. After {@code ALTER COLUMN ... TYPE} and {@code CONVERT PARTITION TO NATIVE}, every
     * row must read back its own value.
     */
    @Test
    public void testVarToVarConvertToNativeMultiRowGroup() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            // Both directions go through the same append-decoded-var arm; the rendered text is
            // identical because var->var changes only the in-memory encoding, not the content.
            for (String[] pair : new String[][]{{"STRING", "VARCHAR"}, {"VARCHAR", "STRING"}}) {
                String source = pair[0];
                String target = pair[1];
                try {
                    execute("CREATE TABLE pt (val " + source + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    // 12 rows in one partition, row group size 4 => three row groups. Widths vary
                    // within and across row groups; every row group carries values longer than the
                    // 9-byte VARCHAR inline limit; non-ASCII spans the 2-, 3- and 4-byte UTF-8
                    // classes.
                    execute("""
                            INSERT INTO pt VALUES
                            ('alpha_value_1',     '2024-01-01T00:00:01.000000Z'),
                            ('b',                 '2024-01-01T00:00:02.000000Z'),
                            ('gamma_value_33',    '2024-01-01T00:00:03.000000Z'),
                            ('café',              '2024-01-01T00:00:04.000000Z'),
                            ('delta_value_5555',  '2024-01-01T00:00:05.000000Z'),
                            ('тест',              '2024-01-01T00:00:06.000000Z'),
                            ('epsilon_value_7',   '2024-01-01T00:00:07.000000Z'),
                            ('日本語',             '2024-01-01T00:00:08.000000Z'),
                            ('zeta_value_999999', '2024-01-01T00:00:09.000000Z'),
                            ('emoji 🦆 duck', '2024-01-01T00:00:10.000000Z'),
                            ('eta_value_11',      '2024-01-01T00:00:11.000000Z'),
                            (NULL,                '2024-01-01T00:00:12.000000Z')""");
                    drainWalQueue();

                    execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                    drainWalQueue();

                    execute("ALTER TABLE pt ALTER COLUMN val TYPE " + target);
                    drainWalQueue();

                    String expected = """
                            val\tts
                            alpha_value_1\t2024-01-01T00:00:01.000000Z
                            b\t2024-01-01T00:00:02.000000Z
                            gamma_value_33\t2024-01-01T00:00:03.000000Z
                            café\t2024-01-01T00:00:04.000000Z
                            delta_value_5555\t2024-01-01T00:00:05.000000Z
                            тест\t2024-01-01T00:00:06.000000Z
                            epsilon_value_7\t2024-01-01T00:00:07.000000Z
                            日本語\t2024-01-01T00:00:08.000000Z
                            zeta_value_999999\t2024-01-01T00:00:09.000000Z
                            emoji 🦆 duck\t2024-01-01T00:00:10.000000Z
                            eta_value_11\t2024-01-01T00:00:11.000000Z
                            \t2024-01-01T00:00:12.000000Z
                            """;

                    // Lazy parquet read: per-row-group decode on the fly.
                    assertQuery("SELECT val, ts FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);

                    // Eager rewrite: materializes the var->var append through produceNativeFromParquet,
                    // exercising the dataVecBytesWritten running offset across the three row groups.
                    execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                    drainWalQueue();

                    assertQuery("SELECT val, ts FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
                } finally {
                    tryDrop("pt");
                }
            }
        });
    }

    /**
     * Var-source mirror of {@link #testFixedWithAllEncodings}. STRING and VARCHAR columns
     * stored under each writer-supported parquet encoding must round-trip identically
     * through both the native and lazy-parquet conversion paths. This exercises the
     * encoding axis of {@code decode_byte_array_dispatch} in {@code decode.rs}:
     * <ul>
     *     <li>{@code (RleDictionary | PlainDictionary, _, String/Varchar)}</li>
     *     <li>{@code (DeltaLengthByteArray, _, String/Varchar)}</li>
     * </ul>
     * The Plain and DeltaByteArray dispatch arms only fire for externally produced parquet
     * (the QuestDB writer rejects {@code plain} on var-size columns and does not emit
     * {@code delta_byte_array}) and are out of scope for a writer-driven test.
     */
    @Test
    public void testVarTypesWithAllEncodings() throws Exception {
        assertMemoryLeak(() -> {
            String numericValues = """
                    ('42', '2024-01-01T00:00:01.000000Z'),
                    ('0', '2024-01-01T00:00:02.000000Z'),
                    ('-1', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            String floatValues = """
                    ('1.5', '2024-01-01T00:00:01.000000Z'),
                    ('0.0', '2024-01-01T00:00:02.000000Z'),
                    ('-1.5', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";
            String boolValues = """
                    ('true', '2024-01-01T00:00:01.000000Z'),
                    ('false', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String charValues = """
                    ('a', '2024-01-01T00:00:01.000000Z'),
                    ('Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String ipv4Values = """
                    ('192.168.1.1', '2024-01-01T00:00:01.000000Z'),
                    ('10.0.0.1', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String uuidValues = """
                    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-01-01T00:00:01.000000Z'),
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String dateValues = """
                    ('2020-06-15T12:00:00.000Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String tsValues = """
                    ('2020-06-15T12:30:00.123456Z', '2024-01-01T00:00:01.000000Z'),
                    ('1970-01-01T00:00:00.000000Z', '2024-01-01T00:00:02.000000Z'),
                    (NULL, '2024-01-01T00:00:03.000000Z')""";
            String unicodeValues = """
                    ('hello', '2024-01-01T00:00:01.000000Z'),
                    ('café', '2024-01-01T00:00:02.000000Z'),
                    ('日本語', '2024-01-01T00:00:03.000000Z'),
                    (NULL, '2024-01-01T00:00:04.000000Z')""";

            // Encodings the QuestDB writer can emit for var-size columns. Var-to-fixed and
            // var-to-var conversions both pass the SOURCE var type to the Rust decoder
            // (Java post-converts), so each combination lands on a different dispatch arm.
            // The writer rejects {@code plain} for var-size columns and never emits
            // {@code delta_binary_packed} for them, so we stick to the supported set.
            String[] varEncodings = {"default", "rle_dictionary", "delta_length_byte_array"};
            for (String source : new String[]{"STRING", "VARCHAR"}) {
                String otherVar = "STRING".equals(source) ? "VARCHAR" : "STRING";
                for (String encoding : varEncodings) {
                    // Var → Var transcode (UTF-16 ↔ UTF-8) across each encoding.
                    assertConversionWithEncoding(source, otherVar, unicodeValues, encoding);

                    // Var → Fixed: small integer family
                    for (String target : new String[]{"BYTE", "SHORT", "INT", "LONG"}) {
                        assertConversionWithEncoding(source, target, numericValues, encoding);
                    }
                    // Var → Fixed: float family
                    for (String target : new String[]{"FLOAT", "DOUBLE"}) {
                        assertConversionWithEncoding(source, target, floatValues, encoding);
                    }
                    // Var → Fixed: BOOLEAN (no null sentinel — NULL maps to false).
                    assertConversionWithEncoding(source, "BOOLEAN", boolValues, encoding);
                    // Var → Fixed: CHAR (single code unit). Multi-byte UTF-8 already covered
                    // by testVarcharToCharPreservesNonAsciiCodepoint.
                    assertConversionWithEncoding(source, "CHAR", charValues, encoding);
                    // Var → Fixed: IPv4
                    assertConversionWithEncoding(source, "IPV4", ipv4Values, encoding);
                    // Var → Fixed: UUID
                    assertConversionWithEncoding(source, "UUID", uuidValues, encoding);
                    // Var → Fixed: temporal types
                    assertConversionWithEncoding(source, "DATE", dateValues, encoding);
                    assertConversionWithEncoding(source, "TIMESTAMP", tsValues, encoding);
                }
            }
        });
    }

    /**
     * Same absolute-oracle approach for the O3PartitionJob.convertVarColumnToFixed
     * path, which is taken by CONVERT PARTITION TO NATIVE (and by O3 merge into a
     * parquet partition whose column was ALTERed to a fixed type). The materializer
     * must UTF-8 decode the source bytes before taking the first CHAR; routing the
     * bytes through {@code Utf8StringSink} + {@code asAsciiCharSequence()} would
     * map the multi-byte sequence for U+00E9 (0xC3 0xA9) to two Latin-1 chars
     * (U+00C3, U+00A9) and CHAR would materialize as U+00C3 on disk.
     * The differential assertConversion helper does not catch this because both
     * native and the lazy parquet read path use a proper UTF-8 decode, while
     * this materialization path is only reachable through the O3 merge / convert
     * pipeline.
     */
    @Test
    public void testVarcharToCharNonAsciiThroughConvertToNative() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO pt VALUES
                        ('a', '2024-01-01T00:00:01.000000Z'),
                        ('élite', '2024-01-01T00:00:02.000000Z'),
                        ('日本', '2024-01-01T00:00:03.000000Z'),
                        (NULL, '2024-01-01T00:00:04.000000Z')""");
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                execute("ALTER TABLE pt ALTER COLUMN val TYPE CHAR");
                drainWalQueue();
                // Materializes the var->fixed conversion through O3PartitionJob, so
                // the bytes on disk are whatever writeFixedParsedValue wrote. The
                // SELECT below reads native files, not the lazy parquet path.
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();

                assertQuery("SELECT val FROM pt ORDER BY ts").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                        "val\na\né\n日\n\n");
            } finally {
                tryDrop("pt");
            }
        });
    }

    /**
     * Asserts VARCHAR-&gt;CHAR conversion on a parquet partition against an absolute
     * oracle. This is a lazy conversion path: the parquet decoder hands back a
     * {@link io.questdb.std.str.Utf8Sequence} and Java takes the first char via
     * io.questdb.cairo.sql.PageFrameMemoryRecord#convertVarToChar, which reads
     * through {@code readVarValueForConversion -&gt; asAsciiCharSequence()}. The latter
     * exposes each raw UTF-8 byte as a char, so a non-ASCII value like 'é'
     * (UTF-8: 0xC3 0xA9) yields {@code charAt(0) == U+00C3} ('Ã') instead of the
     * correct U+00E9. The differential assertion {@code nt==pt} does NOT catch this
     * because io.questdb.cairo.ColumnTypeConverter#convertFromVarcharToFixed
     * uses the same {@code asAsciiCharSequence()} call, so native and parquet produce
     * the same mojibake. Fix: use a proper UTF-8-to-UTF-16 decoder.
     */
    @Test
    public void testVarcharToCharPreservesNonAsciiCodepoint() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO pt VALUES
                        ('a', '2024-01-01T00:00:01.000000Z'),
                        ('é', '2024-01-01T00:00:02.000000Z'),
                        ('日', '2024-01-01T00:00:03.000000Z')""");
                drainWalQueue();

                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                execute("ALTER TABLE pt ALTER COLUMN val TYPE CHAR");
                drainWalQueue();

                // Expected: the first UTF-16 code unit of each stored value.
                //   'a'      -> 'a'
                //   U+00E9   -> U+00E9
                //   U+65E5   -> U+65E5
                // The VARCHAR-to-CHAR materializer must decode UTF-8 before taking
                // the first code unit; treating the leading UTF-8 byte as a char
                // would map U+00E9 to U+00C3 (the C3 byte of the C3 A9 sequence).
                assertQuery("SELECT val FROM pt ORDER BY ts").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                        """
                                val
                                a
                                é
                                日
                                """);
            } finally {
                tryDrop("pt");
            }
        });
    }

    @Test
    public void testVarcharToString() throws Exception {
        // VARCHAR is stored as UTF-8 in parquet; STRING is UTF-16 in memory. Non-ASCII
        // must survive the decode intact. If the lazy path (convertVarToStr ->
        // readVarValueForConversion) is ever reached, asAsciiCharSequence() would expose
        // each UTF-8 byte as a char (Latin-1), producing mojibake: e.g. UTF-8 'e-acute'
        // 0xC3 0xA9 would become two UTF-16 code units U+00C3 U+00A9 ('A-tilde,
        // copyright') instead of the single 'e-acute'. Each width class exercises a
        // distinct UTF-8 decode path:
        //   - 2-byte UTF-8: Latin-1 supplement diacritics and Cyrillic
        //   - 3-byte UTF-8: CJK
        //   - 4-byte UTF-8: supplementary plane / emoji (surrogate pairs)
        //   - mixed ASCII + non-ASCII boundary within one value
        assertMemoryLeak(() -> assertConversion("VARCHAR", "STRING", """
                ('hello', '2024-01-01T00:00:01.000000Z'),
                ('', '2024-01-01T00:00:02.000000Z'),
                ('тест', '2024-01-01T00:00:03.000000Z'),
                ('café naïve über', '2024-01-01T00:00:04.000000Z'),
                ('日本語 中文 한글', '2024-01-01T00:00:05.000000Z'),
                ('emoji \ud83e\udd86 \ud83d\ude00 mixed', '2024-01-01T00:00:06.000000Z'),
                ('ascii then é', '2024-01-01T00:00:07.000000Z'),
                (NULL, '2024-01-01T00:00:08.000000Z')"""));
    }

    /**
     * Same absolute-oracle approach for VARCHAR-&gt;VARCHAR/STRING paths that go through
     * the lazy per-row Java conversion. The native path performs a proper UTF-8 decode,
     * so this test pins the expected behaviour regardless of what the peer native path does.
     */
    @Test
    public void testVarcharToStringPreservesNonAsciiUtf8() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE pt (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO pt VALUES
                        ('hello', '2024-01-01T00:00:01.000000Z'),
                        ('café naïve', '2024-01-01T00:00:02.000000Z'),
                        ('日本語', '2024-01-01T00:00:03.000000Z'),
                        ('emoji 🦆', '2024-01-01T00:00:04.000000Z')""");
                drainWalQueue();

                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();

                execute("ALTER TABLE pt ALTER COLUMN val TYPE STRING");
                drainWalQueue();

                // Expected: the stored UTF-8 bytes decoded as UTF-16 code points.
                // With asAsciiCharSequence(), each UTF-8 byte becomes a char and the
                // output is mojibake (e.g. 'é' -> 'Ã©').
                assertQuery("SELECT val FROM pt ORDER BY ts").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                        """
                                val
                                hello
                                café naïve
                                日本語
                                emoji 🦆
                                """);
            } finally {
                tryDrop("pt");
            }
        });
    }

    @Test
    public void testVectorizedGroupByOverAlteredParquetColumn() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE nt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("CREATE TABLE pt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                String insert = """
                        INSERT INTO $T
                        SELECT x::STRING AS val,
                               ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                               timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000) AS ts
                        FROM long_sequence(200)""";
                execute(insert.replace("$T", "nt"));
                execute(insert.replace("$T", "pt"));
                drainWalQueue();
                execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                execute("ALTER TABLE nt ALTER COLUMN val TYPE INT");
                execute("ALTER TABLE pt ALTER COLUMN val TYPE INT");
                drainWalQueue();

                // No WHERE clause + single symbol key + sum over a fixed-width column would
                // pick the vectorized Rosti GroupBy. The parquet-converted-column guard must
                // force the safe Async path instead (note: NOT "vectorized: true").
                String query = "SELECT sym, sum(val) s FROM pt GROUP BY sym ORDER BY sym";
                assertQuery(query).noLeakCheck().assertsPlan(
                        """
                                Encode sort light
                                  keys: [sym]
                                    Async Group By workers: 1
                                      keys: [sym]
                                      values: [sum(val)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: pt
                                """);

                assertSqlCursors(
                        "SELECT sym, sum(val) s FROM nt GROUP BY sym ORDER BY sym",
                        query
                );
            } finally {
                tryDrop("nt");
                tryDrop("pt");
            }
        });
    }

    /**
     * Mirror of {@link #assertAsyncFactoryParity(String)} for the reverse cast direction:
     * {@code val} starts as INT, the partition is converted to parquet, and {@code val}
     * is ALTERed to a var type ({@code STRING} or {@code VARCHAR}). On {@code pt} the
     * parquet keeps the INT storage so reads go through lazy fixed-to-var conversion,
     * setting {@code hasColumnTypeCasts()=true} on every parquet frame. The extra
     * {@code other LONG} column carries the WHERE predicate so {@code val} can remain
     * a non-filter column (in compacted layout under
     * {@link io.questdb.cairo.sql.PageFrameFilteredMemoryRecord}).
     */
    private void assertAsyncFactoryFixedToVarParity(String targetType, String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val INT, other LONG, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val INT, other LONG, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String insert = """
                    INSERT INTO $T
                    SELECT x::INT AS val,
                           (x * 7)::LONG AS other,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000) AS ts
                    FROM long_sequence(200)""";
            execute(insert.replace("$T", "nt"));
            execute(insert.replace("$T", "pt"));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE " + targetType);
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * Builds a native control table {@code nt} and a parquet-backed table {@code pt} with
     * identical data, then ALTERs {@code val} from STRING to INT on both. On {@code pt} the
     * parquet keeps the STRING storage so reads go through lazy var to fixed conversion,
     * setting {@code hasColumnTypeCasts()=true} on every parquet frame. The supplied
     * query template (with {@code $T} placeholder) runs against both tables and must
     * produce identical cursors.
     */
    private void assertAsyncFactoryParity(String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String insert = """
                    INSERT INTO $T
                    SELECT x::STRING AS val,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000) AS ts
                    FROM long_sequence(200)""";
            execute(insert.replace("$T", "nt"));
            execute(insert.replace("$T", "pt"));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE INT");
            execute("ALTER TABLE pt ALTER COLUMN val TYPE INT");
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * Mirror of {@link #assertAsyncFactoryFixedToVarParity(String, String)} for the reverse
     * cast direction: {@code val} starts as STRING, the partition is converted to parquet,
     * and {@code val} is ALTERed to a fixed-width type (e.g. {@code INT}). On {@code pt}
     * the parquet keeps the STRING storage so reads go through lazy var-to-fixed conversion,
     * setting {@code hasColumnTypeCasts()=true} on every parquet frame. The extra
     * {@code other LONG} column carries the WHERE predicate so {@code val} can remain
     * a non-filter column (in compacted layout under
     * {@link io.questdb.cairo.sql.PageFrameFilteredMemoryRecord}).
     */
    private void assertAsyncFactoryStrToFixedParity(String targetType, String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val STRING, other LONG, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val STRING, other LONG, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String insert = """
                    INSERT INTO $T
                    SELECT x::STRING AS val,
                           (x * 7)::LONG AS other,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000) AS ts
                    FROM long_sequence(200)""";
            execute(insert.replace("$T", "nt"));
            execute(insert.replace("$T", "pt"));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE " + targetType);
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * Mirror of {@link #assertAsyncFactoryStrToFixedParity(String, String)} for a wide-fixed
     * target ({@code UUID}): {@code val} starts as STRING holding canonical UUID text, the
     * partition is converted to parquet, then {@code val} is ALTERed to UUID. On {@code pt} the
     * parquet keeps the STRING storage, so a single-column {@code ORDER BY val ... LIMIT} routes
     * through the wide-fixed batch path {@code SortKeyEncoder.encodeFixedWideBatch}, exercising
     * its lazy var-to-fixed fallback. {@code nt} seeds the random UUIDs and {@code pt} copies them
     * so both tables hold identical data.
     */
    private void assertAsyncFactoryStrToUuidParity(String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val STRING, other LONG, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val STRING, other LONG, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO nt
                    SELECT rnd_uuid4()::STRING AS val,
                           (x * 7)::LONG AS other,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000) AS ts
                    FROM long_sequence(200)""");
            drainWalQueue();
            execute("INSERT INTO pt SELECT * FROM nt");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE UUID");
            execute("ALTER TABLE pt ALTER COLUMN val TYPE UUID");
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * Same as {@link #assertAsyncFactoryParity(String)} but also builds a shared native
     * {@code prices} table used as the right side of HORIZON JOIN / WINDOW JOIN queries.
     */
    private void assertAsyncJoinFactoryParity(String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE prices (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String insertLeft = """
                    INSERT INTO $T
                    SELECT x::STRING AS val,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000) AS ts
                    FROM long_sequence(100)""";
            execute(insertLeft.replace("$T", "nt"));
            execute(insertLeft.replace("$T", "pt"));
            execute("""
                    INSERT INTO prices
                    SELECT (x * 1.5) AS price,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 30_000_000) AS ts
                    FROM long_sequence(200)""");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE INT");
            execute("ALTER TABLE pt ALTER COLUMN val TYPE INT");
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
            tryDrop("prices");
        }
    }

    /**
     * Same as {@link #assertAsyncJoinFactoryParity(String)} but builds two native slave
     * tables ({@code bids} and {@code asks}) so MULTI HORIZON JOIN queries route through
     * the {@code AsyncMultiHorizonJoin(NotKeyed)RecordCursorFactory} pair.
     * <p>
     * Spreads data across many daily partitions on the master side. The compiled-filter
     * fallback under {@code hasColumnTypeCasts()} only fires when SelectivityStats
     * decides against late materialization (selectivity above 20% with at least two
     * recorded samples). A single-partition setup keeps every frame on the late-material
     * path, where {@code hasColumnTops()} already routes around the compiled filter and
     * the cast bug stays hidden. Spreading rows over ~20 days produces enough frames per
     * worker for the SelectivityStats EMA to disable late materialization on subsequent
     * frames.
     */
    private void assertAsyncMultiJoinFactoryParity(String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val STRING, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE bids (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE asks (price DOUBLE, sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String insertLeft = """
                    INSERT INTO $T
                    SELECT x::STRING AS val,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000_000L) AS ts
                    FROM long_sequence(200)""";
            execute(insertLeft.replace("$T", "nt"));
            execute(insertLeft.replace("$T", "pt"));
            execute("""
                    INSERT INTO bids
                    SELECT (x * 1.5) AS price,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 30_000_000_000L) AS ts
                    FROM long_sequence(400)""");
            execute("""
                    INSERT INTO asks
                    SELECT (x * 2.5) AS price,
                           ('s' || (x % 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 30_000_000_000L) AS ts
                    FROM long_sequence(400)""");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE INT");
            execute("ALTER TABLE pt ALTER COLUMN val TYPE INT");
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
            tryDrop("bids");
            tryDrop("asks");
        }
    }

    private void assertConversion(String sourceType, String targetType, String values) throws Exception {
        assertConversionWithEncoding(sourceType, targetType, values, null);
    }

    private void assertConversionWithEncoding(String sourceType, String targetType, String values, String encoding) throws Exception {
        try {
            // Create the table without 'val' and seed one row before adding the column, so
            // 'val' carries a column-top region in the 2024-01-01 partition. The seed row
            // sorts before every value row (which start at 00:00:01), so the parquet file
            // stores it as a def-level=0 (NULL) entry. This makes every conversion test also
            // assert that column-top rows materialise as the target type's NULL -- both on the
            // lazy parquet read and after CONVERT PARTITION TO NATIVE -- exactly as the native
            // ALTER path does. The remaining value rows still cover the non-column-top path.
            execute("CREATE TABLE nt (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            execute("INSERT INTO nt(ts) VALUES ('2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO pt(ts) VALUES ('2024-01-01T00:00:00.000000Z')");
            drainWalQueue();

            execute("ALTER TABLE nt ADD COLUMN val " + sourceType);
            execute("ALTER TABLE pt ADD COLUMN val " + sourceType);
            drainWalQueue();

            execute("INSERT INTO nt(val, ts) VALUES " + values);
            execute("INSERT INTO pt(val, ts) VALUES " + values);
            drainWalQueue();

            if (encoding != null) {
                execute("ALTER TABLE pt ALTER COLUMN val SET PARQUET(" + encoding + ")");
                drainWalQueue();
            }

            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("ALTER TABLE nt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();

            // First assertion: lazy parquet read path (pt partition is still parquet).
            assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");

            // Second assertion: eager rewrite path. CONVERT PARTITION TO NATIVE
            // materializes the lazy conversion through produceNativeFromParquet ->
            // O3PartitionJob.convertVarColumnToFixed / convertFixedColumnToString /
            // convertFixedColumnToVarchar (and the in-place var->var copy), so the
            // re-read goes against native files written by the eager kernel.
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * Shared body for the equi-JOIN-on-converted-key tests. Builds a native control {@code nt}
     * and a parquet {@code pt} with a {@code val} column, converts {@code pt}'s partition to
     * parquet, then ALTERs {@code val} from {@code sourceType} to {@code targetType} on both --
     * on {@code pt} the parquet keeps the source storage, so reads go through the lazy
     * conversion. A small native {@code dim} table holds the join key {@code k} (of
     * {@code targetType}) for the even values 2,4,...,100 only.
     * <p>
     * The query joins on the converted column itself ({@code t.val = d.k}), the case the rest of
     * the JOIN suite never exercises (it always joins on the unconverted {@code sym}). The join
     * driver must read each {@code pt} row's key through the lazy conversion before hashing/probing;
     * reading the raw parquet bytes of the source type would mis-key the match. Because {@code dim}
     * carries only even keys, an INNER join keeps the even-valued rows while a LEFT join also
     * surfaces the odd-valued rows with a NULL slave side. Parity against {@code nt} pins both.
     *
     * @param joinClause {@code "JOIN"} (inner) or {@code "LEFT JOIN"} (left outer).
     */
    private void assertEquiJoinOnConvertedKeyParity(String sourceType, String targetType, String joinClause) throws Exception {
        try {
            execute("CREATE TABLE nt (val " + sourceType + ", sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val " + sourceType + ", sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE dim (k " + targetType + ", label SYMBOL)");
            String insertLeft = """
                    INSERT INTO $T
                    SELECT x::%s AS val,
                           ('s' || (x %% 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000) AS ts
                    FROM long_sequence(100)""".formatted(sourceType);
            execute(insertLeft.replace("$T", "nt"));
            execute(insertLeft.replace("$T", "pt"));
            // Even keys only: 2,4,...,100. The converted target representation matches t.val
            // (e.g. STRING->INT yields INT 2; INT->STRING yields STRING "2").
            execute("""
                    INSERT INTO dim
                    SELECT (x * 2)::%s AS k, ('d' || (x * 2)::STRING)::SYMBOL AS label
                    FROM long_sequence(50)""".formatted(targetType));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE " + targetType);
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            String query = """
                    SELECT t.ts, t.val, t.sym, d.label
                    FROM $T t
                    %s dim d ON (t.val = d.k)
                    ORDER BY t.ts""".formatted(joinClause);
            assertSqlCursors(query.replace("$T", "nt"), query.replace("$T", "pt"));
        } finally {
            tryDrop("nt");
            tryDrop("pt");
            tryDrop("dim");
        }
    }

    /**
     * Runs a two-step (chained) {@code ALTER COLUMN TYPE} over a parquet partition, asserting
     * the parquet read (pt) matches the native conversion (nt) after each step and after a final
     * CONVERT PARTITION TO NATIVE. The first step reads the parquet lazily; the second step finds
     * a prior conversion, so the ConvertOperatorImpl pre-pass eagerly materialises the partition
     * to native before applying it. Mirrors {@link #assertConversionWithEncoding}.
     */
    private void assertTwoStepConversion(String sourceType, String midType, String targetType, String values) throws Exception {
        try {
            execute("CREATE TABLE nt (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

            execute("INSERT INTO nt(ts) VALUES ('2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO pt(ts) VALUES ('2024-01-01T00:00:00.000000Z')");
            drainWalQueue();

            execute("ALTER TABLE nt ADD COLUMN val " + sourceType);
            execute("ALTER TABLE pt ADD COLUMN val " + sourceType);
            drainWalQueue();

            execute("INSERT INTO nt(val, ts) VALUES " + values);
            execute("INSERT INTO pt(val, ts) VALUES " + values);
            drainWalQueue();

            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // Step 1: lazy parquet read path.
            execute("ALTER TABLE nt ALTER COLUMN val TYPE " + midType);
            drainWalQueue();
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + midType);
            drainWalQueue();
            assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");

            // Step 2: chained conversion. pt now has a prior conversion, so the pre-pass
            // eagerly converts the parquet partition to native before applying this step.
            execute("ALTER TABLE nt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");

            // Final: any partition still parquet is materialised to native.
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertSqlCursors("SELECT * FROM nt ORDER BY ts", "SELECT * FROM pt ORDER BY ts");
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * Shared body for the M1 filtered column-top tests. 'v' is ADDed after seed rows so it
     * carries a column top (NULL) for the first 20 rows; value rows get a larger 'sel' so the
     * WHERE filter excludes them. GROUP BY v with a selective filter on sel routes v through the
     * late-materialization filtered decode (decode_row_group_filtered in parquet_meta_decode.rs).
     * For a no-sentinel source ({@code srcType} in BOOLEAN/BYTE/SHORT) widened to a sentinel
     * target, the selected column-top rows must group under NULL, not 0 -- matching native (nt).
     */
    private void assertFilteredColumnTopNull(String srcType, String dstType, String valueExpr) throws Exception {
        try {
            execute("CREATE TABLE nt (sel INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (sel INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String seed = """
                    INSERT INTO %s(sel, ts)
                    SELECT x::INT, timestamp_sequence('2024-01-01T00:00:00.000000Z', 1_000_000)
                    FROM long_sequence(20)""";
            execute(seed.formatted("nt"));
            execute(seed.formatted("pt"));
            drainWalQueue();

            execute("ALTER TABLE nt ADD COLUMN v " + srcType);
            execute("ALTER TABLE pt ADD COLUMN v " + srcType);
            drainWalQueue();

            // Use replace (not formatted): valueExpr may contain '%' (e.g. BOOLEAN "x % 2 = 0").
            String vals = "INSERT INTO $T(sel, v, ts)\n"
                    + "SELECT (x + 1000)::INT, " + valueExpr + ", timestamp_sequence('2024-01-01T01:00:00.000000Z', 1_000_000)\n"
                    + "FROM long_sequence(200)";
            execute(vals.replace("$T", "nt"));
            execute(vals.replace("$T", "pt"));
            drainWalQueue();

            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN v TYPE " + dstType);
            execute("ALTER TABLE pt ALTER COLUMN v TYPE " + dstType);
            drainWalQueue();

            assertSqlCursors(
                    "SELECT v, count() c FROM nt WHERE sel < 10 GROUP BY v ORDER BY v",
                    "SELECT v, count() c FROM pt WHERE sel < 10 GROUP BY v ORDER BY v"
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * Shared body for the C2 O3-dedup-on-converted-key reproducers. Builds a native control table
     * (nt) and a parquet table (pt), both WAL + DEDUP UPSERT KEYS(ts, k), seeds three rows,
     * converts pt's partition to parquet, then ALTERs the dedup key 'k' from srcKeyType to
     * dstKeyType on both tables (changeColumnType preserves the dedup-key flag). A final O3 insert
     * lands one duplicate key (dupKey at the same ts as seedKeyB, which must collapse and replace
     * val) and one new key (newKey at an out-of-order ts, which forces the merge into the parquet
     * partition). nt dedups against native data; pt dedups inside O3PartitionJob.mergeRowGroup
     * against the parquet row-group decode buffer, so the two cursors must match.
     * <p>
     * seedKeyA/B/C are srcKeyType literals; dupKey equals seedKeyB after conversion and newKey is a
     * distinct dstKeyType literal -- both O3 keys are written post-ALTER, so they use dstKeyType.
     */
    private void assertO3DedupKeyConversion(
            String srcKeyType,
            String dstKeyType,
            String seedKeyA,
            String seedKeyB,
            String seedKeyC,
            String dupKey,
            String newKey
    ) throws Exception {
        try {
            String create = "CREATE TABLE %s (k " + srcKeyType + ", val INT, ts TIMESTAMP)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, k)";
            execute(create.formatted("nt"));
            execute(create.formatted("pt"));

            String seed = "INSERT INTO %s VALUES"
                    + " (" + seedKeyA + ", 100, '2024-01-01T00:00:01.000000Z'),"
                    + " (" + seedKeyB + ", 300, '2024-01-01T00:00:03.000000Z'),"
                    + " (" + seedKeyC + ", 500, '2024-01-01T00:00:05.000000Z')";
            execute(seed.formatted("nt"));
            execute(seed.formatted("pt"));
            drainWalQueue();

            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            execute("ALTER TABLE nt ALTER COLUMN k TYPE " + dstKeyType);
            execute("ALTER TABLE pt ALTER COLUMN k TYPE " + dstKeyType);
            drainWalQueue();

            // The crossing ALTER on a dedup key no longer eagerly materialises the parquet
            // partition to native: ConvertOperatorImpl has no dedup-key pre-pass. The partition
            // stays lazy parquet (still storing the source-typed key), and the O3 merge below
            // converts it on the fly. Confirm it really stayed parquet.
            try (TableWriter writer = getWriter("pt")) {
                Assert.assertEquals(PartitionFormat.PARQUET, writer.getPartitionFormat(0));
            }

            // O3 insert: dupKey collides with the seed row at 00:00:03 (must collapse to one row,
            // val replaced by 999); newKey at 00:00:02 is out-of-order, forcing the O3 merge into
            // the parquet partition, and carries a brand-new key (no collapse).
            String o3 = "INSERT INTO %s VALUES"
                    + " (" + dupKey + ", 999, '2024-01-01T00:00:03.000000Z'),"
                    + " (" + newKey + ", 777, '2024-01-01T00:00:02.000000Z')";
            execute(o3.formatted("nt"));
            execute(o3.formatted("pt"));
            drainWalQueue();

            assertSqlCursors(
                    "SELECT k, val, ts FROM nt ORDER BY ts, k",
                    "SELECT k, val, ts FROM pt ORDER BY ts, k"
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * C1 repro: same crossing dedup-key conversion as {@link #assertO3DedupKeyConversion}, but with
     * the DDL ordering swapped to ALTER-then-DEDUP. The table is created WITHOUT deduplication, so
     * when {@code ALTER COLUMN k TYPE} runs, {@code k} is not yet a dedup key and the Case-3 pre-pass
     * in ConvertOperatorImpl is SKIPPED -- the crossing column stays lazy in parquet. DEDUP is then
     * enabled via {@code ALTER TABLE ... DEDUP ENABLE}, which only flips metadata flags and runs no
     * parquet->native pre-pass. The subsequent O3 insert merges into the parquet row group, and
     * O3PartitionJob.mergeRowGroup builds the native dedup-compare address from the lazy parquet
     * decode buffer in the wrong representation: a fixed->var key SIGSEGVs (var-path getChunkAuxPtr
     * over a fixed decode with no aux vector), while var->fixed and symbol->fixed silently misread
     * the slice bytes as fixed keys and make the wrong dedup decision. The native control table (nt)
     * goes through the identical ALTER-then-DEDUP ordering but stays native, so it dedups correctly;
     * the cursors must match once C1 is fixed.
     */
    private void assertO3DedupKeyConversionDedupAfterAlter(
            String srcKeyType,
            String dstKeyType,
            String seedKeyA,
            String seedKeyB,
            String seedKeyC,
            String dupKey,
            String newKey
    ) throws Exception {
        try {
            // No DEDUP at creation: k is not a dedup key when ALTER COLUMN TYPE runs below.
            String create = "CREATE TABLE %s (k " + srcKeyType + ", val INT, ts TIMESTAMP)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL";
            execute(create.formatted("nt"));
            execute(create.formatted("pt"));

            String seed = "INSERT INTO %s VALUES"
                    + " (" + seedKeyA + ", 100, '2024-01-01T00:00:01.000000Z'),"
                    + " (" + seedKeyB + ", 300, '2024-01-01T00:00:03.000000Z'),"
                    + " (" + seedKeyC + ", 500, '2024-01-01T00:00:05.000000Z')";
            execute(seed.formatted("nt"));
            execute(seed.formatted("pt"));
            drainWalQueue();

            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();

            // ALTER before DEDUP: k is not a dedup key yet, so the crossing parquet column is not
            // materialised to native and stays lazy.
            execute("ALTER TABLE nt ALTER COLUMN k TYPE " + dstKeyType);
            execute("ALTER TABLE pt ALTER COLUMN k TYPE " + dstKeyType);
            drainWalQueue();

            // Now promote k to a dedup key. This runs no parquet->native pre-pass.
            execute("ALTER TABLE nt DEDUP ENABLE UPSERT KEYS(ts, k)");
            execute("ALTER TABLE pt DEDUP ENABLE UPSERT KEYS(ts, k)");
            drainWalQueue();

            // O3 insert: dupKey collides with the seed row at 00:00:03 (must collapse to one row,
            // val replaced by 999); newKey at 00:00:02 is out-of-order, forcing the O3 merge into
            // the parquet partition, and carries a brand-new key (no collapse).
            String o3 = "INSERT INTO %s VALUES"
                    + " (" + dupKey + ", 999, '2024-01-01T00:00:03.000000Z'),"
                    + " (" + newKey + ", 777, '2024-01-01T00:00:02.000000Z')";
            execute(o3.formatted("nt"));
            execute(o3.formatted("pt"));
            drainWalQueue();

            assertSqlCursors(
                    "SELECT k, val, ts FROM nt ORDER BY ts, k",
                    "SELECT k, val, ts FROM pt ORDER BY ts, k"
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    private void assertParquetFloatOutOfRangeNull(String sourceType, String targetType, String floatExpr) throws Exception {
        // CursorPrinter renders INT/LONG null as the literal "null"
        // (Numbers.append special-cases the null sentinel), while DATE/TIMESTAMP
        // null renders as an empty cell (DateFormatUtils/TimestampFormatUtils
        // early-return on MIN_VALUE).
        String nullRendering = switch (targetType.toUpperCase()) {
            case "INT", "LONG" -> "null";
            case "DATE", "TIMESTAMP" -> "";
            default -> throw new IllegalArgumentException("unsupported target: " + targetType);
        };
        try {
            execute("CREATE TABLE pt (val " + sourceType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO pt VALUES (" + floatExpr + ", '2024-01-01T00:00:01.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            // Out-of-range floats must read back as the target type's NULL sentinel.
            assertQuery("SELECT val FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns("val\n" + nullRendering + "\n");
        } finally {
            tryDrop("pt");
        }
    }

    private void assertIntToDecimalOverflowNull(String targetType, String overflowValue) throws Exception {
        try {
            // nt stays native (eager ALTER); pt converts to parquet so its ALTER takes the lazy
            // decode path. Both must read the overflowing row back as NULL.
            execute("CREATE TABLE nt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO nt VALUES (" + overflowValue + ", '2024-01-01T00:00:01.000000Z')");
            execute("INSERT INTO pt VALUES (" + overflowValue + ", '2024-01-01T00:00:01.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE " + targetType);
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + targetType);
            drainWalQueue();
            // CursorPrinter renders Decimal*_NULL as an empty cell. Neither ALTER suspends the WAL.
            assertQuery("SELECT val FROM nt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns("val\n\n");
            assertQuery("SELECT val FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns("val\n\n");
            // Materialize the lazy conversion eagerly (parquet -> native rewrite) and re-assert.
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertQuery("SELECT val FROM pt").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns("val\n\n");
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    /**
     * Shared body for the SAMPLE BY first()/last() tests. Builds a native control {@code nt} and a
     * parquet {@code pt}, both with an INDEXED {@code sym} (required to produce the
     * {@code DeferredSingleSymbolFilterPageFrame} base that the factory disassembles). {@code val}
     * is seeded via {@code srcValueExpr}, the 2024-01-01 partition of {@code pt} is converted to
     * parquet, then {@code val} is ALTERed to {@code dstType} on both. The 200 rows at 10-minute
     * spacing straddle 2024-01-01 (parquet) and 2024-01-02 (native). The cursors must match.
     */
    private void assertSampleByFirstLastParity(String srcType, String srcValueExpr, String dstType, String queryTemplate) throws Exception {
        try {
            execute("CREATE TABLE nt (val " + srcType + ", sym SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE pt (val " + srcType + ", sym SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            String insert = """
                    INSERT INTO $T
                    SELECT %s AS val,
                           ('s' || (x %% 5)::STRING)::SYMBOL AS sym,
                           timestamp_sequence('2024-01-01T00:00:00.000000Z', 600_000_000) AS ts
                    FROM long_sequence(200)""".formatted(srcValueExpr);
            execute(insert.replace("$T", "nt"));
            execute(insert.replace("$T", "pt"));
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE nt ALTER COLUMN val TYPE " + dstType);
            execute("ALTER TABLE pt ALTER COLUMN val TYPE " + dstType);
            drainWalQueue();
            assertSqlCursors(
                    queryTemplate.replace("$T", "nt"),
                    queryTemplate.replace("$T", "pt")
            );
        } finally {
            tryDrop("nt");
            tryDrop("pt");
        }
    }

    private void tryDrop(String tableName) {
        try {
            execute("DROP TABLE " + tableName);
        } catch (Exception ignored) {
        }
    }
}
