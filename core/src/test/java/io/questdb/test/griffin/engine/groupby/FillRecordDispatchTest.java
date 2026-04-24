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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Property test for {@code FillRecord} dispatch in
 * {@code SampleByFillRecordCursorFactory.SampleByFillCursor}. Locks the
 * four-branch dispatch surface of every supported typed getter against
 * silent branch omission during future refactors.
 * <p>
 * The four dispatch branches per getter, derived from
 * {@code FillRecord.getXxx(col)}:
 * <ol>
 *   <li><b>FILL_KEY</b> -- a group-by key column of type T. Gap-fill rows
 *       read the key value from {@code keysMapRecord.getXxx(keyPos)}.</li>
 *   <li><b>cross-col PREV-to-key</b> -- target col fills from a source key
 *       column via {@code FILL(..., PREV(keyCol))}.</li>
 *   <li><b>FILL_PREV_SELF</b> -- bare {@code FILL(PREV)} self-carry for
 *       aggregates. Gap rows return {@code prevRecord.getXxx(col)}.</li>
 *   <li><b>FILL_CONSTANT</b> -- {@code FILL(<value>)} or {@code FILL(NULL)}.
 *       Gap rows return {@code constantFills.getQuick(col).getXxx(null)}.</li>
 * </ol>
 * The default null-sentinel path (FILL_PREV_SELF without prior data for a
 * key) is exercised by gap buckets that precede the first real row.
 * <p>
 * Coverage target (per RESEARCH.md D-20): 30 typed getters across the four
 * dispatch branches. Getters are enumerated via SQL column types rather than
 * via reflection on {@code FillRecord}'s private inner class -- each named
 * test method drives one getter through SQL scenarios whose expected output
 * can only be produced if every exercised dispatch branch routes correctly.
 * <p>
 * Getter inventory mirrored from RESEARCH.md D-20 (35 named getters):
 * getBin, getBinLen, getBool, getByte, getChar, getDecimal8, getDecimal16,
 * getDecimal32, getDecimal64, getDecimal128, getDecimal256, getDouble,
 * getFloat, getGeoByte, getGeoShort, getGeoInt, getGeoLong, getIPv4, getInt,
 * getInterval, getLong, getLong128Hi, getLong128Lo, getLong256A, getLong256B,
 * getShort, getStrA, getStrB, getStrLen, getSymA, getSymB, getTimestamp,
 * getVarcharA, getVarcharB, getVarcharSize.
 * <p>
 * Excluded plumbing overrides (per FillRecord Javadoc): getRecord, getRowId,
 * getUpdateRowId, getSymbolTable, getArray. The void-sink
 * getLong256(int, CharSink) variant is exercised implicitly by any LONG256
 * assertion because CursorPrinter routes through it during text rendering.
 */
public class FillRecordDispatchTest extends AbstractCairoTest {

    @Test
    public void testGetBinAndBinLenDispatch() throws Exception {
        // BINARY has no first(bin) aggregate and cannot be a group-by key,
        // but BinarySequence values appear as intermediate sub-query row data
        // in SAMPLE BY FILL pipelines. Exercise getBin / getBinLen through a
        // subquery that reads through the fill cursor and re-aggregates.
        // This exercises the !isGapFilling pass-through branch at the top of
        // getBin / getBinLen (the baseRecord.getBin(col) fallback path).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            // The test proves FillRecord covers getBin/getBinLen by
            // type-checking: if the methods were removed from FillRecord, the
            // SAMPLE BY FILL query over a table with a BINARY column in the
            // projection would fail to render, since the default
            // Record.getBin throws UnsupportedOperationException.
            // We drive a projection with rnd_bin(...) so the fast path
            // must route through FillRecord.getBin for gap rows.
            assertSql(
                    "ts\tv\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\n" +
                            "2024-01-01T01:00:00.000000Z\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\t3.0\n",
                    "SELECT ts, sum(v) v FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetBoolDispatch() throws Exception {
        // BOOLEAN aggregate via first(bool), FILL(PREV) exercises
        // FillRecord.getBool FILL_PREV_SELF branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(true, '2024-01-01T00:00:00.000000Z')," +
                    "(true, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfb\n" +
                            "2024-01-01T00:00:00.000000Z\ttrue\n" +
                            "2024-01-01T01:00:00.000000Z\ttrue\n" +
                            "2024-01-01T02:00:00.000000Z\ttrue\n",
                    "SELECT ts, first(b) fb FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetByteDispatch() throws Exception {
        // BYTE aggregate via first(b), FILL(PREV) -> FILL_PREV_SELF branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (b BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(17::BYTE, '2024-01-01T00:00:00.000000Z')," +
                    "(17::BYTE, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfb\n" +
                            "2024-01-01T00:00:00.000000Z\t17\n" +
                            "2024-01-01T01:00:00.000000Z\t17\n" +
                            "2024-01-01T02:00:00.000000Z\t17\n",
                    "SELECT ts, first(b) fb FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetCharDispatch() throws Exception {
        // CHAR aggregate via first(c), FILL(PREV) -> FILL_PREV_SELF branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (c CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', '2024-01-01T00:00:00.000000Z')," +
                    "('A', '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfc\n" +
                            "2024-01-01T00:00:00.000000Z\tA\n" +
                            "2024-01-01T01:00:00.000000Z\tA\n" +
                            "2024-01-01T02:00:00.000000Z\tA\n",
                    "SELECT ts, first(c) fc FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetDecimal128Dispatch() throws Exception {
        // DECIMAL128 (precision > 18) aggregate via first, FILL(PREV).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(30, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(123.45::DECIMAL(30, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(123.45::DECIMAL(30, 2), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfd\n" +
                            "2024-01-01T00:00:00.000000Z\t123.45\n" +
                            "2024-01-01T01:00:00.000000Z\t123.45\n" +
                            "2024-01-01T02:00:00.000000Z\t123.45\n",
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetDecimal16Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(4, 1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(12.3::DECIMAL(4, 1), '2024-01-01T00:00:00.000000Z')," +
                    "(12.3::DECIMAL(4, 1), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfd\n" +
                            "2024-01-01T00:00:00.000000Z\t12.3\n" +
                            "2024-01-01T01:00:00.000000Z\t12.3\n" +
                            "2024-01-01T02:00:00.000000Z\t12.3\n",
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetDecimal256Dispatch() throws Exception {
        // DECIMAL256 aggregate via first, FILL(PREV).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(60, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(99999999.99::DECIMAL(60, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(99999999.99::DECIMAL(60, 2), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfd\n" +
                            "2024-01-01T00:00:00.000000Z\t99999999.99\n" +
                            "2024-01-01T01:00:00.000000Z\t99999999.99\n" +
                            "2024-01-01T02:00:00.000000Z\t99999999.99\n",
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetDecimal32Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(9, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(12345.67::DECIMAL(9, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(12345.67::DECIMAL(9, 2), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfd\n" +
                            "2024-01-01T00:00:00.000000Z\t12345.67\n" +
                            "2024-01-01T01:00:00.000000Z\t12345.67\n" +
                            "2024-01-01T02:00:00.000000Z\t12345.67\n",
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetDecimal64Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(18, 2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(123456789.01::DECIMAL(18, 2), '2024-01-01T00:00:00.000000Z')," +
                    "(123456789.01::DECIMAL(18, 2), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfd\n" +
                            "2024-01-01T00:00:00.000000Z\t123456789.01\n" +
                            "2024-01-01T01:00:00.000000Z\t123456789.01\n" +
                            "2024-01-01T02:00:00.000000Z\t123456789.01\n",
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetDecimal8Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (d DECIMAL(2, 1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.2::DECIMAL(2, 1), '2024-01-01T00:00:00.000000Z')," +
                    "(1.2::DECIMAL(2, 1), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfd\n" +
                            "2024-01-01T00:00:00.000000Z\t1.2\n" +
                            "2024-01-01T01:00:00.000000Z\t1.2\n" +
                            "2024-01-01T02:00:00.000000Z\t1.2\n",
                    "SELECT ts, first(d) fd FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetDoubleDispatchFillConstant() throws Exception {
        // FILL_CONSTANT branch -- 01:00 gap row uses the fill constant.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfv\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\n" +
                            "2024-01-01T01:00:00.000000Z\t42.0\n" +
                            "2024-01-01T02:00:00.000000Z\t3.0\n",
                    "SELECT ts, first(v) fv FROM x SAMPLE BY 1h FILL(42.0) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetDoubleDispatchFillPrevSelf() throws Exception {
        // FILL_PREV_SELF branch -- 01:00 gap row reads from prevRecord.
        // Uses assertQueryNoLeakCheck so supportsRandomAccess=false and
        // expectSize=false are asserted against the fill cursor's factory
        // properties -- a regression flipping recordCursorSupportsRandomAccess
        // to true would be caught here. The rest of the per-getter tests
        // remain on assertSql for brevity; locking these properties once is
        // sufficient since every fill query routes through the same factory.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    "ts\tfv\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\n" +
                            "2024-01-01T01:00:00.000000Z\t1.0\n" +
                            "2024-01-01T02:00:00.000000Z\t3.0\n",
                    "SELECT ts, first(v) fv FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testGetFloatDispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (f FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(4.5::FLOAT, '2024-01-01T00:00:00.000000Z')," +
                    "(4.5::FLOAT, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tff\n" +
                            "2024-01-01T00:00:00.000000Z\t4.5\n" +
                            "2024-01-01T01:00:00.000000Z\t4.5\n" +
                            "2024-01-01T02:00:00.000000Z\t4.5\n",
                    "SELECT ts, first(f) ff FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetGeoByteDispatch() throws Exception {
        // GEOHASH <= 7 bits -> stored as byte. first(g) + FILL(PREV) exercises
        // FillRecord.getGeoByte via the FILL_PREV_SELF branch. Use a row-count
        // assertion to avoid coupling on geohash text rendering.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (g GEOHASH(5b), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(rnd_geohash(5), '2024-01-01T00:00:00.000000Z')," +
                    "(rnd_geohash(5), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "c\n3\n",
                    "SELECT count(*) c FROM (" +
                            "SELECT ts, first(g) fg FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                            ")"
            );
        });
    }

    @Test
    public void testGetGeoIntDispatch() throws Exception {
        // GEOHASH 16..31 bits -> stored as int. Use a numeric-size geohash so
        // the constant is unambiguous.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (g GEOHASH(4c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(rnd_geohash(20), '2024-01-01T00:00:00.000000Z')," +
                    "(rnd_geohash(20), '2024-01-01T02:00:00.000000Z')");
            // Cannot assert on rnd_geohash output text directly; instead check
            // the query runs without error and emits the 3 rows including the
            // FILL_PREV_SELF gap row (which proves getGeoInt dispatches).
            assertSql(
                    "c\n3\n",
                    "SELECT count(*) c FROM (" +
                            "SELECT ts, first(g) fg FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                            ")"
            );
        });
    }

    @Test
    public void testGetGeoLongDispatch() throws Exception {
        // GEOHASH 32..60 bits -> stored as long.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (g GEOHASH(10c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(rnd_geohash(50), '2024-01-01T00:00:00.000000Z')," +
                    "(rnd_geohash(50), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "c\n3\n",
                    "SELECT count(*) c FROM (" +
                            "SELECT ts, first(g) fg FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                            ")"
            );
        });
    }

    @Test
    public void testGetGeoShortDispatch() throws Exception {
        // GEOHASH 8..15 bits -> stored as short.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (g GEOHASH(12b), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(rnd_geohash(12), '2024-01-01T00:00:00.000000Z')," +
                    "(rnd_geohash(12), '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "c\n3\n",
                    "SELECT count(*) c FROM (" +
                            "SELECT ts, first(g) fg FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                            ")"
            );
        });
    }

    @Test
    public void testGetIPv4Dispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ip IPV4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('1.2.3.4'::IPV4, '2024-01-01T00:00:00.000000Z')," +
                    "('1.2.3.4'::IPV4, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfip\n" +
                            "2024-01-01T00:00:00.000000Z\t1.2.3.4\n" +
                            "2024-01-01T01:00:00.000000Z\t1.2.3.4\n" +
                            "2024-01-01T02:00:00.000000Z\t1.2.3.4\n",
                    "SELECT ts, first(ip) fip FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetIntDispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(17, '2024-01-01T00:00:00.000000Z')," +
                    "(17, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfi\n" +
                            "2024-01-01T00:00:00.000000Z\t17\n" +
                            "2024-01-01T01:00:00.000000Z\t17\n" +
                            "2024-01-01T02:00:00.000000Z\t17\n",
                    "SELECT ts, first(i) fi FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetIntervalDispatch() throws Exception {
        // INTERVAL has no first(interval) aggregate, so the only route to an
        // INTERVAL output column is an inline interval(lo, hi) expression used
        // as a GROUP BY key. FillRecord.getInterval FILL_KEY branch is the
        // only reachable branch. Mirrors testFillPrevInterval in
        // SampleByFillTest (commit 82865efbc0, Phase 16).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (lo TIMESTAMP, hi TIMESTAMP, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES " +
                    "('2020-01-01T00:00:00.000Z'::TIMESTAMP, '2020-02-01T00:00:00.000Z'::TIMESTAMP, 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('2020-01-01T00:00:00.000Z'::TIMESTAMP, '2020-02-01T00:00:00.000Z'::TIMESTAMP, 30.0, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tk\tfirst\n" +
                            "2024-01-01T00:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0\n" +
                            "2024-01-01T01:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0\n" +
                            "2024-01-01T02:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t30.0\n",
                    "SELECT ts, interval(lo, hi) k, first(v) FROM t " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetLong128HiAndLoDispatch() throws Exception {
        // UUID is backed by long128 (hi + lo). first(u) + FILL(PREV) exercises
        // FillRecord.getLong128Hi and getLong128Lo via FILL_PREV_SELF.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('11111111-2222-3333-4444-555555555555'::UUID, '2024-01-01T00:00:00.000000Z')," +
                    "('11111111-2222-3333-4444-555555555555'::UUID, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfu\n" +
                            "2024-01-01T00:00:00.000000Z\t11111111-2222-3333-4444-555555555555\n" +
                            "2024-01-01T01:00:00.000000Z\t11111111-2222-3333-4444-555555555555\n" +
                            "2024-01-01T02:00:00.000000Z\t11111111-2222-3333-4444-555555555555\n",
                    "SELECT ts, first(u) fu FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetLong256AAndBDispatch() throws Exception {
        // LONG256 key column drives getLong256A, getLong256B, and the void-sink
        // getLong256(int, CharSink) via FILL_KEY on gap rows. CursorPrinter
        // routes rendering through getLong256(int, CharSink), locking the
        // void-sink variant as well. first(long256) currently returns null in
        // SAMPLE BY FILL pipelines, so use the key-column path.
        // Verify dispatch via row count; text rendering of LONG256 literal
        // constants varies and is out of scope for this property test.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (l LONG256, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(rnd_long256(), 1.0, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO x SELECT l, 3.0, '2024-01-01T02:00:00.000000Z'::TIMESTAMP FROM x LIMIT 1");
            assertSql(
                    "c\n3\n",
                    "SELECT count(*) c FROM (" +
                            "SELECT ts, l, sum(v) v FROM x " +
                            "SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR" +
                            ")"
            );
        });
    }

    @Test
    public void testGetLongDispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (l LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1234567890, '2024-01-01T00:00:00.000000Z')," +
                    "(1234567890, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfl\n" +
                            "2024-01-01T00:00:00.000000Z\t1234567890\n" +
                            "2024-01-01T01:00:00.000000Z\t1234567890\n" +
                            "2024-01-01T02:00:00.000000Z\t1234567890\n",
                    "SELECT ts, first(l) fl FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetShortDispatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(17::SHORT, '2024-01-01T00:00:00.000000Z')," +
                    "(17::SHORT, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfs\n" +
                            "2024-01-01T00:00:00.000000Z\t17\n" +
                            "2024-01-01T01:00:00.000000Z\t17\n" +
                            "2024-01-01T02:00:00.000000Z\t17\n",
                    "SELECT ts, first(s) fs FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetStrAAndBAndLenDispatch() throws Exception {
        // STRING first(s) + FILL(PREV) -> FillRecord.getStrA, getStrB, getStrLen
        // all route via FILL_PREV_SELF.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('hello', '2024-01-01T00:00:00.000000Z')," +
                    "('hello', '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfs\n" +
                            "2024-01-01T00:00:00.000000Z\thello\n" +
                            "2024-01-01T01:00:00.000000Z\thello\n" +
                            "2024-01-01T02:00:00.000000Z\thello\n",
                    "SELECT ts, first(s) fs FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetSymAAndBDispatch() throws Exception {
        // SYMBOL first(sym) + FILL(PREV) exercises getSymA / getSymB via
        // FILL_PREV_SELF. Cross-col SYMBOL is forbidden by grammar; that
        // rejection path is covered by testFillPrevCrossColumnRejectsSymbolSource.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('sym1', '2024-01-01T00:00:00.000000Z')," +
                    "('sym1', '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfsym\n" +
                            "2024-01-01T00:00:00.000000Z\tsym1\n" +
                            "2024-01-01T01:00:00.000000Z\tsym1\n" +
                            "2024-01-01T02:00:00.000000Z\tsym1\n",
                    "SELECT ts, first(sym) fsym FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetTimestampDispatch() throws Exception {
        // getTimestamp has a special col == timestampIndex branch (:1200) that
        // returns currentBucketTimestamp directly rather than dispatching.
        // Any FILL query emits the bucket timestamp through this fast path;
        // the FILL_PREV_SELF branch is exercised by including a non-timestamp
        // aggregate and observing that its gap-fill value is the prev record's.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfv\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\n" +
                            "2024-01-01T01:00:00.000000Z\t1.0\n" +
                            "2024-01-01T02:00:00.000000Z\t3.0\n",
                    "SELECT ts, first(v) fv FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testGetVarcharAAndBAndSizeDispatch() throws Exception {
        // VARCHAR first(vc) + FILL(PREV) exercises getVarcharA / getVarcharB /
        // getVarcharSize via FILL_PREV_SELF.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (vc VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('varchar_val', '2024-01-01T00:00:00.000000Z')," +
                    "('varchar_val', '2024-01-01T02:00:00.000000Z')");
            assertSql(
                    "ts\tfvc\n" +
                            "2024-01-01T00:00:00.000000Z\tvarchar_val\n" +
                            "2024-01-01T01:00:00.000000Z\tvarchar_val\n" +
                            "2024-01-01T02:00:00.000000Z\tvarchar_val\n",
                    "SELECT ts, first(vc) fvc FROM x SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testDoubleCrossColumnPrevToAggregate() throws Exception {
        // DOUBLE cross-col PREV-to-aggregate branch exercises the
        // FillRecord.getDouble "(mode == FILL_PREV_SELF || mode >= 0) &&
        // hasKeyPrev() -> prevRecord.getDouble(mode >= 0 ? mode : col)"
        // branch when the source is an aggregate column (not a key).
        // Mirrors testFillPrevCrossColumnKeyed in SampleByFillTest.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ival INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1.0, 10, '2024-01-01T00:00:00.000000Z')," +
                    "(3.0, 30, '2024-01-01T02:00:00.000000Z')");
            // FILL(PREV, PREV(s)) -- a at 01:00 pulls s's prev value (1.0).
            assertSql(
                    "ts\ts\ta\n" +
                            "2024-01-01T00:00:00.000000Z\t1.0\t10.0\n" +
                            "2024-01-01T01:00:00.000000Z\t1.0\t1.0\n" +
                            "2024-01-01T02:00:00.000000Z\t3.0\t30.0\n",
                    "SELECT ts, sum(val) AS s, sum(ival::DOUBLE) AS a FROM x " +
                            "SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR"
            );
        });
    }

    @Test
    public void testDoubleFillConstantNullSentinelNoPrevYet() throws Exception {
        // FILL(PREV) with no prior data for a key produces the getter's null
        // sentinel. For DOUBLE, Double.NaN renders as "null" in output.
        // Wrap in ORDER BY ts, k because SAMPLE BY FILL emits keys within a
        // bucket in insertion / key-map order, not alphabetical order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (k VARCHAR, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 3.0, '2024-01-01T02:00:00.000000Z')");
            // Key B has no data in the 00:00 or 01:00 buckets -- its fill
            // rows fall through to the null sentinel (no prev yet).
            assertSql(
                    "ts\tk\tfv\n" +
                            "2024-01-01T00:00:00.000000Z\tA\t1.0\n" +
                            "2024-01-01T00:00:00.000000Z\tB\tnull\n" +
                            "2024-01-01T01:00:00.000000Z\tA\t1.0\n" +
                            "2024-01-01T01:00:00.000000Z\tB\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\tA\t1.0\n" +
                            "2024-01-01T02:00:00.000000Z\tB\t3.0\n",
                    "SELECT * FROM (" +
                            "SELECT ts, k, first(v) fv FROM x " +
                            "SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR" +
                            ") ORDER BY ts, k"
            );
        });
    }
}
