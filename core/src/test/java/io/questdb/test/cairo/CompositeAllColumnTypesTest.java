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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;

import org.junit.Test;

/**
 * EVERY COLUMN TYPE, on a composite table, against its plain twin.
 * <p>
 * The composite test suite's twin fixtures are narrow -- {@code (ts, exch, px)} with a VARCHAR or an
 * indexed SYMBOL in a few places, and the differential fuzz runs on five columns of four types. That
 * leaves the types whose ON-DISK SHAPE differs from a plain 8-byte fixed column untested through the
 * per-cell paths: 16- and 32-byte fixed values (UUID, LONG128, LONG256), the geohash widths, the
 * var-size pair with its own aux file (STRING, VARCHAR, BINARY) and ARRAY.
 * <p>
 * Those are exactly the shapes a cell-blind path breaks on, because per-cell work is about PATHS and
 * per-column bookkeeping: a var-size column has a second file to place in the cell directory, an
 * ARRAY has its own layout, and a 32-byte fixed column exercises a different size class in the copy.
 * A type that never appears in a composite test is a type whose per-cell handling nothing has checked.
 * <p>
 * The oracle is the plain twin, over the same generated rows: identical data, identical query, and the
 * only difference is that one table splits its days into cells. Rows land in several cells and across
 * several days, in order and out of order, so the per-cell dispatch and the O3 merge both run.
 * <p>
 * The parquet arm matters as much as the native one: conversion is per cell, and the encoder is where
 * a type's layout is re-derived rather than copied.
 */
public class CompositeAllColumnTypesTest extends AbstractCairoTest {

    /**
     * Column list covering every type creatable through DDL, plus the dimension SYMBOL.
     * <p>
     * {@code ts} is the designated timestamp; {@code exch} the partition dimension. The rest are
     * payload, in ColumnType order so a reader can see at a glance what is present -- and what is not.
     */
    private static final String COLUMNS = "(ts TIMESTAMP, exch SYMBOL, "
            + "c_boolean BOOLEAN, c_byte BYTE, c_short SHORT, c_char CHAR, c_int INT, c_long LONG, "
            + "c_date DATE, c_ts2 TIMESTAMP, c_float FLOAT, c_double DOUBLE, "
            + "c_string STRING, c_symbol SYMBOL, c_long256 LONG256, "
            + "c_geobyte GEOHASH(5b), c_geoshort GEOHASH(10b), c_geoint GEOHASH(20b), c_geolong GEOHASH(40b), "
            + "c_binary BINARY, c_uuid UUID, c_varchar VARCHAR, c_ipv4 IPV4, c_array DOUBLE[])";

    /**
     * The generated payload, one expression per column above. Deterministic per seed, and identical
     * for both twins because the rows are generated ONCE and inserted into each from the same table.
     */
    private static final String GENERATOR = "SELECT"
            + " timestamp_sequence(1672531200000000L, 3600000000L) ts,"
            + " rnd_symbol('E0','E1','E2') exch,"
            + " rnd_boolean() c_boolean,"
            + " rnd_byte() c_byte,"
            + " rnd_short() c_short,"
            + " rnd_char() c_char,"
            + " rnd_int() c_int,"
            + " rnd_long() c_long,"
            + " cast(rnd_long(1672531200000L, 1704067200000L, 0) as date) c_date,"
            + " cast(rnd_long(1672531200000000L, 1704067200000000L, 0) as timestamp) c_ts2,"
            + " rnd_float() c_float,"
            + " rnd_double() c_double,"
            + " rnd_str(4, 12, 1) c_string,"
            + " rnd_symbol('s1','s2','s3', null) c_symbol,"
            + " rnd_long256() c_long256,"
            + " rnd_geohash(5) c_geobyte,"
            + " rnd_geohash(10) c_geoshort,"
            + " rnd_geohash(20) c_geoint,"
            + " rnd_geohash(40) c_geolong,"
            + " rnd_bin(4, 16, 1) c_binary,"
            + " rnd_uuid4() c_uuid,"
            + " rnd_varchar(4, 12, 1) c_varchar,"
            + " rnd_ipv4() c_ipv4,"
            + " rnd_double_array(1, 0) c_array"
            + " FROM long_sequence(240)";

    /**
     * The shape that used to CRASH THE JVM (SIGSEGV in {@code merge_copy_var_column_int32_AVX512}):
     * composite, a var-size column, a day holding several cells, and an O3 merge into one of them --
     * issued as SINGLE-CELL commits, which is the workaround this branch documents for the refusal on an
     * interleaved multi-cell commit with a var-size column. The documented workaround was what crashed.
     * <p>
     * The root cause was not var-size at all: {@code processO3BlockComposite} never flattened the sorted
     * timestamp index (see its own doc). A var-size column merely turns the resulting out-of-range row
     * id into a wild pointer instead of silently wrong data -- {@link
     * #testFixedSizeColumnsO3AfterEarlierTransactionsMatchThePlainTwin} covers the quiet half.
     * <p>
     * The preceding per-cell inserts are load-bearing, not scene-setting: they put earlier transactions
     * in the SAME WAL segment, which is what makes the index's row ids differ from their positions. A
     * commit alone in its segment flattens to a no-op and would prove nothing.
     */
    @Test
    public void testMultiCellDayO3WithAVarSizeColumnMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src AS (SELECT timestamp_sequence(1672531200000000L, 3600000000L) ts,"
                    + " rnd_symbol('E0','E1','E2') exch, rnd_int() c_int, rnd_str(4, 12, 1) c_string"
                    + " FROM long_sequence(240)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, c_int INT, c_string STRING) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, c_int INT, c_string STRING) TIMESTAMP(ts) "
                    + "PARTITION BY DAY WAL");
            // The identical transaction sequence into both twins, so the segment layering the defect
            // needs exists on each side and the plain table is a fair oracle.
            for (String cell : new String[]{"E0", "E1", "E2"}) {
                execute("INSERT INTO c SELECT * FROM src WHERE exch = '" + cell + "'");
                execute("INSERT INTO p SELECT * FROM src WHERE exch = '" + cell + "'");
                drainWalQueue();
            }
            // ORDER BY c_int makes this genuinely out of order, forcing the gathered (sorted-into-memory)
            // path whose 0-based columns the index has to agree with.
            execute("INSERT INTO c SELECT * FROM src WHERE ts < '2023-01-02' AND exch = 'E0' ORDER BY c_int");
            execute("INSERT INTO p SELECT * FROM src WHERE ts < '2023-01-02' AND exch = 'E0' ORDER BY c_int");
            drainWalQueue();

            assertSqlCursors("SELECT * FROM p ORDER BY ts, c_int", "SELECT * FROM c ORDER BY ts, c_int");
        });
    }

    /**
     * The same missing flatten with NO var-size column anywhere: the merge then reads a fixed-width value
     * from an out-of-range row instead of dereferencing a garbage offset, so it returns WRONG DATA rather
     * than crashing. Silent corruption is the worse half of the defect and needs its own test -- a crash
     * announces itself, this does not.
     */
    @Test
    public void testFixedSizeColumnsO3AfterEarlierTransactionsMatchThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src AS (SELECT timestamp_sequence(1672531200000000L, 3600000000L) ts,"
                    + " rnd_symbol('E0','E1','E2') exch, rnd_int() c_int, rnd_long() c_long,"
                    + " rnd_double() c_double FROM long_sequence(240)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, c_int INT, c_long LONG, c_double DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, c_int INT, c_long LONG, c_double DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String cell : new String[]{"E0", "E1", "E2"}) {
                execute("INSERT INTO c SELECT * FROM src WHERE exch = '" + cell + "'");
                execute("INSERT INTO p SELECT * FROM src WHERE exch = '" + cell + "'");
                drainWalQueue();
            }
            execute("INSERT INTO c SELECT * FROM src WHERE ts < '2023-01-02' AND exch = 'E0' ORDER BY c_int");
            execute("INSERT INTO p SELECT * FROM src WHERE ts < '2023-01-02' AND exch = 'E0' ORDER BY c_int");
            drainWalQueue();

            assertSqlCursors("SELECT * FROM p ORDER BY ts, c_int", "SELECT * FROM c ORDER BY ts, c_int");
        });
    }

    @Test
    public void testEveryColumnTypeMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwinsAndSeed();
            assertTwinsAgree("after the in-order load");

            // The O3 arm, which this test could not have before: an out-of-order merge into a day that
            // already holds several cells used to crash the JVM on any var-size column, and every
            // complete column list has one. It runs every type through the merge path, not just the
            // append path the load above exercises.
            execute("INSERT INTO c SELECT * FROM src WHERE ts < '2023-01-02' AND exch = 'E0' ORDER BY c_int");
            execute("INSERT INTO p SELECT * FROM src WHERE ts < '2023-01-02' AND exch = 'E0' ORDER BY c_int");
            drainWalQueue();
            engine.releaseInactive();

            assertTwinsAgree("after an out-of-order merge into a populated cell");
        });
    }

    /**
     * The same comparison with every cell converted to parquet. Conversion is per cell and the encoder
     * re-derives each column's layout, so this is where a type's shape is most likely to be mishandled.
     */
    @Test
    public void testEveryColumnTypeSurvivesPerCellParquetConversion() throws Exception {
        assertMemoryLeak(() -> {
            createTwinsAndSeed();

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();

            assertTwinsAgree("after converting the first day, per cell");

            // ... and back again: the deferred native conversion re-materialises every column.
            execute("ALTER TABLE c CONVERT PARTITION TO NATIVE LIST '2023-01-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO NATIVE LIST '2023-01-01'");
            drainWalQueue();
            engine.releaseInactive();

            assertTwinsAgree("after converting the first day back to native");
        });
    }

    /**
     * Both twins fed from ONE generated source table, so a divergence can only come from the storage
     * shape, never from the data.
     */
    private void createTwinsAndSeed() throws Exception {
        execute("CREATE TABLE src AS (" + GENERATOR + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE c " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        execute("CREATE TABLE p " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY WAL");
        // PER CELL, one commit each. This USED to be the only shape a fully-typed composite table
        // could be loaded in: a single interleaved insert was refused on any table with a var-size
        // column, and every column list worth calling complete has one. That refusal is gone, and
        // CompositeInterleavedVarSizeTest covers the interleaved shape directly; the per-cell load is
        // kept here because it is the shape this class's parquet arm was built around, and because
        // both shapes are worth having over a column list this wide.
        for (String cell : new String[]{"E0", "E1", "E2"}) {
            execute("INSERT INTO c SELECT * FROM src WHERE exch = '" + cell + "'");
            drainWalQueue();
        }
        execute("INSERT INTO p SELECT * FROM src");
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * Every column of every row, in one deterministic order, compared between the twins. Projecting
     * {@code *} is the point: a type whose per-cell handling is wrong shows up as a value difference,
     * and only a full projection can see it.
     */
    private void assertTwinsAgree(String when) throws Exception {
        try {
            assertSqlCursors("SELECT * FROM p ORDER BY ts, c_int", "SELECT * FROM c ORDER BY ts, c_int");
        } catch (AssertionError e) {
            throw new AssertionError("composite and plain twins diverged " + when, e);
        }
    }
}
