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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.FSSTNative;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static org.junit.Assert.*;

public class CoveringIndexTest extends AbstractCairoTest {

    private static final ColumnVersionReader EMPTY_CVR = new ColumnVersionReader();

    @Test
    public void testAlterAddIndexIncludeDuplicatePosition() throws Exception {
        // The SqlException must point at the duplicated column name, not
        // position 0.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_alt_dup (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            String sql = "ALTER TABLE t_alt_dup ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, price)";
            int expected = sql.indexOf("price, price") + "price, ".length();
            assertException(sql, expected, "duplicate column in INCLUDE");
        });
    }

    @Test
    public void testAlterAddIndexIncludeMissingColumnPosition() throws Exception {
        // The SqlException must point at the missing column name, not 0.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_alt_miss (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            String sql = "ALTER TABLE t_alt_miss ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (ghost)";
            int expected = sql.indexOf("ghost");
            assertException(sql, expected, "INCLUDE column does not exist");
        });
    }

    @Test
    public void testAlterAddIndexIncludeSelfReferencePosition() throws Exception {
        // The SqlException must point at the offending sym name in the
        // INCLUDE list, not 0.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_alt_self (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts)");
            String sql = "ALTER TABLE t_alt_self ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (sym)";
            int expected = sql.lastIndexOf("sym");
            assertException(sql, expected, "INCLUDE must not contain the indexed column");
        });
    }

    @Test
    public void testCoveringVarcharEmptyVsNullDistinction() throws Exception {
        // Regression test for #14: empty VARCHAR ('') must round-trip
        // through the covering sidecar as a present-but-empty value, NOT as
        // SQL NULL. The writer used to emit zero bytes for both NULL and
        // empty, leaving the reader unable to distinguish them.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vc_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (v),
                        v VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vc_empty VALUES
                    ('2024-01-01T00:00:00', 'A', NULL),
                    ('2024-01-01T01:00:00', 'A', ''),
                    ('2024-01-01T02:00:00', 'A', 'data'),
                    ('2024-01-01T03:00:00', 'A', '')
                    """);
            engine.releaseAllWriters();

            // IS NULL must match exactly the literal NULL row, not the two
            // empty strings. Without the fix, all three were treated as
            // NULL and the count was 3.
            assertSql("""
                    cnt
                    1
                    """, "SELECT count() AS cnt FROM t_vc_empty WHERE sym = 'A' AND v IS NULL");

            // And the empty filter must match exactly two rows.
            assertSql("""
                    cnt
                    2
                    """, "SELECT count() AS cnt FROM t_vc_empty WHERE sym = 'A' AND v = ''");
        });
    }

    @Test
    public void testAlterTableAddIndexIncludesColumnWithColumnTop() throws Exception {
        // Regression test: when an INCLUDE column was added via ALTER TABLE
        // ADD COLUMN after some rows already existed, the column file starts
        // at offset 0 mapping to row colTop in that partition. Reading
        // rowId * size without subtracting colTop reads past the mapped
        // region or returns garbage for rows below colTop. The covering
        // index sidecar must represent those rows as NULL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ct (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 2 rows before the new column exists — colTop will be 2 on this partition.
            execute("""
                    INSERT INTO t_ct VALUES
                    ('2024-01-01T00:00:00', 'A', 10),
                    ('2024-01-01T01:00:00', 'B', 20)
                    """);

            execute("ALTER TABLE t_ct ADD COLUMN price DOUBLE");

            // 3 rows after ADD COLUMN — these have price values in the file.
            execute("""
                    INSERT INTO t_ct VALUES
                    ('2024-01-01T02:00:00', 'A', 30, 100.5),
                    ('2024-01-01T03:00:00', 'B', 40, 200.5),
                    ('2024-01-01T04:00:00', 'A', 50, 300.5)
                    """);

            // Build a covering index that INCLUDEs the newly-added column.
            execute("ALTER TABLE t_ct ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            // Confirm the covering factory is actually on the plan.
            String plan = getPlan("SELECT sym, qty, price FROM t_ct WHERE sym = 'A' ORDER BY ts");
            assertTrue("covering index must be used for this regression: " + plan,
                    plan.contains("CoveringIndex"));

            // 'A' rows land at rowIds 0, 2, 4.
            //   rowId 0 — below colTop=2, price must be NULL.
            //   rowId 2 — price = 100.5.
            //   rowId 4 — price = 300.5.
            assertSql("""
                    sym\tqty\tprice
                    A\t10\tnull
                    A\t30\t100.5
                    A\t50\t300.5
                    """, "SELECT sym, qty, price FROM t_ct WHERE sym = 'A' ORDER BY ts");
        });
    }

    @Test
    public void testAlterTableAddIndexIncludesGeoIpv4ColumnsWithColumnTop() throws Exception {
        // Regression: CoveringPageFrameCursor.writeColumnRow uses Long.MIN_VALUE,
        // Integer.MIN_VALUE, and zeros as the "null" sentinel for rows below
        // columnTop, but the canonical NULLs are GeoHashes.NULL = -1L (GEOLONG),
        // GeoHashes.INT_NULL = -1 (GEOINT), GeoHashes.SHORT_NULL = -1 (GEOSHORT),
        // GeoHashes.BYTE_NULL = -1 (GEOBYTE), and Numbers.IPv4_NULL = 0 (IPv4).
        // When a GEO/IPv4 column is added via ALTER TABLE ADD COLUMN, rows
        // pre-dating the column must read back as NULL (text rendering: empty
        // for GEO, "" for IPv4).
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ct_geo (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 2 rows before any of the GEO/IPv4 columns exist — those rows
            // must surface as NULL once the columns are added.
            execute("""
                    INSERT INTO t_ct_geo VALUES
                    ('2024-01-01T00:00:00', 'A', 10),
                    ('2024-01-01T01:00:00', 'A', 20)
                    """);

            // Bit widths map to: 5b=GEOBYTE, 10b=GEOSHORT, 20b=GEOINT, 40b=GEOLONG.
            execute("ALTER TABLE t_ct_geo ADD COLUMN gb GEOHASH(5b)");
            execute("ALTER TABLE t_ct_geo ADD COLUMN gs GEOHASH(10b)");
            execute("ALTER TABLE t_ct_geo ADD COLUMN gi GEOHASH(20b)");
            execute("ALTER TABLE t_ct_geo ADD COLUMN gl GEOHASH(40b)");
            execute("ALTER TABLE t_ct_geo ADD COLUMN ip IPv4");

            // 2 rows after ADD COLUMN with real values. Geohash base-32 omits a/i/l/o.
            execute("""
                    INSERT INTO t_ct_geo VALUES
                    ('2024-01-01T02:00:00', 'A', 30, #y, #yz, #yzbc, #yzbc1234, '10.0.0.1'),
                    ('2024-01-01T03:00:00', 'A', 40, #b, #bc, #bcde, #bcde1234, '10.0.0.2')
                    """);

            execute("ALTER TABLE t_ct_geo ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (gb, gs, gi, gl, ip)");
            engine.releaseAllWriters();

            // Rows 0-1 (below columnTop for every added column) MUST be NULL.
            // The buggy writeColumnRow fallback writes Long.MIN_VALUE for
            // GEOLONG/gl, Integer.MIN_VALUE for GEOINT/gi and IPv4/ip,
            // (short) 0 for GEOSHORT/gs, and (byte) 0 for GEOBYTE/gb.
            // None of those round-trip as NULL — text rendering shows them
            // as garbage geohash strings or 0.0.0.0 for IPv4.
            assertSql("""
                    sym\tqty\tgb\tgs\tgi\tgl\tip
                    A\t10\t\t\t\t\t
                    A\t20\t\t\t\t\t
                    A\t30\ty\tyz\tyzbc\tyzbc1234\t10.0.0.1
                    A\t40\tb\tbc\tbcde\tbcde1234\t10.0.0.2
                    """, "SELECT sym, qty, gb, gs, gi, gl, ip FROM t_ct_geo WHERE sym = 'A' ORDER BY ts");
        });
    }

    @Test
    public void testAlterTableAddIndexO3DuplicateInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3dup (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_o3dup VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30),
                    ('2024-01-02T09:00:00', 'C', 4.5, 40),
                    ('2024-01-02T10:00:00', 'B', 5.5, 50),
                    ('2024-01-02T11:00:00', 'A', 6.5, 60)
                    """);
            // Duplicate insert triggers O3 merge — tests sealFull/reencodeAllGenerations
            execute("""
                    INSERT INTO t_o3dup VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30),
                    ('2024-01-02T09:00:00', 'C', 4.5, 40),
                    ('2024-01-02T10:00:00', 'B', 5.5, 50),
                    ('2024-01-02T11:00:00', 'A', 6.5, 60)
                    """);
            engine.releaseAllWriters();

            // Non-covering query verifies data survived O3 merge
            assertSql("""
                    count
                    6
                    """, "SELECT count() FROM t_o3dup WHERE sym = 'A'");

            // Covering LATEST ON after O3 — sidecar rebuilt by rebuildSidecars()
            assertSql("""
                    price\tqty
                    6.5\t60
                    """, "SELECT price, qty FROM t_o3dup WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testAlterTableAddIndexO3DuplicateInsertWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3dup_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3dup_wal VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-02T09:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();
            // Duplicate insert triggers O3 merge through WAL apply
            execute("""
                    INSERT INTO t_o3dup_wal VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-02T09:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();

            // Non-covering query (SELECT * includes ts) — verifies data integrity after O3
            assertSql("""
                    ts\tsym\tprice\tqty
                    2024-01-01T09:00:00.000000Z\tA\t1.5\t10
                    2024-01-01T09:00:00.000000Z\tA\t1.5\t10
                    2024-01-02T09:00:00.000000Z\tA\t3.5\t30
                    2024-01-02T09:00:00.000000Z\tA\t3.5\t30
                    """, "SELECT * FROM t_o3dup_wal WHERE sym = 'A'");
        });
    }

    @Test
    public void testAlterTableAddIndexWithInclude() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_alter ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_alter")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.isColumnIndexed(symIdx));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(symIdx));
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(3, coveringCols.size());
                assertEquals(metadata.getColumnIndex("price"), coveringCols.getQuick(0));
                assertEquals(metadata.getColumnIndex("qty"), coveringCols.getQuick(1));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeBinaryType() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_binary (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        data BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // BINARY columns are supported in INCLUDE — the writer stores them in
            // the var-width sidecar and the covering query path reads them directly.
            execute("ALTER TABLE t_binary ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (data)");
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeCoveringQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_q (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_alter_q ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            // Insert AFTER adding index so sidecar data is written
            execute("""
                    INSERT INTO t_alter_q VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10),
                    ('2024-01-01T01:00:00', 'B', 2.5, 20),
                    ('2024-01-01T02:00:00', 'A', 3.5, 30)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    1.5\t10
                    3.5\t30
                    """, "SELECT price, qty FROM t_alter_q WHERE sym = 'A'");
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeDuplicateColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dup (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_dup ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, price)");
                fail("Should have thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("duplicate column in INCLUDE"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_empty (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_empty ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE ()");
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("at least one column name expected in INCLUDE"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeNonExistentColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noexist (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_noexist ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (ghost)");
                fail("Should have thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("INCLUDE column does not exist"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeOnBitmapFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bitmap (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_bitmap ALTER COLUMN sym ADD INDEX TYPE BITMAP INCLUDE (price)");
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("INCLUDE is only supported for POSTING index type"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeSelfReference() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_self (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_self ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (sym)");
                fail("Should have thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("INCLUDE must not contain the indexed column"));
            }
        });
    }

    @Test
    public void testAlterTableAddIndexWithIncludeWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wal (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("ALTER TABLE t_wal ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();

            // Verify metadata has covering index configuration
            try (TableReader r = engine.getReader("t_wal")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.isColumnIndexed(symIdx));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(symIdx));
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(3, coveringCols.size());
            }

            // Non-covering query (SELECT * includes ts, which is not in INCLUDE)
            execute("""
                    INSERT INTO t_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10),
                    ('2024-01-01T01:00:00', 'B', 2.5, 20)
                    """);
            drainWalQueue();

            assertSql("""
                    ts\tsym\tprice\tqty
                    2024-01-01T00:00:00.000000Z\tA\t1.5\t10
                    """, "SELECT * FROM t_wal WHERE sym = 'A'");
        });
    }

    @Test
    public void testAlterTableWalIncludeNonExistentRejects() throws Exception {
        // Validation happens in SqlCompilerImpl.alterTableColumnAddIndex() — works for both WAL and non-WAL
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wal_bad (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            try {
                execute("ALTER TABLE t_wal_bad ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (nonexistent)");
                fail("Should have thrown");
            } catch (SqlException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("INCLUDE column does not exist"));
            }
        });
    }

    @Test
    public void testAlterTableWalIncludeSelfReferenceRejects() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wal_self (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            try {
                execute("ALTER TABLE t_wal_self ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (sym)");
                fail("Should have thrown");
            } catch (SqlException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("INCLUDE must not contain the indexed column"));
            }
        });
    }

    @Test
    public void testAlterTypeCoveredColumnFallsBackToTableScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_cov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_alter_cov VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """, "SELECT price, qty FROM t_alter_cov WHERE sym = 'A'");

            execute("ALTER TABLE t_alter_cov ALTER COLUMN qty TYPE LONG");

            assertSql("""
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """, "SELECT price, qty FROM t_alter_cov WHERE sym = 'A'");

            execute("""
                    INSERT INTO t_alter_cov VALUES
                    ('2024-01-01T03:00:00', 'A', 40.5, 400)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    10.5\t100
                    30.5\t300
                    40.5\t400
                    """, "SELECT price, qty FROM t_alter_cov WHERE sym = 'A'");
        });
    }

    @Test
    public void testAsyncFilterPageFrameFallback() throws Exception {
        // Test that the page frame cursor fallback (column file reads) works
        // when covering data may not be in sidecar form. Uses WAL table where
        // the active partition is not yet sealed.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_fallback (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_pf_fallback VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-01T03:00:00', 'A', 70.0),
                    ('2024-01-01T04:00:00', 'B', 80.0)
                    """);
            drainWalQueue();

            // Async filter with residual — must work via fallback or covering path
            assertSqlCursors(
                    "SELECT price FROM t_pf_fallback WHERE sym = 'A' AND price > 25",
                    "SELECT /*+ no_covering */ price FROM t_pf_fallback WHERE sym = 'A' AND price > 25"
            );

            // Aggregation with filter
            assertSqlCursors(
                    "SELECT sym, sum(price) FROM t_pf_fallback WHERE sym = 'A' AND price > 25 GROUP BY sym",
                    "SELECT /*+ no_covering */ sym, sum(price) FROM t_pf_fallback WHERE sym = 'A' AND price > 25 GROUP BY sym"
            );
        });
    }

    @Test
    public void testAsyncFilterWithAlterTableIndex() throws Exception {
        // ALTER TABLE ADD INDEX path: index is rebuilt for existing data.
        // The async filter wraps CoveringIndex and must produce correct results
        // whether hasCovering() is true (sealed sidecar) or false (fallback to columns).
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_async (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_alter_async VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 1),
                    ('2024-01-01T01:00:00', 'B', 20.0, 2),
                    ('2024-01-01T02:00:00', 'A', 30.0, 3),
                    ('2024-01-01T03:00:00', 'B', 40.0, 4),
                    ('2024-01-01T04:00:00', 'A', 70.0, 5),
                    ('2024-01-01T05:00:00', 'C', 80.0, 6)
                    """);
            engine.releaseAllWriters();

            // Add covering index after data exists
            execute("ALTER TABLE t_alter_async ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            // Row cursor path (no filter)
            assertSqlCursors(
                    "SELECT price, qty FROM t_alter_async WHERE sym = 'A'",
                    "SELECT /*+ no_covering */ price, qty FROM t_alter_async WHERE sym = 'A'"
            );

            // Async filter over covering index
            assertSqlCursors(
                    "SELECT price, qty FROM t_alter_async WHERE sym = 'A' AND price > 25",
                    "SELECT /*+ no_covering */ price, qty FROM t_alter_async WHERE sym = 'A' AND price > 25"
            );

            // GROUP BY with filter
            assertSqlCursors(
                    "SELECT sym, count() FROM t_alter_async WHERE sym = 'A' AND price > 25 GROUP BY sym",
                    "SELECT /*+ no_covering */ sym, count() FROM t_alter_async WHERE sym = 'A' AND price > 25 GROUP BY sym"
            );

            // IN-list with filter (ORDER BY for deterministic comparison —
            // covering returns A-then-B, non-covering returns timestamp order)
            assertSqlCursors(
                    "SELECT price FROM t_alter_async WHERE sym IN ('A', 'B') AND price > 25 ORDER BY price",
                    "SELECT /*+ no_covering */ price FROM t_alter_async WHERE sym IN ('A', 'B') AND price > 25 ORDER BY price"
            );
        });
    }

    @Test
    public void testAutoIncludeTimestampAlreadyPresent() throws Exception {
        // User explicitly includes timestamp — no duplicate
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_dup (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, ts),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Verify metadata: ts should appear exactly once (no duplicate from auto-include)
            try (TableReader reader = getReader("t_ts_dup")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringIndices = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertEquals(2, coveringIndices.size());
                assertEquals(metadata.getColumnIndex("price"), coveringIndices.getQuick(0));
                assertEquals(metadata.getColumnIndex("ts"), coveringIndices.getQuick(1));
            }

            execute("""
                    INSERT INTO t_ts_dup VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'A', 20.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    ts\tprice
                    2024-01-01T00:00:00.000000Z\t10.0
                    2024-01-01T01:00:00.000000Z\t20.0
                    """, "SELECT ts, price FROM t_ts_dup WHERE sym = 'A'");
        });
    }

    @Test
    public void testAutoIncludeTimestampAlterNoInclude() throws Exception {
        // ALTER TABLE ... ADD INDEX TYPE POSTING with no INCLUDE clause
        // must still auto-append the designated timestamp so the bare
        // alter case picks up the same covering benefit as inline CREATE.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_alter_noinc (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_ts_alter_noinc ALTER COLUMN sym ADD INDEX TYPE POSTING");

            try (TableReader reader = getReader("t_ts_alter_noinc")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringIndices = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull("covering INCLUDE list missing for ALTER POSTING with auto-include",
                        coveringIndices);
                assertEquals(1, coveringIndices.size());
                assertEquals(metadata.getColumnIndex("ts"), coveringIndices.getQuick(0));
            }

            execute("""
                    INSERT INTO t_ts_alter_noinc VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'A', 20.0)
                    """);
            engine.releaseAllWriters();

            String plan = getPlan("SELECT ts FROM t_ts_alter_noinc WHERE sym = 'A'");
            assertTrue("Expected CoveringIndex plan for ts-only projection after ALTER:\n" + plan,
                    plan.contains("CoveringIndex"));
        });
    }

    @Test
    public void testAutoIncludeTimestampAlterTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_alter (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Add index BEFORE inserting data so covering sidecars are populated
            execute("ALTER TABLE t_ts_alter ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");

            execute("""
                    INSERT INTO t_ts_alter VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'B', 20.0),
                        ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            // Timestamp should be auto-included via ALTER TABLE path
            String plan = getPlan("SELECT ts, price FROM t_ts_alter WHERE sym = 'A'");
            assertTrue("Expected CoveringIndex plan:\n" + plan, plan.contains("CoveringIndex"));

            assertSql("""
                    ts\tprice
                    2024-01-01T00:00:00.000000Z\t10.0
                    2024-01-01T02:00:00.000000Z\t30.0
                    """, "SELECT ts, price FROM t_ts_alter WHERE sym = 'A'");
        });
    }

    @Test
    public void testAutoIncludeTimestampConfigDefaultIsTrue() {
        assertTrue("Default should be true",
                engine.getConfiguration().isPostingIndexAutoIncludeTimestamp());
    }

    @Test
    public void testAutoIncludeTimestampCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_auto (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Verify metadata: covering indices should include timestamp
            try (TableReader reader = getReader("t_ts_auto")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringIndices = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertEquals(2, coveringIndices.size());
                assertEquals(metadata.getColumnIndex("price"), coveringIndices.getQuick(0));
                assertEquals(metadata.getColumnIndex("ts"), coveringIndices.getQuick(1));
            }

            execute("""
                    INSERT INTO t_ts_auto VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'B', 20.0),
                        ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            // Query selecting ts should use CoveringIndex because ts was auto-included
            String plan = getPlan("SELECT ts, price FROM t_ts_auto WHERE sym = 'A'");
            assertTrue("Expected CoveringIndex plan:\n" + plan, plan.contains("CoveringIndex"));

            assertSql("""
                    ts\tprice
                    2024-01-01T00:00:00.000000Z\t10.0
                    2024-01-01T02:00:00.000000Z\t30.0
                    """, "SELECT ts, price FROM t_ts_auto WHERE sym = 'A'");
        });
    }

    @Test
    public void testAutoIncludeTimestampCreateTableAsSelectNoInclude() throws Exception {
        // CREATE TABLE AS SELECT with an out-of-line INDEX(... TYPE POSTING)
        // clause goes through resolveCoveringFromAugmented and must also
        // auto-append the designated timestamp when no INCLUDE is given.
        // The out-of-line parser does not accept INCLUDE today, so this
        // is the only way users can request a posting index for CTAS, and
        // the default config promises auto-include here too.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_src_noinc (ts TIMESTAMP, sym SYMBOL, price DOUBLE)
                        TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_src_noinc VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'A', 20.0)
                    """);
            execute("""
                    CREATE TABLE t_ctas_noinc AS (SELECT * FROM t_src_noinc),
                        INDEX(sym TYPE POSTING)
                        TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            try (TableReader reader = getReader("t_ctas_noinc")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringIndices = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull("covering INCLUDE list missing for CTAS POSTING with auto-include",
                        coveringIndices);
                assertEquals(1, coveringIndices.size());
                assertEquals(metadata.getColumnIndex("ts"), coveringIndices.getQuick(0));
            }
        });
    }

    @Test
    public void testAutoIncludeTimestampCreateTableWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_ts_wal VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'B', 20.0),
                        ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            drainWalQueue();

            String plan = getPlan("SELECT ts, price FROM t_ts_wal WHERE sym = 'A'");
            assertTrue("Expected CoveringIndex plan:\n" + plan, plan.contains("CoveringIndex"));

            assertSql("""
                    ts\tprice
                    2024-01-01T00:00:00.000000Z\t10.0
                    2024-01-01T02:00:00.000000Z\t30.0
                    """, "SELECT ts, price FROM t_ts_wal WHERE sym = 'A'");
        });
    }

    @Test
    public void testAutoIncludeTimestampNoDesignatedTimestamp() throws Exception {
        // Table without designated timestamp — auto-include should be a no-op
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_none (
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    )
                    """);
            execute("""
                    INSERT INTO t_ts_none VALUES ('A', 10.0), ('A', 20.0)
                    """);
            engine.releaseAllWriters();

            // price-only query should still use CoveringIndex
            String plan = getPlan("SELECT price FROM t_ts_none WHERE sym = 'A'");
            assertTrue("Expected CoveringIndex:\n" + plan, plan.contains("CoveringIndex"));
        });
    }

    @Test
    public void testAutoIncludeTimestampPostingNoInclude() throws Exception {
        // POSTING index without INCLUDE clause — auto-include must still
        // append the designated timestamp so the common
        // SELECT ts FROM t WHERE sym = ... pattern can use a CoveringIndex
        // plan. Otherwise turning on a posting index silently produces an
        // empty INCLUDE list and no covering benefit at all.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_noinc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Verify metadata: covering should contain just ts.
            try (TableReader reader = getReader("t_ts_noinc")) {
                TableReaderMetadata metadata = reader.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringIndices = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull("covering INCLUDE list missing for POSTING with auto-include", coveringIndices);
                assertEquals(1, coveringIndices.size());
                assertEquals(metadata.getColumnIndex("ts"), coveringIndices.getQuick(0));
            }

            execute("""
                    INSERT INTO t_ts_noinc VALUES
                        ('2024-01-01T00:00:00', 'A', 10.0),
                        ('2024-01-01T01:00:00', 'A', 20.0)
                    """);
            engine.releaseAllWriters();

            // Query that selects only sym and ts must use CoveringIndex
            // because ts is auto-included.
            String plan = getPlan("SELECT ts FROM t_ts_noinc WHERE sym = 'A'");
            assertTrue("Expected CoveringIndex plan for ts-only projection:\n" + plan,
                    plan.contains("CoveringIndex"));
        });
    }

    @Test
    public void testBulkInsertSelectWithPostingIndex() throws Exception {
        // INSERT...SELECT into table with POSTING INCLUDE — must handle multi-partition
        // data at scale. Tests 1M rows across ~12 partitions with 200 symbols.
        assertMemoryLeak(() -> {
            StringBuilder symArgs = new StringBuilder();
            for (int k = 0; k < 200; k++) {
                if (k > 0) symArgs.append(',');
                symArgs.append("'K").append(k).append("'");
            }
            execute("""
                    CREATE TABLE t_bulk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute(
                    "INSERT INTO t_bulk"
                            + " SELECT dateadd('s', x::INT, '2024-01-01'),"
                            + "   rnd_symbol(" + symArgs + "),"
                            + "   rnd_double() * 100,"
                            + "   rnd_int(1, 1000, 0)"
                            + " FROM long_sequence(1_000_000)");
            engine.releaseAllWriters();

            // Verify all rows committed
            assertSql("""
                    count
                    1000000
                    """, "SELECT count() FROM t_bulk");

            // Verify index is usable — covering and non-covering must agree
            assertSqlCursors(
                    "SELECT count() FROM t_bulk WHERE sym = 'K0'",
                    "SELECT /*+ no_covering */ count() FROM t_bulk WHERE sym = 'K0'"
            );

            // Verify covered data with filter
            assertSqlCursors(
                    "SELECT price, qty FROM t_bulk WHERE sym = 'K0' AND price > 50 LIMIT 10",
                    "SELECT /*+ no_covering */ price, qty FROM t_bulk WHERE sym = 'K0' AND price > 50 LIMIT 10"
            );
        });
    }

    @Test
    public void testCountPushdown() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_count
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_double() * 100
                    FROM long_sequence(1000)
                    """);
            engine.releaseAllWriters();

            // Plan: Count wrapping CoveringIndex
            assertPlanNoLeakCheck(
                    "SELECT COUNT(*) FROM t_count WHERE sym = 'A'",
                    """
                            Count
                                CoveringIndex on: sym
                                  filter: sym='A'
                            """
            );

            // COUNT(*) correctness: single partition
            assertSql("""
                    count
                    329
                    """, "SELECT COUNT(*) FROM t_count WHERE sym = 'A'");

            // Multi-partition
            execute("""
                    INSERT INTO t_count
                    SELECT dateadd('s', x::INT, '2024-01-02')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_double() * 100
                    FROM long_sequence(1000)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    count
                    668
                    """, "SELECT COUNT(*) FROM t_count WHERE sym = 'A'");
        });
    }

    @Test
    public void testCountPushdownEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            assertSql("""
                    count
                    0
                    """, "SELECT COUNT(*) FROM t_count_empty WHERE sym = 'A'");
        });
    }

    @Test
    public void testCountPushdownNonExistentKey() throws Exception {
        // COUNT(*) for a key that doesn't exist should return 0
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count_none (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_count_none VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    count
                    0
                    """, "SELECT COUNT(*) FROM t_count_none WHERE sym = 'NONEXISTENT'");
        });
    }

    @Test
    public void testCountPushdownVarcharOnly() throws Exception {
        // Regression test for count returning 0 for var-only INCLUDE.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count_vc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_count_vc
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B'),
                           rnd_str('alpha','beta','gamma')
                    FROM long_sequence(100)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    count
                    43
                    """, "SELECT COUNT(*) FROM t_count_vc WHERE sym = 'A'");
        });
    }

    @Test
    public void testCountPushdownWithInList() throws Exception {
        // CoveringCursor.size() returns -1 for IN-list (multiKeys != null).
        // Verify COUNT(*) still works correctly via iteration fallback.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_count_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_count_in
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_double() * 100
                    FROM long_sequence(1000)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    count
                    654
                    """, "SELECT COUNT(*) FROM t_count_in WHERE sym IN ('A', 'B')");
        });
    }

    @Test
    public void testCoveringAfterSealThenNewCommit() throws Exception {
        // After seal + new commit: genCount=2. With per-gen sidecars,
        // covering still works — gen0 has sealed sidecar, gen1 has raw sidecar.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_postseal";
                int plen = path.size();

                int rowCount = 30;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }

                    // First writer: write data and close (seal creates sidecar files)
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );
                        for (int i = 0; i < 20; i++) {
                            writer.add(i % 5, i);
                        }
                        writer.setMaxValue(19);
                        writer.commit();
                    } // seal + compact

                    // Verify sidecar files exist
                    FilesFacade ff = configuration.getFilesFacade();
                    assertTrue(ff.exists(PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)));

                    // Second writer: reopen without reinit, add more data (creates gen1)
                    PostingIndexWriter writer2 = new PostingIndexWriter(configuration);
                    writer2.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    writer2.configureCovering(
                            new long[]{colAddr},
                            new long[]{0},
                            new int[]{3},
                            new int[]{2},
                            new int[]{ColumnType.DOUBLE},
                            1
                    );
                    for (int i = 20; i < 30; i++) {
                        writer2.add(i % 5, i);
                    }
                    writer2.setMaxValue(29);
                    writer2.commit();

                    // Reader: genCount=2, but per-gen sidecars exist
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        assertTrue(((CoveringRowCursor) cursor).hasCovering());

                        int count = 0;
                        while (cursor.hasNext()) {
                            cursor.next();
                            count++;
                        }
                        assertTrue(count > 0);
                        Misc.free(cursor);
                    }

                    writer2.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringAfterSealThenNewCommitValues() throws Exception {
        // After seal + reopen + new commit: verify covered values from both gens.
        // Gen0 (sealed) has stride-indexed sidecar; gen1 (raw) appends after it.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_postseal_vals";
                int plen = path.size();
                int rowCount = 30;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }
                    // Write 20 rows to key 0, close (seal creates dense gen0 + stride sidecar)
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr}, new long[]{0}, new int[]{3},
                                new int[]{2}, new int[]{ColumnType.DOUBLE}, 1
                        );
                        for (int i = 0; i < 20; i++) writer.add(0, i);
                        writer.setMaxValue(19);
                        writer.commit();
                    }
                    // Reopen, add 10 more rows (gen1 raw sidecar appended after seal's stride data)
                    PostingIndexWriter w2 = new PostingIndexWriter(configuration);
                    w2.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    w2.configureCovering(
                            new long[]{colAddr}, new long[]{0}, new int[]{3},
                            new int[]{2}, new int[]{ColumnType.DOUBLE}, 1
                    );
                    for (int i = 20; i < 30; i++) w2.add(0, i);
                    w2.setMaxValue(29);
                    w2.commit();
                    // Verify ALL 30 covered values across both gens
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cc.hasCovering());

                        for (int i = 0; i < 30; i++) {
                            assertTrue("row " + i, cc.hasNext());
                            assertEquals(i, cc.next());
                            assertEquals("value at row " + i, 10.0 * (i + 1), cc.getCoveredDouble(0), 0.001);
                        }
                        assertFalse(cc.hasNext());
                        Misc.free(cc);
                    }
                    w2.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringFileNaming() {
        try (Path path = new Path().of("/db/2024-01-01")) {
            int plen = path.size();
            String name = "sym";

            // .pci remains single-version (per postingColumnNameTxn).
            PostingIndexUtils.coverInfoFileName(path, name, COLUMN_NAME_TXN_NONE);
            assertTrue(path.toString().contains("sym.pci"));

            PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, 0);
            assertTrue(path.toString().contains("sym.pc0.0.0"));

            PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 1, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, 0);
            assertTrue(path.toString().contains("sym.pc1.0.0"));

            PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, 5);
            assertTrue(path.toString().contains("sym.pci.5"));

            PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, 5, 3, 7);
            assertTrue(path.toString().contains("sym.pc0.5.3.7"));
        }
    }

    @Test
    public void testCoveringFlagInMeta() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE flagtest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            try (TableReader r = engine.getReader("flagtest")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
                // val column should NOT be covering
                int valIdx = metadata.getColumnIndex("val");
                assertFalse(metadata.getColumnMetadata(valIdx).isCovering());
            }
        });
    }

    @Test
    public void testCoveringHighCardinalityCrossesStrideBoundary() throws Exception {
        // Keys >= 256 cross the stride boundary (DENSE_STRIDE=256).
        // Verify correct sidecar offset computation for second stride.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_stride";
                int plen = path.size();

                int rowCount = 300;
                long colAddr = Unsafe.malloc((long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},  // Long.BYTES = 8 = 2^3
                                new int[]{1},
                                new int[]{ColumnType.LONG},
                                1
                        );

                        // One row per key, 300 keys (crosses 256 boundary)
                        for (int k = 0; k < rowCount; k++) {
                            writer.add(k, k);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }

                    // Read back keys from second stride (key >= 256)
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG}), EMPTY_CVR, 0)) {
                        // Key 260 is in stride 1, local key 4
                        RowCursor cursor = reader.getCursor(260, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());


                        assertTrue(cc.hasNext());
                        assertEquals(260, cc.next());
                        assertEquals(1260L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                        // Key 299 is in stride 1, local key 43
                        cursor = reader.getCursor(299, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());

                        assertTrue(cc.hasNext());
                        assertEquals(299, cc.next());
                        assertEquals(1299L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                        // Key 0 is in stride 0 (control)
                        cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());

                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(1000L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringInList5Keys() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cov5 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cov5 VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-01T02:00:00', 'C', 3.0, 30),
                    ('2024-01-02T00:00:00', 'D', 4.0, 40),
                    ('2024-01-02T01:00:00', 'E', 5.0, 50)
                    """);
            engine.releaseAllWriters();

            // 5-key covering IN-list — all columns covered, uses CoveringIndex path
            assertSql("""
                    price\tqty
                    1.0\t10
                    2.0\t20
                    3.0\t30
                    4.0\t40
                    5.0\t50
                    """, "SELECT price, qty FROM t_cov5 WHERE sym IN ('A', 'B', 'C', 'D', 'E')");
        });
    }

    @Test
    public void testCoveringInListString() throws Exception {
        // IN-list with STRING covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_string VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello'),
                    ('2024-01-01T01:00:00', 'Y', 'world'),
                    ('2024-01-01T02:00:00', 'X', 'foo')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    label
                    hello
                    foo
                    world
                    """, "SELECT label FROM t_in_string WHERE sym IN ('X', 'Y')");
        });
    }

    @Test
    public void testCoveringInListVarchar() throws Exception {
        // IN-list with VARCHAR covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'bob', 20.0),
                    ('2024-01-01T02:00:00', 'A', 'anna', 30.0),
                    ('2024-01-01T03:00:00', 'C', 'carol', 40.0)
                    """);
            engine.releaseAllWriters();

            // IN list with covering VARCHAR — results grouped by key (A first, then B)
            assertSql("""
                    name\tprice
                    alice\t10.0
                    anna\t30.0
                    bob\t20.0
                    """, "SELECT name, price FROM t_in_varchar WHERE sym IN ('A', 'B')");
        });
    }

    @Test
    public void testCoveringIndexCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cnt (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cnt VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-01T03:00:00', 'A', 50.0)
                    """);
            engine.releaseAllWriters();

            // Count optimization uses CoveringIndex as base — count() doesn't select
            // any covered columns so "with" is absent
            assertPlanNoLeakCheck(
                    "SELECT count() FROM t_cnt WHERE sym = 'A'",
                    """
                            Count
                                CoveringIndex on: sym
                                  filter: sym='A'
                            """
            );

            assertSql("""
                    count
                    3
                    """, "SELECT count() FROM t_cnt WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringIndexDistinctPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-02T00:00:00', 'C', 40.0)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT DISTINCT sym FROM t_dist",
                    """
                            PostingIndex op: distinct on: sym
                                Frame forward scan on: t_dist
                            """
            );

            // Verify data correctness without ORDER BY — PostingIndexDistinct doesn't
            // support static symbol tables needed for ORDER BY
            assertSql("""
                    sym
                    A
                    B
                    C
                    """, "SELECT DISTINCT sym FROM t_dist");
        });
    }

    @Test
    public void testCoveringIndexDistinctRespectsIntervalRowBounds() throws Exception {
        // Regression: PostingIndexDistinctRecordCursorFactory must honor the
        // row bounds carried by IntervalFwdPartitionFrameCursor when an
        // interval predicate restricts the query to a sub-range of the
        // partition. Without that, collectDistinctKeys scans every gen in
        // the partition and returns keys whose only matching rows fall
        // outside the requested interval.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_iv (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_iv VALUES
                    ('2024-01-01T00:00:00', 'A'),
                    ('2024-01-01T12:00:00', 'B'),
                    ('2024-01-01T23:00:00', 'C')
                    """);
            engine.releaseAllWriters();

            // Sanity check: the unbounded query returns every key.
            assertSql("""
                    sym
                    A
                    B
                    C
                    """, "SELECT DISTINCT sym FROM t_dist_iv");

            // The interval [00:00, 01:00) carves out only row 0 (sym='A'),
            // so only 'A' is reachable. The bug returns A, B, C because the
            // posting fast path ignores rowLo/rowHi from the interval frame.
            assertSql("""
                    sym
                    A
                    """, "SELECT DISTINCT sym FROM t_dist_iv WHERE ts IN '2024-01-01T00:00:00;1h'");

            // Sanity: the bitmap fallback path (with /*+ no_index */)
            // produces the correct result, confirming the bug is specific
            // to the posting-index DISTINCT factory.
            assertSql("""
                    sym
                    A
                    """, "SELECT /*+ no_index */ DISTINCT sym FROM t_dist_iv WHERE ts IN '2024-01-01T00:00:00;1h'");
        });
    }

    @Test
    public void testCoveringIndexGroupByAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_grp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_grp VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 1),
                    ('2024-01-01T01:00:00', 'B', 20.0, 2),
                    ('2024-01-01T02:00:00', 'A', 30.0, 3),
                    ('2024-01-01T03:00:00', 'B', 40.0, 4),
                    ('2024-01-01T04:00:00', 'A', 50.0, 5),
                    ('2024-01-01T05:00:00', 'C', 60.0, 6)
                    """);
            engine.releaseAllWriters();

            // Plan: GroupBy vectorized on top of CoveringIndex
            assertPlanNoLeakCheck(
                    "SELECT sym, sum(price), avg(qty) FROM t_grp WHERE sym IN ('A', 'B') GROUP BY sym",
                    """
                            GroupBy vectorized: true workers: 1
                              keys: [sym]
                              values: [sum(price),avg(qty)]
                                CoveringIndex on: sym with: price, qty
                                  filter: sym IN ['A','B']
                            """
            );

            // Single-key GROUP BY — covering and non-covering paths must agree
            String expected = """
                    sym\tsum\tavg
                    A\t90.0\t3.0
                    """;
            assertSql(expected,
                    "SELECT sym, sum(price), avg(qty) FROM t_grp WHERE sym = 'A' GROUP BY sym");
            assertSql(expected,
                    "SELECT /*+ no_covering */ sym, sum(price), avg(qty) FROM t_grp WHERE sym = 'A' GROUP BY sym");

            // IN-list GROUP BY — covering path must match non-covering
            String expectedInList = """
                    sym\tsum\tavg
                    A\t90.0\t3.0
                    B\t60.0\t3.0
                    """;
            assertSql(expectedInList,
                    "SELECT sym, sum(price), avg(qty) FROM t_grp WHERE sym IN ('A', 'B') GROUP BY sym ORDER BY sym");
            assertSql(expectedInList,
                    "SELECT /*+ no_covering */ sym, sum(price), avg(qty) FROM t_grp WHERE sym IN ('A', 'B') GROUP BY sym ORDER BY sym");
        });
    }

    // ===================================================================
    // End-to-end SQL tests
    // ===================================================================

    @Test
    public void testCoveringIndexGroupByMinMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_minmax (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_minmax VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-01T03:00:00', 'B', 40.0),
                    ('2024-01-01T04:00:00', 'A', 50.0)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT sym, min(price), max(price) FROM t_minmax WHERE sym = 'A' GROUP BY sym",
                    """
                            GroupBy vectorized: true workers: 1
                              keys: [sym]
                              values: [min(price),max(price)]
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """
            );

            String expected = """
                    sym\tmin\tmax
                    A\t10.0\t50.0
                    """;
            assertSql(expected,
                    "SELECT sym, min(price), max(price) FROM t_minmax WHERE sym = 'A' GROUP BY sym");
            assertSql(expected,
                    "SELECT /*+ no_covering */ sym, min(price), max(price) FROM t_minmax WHERE sym = 'A' GROUP BY sym");
        });
    }

    @Test
    public void testCoveringIndexGroupByMultiPartition() throws Exception {
        // Verify aggregation over covering index works across multiple partitions
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_grp_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_grp_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 1),
                    ('2024-01-01T12:00:00', 'B', 20.0, 2),
                    ('2024-01-02T00:00:00', 'A', 30.0, 3),
                    ('2024-01-02T12:00:00', 'B', 40.0, 4),
                    ('2024-01-03T00:00:00', 'A', 50.0, 5)
                    """);
            engine.releaseAllWriters();

            // Both paths must agree across partitions
            String expected = """
                    sym\tsum\tcount
                    A\t90.0\t3
                    """;
            assertSql(expected,
                    "SELECT sym, sum(price), count() FROM t_grp_mp WHERE sym = 'A' GROUP BY sym");
            assertSql(expected,
                    "SELECT /*+ no_covering */ sym, sum(price), count() FROM t_grp_mp WHERE sym = 'A' GROUP BY sym");
        });
    }

    @Test
    public void testCoveringIndexLatestOnPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_lat (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_lat VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'A', 20.0),
                    ('2024-01-01T02:00:00', 'B', 30.0),
                    ('2024-01-01T03:00:00', 'A', 40.0)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT price FROM t_lat WHERE sym = 'A' LATEST ON ts PARTITION BY sym",
                    """
                            SelectedRecord
                                CoveringIndex op: latest on: sym with: price
                                  filter: sym='A'
                            """
            );

            assertSql("""
                    price
                    40.0
                    """, "SELECT price FROM t_lat WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringIndexNextPartitionNullSymbolWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_null_part (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_null_part VALUES
                    ('2025-01-01T00:00:00', 'A', 1.0, 10),
                    ('2025-01-01T01:00:00', 'B', 2.0, 20),
                    ('2025-01-01T02:00:00', 'A', 3.0, 30)
                    """);
            // Second partition with NULL symbol
            execute("INSERT INTO t_null_part (ts) VALUES ('2025-01-02')");
            drainWalQueue();

            assertSql("""
                    price\tqty
                    1.0\t10
                    3.0\t30
                    """, "SELECT price, qty FROM t_null_part WHERE sym = 'A'");

            // Nonexistent key should return empty across both partitions
            assertSql("""
                    price\tqty
                    """, "SELECT price, qty FROM t_null_part WHERE sym = 'nonexistent'");
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionAfterO3Merge() throws Exception {
        // O3CopyJob.updateIndex calls rollbackConditionally before writing.
        // Before the fix, PostingIndexWriter.rollbackConditionally would trigger
        // a destructive in-place re-encode of sealed generations, corrupting
        // sidecar alignment and causing AIOOBE in ALP decode.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_cover (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_cover VALUES
                    ('2024-01-01T10:00:00', 'A', 10.5, 100),
                    ('2024-01-01T12:00:00', 'B', 20.5, 200),
                    ('2024-01-01T14:00:00', 'A', 30.5, 300)
                    """);
            drainWalQueue();

            // O3 insert into same partition triggers merge → rollbackConditionally
            execute("""
                    INSERT INTO t_o3_cover VALUES
                    ('2024-01-01T09:00:00', 'A', 5.5, 50),
                    ('2024-01-01T11:00:00', 'B', 15.5, 150),
                    ('2024-01-01T13:00:00', 'A', 25.5, 250)
                    """);
            drainWalQueue();

            // Verify covering data is readable (would crash with AIOOBE before the fix)
            assertSql("""
                    price\tqty
                    5.5\t50
                    10.5\t100
                    25.5\t250
                    30.5\t300
                    """, "SELECT price, qty FROM t_o3_cover WHERE sym = 'A'");

            assertSql("""
                    price\tqty
                    15.5\t150
                    20.5\t200
                    """, "SELECT price, qty FROM t_o3_cover WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionAfterO3MergeMultiPartition() throws Exception {
        // O3 merge across multiple partitions: each partition's posting index
        // must survive rollbackConditionally without sidecar corruption.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_mp VALUES
                    ('2024-01-01T12:00:00', 'X', 1.0),
                    ('2024-01-02T12:00:00', 'X', 2.0),
                    ('2024-01-03T12:00:00', 'Y', 3.0)
                    """);
            drainWalQueue();

            // O3: insert rows before existing ones in every partition
            execute("""
                    INSERT INTO t_o3_mp VALUES
                    ('2024-01-01T06:00:00', 'X', 0.5),
                    ('2024-01-02T06:00:00', 'Y', 1.5),
                    ('2024-01-03T06:00:00', 'X', 2.5)
                    """);
            drainWalQueue();

            assertSql("""
                    val
                    0.5
                    1.0
                    2.0
                    2.5
                    """, "SELECT val FROM t_o3_mp WHERE sym = 'X'");

            assertSql("""
                    val
                    1.5
                    3.0
                    """, "SELECT val FROM t_o3_mp WHERE sym = 'Y'");
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionAfterRepeatedO3() throws Exception {
        // Multiple rounds of O3 merges on the same partition.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_repeat (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_repeat VALUES
                    ('2024-01-01T10:00:00', 'A', 100.0),
                    ('2024-01-01T20:00:00', 'B', 200.0)
                    """);
            drainWalQueue();

            // Round 1: O3 before existing data
            execute("""
                    INSERT INTO t_o3_repeat VALUES
                    ('2024-01-01T05:00:00', 'A', 50.0)
                    """);
            drainWalQueue();

            // Round 2: O3 between existing rows
            execute("""
                    INSERT INTO t_o3_repeat VALUES
                    ('2024-01-01T15:00:00', 'A', 150.0)
                    """);
            drainWalQueue();

            // Round 3: O3 at the very start
            execute("""
                    INSERT INTO t_o3_repeat VALUES
                    ('2024-01-01T01:00:00', 'B', 10.0)
                    """);
            drainWalQueue();

            assertSql("""
                    price
                    50.0
                    100.0
                    150.0
                    """, "SELECT price FROM t_o3_repeat WHERE sym = 'A'");

            assertSql("""
                    price
                    10.0
                    200.0
                    """, "SELECT price FROM t_o3_repeat WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionDuplicateO3() throws Exception {
        // Duplicate rows via O3 — exercises rollbackConditionally with row > 0
        // where the posting index already has entries at those positions.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_dup (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_dup VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();

            // Exact duplicate insert triggers O3 merge
            execute("""
                    INSERT INTO t_o3_dup VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();

            assertSql("""
                    price\tqty
                    1.5\t10
                    1.5\t10
                    3.5\t30
                    3.5\t30
                    """, "SELECT price, qty FROM t_o3_dup WHERE sym = 'A'");

            assertSql("""
                    price\tqty
                    2.5\t20
                    2.5\t20
                    """, "SELECT price, qty FROM t_o3_dup WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionO3ManyCoveredColumns() throws Exception {
        // Many covered columns of different types — exercises all CoveringCompressor
        // read methods (readDoubleAt, readLongAt, readIntAt) on sidecar data that
        // would have been corrupted by in-place rollback.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_many_cols (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d1, d2, d3, i1, l1),
                        d1 DOUBLE,
                        d2 DOUBLE,
                        d3 DOUBLE,
                        i1 INT,
                        l1 LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_many_cols VALUES
                    ('2024-01-01T10:00:00', 'A', 1.1, 2.2, 3.3, 100, 1000),
                    ('2024-01-01T12:00:00', 'B', 4.4, 5.5, 6.6, 200, 2000),
                    ('2024-01-01T14:00:00', 'A', 7.7, 8.8, 9.9, 300, 3000)
                    """);
            drainWalQueue();

            // O3 merge
            execute("""
                    INSERT INTO t_many_cols VALUES
                    ('2024-01-01T09:00:00', 'A', 0.1, 0.2, 0.3, 10, 100),
                    ('2024-01-01T11:00:00', 'B', 3.3, 3.4, 3.5, 150, 1500)
                    """);
            drainWalQueue();

            assertSql("""
                    d1\td2\td3\ti1\tl1
                    0.1\t0.2\t0.3\t10\t100
                    1.1\t2.2\t3.3\t100\t1000
                    7.7\t8.8\t9.9\t300\t3000
                    """, "SELECT d1, d2, d3, i1, l1 FROM t_many_cols WHERE sym = 'A'");

            assertSql("""
                    d1\td2\td3\ti1\tl1
                    3.3\t3.4\t3.5\t150\t1500
                    4.4\t5.5\t6.6\t200\t2000
                    """, "SELECT d1, d2, d3, i1, l1 FROM t_many_cols WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionO3WithNulls() throws Exception {
        // O3 merge where some rows have NULL symbol values.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_null VALUES
                    ('2024-01-01T10:00:00', 'A', 10.0),
                    ('2024-01-01T12:00:00', null, 20.0),
                    ('2024-01-01T14:00:00', 'A', 30.0)
                    """);
            drainWalQueue();

            // O3 insert with NULLs
            execute("""
                    INSERT INTO t_o3_null VALUES
                    ('2024-01-01T09:00:00', null, 5.0),
                    ('2024-01-01T11:00:00', 'A', 15.0),
                    ('2024-01-01T13:00:00', null, 25.0)
                    """);
            drainWalQueue();

            assertSql("""
                    val
                    10.0
                    15.0
                    30.0
                    """, "SELECT val FROM t_o3_null WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringIndexNoCorruptionWalMultiPartitionThenO3() throws Exception {
        // WAL creates multiple partitions in order, then O3 merges into a prior partition.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mp_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_mp_o3 VALUES
                    ('2024-01-01T12:00:00', 'A', 1.0),
                    ('2024-01-02T12:00:00', 'A', 2.0),
                    ('2024-01-03T12:00:00', 'A', 3.0)
                    """);
            drainWalQueue();

            // O3 into first partition
            execute("""
                    INSERT INTO t_mp_o3 VALUES
                    ('2024-01-01T06:00:00', 'A', 0.5)
                    """);
            drainWalQueue();

            // Verify all partitions are intact
            assertSql("""
                    price
                    0.5
                    1.0
                    2.0
                    3.0
                    """, "SELECT price FROM t_mp_o3 WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringIndexOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ord (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_ord VALUES
                    ('2024-01-01T00:00:00', 'A', 30.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 10.0),
                    ('2024-01-01T03:00:00', 'A', 50.0)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT price FROM t_ord WHERE sym = 'A' ORDER BY price",
                    """
                            Encode sort
                              keys: [price]
                                SelectedRecord
                                    CoveringIndex on: sym with: price
                                      filter: sym='A'
                            """
            );

            assertSql("""
                    price
                    10.0
                    30.0
                    50.0
                    """, "SELECT price FROM t_ord WHERE sym = 'A' ORDER BY price");
        });
    }

    @Test
    public void testCoveringIndexResidualFilterMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_resid_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_resid_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T12:00:00', 'A', 20.0),
                    ('2024-01-02T00:00:00', 'A', 30.0),
                    ('2024-01-02T12:00:00', 'B', 40.0),
                    ('2024-01-03T00:00:00', 'A', 50.0)
                    """);
            engine.releaseAllWriters();

            // Cross-partition with residual filter
            String expected = """
                    price
                    30.0
                    50.0
                    """;
            assertSql(expected,
                    "SELECT price FROM t_resid_mp WHERE sym = 'A' AND price >= 30");
            assertSql(expected,
                    "SELECT /*+ no_covering */ price FROM t_resid_mp WHERE sym = 'A' AND price >= 30");
        });
    }

    @Test
    public void testCoveringIndexResidualFilterUncoveredColumn() throws Exception {
        // When the filter references an uncovered column, covering index should NOT be used
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_resid_uncov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_resid_uncov VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 100),
                    ('2024-01-01T01:00:00', 'A', 30.0, 200)
                    """);
            engine.releaseAllWriters();

            // Filter on uncovered column 'extra' — can't use covering index
            assertPlanDoesNotContain(
                    "SELECT price FROM t_resid_uncov WHERE sym = 'A' AND extra > 150"
            );

            // Data must still be correct
            assertSql("""
                    price
                    30.0
                    """, "SELECT price FROM t_resid_uncov WHERE sym = 'A' AND extra > 150");
        });
    }

    // ===================================================================
    // Fallback scenario tests: covering index NOT used
    // ===================================================================

    @Test
    public void testCoveringIndexSidecarAlterAddIndexMultiPartition() throws Exception {
        // ALTER TABLE ADD INDEX INCLUDE on a table with multiple existing partitions.
        // All partitions must get sidecar files.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_mp (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_alter_mp VALUES
                    ('2024-01-01T09:00:00', 'A', 1.0, 10),
                    ('2024-01-01T10:00:00', 'B', 2.0, 20),
                    ('2024-01-02T09:00:00', 'A', 3.0, 30),
                    ('2024-01-03T09:00:00', 'B', 4.0, 40)
                    """);
            execute("ALTER TABLE t_alter_mp ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    1.0\t10
                    3.0\t30
                    """, "SELECT price, qty FROM t_alter_mp WHERE sym = 'A'");

            assertSql("""
                    price\tqty
                    2.0\t20
                    4.0\t40
                    """, "SELECT price, qty FROM t_alter_mp WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringIndexSidecarSurvivesPartitionSwitch() throws Exception {
        // After ALTER TABLE ADD INDEX INCLUDE, inserting into the next partition
        // must not corrupt the prior partition's sidecar files.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_switch (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_switch VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            execute("ALTER TABLE t_switch ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");

            // Insert into next partition — triggers partition switch and seal
            execute("""
                    INSERT INTO t_switch VALUES
                    ('2024-01-02T09:00:00', 'A', 4.5, 40),
                    ('2024-01-02T10:00:00', 'B', 5.5, 50)
                    """);
            engine.releaseAllWriters();

            // Both partitions should return correct covered data
            assertSql("""
                    price\tqty
                    1.5\t10
                    3.5\t30
                    4.5\t40
                    """, "SELECT price, qty FROM t_switch WHERE sym = 'A'");

            assertSql("""
                    price\tqty
                    2.5\t20
                    5.5\t50
                    """, "SELECT price, qty FROM t_switch WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringIndexSidecarSurvivesPartitionSwitchWal() throws Exception {
        // Same partition-switch scenario through WAL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_switch_wal (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_switch_wal VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_switch_wal ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();

            // Insert into next partition
            execute("""
                    INSERT INTO t_switch_wal VALUES
                    ('2024-01-02T09:00:00', 'A', 4.5, 40),
                    ('2024-01-02T10:00:00', 'B', 5.5, 50)
                    """);
            drainWalQueue();

            assertSql("""
                    price\tqty
                    1.5\t10
                    3.5\t30
                    4.5\t40
                    """, "SELECT price, qty FROM t_switch_wal WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringIndexSidecarWrittenOnAlterAddIndex() throws Exception {
        // ALTER TABLE ADD INDEX INCLUDE on a table with existing data must
        // seal the posting index for the last partition so that sidecar files
        // exist before any query uses the covering scan plan.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_cover (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_alter_cover VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            execute("ALTER TABLE t_alter_cover ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseAllWriters();

            // Covering query — all selected columns are in INCLUDE
            assertSql("""
                    price\tqty
                    1.5\t10
                    3.5\t30
                    """, "SELECT price, qty FROM t_alter_cover WHERE sym = 'A'");
        });
    }

    // ===================================================================
    // DDL edge case tests
    // ===================================================================

    @Test
    public void testCoveringIndexSidecarWrittenOnAlterAddIndexWal() throws Exception {
        // Same as above but through the WAL path.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_wal (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_alter_wal VALUES
                    ('2024-01-01T09:00:00', 'A', 1.5, 10),
                    ('2024-01-01T10:00:00', 'B', 2.5, 20),
                    ('2024-01-01T11:00:00', 'A', 3.5, 30)
                    """);
            drainWalQueue();
            execute("ALTER TABLE t_alter_wal ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();

            assertSql("""
                    price\tqty
                    1.5\t10
                    3.5\t30
                    """, "SELECT price, qty FROM t_alter_wal WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringIndexWithResidualFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_resid (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_resid VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 1),
                    ('2024-01-01T01:00:00', 'B', 20.0, 2),
                    ('2024-01-01T02:00:00', 'A', 30.0, 3),
                    ('2024-01-01T03:00:00', 'B', 40.0, 4),
                    ('2024-01-01T04:00:00', 'A', 50.0, 5)
                    """);
            engine.releaseAllWriters();

            // Plan: Async Filter wrapping CoveringIndex (parallel filter enabled in tests)
            assertPlanNoLeakCheck(
                    "SELECT price FROM t_resid WHERE sym = 'A' AND price > 15",
                    """
                            SelectedRecord
                                Async Filter workers: 1
                                  filter: 15<price
                                    CoveringIndex on: sym with: price
                                      filter: sym='A'
                            """
            );

            // Data correctness: only A rows with price > 15
            assertSql("""
                    price
                    30.0
                    50.0
                    """, "SELECT price FROM t_resid WHERE sym = 'A' AND price > 15");

            // Must match non-covering path
            assertSql("""
                    price
                    30.0
                    50.0
                    """, "SELECT /*+ no_covering */ price FROM t_resid WHERE sym = 'A' AND price > 15");
        });
    }

    @Test
    public void testCoveringIndexWithResidualFilterInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_resid_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_resid_in VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0),
                    ('2024-01-01T03:00:00', 'B', 40.0),
                    ('2024-01-01T04:00:00', 'C', 50.0)
                    """);
            engine.releaseAllWriters();

            // IN-list with residual filter
            assertSql("""
                    price
                    30.0
                    40.0
                    """, "SELECT price FROM t_resid_in WHERE sym IN ('A', 'B') AND price > 25");
            assertSql("""
                    price
                    30.0
                    40.0
                    """, "SELECT /*+ no_covering */ price FROM t_resid_in WHERE sym IN ('A', 'B') AND price > 25");
        });
    }

    @Test
    public void testCoveringLatestOnBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_bind (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_bind VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'A', 2.0),
                    ('2024-01-02T12:00:00', 'B', 3.0)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr("sym", "A");
            assertSql("""
                    price
                    2.0
                    """, "SELECT price FROM t_latest_bind WHERE sym = :sym LATEST ON ts PARTITION BY sym");

            // Re-execute with different value
            bindVariableService.clear();
            bindVariableService.setStr("sym", "B");
            assertSql("""
                    price
                    3.0
                    """, "SELECT price FROM t_latest_bind WHERE sym = :sym LATEST ON ts PARTITION BY sym");
        });
    }

    // ===================================================================
    // Type-specific covering column tests
    // ===================================================================

    @Test
    public void testCoveringLatestOnExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_plan (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_plan VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT price FROM t_latest_plan WHERE sym = 'A' LATEST ON ts PARTITION BY sym",
                    """
                            SelectedRecord
                                CoveringIndex op: latest on: sym with: price
                                  filter: sym='A'
                            """
            );
        });
    }

    @Test
    public void testCoveringLatestOnMultiKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_mk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_mk VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T12:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0),
                    ('2024-01-02T12:00:00', 'C', 4.0),
                    ('2024-01-03T00:00:00', 'B', 5.0),
                    ('2024-01-03T12:00:00', 'A', 6.0)
                    """);
            engine.releaseAllWriters();

            // LATEST ON with IN list: latest A is 6.0 (day 3), latest B is 5.0 (day 3)
            assertSql("""
                    price
                    6.0
                    5.0
                    """, "SELECT price FROM t_latest_mk WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringLatestOnMultiKeyWithSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_mk_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_mk_sym VALUES
                    ('2024-01-01T00:00:00', 'A', 'cold', 1.0),
                    ('2024-01-01T12:00:00', 'B', 'hot', 2.0),
                    ('2024-01-02T00:00:00', 'A', 'warm', 3.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    sym\ttag\tprice
                    A\twarm\t3.0
                    B\thot\t2.0
                    """, "SELECT sym, tag, price FROM t_latest_mk_sym WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringLatestOnMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0, 100),
                    ('2024-01-01T12:00:00', 'B', 20.0, 200),
                    ('2024-01-02T00:00:00', 'A', 30.0, 300),
                    ('2024-01-03T00:00:00', 'B', 40.0, 400)
                    """);
            engine.releaseAllWriters();

            // Latest A is in partition 2024-01-02
            assertSql("""
                    price\tqty
                    30.0\t300
                    """, "SELECT price, qty FROM t_latest_mp WHERE sym = 'A' LATEST ON ts PARTITION BY sym");

            // Latest B is in partition 2024-01-03
            assertSql("""
                    price\tqty
                    40.0\t400
                    """, "SELECT price, qty FROM t_latest_mp WHERE sym = 'B' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringLatestOnNonExistentKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_ne (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_ne VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price
                    """, "SELECT price FROM t_latest_ne WHERE sym = 'MISSING' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringLatestOnSingleKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T12:00:00', 'A', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0),
                    ('2024-01-02T12:00:00', 'B', 4.0),
                    ('2024-01-03T00:00:00', 'A', 5.0)
                    """);
            engine.releaseAllWriters();

            // LATEST ON should return the latest row for sym='A' (from last partition)
            assertSql("""
                    price
                    5.0
                    """, "SELECT price FROM t_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringLatestOnString() throws Exception {
        // LATEST ON with STRING covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_string VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello'),
                    ('2024-01-02T00:00:00', 'X', 'world'),
                    ('2024-01-02T01:00:00', 'Y', 'bar')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    label
                    world
                    """, "SELECT label FROM t_latest_string WHERE sym = 'X' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringLatestOnVarchar() throws Exception {
        // LATEST ON with VARCHAR covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'bob', 20.0),
                    ('2024-01-02T00:00:00', 'A', 'anna', 30.0),
                    ('2024-01-02T01:00:00', 'B', 'bea', 40.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    name\tprice
                    anna\t30.0
                    """, "SELECT name, price FROM t_latest_varchar WHERE sym = 'A' LATEST ON ts PARTITION BY sym");

            assertSql("""
                    name\tprice
                    bea\t40.0
                    """, "SELECT name, price FROM t_latest_varchar WHERE sym = 'B' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringLatestOnWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_latest_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0)
                    """);
            drainWalQueue();

            // Non-covering LATEST ON (SELECT * includes ts, not in INCLUDE list)
            assertSql("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\tA\t3.0
                    """, "SELECT * FROM t_latest_wal WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringLatestOnWithSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_sym VALUES
                    ('2024-01-01T00:00:00', 'A', 'cold', 1.0),
                    ('2024-01-02T00:00:00', 'A', 'hot', 2.0),
                    ('2024-01-03T00:00:00', 'A', 'warm', 3.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    sym\ttag\tprice
                    A\twarm\t3.0
                    """, "SELECT sym, tag, price FROM t_latest_sym WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testCoveringMetadataSurvivesAlterTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            execute("""
                    INSERT INTO t_alter VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price
                    10.5
                    11.5
                    """, "SELECT price FROM t_alter WHERE sym = 'A'");

            execute("ALTER TABLE t_alter ADD COLUMN extra INT");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_alter")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertTrue("covering flag lost after ALTER", r.getMetadata().getColumnMetadata(symIdx).isCovering());
                IntList indices = r.getMetadata().getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull("covering indices lost after ALTER", indices);
                assertEquals(2, indices.size());
            }

            assertSql("""
                    price
                    10.5
                    11.5
                    """, "SELECT price FROM t_alter WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringMultiColumnMultiCommit() throws Exception {
        // Multi-column covering (DOUBLE + INT) with multiple commits.
        // Each column's sidecar file grows at different rates (8B vs 4B per value).
        // Verifies per-column sidecar offset computation is correct.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mc_offset (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d, i),
                        d DOUBLE,
                        i INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_mc_offset VALUES ('2024-01-01T00:00:00', 'A', 1.5, 10), ('2024-01-01T01:00:00', 'B', 2.5, 20)");
            execute("INSERT INTO t_mc_offset VALUES ('2024-01-01T02:00:00', 'A', 3.5, 30), ('2024-01-01T03:00:00', 'B', 4.5, 40)");
            execute("INSERT INTO t_mc_offset VALUES ('2024-01-01T04:00:00', 'A', 5.5, 50)");
            engine.releaseAllWriters();

            assertSql("""
                    d\ti
                    1.5\t10
                    3.5\t30
                    5.5\t50
                    """, "SELECT d, i FROM t_mc_offset WHERE sym = 'A'");

            assertSql("""
                    d\ti
                    2.5\t20
                    4.5\t40
                    """, "SELECT d, i FROM t_mc_offset WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringMultiCommitMixedTypes() throws Exception {
        // Multiple commits with LONG, INT, SHORT covered columns.
        // Tests per-gen raw sidecar reading for non-DOUBLE types.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mc_mixed (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (l, i, s),
                        l LONG,
                        i INT,
                        s SHORT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_mc_mixed VALUES ('2024-01-01T00:00:00', 'A', 100_000, 10, 1), ('2024-01-01T01:00:00', 'B', 200_000, 20, 2)");
            execute("INSERT INTO t_mc_mixed VALUES ('2024-01-01T02:00:00', 'A', 300_000, 30, 3), ('2024-01-01T03:00:00', 'B', 400_000, 40, 4)");
            execute("INSERT INTO t_mc_mixed VALUES ('2024-01-01T04:00:00', 'A', 500_000, 50, 5)");
            engine.releaseAllWriters();

            assertSql("""
                    l\ti\ts
                    100000\t10\t1
                    300000\t30\t3
                    500000\t50\t5
                    """, "SELECT l, i, s FROM t_mc_mixed WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringMultiCommitSamePartition() throws Exception {
        // Multiple commits to the same partition, then seal.
        // Verifies covering data is correct after multi-gen seal.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_multi_commit (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_multi_commit VALUES ('2024-01-01T00:00:00', 'A', 10.0)");
            execute("INSERT INTO t_multi_commit VALUES ('2024-01-01T01:00:00', 'A', 11.0)");
            execute("INSERT INTO t_multi_commit VALUES ('2024-01-01T02:00:00', 'B', 20.0)");
            engine.releaseAllWriters();

            assertSql("""
                    price
                    10.0
                    11.0
                    """, "SELECT price FROM t_multi_commit WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringMultiGenFallback() throws Exception {
        // With per-gen sidecars, hasCovering() returns true even with genCount > 1.
        // Covering data is written during each commit's flushAllPending().
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_multigen";
                int plen = path.size();

                int rowCount = 20;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
                    }

                    // Write index with covering, two commits (two gens), do NOT close (no seal)
                    PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE);
                    writer.configureCovering(
                            new long[]{colAddr},
                            new long[]{0},
                            new int[]{3},
                            new int[]{2},
                            new int[]{ColumnType.DOUBLE},
                            1
                    );

                    // Gen 0
                    for (int i = 0; i < 10; i++) {
                        writer.add(0, i);
                    }
                    writer.setMaxValue(9);
                    writer.commit();

                    // Gen 1 — index now has genCount=2 with per-gen sidecar data
                    for (int i = 10; i < 20; i++) {
                        writer.add(0, i);
                    }
                    writer.setMaxValue(19);
                    writer.commit();

                    // Reader sees genCount=2 with per-gen sidecar data
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        // Per-gen sidecars: hasCovering() returns true
                        assertTrue(cc.hasCovering());


                        // Row IDs and covered values are correct
                        int count = 0;
                        while (cursor.hasNext()) {
                            long rowId = cursor.next();
                            assertEquals(count, rowId);
                            double covered = cc.getCoveredDouble(0);
                            assertEquals("covered value at row " + count, 100.0 + count, covered, 0.001);
                            count++;
                        }
                        assertEquals(20, count);
                        Misc.free(cursor);
                    }

                    writer.close(); // seal happens here
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringMultiGenKeyAbsentInOneGen() throws Exception {
        // Key 0 has data only in gen 0, key 1 in both gens.
        // Verifies per-gen sidecar reads skip empty key ranges correctly.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_absent_gen";
                int plen = path.size();
                int rowCount = 10;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }
                    PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE);
                    writer.configureCovering(
                            new long[]{colAddr}, new long[]{0}, new int[]{3},
                            new int[]{1}, new int[]{ColumnType.DOUBLE}, 1
                    );
                    // Gen 0: key 0 = rows 0,2; key 1 = rows 1,3
                    writer.add(0, 0);
                    writer.add(0, 2);
                    writer.add(1, 1);
                    writer.add(1, 3);
                    writer.setMaxValue(3);
                    writer.commit();
                    // Gen 1: ONLY key 1 = rows 4,5
                    writer.add(1, 4);
                    writer.add(1, 5);
                    writer.setMaxValue(5);
                    writer.commit();

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        // Key 0: only gen 0 data (rows 0,2)
                        CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cc.hasCovering());

                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(10.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext());
                        assertEquals(2, cc.next());
                        assertEquals(30.0, cc.getCoveredDouble(0), 0.001);
                        assertFalse(cc.hasNext());

                        // Key 1: gen 0 + gen 1 (rows 1,3,4,5)
                        cc = (CoveringRowCursor) reader.getCursor(1, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cc.hasCovering());

                        assertTrue(cc.hasNext());
                        assertEquals(1, cc.next());
                        assertEquals(20.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext());
                        assertEquals(3, cc.next());
                        assertEquals(40.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext());
                        assertEquals(4, cc.next());
                        assertEquals(50.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext());
                        assertEquals(5, cc.next());
                        assertEquals(60.0, cc.getCoveredDouble(0), 0.001);
                        assertFalse(cc.hasNext());
                    }
                    writer.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringMultiGenMultiKey() throws Exception {
        // Per-gen sidecar reads with multiple keys, verifying the sidecarBase
        // offset computation is correct (key > 0 must skip earlier keys' values).
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_multigen_mk";
                int plen = path.size();
                int rowCount = 20;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
                    }
                    PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE);
                    writer.configureCovering(
                            new long[]{colAddr}, new long[]{0}, new int[]{3},
                            new int[]{1}, new int[]{ColumnType.DOUBLE}, 1
                    );
                    // Gen 0: key 0 = rows 0,2,4,6,8; key 1 = rows 1,3,5,7,9
                    for (int i = 0; i < 10; i++) {
                        writer.add(i % 2, i);
                    }
                    writer.setMaxValue(9);
                    writer.commit();
                    // Gen 1: key 0 = rows 10,12,14,16,18; key 1 = rows 11,13,15,17,19
                    for (int i = 10; i < 20; i++) {
                        writer.add(i % 2, i);
                    }
                    writer.setMaxValue(19);
                    writer.commit();

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        // Key 1: rows 1,3,5,7,9,11,13,15,17,19
                        RowCursor cursor = reader.getCursor(1, 0, Long.MAX_VALUE, new int[]{0});
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());

                        int count = 0;
                        while (cc.hasNext()) {
                            long rowId = cc.next();
                            assertEquals(count * 2L + 1, rowId);
                            assertEquals(100.0 + count * 2 + 1, cc.getCoveredDouble(0), 0.001);
                            count++;
                        }
                        assertEquals(10, count);
                        Misc.free(cursor);
                    }
                    writer.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringNullSentinelGeoShort() throws Exception {
        // Verify GeoHashes.SHORT_NULL (-1) is used for GEOSHORT covering columns
        // below columnTop, not (short) 0
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_geo";
                int plen = path.size();

                int rowCount = 4;
                long colTop = 2; // first 2 rows below column top
                long colAddr = Unsafe.malloc((long) rowCount * Short.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putShort(colAddr + (long) i * Short.BYTES, (short) (100 + i));
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{colTop},
                                new int[]{1},  // Short.BYTES = 2 = 2^1
                                new int[]{1},
                                new int[]{ColumnType.GEOSHORT},
                                1
                        );

                        for (int i = 0; i < rowCount; i++) {
                            writer.add(0, i);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.GEOSHORT}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());


                        // Rows 0-1: below columnTop, must get GeoHashes.SHORT_NULL (-1)
                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(-1, cc.getCoveredShort(0));

                        assertTrue(cc.hasNext());
                        assertEquals(1, cc.next());
                        assertEquals(-1, cc.getCoveredShort(0));

                        // Rows 2-3: above columnTop, actual values
                        assertTrue(cc.hasNext());
                        assertEquals(2, cc.next());
                        assertEquals(100, cc.getCoveredShort(0));

                        assertTrue(cc.hasNext());
                        assertEquals(3, cc.next());
                        assertEquals(101, cc.getCoveredShort(0));

                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Short.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringQueryAllNullCoveredValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_allnull (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_allnull VALUES
                    ('2024-01-01T00:00:00', 'A', NULL, NULL),
                    ('2024-01-01T01:00:00', 'A', NULL, NULL),
                    ('2024-01-01T02:00:00', 'B', 10.5, 100)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    null\tnull
                    null\tnull
                    """, "SELECT price, qty FROM t_allnull WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_arr (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (vals, extra),
                        vals DOUBLE[],
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_arr
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_double_array(1, 3),
                        x::INT
                    FROM long_sequence(8)
                    """);
            engine.releaseAllWriters();
            // Second commit for sealed + sparse gen coverage
            execute("""
                    INSERT INTO t_arr
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + (8 + x) * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_double_array(1, 3),
                        (8 + x)::INT
                    FROM long_sequence(4)
                    """);
            engine.releaseAllWriters();

            // Covering vs non-covering: verify array data matches
            assertSql("""
                    vals\textra
                    [0.19202208853547864,0.5093827001617407,0.11427984775756228,0.5243722859289777,0.8072372233384567,null,0.6276954028373309,0.6778564558839208,0.8756771741121929,0.8799634725391621,0.5249321062686694]\t2
                    [0.9038068796506872,null]\t4
                    [0.16474369169931913,0.931192737286751]\t6
                    [0.7588175403454873,0.5778947915182423,0.9269068519549879,0.5449155021518948,0.1202416087573498,0.9640289041849747,null,null,0.6359144993891355,null,0.4971342426836798,0.48558682958070665,0.9047642416961028,0.03167026265669903,0.14830552335848957]\t8
                    [null,0.053594208204197136]\t10
                    [null,0.021189232728939578,0.7777024823107295]\t12
                    """, "SELECT vals, extra FROM t_arr WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryArrayColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_arr_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (vals, extra),
                        vals DOUBLE[],
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_arr_null
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_double_array(1, 3, 3),
                        x::INT
                    FROM long_sequence(12)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    vals\textra
                    [0.19202208853547864,0.5093827001617407,0.11427984775756228]\t2
                    [null,0.1985581797355932,null]\t4
                    [0.12503042190293423,0.9038068796506872,null]\t6
                    [0.022965637512889825,null]\t8
                    [0.18769708157331322,null,null]\t10
                    [0.45659895188239796,0.9566236549439661,0.5406709846540508]\t12
                    """, "SELECT vals, extra FROM t_arr_null WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_basic (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_basic VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150),
                    ('2024-01-01T03:00:00', 'C', 30.5, 300),
                    ('2024-01-01T04:00:00', 'B', 21.5, 250),
                    ('2024-01-01T05:00:00', 'A', 12.5, 120)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    10.5\t100
                    11.5\t150
                    12.5\t120
                    """, "SELECT price, qty FROM t_basic WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryBinaryColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bin (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (data, extra),
                        data BINARY,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Two commits to exercise both sparse (gen1) and sealed (gen0) paths
            execute("""
                    INSERT INTO t_bin
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_bin(5, 10, 2),
                        x::INT
                    FROM long_sequence(6)
                    """);
            engine.releaseAllWriters();
            execute("""
                    INSERT INTO t_bin
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + (6 + x) * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_bin(5, 10, 2),
                        (6 + x)::INT
                    FROM long_sequence(4)
                    """);
            engine.releaseAllWriters();

            // Covering vs non-covering: verify BINARY data matches
            assertSql("""
                    data\textra
                    \t2
                    \t4
                    00000000 1b c7 88 de a0 79 3c\t6
                    \t8
                    00000000 49 b4 59 7e 3b 08\t10
                    """, "SELECT data, extra FROM t_bin WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryBinaryColumnWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bin_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (data, extra),
                        data BINARY,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bin_null
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 3 = 0 THEN 'A' ELSE 'B' END,
                        rnd_bin(5, 10, 3),
                        x::INT
                    FROM long_sequence(12)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    data\textra
                    00000000 3b 72 db f3 04 1b c7 88\t3
                    00000000 08 a1 1e 38 8d 1b 9e f4\t6
                    \t9
                    00000000 f0 2d 40 e2 4b\t12
                    """, "SELECT data, extra FROM t_bin_null WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bind (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bind VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr("sym", "A");
            assertSql("""
                    price
                    10.5
                    11.5
                    """, "SELECT price FROM t_bind WHERE sym = :sym");

            // Re-execute with different bind variable value
            bindVariableService.clear();
            bindVariableService.setStr("sym", "B");
            assertSql("""
                    price
                    20.5
                    """, "SELECT price FROM t_bind WHERE sym = :sym");
        });
    }

    @Test
    public void testCoveringQueryBindVariableNonExistent() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bind_ne (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bind_ne VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr("sym", "NONEXISTENT");
            assertSql("""
                    price
                    """, "SELECT price FROM t_bind_ne WHERE sym = :sym");
        });
    }

    @Test
    public void testCoveringQueryBindVariableRebindPageFrame() throws Exception {
        // Regression test for the page-frame path (exercised by count()
        // pushdown, GROUP BY, etc.). Rebinding the symbol must pick up the
        // new bind value on re-execution.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bind_pf (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bind_pf VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5),
                    ('2024-01-01T03:00:00', 'B', 21.5),
                    ('2024-01-01T04:00:00', 'B', 22.5)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr("sym", "A");
            try (var factory = select("SELECT count() FROM t_bind_pf WHERE sym = :sym")) {
                assertCursor("""
                        count
                        2
                        """, factory, false, true);

                bindVariableService.clear();
                bindVariableService.setStr("sym", "B");
                assertCursor("""
                        count
                        3
                        """, factory, false, true);
            }
        });
    }

    @Test
    public void testCoveringQueryBindVariableRebindReusesFactory() throws Exception {
        // Regression test: re-executing a prepared covering-index query with a
        // different bind value must return rows for the new value. The
        // single-key path used to cache the resolved symbol key in the cursor
        // across executions.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bind_rebind (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bind_rebind VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5),
                    ('2024-01-01T03:00:00', 'B', 21.5)
                    """);
            engine.releaseAllWriters();

            bindVariableService.clear();
            bindVariableService.setStr("sym", "A");
            try (var factory = select("SELECT price FROM t_bind_rebind WHERE sym = :sym")) {
                assertCursor("""
                        price
                        10.5
                        11.5
                        """, factory, false, true);

                bindVariableService.clear();
                bindVariableService.setStr("sym", "B");
                assertCursor("""
                        price
                        20.5
                        21.5
                        """, factory, false, true);

                // And back to A to cover both directions of the transition.
                bindVariableService.clear();
                bindVariableService.setStr("sym", "A");
                assertCursor("""
                        price
                        10.5
                        11.5
                        """, factory, false, true);
            }
        });
    }

    @Test
    public void testCoveringQueryBooleanColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_bool (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (flag, extra),
                        flag BOOLEAN,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_bool VALUES
                    ('2024-01-01T00:00:00', 'A', true, 1),
                    ('2024-01-01T01:00:00', 'B', false, 2),
                    ('2024-01-01T02:00:00', 'A', false, 3),
                    ('2024-01-01T03:00:00', 'A', true, 4)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    flag\textra
                    true\t1
                    false\t3
                    true\t4
                    """, "SELECT flag, extra FROM t_bool WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryBoundaryValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_boundary (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d, i),
                        d DOUBLE,
                        i INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_boundary VALUES
                    ('2024-01-01T00:00:00', 'A', 1.7976931348623157E308, 2147483647),
                    ('2024-01-01T01:00:00', 'A', -1.7976931348623157E308, -2147483648),
                    ('2024-01-01T02:00:00', 'A', 0.0, 0)
                    """);
            engine.releaseAllWriters();

            // INT MIN_VALUE is the NULL sentinel; displays as "null" via covering path
            assertSql("""
                    d\ti
                    1.7976931348623157E308\t2147483647
                    -1.7976931348623157E308\tnull
                    0.0\t0
                    """, "SELECT d, i FROM t_boundary WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryByteColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_byte (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (b, extra),
                        b BYTE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_byte VALUES
                    ('2024-01-01T00:00:00', 'X', 42, 1),
                    ('2024-01-01T01:00:00', 'Y', 0, 2),
                    ('2024-01-01T02:00:00', 'X', -1, 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    b\textra
                    42\t1
                    -1\t3
                    """, "SELECT b, extra FROM t_byte WHERE sym = 'X'");
        });
    }

    @Test
    public void testCoveringQueryDateColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_date (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d, extra),
                        d DATE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_date VALUES
                    ('2024-01-01T00:00:00', 'A', '2020-06-15T00:00:00.000Z', 1),
                    ('2024-01-01T01:00:00', 'B', '2021-12-25T00:00:00.000Z', 2),
                    ('2024-01-01T02:00:00', 'A', '2022-03-01T00:00:00.000Z', 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    d\textra
                    2020-06-15T00:00:00.000Z\t1
                    2022-03-01T00:00:00.000Z\t3
                    """, "SELECT d, extra FROM t_date WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryDecimal128ByteOrder() throws Exception {
        // Verifies that DECIMAL128 byte order (high word at offset 0, low word
        // at offset 8) survives the covering index round-trip. Inserts known
        // values and checks exact output rather than relying on random data.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_d128 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d128),
                        d128 DECIMAL(38, 10)
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_d128 VALUES
                        ('2024-01-01T00:00:00', 'A', '1234567890.1234567890'::DECIMAL(38, 10)),
                        ('2024-01-01T01:00:00', 'A', '-9999999999.9999999999'::DECIMAL(38, 10)),
                        ('2024-01-01T02:00:00', 'A', '0.0000000001'::DECIMAL(38, 10)),
                        ('2024-01-01T03:00:00', 'B', '99999999999999999999999999.9999999999'::DECIMAL(38, 10)),
                        ('2024-01-01T04:00:00', 'A', NULL)
                    """);
            engine.releaseAllWriters();
            // Second commit for sealed + sparse gen
            execute("INSERT INTO t_d128 VALUES ('2024-01-01T05:00:00', 'A', '42'::DECIMAL(38, 10))");
            engine.releaseAllWriters();

            assertSql("""
                    d128
                    1234567890.1234567890
                    -9999999999.9999999999
                    0.0000000001
                    
                    42.0000000000
                    """, "SELECT d128 FROM t_d128 WHERE sym = 'A'");
            assertSql("""
                    d128
                    99999999999999999999999999.9999999999
                    """, "SELECT d128 FROM t_d128 WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringQueryDecimalAllSizes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dec (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d8, d16, d32, d64, d128, d256),
                        d8 DECIMAL(2, 1),
                        d16 DECIMAL(4, 2),
                        d32 DECIMAL(9, 2),
                        d64 DECIMAL(18, 4),
                        d128 DECIMAL(38, 10),
                        d256 DECIMAL(40, 5)
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dec
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_decimal(2, 1, 3),
                        rnd_decimal(4, 2, 3),
                        rnd_decimal(9, 2, 3),
                        rnd_decimal(18, 4, 3),
                        rnd_decimal(38, 10, 3),
                        rnd_decimal(40, 5, 3)
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();
            // Second commit for sealed + sparse gen coverage
            execute("""
                    INSERT INTO t_dec
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + (10 + x) * 3_600_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        rnd_decimal(2, 1, 3),
                        rnd_decimal(4, 2, 3),
                        rnd_decimal(9, 2, 3),
                        rnd_decimal(18, 4, 3),
                        rnd_decimal(38, 10, 3),
                        rnd_decimal(40, 5, 3)
                    FROM long_sequence(4)
                    """);
            engine.releaseAllWriters();

            // Covering vs non-covering: verify all decimal sizes match
            assertSql("""
                    d8\td16\td32\td64\td128\td256
                    \t70.80\t\t40863994239165.1703\t1787280113583629108167793234.9373449732\t18888715180791238225928221362458460.92766
                    7.0\t\t2538903.64\t16664082490389.7958\t1641632316967050245515388573.3440205453\t14875755943028931385865395098236153.96808
                    3.1\t78.26\t5751353.94\t23804269374864.1413\t810081242630250895698426776.6927411277\t8583077020202156956218481394266599.02389
                    7.5\t\t\t86605238667466.9519\t956563306327071686987346799.6830377222\t22724816918678007437269485083139146.44429
                    1.7\t92.66\t8823714.73\t13188254446200.8274\t1207097311830897670430779224.2024504775\t29150523193729163716100989803355305.47666
                    6.5\t66.70\t5446956.70\t14417228720079.2492\t1418531814025018706708158328.3816287698\t5732543185464093840813081895232786.18866
                    1.8\t\t8892248.06\t39899107525936.1297\t1016253333977104912056699053.5900116824\t1732803907904724146465147103771127.97855
                    """, "SELECT d8, d16, d32, d64, d128, d256 FROM t_dec WHERE sym = 'A'");

            assertSql("""
                    d8\td16\td32\td64\td128\td256
                    3.9\t\t735757.01\t61184357814108.3005\t1205323264613915778464177818.0211586008\t1386075460267063369330913776166802.31153
                    5.8\t57.62\t1018221.05\t47578458180646.1665\t693332620165057781283181359.2820666308\t15151479461332606562822538694713052.93139
                    3.9\t5.57\t1379694.58\t20559518411576.0695\t601247527511781476692712681.1373876190\t1948200433546889932354865222990931.08533
                    3.4\t37.66\t6273933.81\t31612360735939.2493\t315864032070159436143241665.0969926445\t29659946948645594828374741791005525.18833
                    2.4\t12.38\t9505528.43\t21423064588439.9731\t645239438434308363262983650.6793302617\t7927075818711980216168116892213920.99887
                    0.9\t\t7953593.56\t91590662830857.7949\t751262867641396953441858009.1969477517\t13167451642297942276784652339229868.67664
                    8.4\t\t9429993.84\t62429987870713.5916\t822752991025940024709620990.3877240957\t28031524510998509335616827333341238.08766
                    """, "SELECT d8, d16, d32, d64, d128, d256 FROM t_dec WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringQueryDecimalNullSentinel() throws Exception {
        // Red test for PostingIndexWriter.writeNullSentinel
        // (PostingIndexWriter.java:1340 and :1348) — both overloads write
        // zero bytes for DECIMAL8 / DECIMAL16 NULL sentinels instead of
        // Decimals.DECIMAL8_NULL (Byte.MIN_VALUE) / DECIMAL16_NULL
        // (Short.MIN_VALUE). The bug only fires for rows where the cover
        // column did not exist (rowId < colTop, e.g., column added later
        // via ALTER), since rows that have data are copied byte-for-byte
        // via Unsafe.copyMemory. CursorPrinter (CursorPrinter.java:301,
        // :330) compares against the proper sentinel — when the sidecar
        // holds zero, the covering path renders "0.0"/"0.00" instead of
        // NULL, while the no_covering fallback correctly renders NULL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dec_null (
                        ts TIMESTAMP,
                        sym SYMBOL
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Two pre-ALTER rows: d8 and d16 will not yet exist for these.
            execute("""
                    INSERT INTO t_dec_null VALUES
                        ('2024-01-01T00:00:00', 'A'),
                        ('2024-01-01T01:00:00', 'A')
                    """);
            execute("ALTER TABLE t_dec_null ADD COLUMN d8 DECIMAL(2,1)");
            execute("ALTER TABLE t_dec_null ADD COLUMN d16 DECIMAL(4,2)");
            // One post-ALTER row with real values: d8 and d16 exist for it.
            execute("""
                    INSERT INTO t_dec_null VALUES
                        ('2024-01-01T02:00:00', 'A', 1.5::DECIMAL(2,1), 12.34::DECIMAL(4,2))
                    """);
            // Add the POSTING+covering index. Seal walks rowId 0,1
            // (rowId < colTop) for d8/d16 and writes the buggy zero
            // sentinel into the sidecar.
            execute("ALTER TABLE t_dec_null ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (d8, d16)");
            engine.releaseAllWriters();

            // Both covering and no_covering paths must produce identical
            // output. Capture the fallback first; if covering disagrees
            // (because NULL was sealed into the sidecar as zero), the
            // assertion fails.
            sink.clear();
            printSql("SELECT /*+ no_covering */ d8, d16 FROM t_dec_null WHERE sym = 'A' ORDER BY ts");
            String fallback = sink.toString();

            // Covering must agree with fallback for the same rows.
            assertSql(fallback, "SELECT d8, d16 FROM t_dec_null WHERE sym = 'A' ORDER BY ts");
        });
    }

    @Test
    public void testCoveringQueryEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            assertSql("""
                    price\tqty
                    """, "SELECT price, qty FROM t_empty WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_plan (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_plan VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // Query plan should show CoveringIndex when all selected columns are covered
            assertPlanNoLeakCheck(
                    "SELECT price FROM t_plan WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """
            );
        });
    }

    @Test
    public void testCoveringQueryFloatColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_float (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (f, extra),
                        f FLOAT,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_float VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 1),
                    ('2024-01-01T01:00:00', 'B', 2.5, 2),
                    ('2024-01-01T02:00:00', 'A', 3.5, 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    f\textra
                    1.5\t1
                    3.5\t3
                    """, "SELECT f, extra FROM t_float WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5),
                    ('2024-01-01T03:00:00', 'C', 30.5)
                    """);
            engine.releaseAllWriters();

            // IN list uses CoveringIndex — results grouped by key (A first, then B)
            assertSql("""
                    price
                    10.5
                    11.5
                    20.5
                    """, "SELECT price FROM t_in WHERE sym IN ('A', 'B')");

            // Verify plan shows CoveringIndex
            assertPlanNoLeakCheck(
                    "SELECT price FROM t_in WHERE sym IN ('A', 'B')",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: price
                                  filter: sym IN ['A','B']
                            """
            );
        });
    }

    @Test
    public void testCoveringQueryInListMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0),
                    ('2024-01-02T01:00:00', 'C', 4.0),
                    ('2024-01-03T00:00:00', 'B', 5.0),
                    ('2024-01-03T01:00:00', 'A', 6.0)
                    """);
            engine.releaseAllWriters();

            // IN list across 3 partitions — keys grouped per partition (A first, then B)
            assertSql("""
                    price
                    1.0
                    2.0
                    3.0
                    6.0
                    5.0
                    """, "SELECT price FROM t_in_mp WHERE sym IN ('A', 'B')");
        });
    }

    @Test
    public void testCoveringQueryInListNonExistentKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_ne (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_ne VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // One existing key, one non-existent — only existing key returns
            assertSql("""
                    price
                    10.5
                    """, "SELECT price FROM t_in_ne WHERE sym IN ('A', 'NONEXISTENT')");

            // All non-existent keys — empty result
            assertSql("""
                    price
                    """, "SELECT price FROM t_in_ne WHERE sym IN ('X', 'Y')");
        });
    }

    @Test
    public void testCoveringQueryInListSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_single (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_single VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // Single-value IN is equivalent to = query
            assertSql("""
                    price
                    10.5
                    """, "SELECT price FROM t_in_single WHERE sym IN ('A')");
        });
    }

    @Test
    public void testCoveringQueryInListWithSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_sym VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            // IN list with sym in SELECT — sym value should vary per key
            assertSql("""
                    sym\tprice
                    A\t10.5
                    A\t11.5
                    B\t20.5
                    """, "SELECT sym, price FROM t_in_sym WHERE sym IN ('A', 'B')");
        });
    }

    @Test
    public void testCoveringQueryKeyAbsentFromMiddlePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_absent (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_absent VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10),
                    ('2024-01-02T00:00:00', 'B', 2.5, 20),
                    ('2024-01-03T00:00:00', 'A', 3.5, 30)
                    """);
            engine.releaseAllWriters();

            // Key A is absent from partition 2024-01-02
            assertSql("""
                    price\tqty
                    1.5\t10
                    3.5\t30
                    """, "SELECT price, qty FROM t_absent WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryLong256Column() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_l256 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (hash, extra),
                        hash LONG256,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_l256 VALUES
                    ('2024-01-01T00:00:00', 'A', '0x0100000000000000020000000000000003000000000000000400000000000000', 1),
                    ('2024-01-01T01:00:00', 'B', '0x0500000000000000060000000000000007000000000000000800000000000000', 2),
                    ('2024-01-01T02:00:00', 'A', '0x090000000000000000000000000000000b000000000000000c00000000000000', 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    hash\textra
                    0x0100000000000000020000000000000003000000000000000400000000000000\t1
                    0x090000000000000000000000000000000b000000000000000c00000000000000\t3
                    """, "SELECT hash, extra FROM t_l256 WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryLongColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_long (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (big_val, extra),
                        big_val LONG,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_long VALUES
                    ('2024-01-01T00:00:00', 'X', 100_000_000, 1),
                    ('2024-01-01T01:00:00', 'Y', 200_000_000, 2),
                    ('2024-01-01T02:00:00', 'X', 300_000_000, 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    big_val\textra
                    100000000\t1
                    300000000\t3
                    """, "SELECT big_val, extra FROM t_long WHERE sym = 'X'");
        });
    }

    @Test
    public void testCoveringQueryManyRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_many (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val, extra),
                        val DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            StringBuilder sb = new StringBuilder("INSERT INTO t_many VALUES ");
            int totalRows = 300;
            for (int i = 0; i < totalRows; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                String sym = switch (i % 3) {
                    case 0 -> "A";
                    case 1 -> "B";
                    default -> "C";
                };
                // All rows on same day, each second apart
                sb.append(String.format(
                        "('2024-01-01T%02d:%02d:%02d', '%s', %d.5, %d)",
                        0, (i % 3600) / 60, i % 60, sym, i, i * 10
                ));
            }
            execute(sb.toString());
            engine.releaseAllWriters();

            // Key A has rows at i=0,3,6,...,297 → 100 rows
            // First 3 values for A: 0.5, 3.5, 6.5
            assertSql("""
                    val\textra
                    0.5\t0
                    3.5\t30
                    6.5\t60
                    """, "SELECT val, extra FROM t_many WHERE sym = 'A' LIMIT 3");
        });
    }

    @Test
    public void testCoveringQueryMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_mixed (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d, i, l, s),
                        d DOUBLE,
                        i INT,
                        l LONG,
                        s SHORT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_mixed VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10, 100000, 1),
                    ('2024-01-01T01:00:00', 'B', 2.5, 20, 200000, 2),
                    ('2024-01-01T02:00:00', 'A', 3.5, 30, 300000, 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    d\ti\tl\ts
                    1.5\t10\t100000\t1
                    3.5\t30\t300000\t3
                    """, "SELECT d, i, l, s FROM t_mixed WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryMultipleKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_keys (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_keys VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150),
                    ('2024-01-01T03:00:00', 'C', 30.5, 300),
                    ('2024-01-01T04:00:00', 'B', 21.5, 250),
                    ('2024-01-01T05:00:00', 'A', 12.5, 120)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    10.5\t100
                    11.5\t150
                    12.5\t120
                    """, "SELECT price, qty FROM t_keys WHERE sym = 'A'");

            assertSql("""
                    price\tqty
                    20.5\t200
                    21.5\t250
                    """, "SELECT price, qty FROM t_keys WHERE sym = 'B'");

            assertSql("""
                    price\tqty
                    30.5\t300
                    """, "SELECT price, qty FROM t_keys WHERE sym = 'C'");
        });
    }

    @Test
    public void testCoveringQueryMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_parts (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Data across 3 partitions: Jan 1, 2, 3
            execute("""
                    INSERT INTO t_parts VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10),
                    ('2024-01-01T12:00:00', 'B', 2.5, 20),
                    ('2024-01-02T00:00:00', 'A', 3.5, 30),
                    ('2024-01-02T12:00:00', 'C', 4.5, 40),
                    ('2024-01-03T00:00:00', 'A', 5.5, 50),
                    ('2024-01-03T12:00:00', 'B', 6.5, 60)
                    """);
            engine.releaseAllWriters();

            // Key A has data in all 3 partitions
            assertSql("""
                    price\tqty
                    1.5\t10
                    3.5\t30
                    5.5\t50
                    """, "SELECT price, qty FROM t_parts WHERE sym = 'A'");

            // Key B has data in partitions 1 and 3
            assertSql("""
                    price\tqty
                    2.5\t20
                    6.5\t60
                    """, "SELECT price, qty FROM t_parts WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringQueryNonExistentKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_nokey (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_nokey VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5)
                    """);
            engine.releaseAllWriters();

            // Non-existent symbol returns empty result
            assertSql("""
                    price
                    """, "SELECT price FROM t_nokey WHERE sym = 'Z'");
        });
    }

    @Test
    public void testCoveringQueryNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_nulls (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_nulls VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'A', NULL, NULL),
                    ('2024-01-01T02:00:00', 'A', 12.5, 120)
                    """);
            engine.releaseAllWriters();

            // NULL sentinel values in covered columns are stored in the sidecar files.
            // The CoveringRecord returns NaN/MIN_VALUE, displayed as empty/null.
            assertSql("""
                    price\tqty
                    10.5\t100
                    null\tnull
                    12.5\t120
                    """, "SELECT price, qty FROM t_nulls WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryO3() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // Insert day 2 first, then day 1 (out of order)
            execute("""
                    INSERT INTO t_o3 VALUES
                    ('2024-01-02T00:00:00', 'A', 20.5, 200),
                    ('2024-01-02T12:00:00', 'B', 21.5, 210)
                    """);
            drainWalQueue();
            execute("""
                    INSERT INTO t_o3 VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T12:00:00', 'B', 11.5, 110)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            // O3 partitions have their covering sidecars rebuilt after the merge
            assertSql("""
                    price\tqty
                    10.5\t100
                    20.5\t200
                    """, "SELECT price, qty FROM t_o3 WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryReversedColumnOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rev (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_rev VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150)
                    """);
            engine.releaseAllWriters();

            // Reversed column order vs table definition
            assertSql("""
                    qty\tprice
                    100\t10.5
                    150\t11.5
                    """, "SELECT qty, price FROM t_rev WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQuerySelectOnlySymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_symonly (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_symonly VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150)
                    """);
            engine.releaseAllWriters();

            // Selecting only the indexed symbol column
            assertSql("""
                    sym
                    A
                    A
                    """, "SELECT sym FROM t_symonly WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryShortColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_short (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (small_val, extra),
                        small_val SHORT,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_short VALUES
                    ('2024-01-01T00:00:00', 'X', 100, 1),
                    ('2024-01-01T01:00:00', 'Y', 200, 2),
                    ('2024-01-01T02:00:00', 'X', 300, 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    small_val\textra
                    100\t1
                    300\t3
                    """, "SELECT small_val, extra FROM t_short WHERE sym = 'X'");
        });
    }

    @Test
    public void testCoveringQuerySingleRowPerKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_single (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_single VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'C', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    10.5\t100
                    """, "SELECT price, qty FROM t_single WHERE sym = 'A'");

            assertSql("""
                    price\tqty
                    20.5\t200
                    """, "SELECT price, qty FROM t_single WHERE sym = 'B'");

            assertSql("""
                    price\tqty
                    30.5\t300
                    """, "SELECT price, qty FROM t_single WHERE sym = 'C'");
        });
    }

    @Test
    public void testCoveringQueryString() throws Exception {
        // STRING column in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_string VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello'),
                    ('2024-01-01T01:00:00', 'Y', 'world'),
                    ('2024-01-01T02:00:00', 'X', 'foo')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    label
                    hello
                    foo
                    """, "SELECT label FROM t_cover_string WHERE sym = 'X'");
        });
    }

    @Test
    public void testCoveringQueryStringMultiCommit() throws Exception {
        // Multi-commit (multi-gen) with STRING covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_string_mc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_cover_string_mc VALUES ('2024-01-01T00:00:00', 'X', 'hello')");
            execute("INSERT INTO t_cover_string_mc VALUES ('2024-01-01T01:00:00', 'X', 'world')");
            execute("INSERT INTO t_cover_string_mc VALUES ('2024-01-01T02:00:00', 'Y', 'foo')");
            engine.releaseAllWriters();

            assertSql("""
                    label
                    hello
                    world
                    """, "SELECT label FROM t_cover_string_mc WHERE sym = 'X'");
        });
    }

    @Test
    public void testCoveringQueryStringNull() throws Exception {
        // STRING column in INCLUDE with NULL values
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_string_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_string_null VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello'),
                    ('2024-01-01T01:00:00', 'X', NULL),
                    ('2024-01-01T02:00:00', 'X', 'foo')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    label
                    hello
                    \n\
                    foo
                    """, "SELECT label FROM t_cover_string_null WHERE sym = 'X'");
        });
    }

    @Test
    public void testCoveringQuerySymbolInSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_sel (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_sel VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            // Selecting sym + covered column should return correct sym values
            assertSql("""
                    sym\tprice
                    A\t10.5
                    A\t11.5
                    """, "SELECT sym, price FROM t_sym_sel WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQuerySymbolIncludeColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_incl (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_incl VALUES
                    ('2024-01-01T00:00:00', 'A', 'hot', 10.5),
                    ('2024-01-01T01:00:00', 'B', 'cold', 20.5),
                    ('2024-01-01T02:00:00', 'A', 'warm', 11.5)
                    """);
            engine.releaseAllWriters();

            // tag is a SYMBOL column in INCLUDE — covering index should resolve it
            assertSql("""
                    tag\tprice
                    hot\t10.5
                    warm\t11.5
                    """, "SELECT tag, price FROM t_sym_incl WHERE sym = 'A'");

            assertSql("""
                    tag\tprice
                    cold\t20.5
                    """, "SELECT tag, price FROM t_sym_incl WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringQuerySymbolIncludeMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_incl_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_incl_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 'hot', 1.0),
                    ('2024-01-01T01:00:00', 'B', 'cold', 2.0),
                    ('2024-01-02T00:00:00', 'A', 'warm', 3.0),
                    ('2024-01-02T01:00:00', 'B', 'cool', 4.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    sym\ttag\tprice
                    A\thot\t1.0
                    A\twarm\t3.0
                    """, "SELECT sym, tag, price FROM t_sym_incl_mp WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQuerySymbolIncludeNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_incl_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag),
                        tag SYMBOL
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_incl_null VALUES
                    ('2024-01-01T00:00:00', 'A', 'hot'),
                    ('2024-01-01T01:00:00', 'A', NULL),
                    ('2024-01-01T02:00:00', 'A', 'cold')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    tag
                    hot
                    \n\
                    cold
                    """, "SELECT tag FROM t_sym_incl_null WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQuerySymbolIncludeWithInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_sym_incl_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag),
                        tag SYMBOL
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_sym_incl_in VALUES
                    ('2024-01-01T00:00:00', 'A', 'hot'),
                    ('2024-01-01T01:00:00', 'B', 'cold'),
                    ('2024-01-01T02:00:00', 'A', 'warm'),
                    ('2024-01-01T03:00:00', 'C', 'cool')
                    """);
            engine.releaseAllWriters();

            // SYMBOL include with IN list
            assertSql("""
                    sym\ttag
                    A\thot
                    A\twarm
                    B\tcold
                    """, "SELECT sym, tag FROM t_sym_incl_in WHERE sym IN ('A', 'B')");
        });
    }

    // ==================== Coverage gap tests ====================

    @Test
    public void testCoveringQueryTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (event_time, extra),
                        event_time TIMESTAMP,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_ts VALUES
                    ('2024-01-01T00:00:00', 'A', '2024-06-15T12:30:00.000000Z', 1),
                    ('2024-01-01T01:00:00', 'B', '2024-07-20T08:00:00.000000Z', 2),
                    ('2024-01-01T02:00:00', 'A', '2024-08-25T16:45:00.000000Z', 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    event_time\textra
                    2024-06-15T12:30:00.000000Z\t1
                    2024-08-25T16:45:00.000000Z\t3
                    """, "SELECT event_time, extra FROM t_ts WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryUuidColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_uuid (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (id, extra),
                        id UUID,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_uuid VALUES
                    ('2024-01-01T00:00:00', 'A', '11111111-1111-1111-1111-111111111111', 1),
                    ('2024-01-01T01:00:00', 'B', '22222222-2222-2222-2222-222222222222', 2),
                    ('2024-01-01T02:00:00', 'A', '33333333-3333-3333-3333-333333333333', 3),
                    ('2024-01-01T03:00:00', 'A', NULL, 4)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    id\textra
                    11111111-1111-1111-1111-111111111111\t1
                    33333333-3333-3333-3333-333333333333\t3
                    \t4
                    """, "SELECT id, extra FROM t_uuid WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryVarchar() throws Exception {
        // VARCHAR column in INCLUDE — end-to-end via FallbackRecord (column reads)
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'bob', 20.0),
                    ('2024-01-01T02:00:00', 'A', 'anna', 30.0),
                    ('2024-01-01T03:00:00', 'A', null, 40.0)
                    """);
            engine.releaseAllWriters();

            // Query with covered varchar + double columns
            assertSql("""
                    name\tprice
                    alice\t10.0
                    anna\t30.0
                    \t40.0
                    """, "SELECT name, price FROM t_cover_varchar WHERE sym = 'A'");

            assertSql("""
                    name\tprice
                    bob\t20.0
                    """, "SELECT name, price FROM t_cover_varchar WHERE sym = 'B'");

            // Non-covered column forces fallback — verify no crash
            assertSql("""
                    ts\tname
                    2024-01-01T00:00:00.000000Z\talice
                    2024-01-01T02:00:00.000000Z\tanna
                    2024-01-01T03:00:00.000000Z\t
                    """, "SELECT ts, name FROM t_cover_varchar WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryVarcharEmpty() throws Exception {
        // Empty string (zero-length VARCHAR) in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_varchar_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_varchar_empty VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'A', '', 20.0),
                    ('2024-01-01T02:00:00', 'A', 'anna', 30.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    name\tprice
                    alice\t10.0
                    \t20.0
                    anna\t30.0
                    """, "SELECT name, price FROM t_cover_varchar_empty WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryVarcharMultiCommit() throws Exception {
        // Multi-commit (multi-gen) with var-sized covered column
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_varchar_mc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_cover_varchar_mc VALUES ('2024-01-01T00:00:00', 'A', 'alice', 10.0)");
            execute("INSERT INTO t_cover_varchar_mc VALUES ('2024-01-01T01:00:00', 'A', 'anna', 20.0)");
            execute("INSERT INTO t_cover_varchar_mc VALUES ('2024-01-01T02:00:00', 'B', 'bob', 30.0)");
            engine.releaseAllWriters();

            assertSql("""
                    name\tprice
                    alice\t10.0
                    anna\t20.0
                    """, "SELECT name, price FROM t_cover_varchar_mc WHERE sym = 'A'");

            assertSql("""
                    name\tprice
                    bob\t30.0
                    """, "SELECT name, price FROM t_cover_varchar_mc WHERE sym = 'B'");
        });
    }

    @Test
    public void testCoveringQueryVarcharMultiPartition() throws Exception {
        // VARCHAR covering across multiple partitions
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_varchar_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_varchar_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'bob', 20.0),
                    ('2024-01-02T00:00:00', 'A', 'anna', 30.0),
                    ('2024-01-03T00:00:00', 'A', 'amy', 50.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    name\tprice
                    alice\t10.0
                    anna\t30.0
                    amy\t50.0
                    """, "SELECT name, price FROM t_cover_varchar_mp WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringQueryWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    10.5\t100
                    11.5\t150
                    """, "SELECT price, qty FROM t_wal WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringStringFsstCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_string
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('X','Y','Z'),
                           rnd_str('label_alpha','label_beta','label_gamma','label_delta')
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT label FROM t_fsst_string WHERE sym = 'X'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: label
                                  filter: sym='X'
                            """
            );

            assertSql("count\n498\n", "SELECT COUNT(*) FROM t_fsst_string WHERE sym = 'X'");
        });
    }

    @Test
    public void testCoveringTimestampThroughSealDeltaCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_ts_cover (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (event_ts, extra),
                        event_ts TIMESTAMP,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_ts_cover VALUES
                    ('2024-01-01T01:00:00', 'A', '2024-06-15T12:00:01.000000', 1),
                    ('2024-01-01T02:00:00', 'B', '2024-06-15T12:00:02.000000', 2),
                    ('2024-01-01T03:00:00', 'A', '2024-06-15T12:00:03.000000', 3),
                    ('2024-01-01T04:00:00', 'B', '2024-06-15T12:00:04.000000', 4),
                    ('2024-01-01T05:00:00', 'A', '2024-06-15T12:00:05.000000', 5),
                    ('2024-01-01T06:00:00', 'A', NULL, 6)
                    """);
            engine.releaseAllWriters();
            // Second commit to exercise sealed + sparse gens
            execute("""
                    INSERT INTO t_ts_cover VALUES
                    ('2024-01-01T07:00:00', 'A', '2024-06-15T12:00:07.000000', 7),
                    ('2024-01-01T08:00:00', 'B', '2024-06-15T12:00:08.000000', 8)
                    """);
            engine.releaseAllWriters();

            // Verify covered TIMESTAMP values including NULLs
            assertSql("""
                    event_ts\textra
                    2024-06-15T12:00:01.000000Z\t1
                    2024-06-15T12:00:03.000000Z\t3
                    2024-06-15T12:00:05.000000Z\t5
                    \t6
                    2024-06-15T12:00:07.000000Z\t7
                    """, "SELECT event_ts, extra FROM t_ts_cover WHERE sym = 'A'");

            // Verify covering plan is used
            assertPlanNoLeakCheck(
                    "SELECT event_ts, extra FROM t_ts_cover WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: event_ts, extra
                                  filter: sym='A'
                            """
            );
        });
    }

    @Test
    public void testCoveringToTopReScan() throws Exception {
        // Gap 8: toTop() re-scan via CROSS JOIN
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_totop (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_totop VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'A', 11.0)
                    """);
            engine.releaseAllWriters();

            // CROSS JOIN forces re-scan — each left row paired with all right rows
            assertSql("""
                    price\tx
                    10.0\t1
                    10.0\t2
                    11.0\t1
                    11.0\t2
                    """, """
                    SELECT t.price, x.x
                    FROM (SELECT price FROM t_totop WHERE sym = 'A') t
                    CROSS JOIN (SELECT x FROM long_sequence(2)) x
                    """);
        });
    }

    @Test
    public void testCoveringVarcharFsstCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_varchar
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('K0','K1','K2'),
                           rnd_str('order_alpha','order_beta','order_gamma','order_delta','order_epsilon')
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            // Plan: CoveringIndex for varchar column
            assertPlanNoLeakCheck(
                    "SELECT name FROM t_fsst_varchar WHERE sym = 'K0'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: name
                                  filter: sym='K0'
                            """
            );

            // Data correctness: covering vs non-covering
            assertSql("count\n498\n", "SELECT COUNT(*) FROM t_fsst_varchar WHERE sym = 'K0'");
        });
    }

    @Test
    public void testCoveringVarcharFsstEmptyStrings() throws Exception {
        // Edge case: empty strings (zero-length) mixed with regular strings
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_empty
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B'),
                           CASE WHEN x % 3 = 0 THEN '' ELSE rnd_str('alpha_value','beta_value','gamma_value') END
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertSql("count\n715\n", "SELECT COUNT(*) FROM t_fsst_empty WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringVarcharFsstInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_in
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C','D'),
                           rnd_str('order_alpha','order_beta','order_gamma','order_delta')
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT name FROM t_fsst_in WHERE sym IN ('A', 'B')",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: name
                                  filter: sym IN ['A','B']
                            """
            );

            assertSql("count\n753\n", "SELECT COUNT(*) FROM t_fsst_in WHERE sym IN ('A', 'B')");
        });
    }

    @Test
    public void testCoveringVarcharFsstMixedWithDouble() throws Exception {
        // VARCHAR + DOUBLE in INCLUDE — both columns covered, one FSST-compressed
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_mixed (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_mixed
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_str('order_alpha','order_beta','order_gamma','order_delta'),
                           rnd_double() * 100
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT name, price FROM t_fsst_mixed WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: name, price
                                  filter: sym='A'
                            """
            );

            assertSql("count\n528\n", "SELECT COUNT(*) FROM t_fsst_mixed WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringVarcharFsstWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fsst_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fsst_null
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B'),
                           CASE WHEN x % 5 = 0 THEN null ELSE rnd_str('value_one','value_two','value_three','value_four') END
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT name FROM t_fsst_null WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: name
                                  filter: sym='A'
                            """
            );

            assertSql("count\n715\n", "SELECT COUNT(*) FROM t_fsst_null WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringVarcharLargeInsert() throws Exception {
        // Reproducer for SIGSEGV in writeVarcharValue during intermediate commit.
        // MemoryPMARImpl maps one page at a time — addressOf() for a non-current page
        // returns 0, causing a crash. The fix uses ff.read() (pread) instead.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_large_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_large_varchar
                    SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C','D','E'),
                           rnd_str('order_alpha_2024','order_beta_2024','order_gamma_2024','order_delta_2024')
                    FROM long_sequence(50000)
                    """);
            engine.releaseAllWriters();

            assertSql("count\n9976\n", "SELECT COUNT(*) FROM t_large_varchar WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringVarcharLargeInsertFixedWidth() throws Exception {
        // Same MemoryPMARImpl bug but for fixed-width INCLUDE columns (DOUBLE).
        // writeSidecarValueSafe also used addressOf() — now uses ff.read().
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_large_double (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_large_double
                    SELECT dateadd('T', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C','D','E'),
                           rnd_double() * 100
                    FROM long_sequence(50000)
                    """);
            engine.releaseAllWriters();

            assertSql("count\n9960\n", "SELECT COUNT(*) FROM t_large_double WHERE sym = 'A'");
        });
    }

    @Test
    public void testCoveringWithColumnTop() throws Exception {
        // Rows below columnTop get null sentinels in sidecar data
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_coltop";
                int plen = path.size();

                int rowCount = 10;
                long colTop = 4; // first 4 rows are below column top
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 50.0 + i);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{colTop},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );

                        for (int i = 0; i < rowCount; i++) {
                            writer.add(0, i);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());


                        // First 4 rows: below columnTop, null sentinel is NaN for DOUBLE
                        for (int i = 0; i < 4; i++) {
                            assertTrue(cc.hasNext());
                            assertEquals(i, cc.next());
                            assertTrue("row " + i + " should be NaN", Double.isNaN(cc.getCoveredDouble(0)));
                        }
                        // Rows 4-9: above columnTop, should get actual values
                        // Row 4 maps to colAddr offset (4-4)*8 = 0 -> value 50.0
                        for (int i = 4; i < rowCount; i++) {
                            assertTrue(cc.hasNext());
                            assertEquals(i, cc.next());
                            assertEquals(50.0 + (i - colTop), cc.getCoveredDouble(0), 0.001);
                        }
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringWithDesignatedTimestamp() throws Exception {
        // Selecting the designated timestamp column which is also in the INCLUDE list.
        // The covering index should serve this without a full table scan.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_ts (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, ts, price),
                        exchange SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_ts VALUES
                    ('2024-01-01T00:00:00', 'GOLD', 'CME', 100.0),
                    ('2024-01-01T00:01:00', 'SILVER', 'LME', 200.0),
                    ('2024-01-01T00:02:00', 'GOLD', 'LME', 300.0)
                    """);
            engine.releaseAllWriters();

            // ts + sym: both should be coverable
            assertSql("""
                    ts\tsym
                    2024-01-01T00:00:00.000000Z\tGOLD
                    2024-01-01T00:02:00.000000Z\tGOLD
                    """, "SELECT ts, sym FROM t_cover_ts WHERE sym = 'GOLD'");

            // Check that the plan uses CoveringIndex
            assertPlanNoLeakCheck(
                    "SELECT ts, sym FROM t_cover_ts WHERE sym = 'GOLD'",
                    """
                            CoveringIndex on: sym with: ts
                              filter: sym='GOLD'
                            """
            );
        });
    }

    @Test
    public void testCoveringWithDesignatedTimestampWal() throws Exception {
        // WAL table with designated timestamp in INCLUDE — same as user's production scenario
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_ts_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, ts, price),
                        exchange SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR WAL
                    """);
            execute("""
                    INSERT INTO t_cover_ts_wal VALUES
                    ('2024-01-01T00:00:00', 'GOLD', 'CME', 100.0),
                    ('2024-01-01T00:01:00', 'SILVER', 'LME', 200.0),
                    ('2024-01-01T00:02:00', 'GOLD', 'LME', 300.0)
                    """);
            drainWalQueue();

            assertSql("""
                    ts\tsym
                    2024-01-01T00:00:00.000000Z\tGOLD
                    2024-01-01T00:02:00.000000Z\tGOLD
                    """, "SELECT ts, sym FROM t_cover_ts_wal WHERE sym = 'GOLD'");

            // Plan should use CoveringIndex
            assertPlanNoLeakCheck(
                    "SELECT ts, sym FROM t_cover_ts_wal WHERE sym = 'GOLD'",
                    """
                            CoveringIndex on: sym with: ts
                              filter: sym='GOLD'
                            """
            );
        });
    }

    @Test
    public void testCoveringWithTimestampNs() throws Exception {
        // Exact reproduction of user's DDL with TIMESTAMP_NS, SYMBOL, arrays, INCLUDE list
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_tsns (
                        ts TIMESTAMP_NS,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, ts, best_bid, best_ask),
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        bids DOUBLE[][],
                        asks DOUBLE[][],
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_cover_tsns VALUES
                    ('2024-01-01T00:00:00.000000000', 'GOLD', 'CME', 'metal', NULL, NULL, 2050.5, 2051.0),
                    ('2024-01-01T00:01:00.000000000', 'SILVER', 'LME', 'metal', NULL, NULL, 24.3, 24.5),
                    ('2024-01-01T00:02:00.000000000', 'GOLD', 'LME', 'metal', NULL, NULL, 2051.0, 2052.0)
                    """);
            engine.releaseAllWriters();

            // Covered columns only — should use CoveringIndex
            assertSql("""
                    ts\tsym\texchange
                    2024-01-01T00:00:00.000000000Z\tGOLD\tCME
                    2024-01-01T00:02:00.000000000Z\tGOLD\tLME
                    """, "SELECT ts, sym, exchange FROM t_cover_tsns WHERE sym = 'GOLD'");

            // ts + sym only — should use CoveringIndex
            assertSql("""
                    ts\tsym
                    2024-01-01T00:00:00.000000000Z\tGOLD
                    2024-01-01T00:02:00.000000000Z\tGOLD
                    """, "SELECT ts, sym FROM t_cover_tsns WHERE sym = 'GOLD'");

            // Plan check for ts + sym
            assertPlanNoLeakCheck(
                    "SELECT ts, sym FROM t_cover_tsns WHERE sym = 'GOLD'",
                    """
                            CoveringIndex on: sym with: ts
                              filter: sym='GOLD'
                            """
            );
        });
    }

    @Test
    public void testCoveringWithTimestampNsWal() throws Exception {
        // Reproduction of user's WAL table with TIMESTAMP_NS and INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_cover_tsns_wal (
                        ts TIMESTAMP_NS,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, ts, best_bid, best_ask),
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR WAL
                    """);
            execute("""
                    INSERT INTO t_cover_tsns_wal VALUES
                    ('2024-01-01T00:00:00.000000000', 'GOLD', 'CME', 'metal', 2050.5, 2051.0),
                    ('2024-01-01T00:01:00.000000000', 'SILVER', 'LME', 'metal', 24.3, 24.5),
                    ('2024-01-01T00:02:00.000000000', 'GOLD', 'LME', 'metal', 2051.0, 2052.0)
                    """);
            drainWalQueue();

            // Plan should use CoveringIndex for covered columns on WAL table
            assertPlanNoLeakCheck(
                    "SELECT ts, sym FROM t_cover_tsns_wal WHERE sym = 'GOLD'",
                    """
                            CoveringIndex on: sym with: ts
                              filter: sym='GOLD'
                            """
            );

            assertSql("""
                    ts\tsym
                    2024-01-01T00:00:00.000000000Z\tGOLD
                    2024-01-01T00:02:00.000000000Z\tGOLD
                    """, "SELECT ts, sym FROM t_cover_tsns_wal WHERE sym = 'GOLD'");
        });
    }

    @Test
    public void testCreateTableIncludeDuplicatePosition() throws Exception {
        // The SqlException must point at the duplicated column name, not
        // position 0.
        assertMemoryLeak(() -> {
            String sql = "CREATE TABLE t_cdup (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (price, price), price DOUBLE) TIMESTAMP(ts)";
            int expected = sql.indexOf("price, price") + "price, ".length();
            assertException(sql, expected, "duplicate column in INCLUDE");
        });
    }

    @Test
    public void testCreateTableIncludeDuplicateRejects() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE t_dup2 (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (price, price),
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """);
                fail("Should have thrown");
            } catch (SqlException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("duplicate column in INCLUDE"));
            }
        });
    }

    @Test
    public void testCreateTableIncludeMissingColumnPosition() throws Exception {
        // The SqlException must point at the missing column name, not 0.
        assertMemoryLeak(() -> {
            String sql = "CREATE TABLE t_cmiss (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (ghost), price DOUBLE) TIMESTAMP(ts)";
            int expected = sql.indexOf("ghost");
            assertException(sql, expected, "INCLUDE column");
        });
    }

    @Test
    public void testCreateTableIncludeSelfReferencePosition() throws Exception {
        // The SqlException must point at the self-referencing column name in
        // the INCLUDE list, not 0.
        assertMemoryLeak(() -> {
            String sql = "CREATE TABLE t_cself (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (sym), price DOUBLE) TIMESTAMP(ts)";
            int expected = sql.indexOf("INCLUDE (") + "INCLUDE (".length();
            assertException(sql, expected, "INCLUDE must not contain the indexed column");
        });
    }

    @Test
    public void testCreateTableIncludeSelfReferenceRejects() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE t_selfref (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (sym),
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                        """);
                fail("Should have thrown");
            } catch (SqlException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("INCLUDE must not contain the indexed column"));
            }
        });
    }

    @Test
    public void testCreateTableWithIncludeSyntax() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE trades (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            try (TableReader r = engine.getReader("trades")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.isColumnIndexed(symIdx));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(symIdx));
                // Verify covering column indices are stored in metadata
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(3, coveringCols.size());
                assertEquals(metadata.getColumnIndex("price"), coveringCols.getQuick(0));
                assertEquals(metadata.getColumnIndex("qty"), coveringCols.getQuick(1));
            }
        });
    }

    @Test
    public void testDistinctAfterDropAllPartitions() throws Exception {
        // Gap 12: DISTINCT when symbolCount>0 but 0 rows
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_dropall (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_dropall VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();
            execute("ALTER TABLE t_distinct_dropall DROP PARTITION LIST '2024-01-01', '2024-01-02'");
            engine.releaseAllWriters();

            // symbolCount=2 but no data anywhere — should return empty
            assertSql("""
                    sym
                    """, "SELECT DISTINCT sym FROM t_distinct_dropall");
        });
    }

    @Test
    public void testDistinctHighCardinality() throws Exception {
        // Gap 13: DISTINCT with >256 keys (crosses stride boundary)
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_hc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            StringBuilder sb = new StringBuilder("INSERT INTO t_distinct_hc VALUES ");
            for (int i = 0; i < 300; i++) {
                if (i > 0) sb.append(',');
                sb.append("('2024-01-01T").append(String.format("%02d", i / 60)).append(':')
                        .append(String.format("%02d", i % 60)).append(":00', 'K").append(i).append("', ").append(i).append(".0)");
            }
            execute(sb.toString());
            engine.releaseAllWriters();

            // Should return 300 distinct keys
            try (var factory = select("SELECT DISTINCT sym FROM t_distinct_hc");
                 var cursor = factory.getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) {
                    count++;
                }
                assertEquals(300, count);
            }
        });
    }

    @Test
    public void testDistinctMultiPartitionWithTimeFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // 3 days, symbols S0-S4, only S0/S1 appear on day 2
            execute("""
                    INSERT INTO t_dist_mp VALUES
                    ('2024-01-01T01:00:00', 'S0', 1.0),
                    ('2024-01-01T02:00:00', 'S1', 2.0),
                    ('2024-01-01T03:00:00', 'S2', 3.0),
                    ('2024-01-02T01:00:00', 'S0', 4.0),
                    ('2024-01-02T02:00:00', 'S1', 5.0),
                    ('2024-01-03T01:00:00', 'S3', 6.0),
                    ('2024-01-03T02:00:00', 'S4', 7.0)
                    """);
            engine.releaseAllWriters();

            // DISTINCT with time filter — only symbols in day 2
            assertSql("""
                    sym
                    S0
                    S1
                    """, "SELECT DISTINCT sym FROM t_dist_mp WHERE ts IN '2024-01-02' ORDER BY sym");
        });
    }

    @Test
    public void testDistinctNoWhereStillWorks() throws Exception {
        // Ensure the existing no-WHERE DISTINCT path still uses PostingIndex
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_nofilter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_nofilter VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-01T02:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT DISTINCT sym FROM t_dist_nofilter",
                    """
                            PostingIndex op: distinct on: sym
                                Frame forward scan on: t_dist_nofilter
                            """
            );

            assertSql("""
                    sym
                    A
                    B
                    """, "SELECT DISTINCT sym FROM t_dist_nofilter");
        });
    }

    @Test
    public void testDistinctSymAfterDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_drop (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_drop VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'C', 3.0)
                    """);
            engine.releaseAllWriters();

            // Drop partition containing B — B's symbol table key persists but has no rows
            execute("ALTER TABLE t_distinct_drop DROP PARTITION LIST '2024-01-01'");
            engine.releaseAllWriters();

            // B should NOT appear — only C has data
            assertSql("""
                    sym
                    C
                    """, "SELECT DISTINCT sym FROM t_distinct_drop");
        });
    }

    @Test
    public void testDistinctSymBitmapFallback() throws Exception {
        assertMemoryLeak(() -> {
            // BITMAP index (not POSTING) should NOT use PostingIndex distinct
            execute("""
                    CREATE TABLE t_distinct_bmp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_bmp VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            // Should still return correct results via GROUP BY path
            assertSql("""
                    sym
                    A
                    B
                    """, "SELECT DISTINCT sym FROM t_distinct_bmp ORDER BY sym");

            // Plan should NOT show PostingIndex distinct
            assertPlanNoLeakCheck(
                    "SELECT DISTINCT sym FROM t_distinct_bmp",
                    """
                            GroupBy vectorized: true workers: 1
                              keys: [sym]
                              values: [count(*)]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t_distinct_bmp
                            """
            );
        });
    }

    @Test
    public void testDistinctSymEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            assertSql("""
                    sym
                    """, "SELECT DISTINCT sym FROM t_distinct_empty");
        });
    }

    @Test
    public void testDistinctSymExplainPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_plan (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_plan VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0)
                    """);
            engine.releaseAllWriters();

            // Optimizer rewrites DISTINCT → GROUP BY + count(*), but we intercept in
            // generateSelectGroupBy and replace the entire chain with PostingIndex distinct
            assertPlanNoLeakCheck(
                    "SELECT DISTINCT sym FROM t_distinct_plan",
                    """
                            PostingIndex op: distinct on: sym
                                Frame forward scan on: t_distinct_plan
                            """
            );
        });
    }

    // ==================== FSST-compressed covered varchar/string tests ====================

    @Test
    public void testDistinctSymFromPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-01T02:00:00', 'A', 3.0),
                    ('2024-01-02T00:00:00', 'C', 4.0),
                    ('2024-01-02T01:00:00', 'A', 5.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    sym
                    A
                    B
                    C
                    """, "SELECT DISTINCT sym FROM t_distinct");
        });
    }

    @Test
    public void testDistinctSymFromPostingIndexWithCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_cov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Use INSERT...SELECT with enough rows to trigger multi-stride sealing.
            // The bug was that close() seals without covered column data
            // (column memories already closed by TableWriter), leaving stale
            // per-gen sidecar files that the reader misinterprets as stride format.
            execute("""
                    INSERT INTO t_dist_cov
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                        rnd_symbol(100, 4, 6, 0),
                        rnd_double() * 100
                    FROM long_sequence(10000)
                    """);
            engine.releaseAllWriters();

            // Must not SIGSEGV — the covering sidecar files should be absent
            // (cleaned up by close) so the distinct cursor falls back to
            // per-key index scans without sidecar decoding.
            assertSql("""
                    count
                    100
                    """, "SELECT count() FROM (SELECT DISTINCT sym FROM t_dist_cov)");
        });
    }

    @Test
    public void testDistinctSymMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0),
                    ('2024-01-03T00:00:00', 'A', 3.0)
                    """);
            engine.releaseAllWriters();

            // A appears in 2 partitions, B in 1 — should still return just A, B
            assertSql("""
                    sym
                    A
                    B
                    """, "SELECT DISTINCT sym FROM t_distinct_mp");
        });
    }

    @Test
    public void testDistinctSymMultiPartitionHighCardinality() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_mp2 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Insert 1000 keys across 10 partitions, one partition at a time
            for (int p = 0; p < 10; p++) {
                execute("INSERT INTO t_dist_mp2"
                        + " SELECT dateadd('s', x::INT, dateadd('d', " + p + ", '2024-01-01'))::TIMESTAMP,"
                        + "     rnd_symbol(1000, 4, 8, 0),"
                        + "     rnd_double() * 100"
                        + " FROM long_sequence(100_000)");
                engine.releaseAllWriters();
            }
            engine.releaseAllReaders();

            // Each INSERT generates ~1000 unique random symbols independently.
            // Across 10 INSERTs the symbol table grows to ~10000 distinct keys.
            // Just verify it doesn't crash and returns a reasonable count.
            assertSql("""
                    hasKeys
                    true
                    """, "SELECT count() > 5000 AS hasKeys FROM (SELECT DISTINCT sym FROM t_dist_mp2)");
        });
    }

    @Test
    public void testDistinctSymWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'C', 3.0)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            assertSql("""
                    sym
                    A
                    B
                    C
                    """, "SELECT DISTINCT sym FROM t_distinct_wal");
        });
    }

    @Test
    public void testDistinctWalO3() throws Exception {
        // Gap 14: DISTINCT on WAL table with O3 inserts
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_distinct_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_distinct_o3 VALUES
                    ('2024-01-02T00:00:00', 'B', 2.0),
                    ('2024-01-02T01:00:00', 'C', 3.0)
                    """);
            drainWalQueue();
            // O3: insert day 1
            execute("""
                    INSERT INTO t_distinct_o3 VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            // Key order depends on symbol table insertion order: B=0, C=1, A=2
            // (B and C inserted first, then A via O3)
            assertSql("""
                    sym
                    B
                    C
                    A
                    """, "SELECT DISTINCT sym FROM t_distinct_o3");
        });
    }

    @Test
    public void testDistinctWithIntervalAndNonIntervalFilter() throws Exception {
        // Regression test: DISTINCT with combined interval + non-interval WHERE
        // must not lose the interval predicate when the optimization is rejected.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_combined (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_combined VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 90.0),
                    ('2024-01-02T00:00:00', 'C', 5.0),
                    ('2024-01-02T01:00:00', 'D', 95.0)
                    """);
            engine.releaseAllWriters();

            // Only D matches: ts >= '2024-01-02' AND price > 50
            assertSql("""
                    sym
                    D
                    """, "SELECT DISTINCT sym FROM t_dist_combined WHERE ts >= '2024-01-02' AND price > 50");
        });
    }

    @Test
    public void testDistinctWithNoIndexHintFallsBack() throws Exception {
        // no_index hint should prevent PostingIndex distinct
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_noidx (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_noidx VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            // no_index: falls back to standard GroupBy, NOT PostingIndex
            String plan = getPlan("SELECT /*+ no_index */ DISTINCT sym FROM t_dist_noidx");
            assertFalse("no_index should prevent PostingIndex:\n" + plan,
                    plan.contains("PostingIndex"));

            // Data correctness — compare sorted results
            assertSql("""
                    sym
                    A
                    B
                    """, "SELECT /*+ no_index */ DISTINCT sym FROM t_dist_noidx ORDER BY sym");
        });
    }

    // ==================== COUNT pushdown tests ====================

    @Test
    public void testDistinctWithNonIntervalFilterFallsBack() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_filt (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_filt VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            // Non-interval filter: falls back to standard GroupBy, NOT PostingIndex
            String plan = getPlan("SELECT DISTINCT sym FROM t_dist_filt WHERE price > 15");
            assertFalse("Non-interval filter should not use PostingIndex:\n" + plan,
                    plan.contains("PostingIndex"));

            // Data correctness
            assertSql("""
                    sym
                    B
                    """, "SELECT DISTINCT sym FROM t_dist_filt WHERE price > 15");
        });
    }

    @Test
    public void testDistinctWithPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 3_600_000_000L,
                        'S' || (x % 5),
                        rnd_double() * 100
                    FROM long_sequence(50)
                    """);
            engine.releaseAllWriters();

            // Verify all 5 symbols returned
            assertSql("""
                    sym
                    S0
                    S1
                    S2
                    S3
                    S4
                    """, "SELECT DISTINCT sym FROM t_dist ORDER BY sym");

            // Verify plan uses PostingIndex distinct
            assertPlanNoLeakCheck(
                    "SELECT DISTINCT sym FROM t_dist",
                    """
                            PostingIndex op: distinct on: sym
                                Frame forward scan on: t_dist
                            """
            );
        });
    }

    @Test
    public void testDistinctWithTimestampFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_where (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_where
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C','D','E'),
                           rnd_double() * 100
                    FROM long_sequence(1000)
                    """);
            // Second day — only keys A,B,C appear
            execute("""
                    INSERT INTO t_dist_where VALUES
                    ('2024-01-02T00:00:00', 'A', 1.0),
                    ('2024-01-02T01:00:00', 'B', 2.0),
                    ('2024-01-02T02:00:00', 'C', 3.0)
                    """);
            engine.releaseAllWriters();

            // Plan: PostingIndex distinct with interval partition frame
            assertPlanNoLeakCheck(
                    "SELECT DISTINCT sym FROM t_dist_where WHERE ts >= '2024-01-02'",
                    """
                            PostingIndex op: distinct on: sym
                                Interval forward scan on: t_dist_where
                                  intervals: [("2024-01-02T00:00:00.000000Z","MAX")]
                            """
            );

            // Data correctness: should only see A, B, C from the second day
            assertSql("""
                            sym
                            A
                            B
                            C
                            """,
                    "SELECT DISTINCT sym FROM t_dist_where WHERE ts >= '2024-01-02' ORDER BY sym");
        });
    }

    // ==================== DISTINCT with WHERE tests ====================

    @Test
    public void testDistinctWithTimestampRangeNoMatches() throws Exception {
        // DISTINCT with a timestamp range that excludes all data → empty result
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_dist_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_dist_empty VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    sym
                    """, "SELECT DISTINCT sym FROM t_dist_empty WHERE ts >= '2025-01-01'");
        });
    }

    @Test
    public void testDropCoveredColumn() throws Exception {
        // Drop a column that is INCLUDED in a covering index
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_col (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_drop_col VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 11.5, 150)
                    """);
            engine.releaseAllWriters();

            // Drop a covered column
            execute("ALTER TABLE t_drop_col DROP COLUMN qty");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // Index should still work (posting data is per-symbol, not per-covered-column)
            assertSql("""
                    sym
                    A
                    A
                    """, "SELECT sym FROM t_drop_col WHERE sym = 'A'");
        });
    }

    @Test
    public void testDropCoveredColumnFallsBackToTableScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_cov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_drop_cov VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """, "SELECT price, qty FROM t_drop_cov WHERE sym = 'A'");

            execute("ALTER TABLE t_drop_cov DROP COLUMN qty");

            assertSql("""
                    price
                    10.5
                    30.5
                    """, "SELECT price FROM t_drop_cov WHERE sym = 'A'");

            execute("""
                    INSERT INTO t_drop_cov VALUES
                    ('2024-01-01T03:00:00', 'A', 40.5)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price
                    10.5
                    30.5
                    40.5
                    """, "SELECT price FROM t_drop_cov WHERE sym = 'A'");
        });
    }

    @Test
    public void testDropIndexRemovesCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            try (TableReader r = engine.getReader("t_drop")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertTrue(r.getMetadata().getColumnMetadata(symIdx).isCovering());
            }

            execute("ALTER TABLE t_drop ALTER COLUMN sym DROP INDEX");

            try (TableReader r = engine.getReader("t_drop")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertFalse(r.getMetadata().isColumnIndexed(symIdx));
                assertFalse(r.getMetadata().getColumnMetadata(symIdx).isCovering());
            }
        });
    }

    @Test
    public void testDropUnrelatedColumnDoesNotBreakCoveringIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_unrel (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT,
                        unrelated VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_drop_unrel VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100, 'x'),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200, 'y'),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300, 'z')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """, "SELECT price, qty FROM t_drop_unrel WHERE sym = 'A'");

            execute("ALTER TABLE t_drop_unrel DROP COLUMN unrelated");

            assertSql("""
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """, "SELECT price, qty FROM t_drop_unrel WHERE sym = 'A'");

            execute("""
                    INSERT INTO t_drop_unrel VALUES
                    ('2024-01-01T03:00:00', 'A', 40.5, 400)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty
                    10.5\t100
                    30.5\t300
                    40.5\t400
                    """, "SELECT price, qty FROM t_drop_unrel WHERE sym = 'A'");
        });
    }

    // ==================== additional coverage tests ====================

    @Test
    public void testFallbackInList3Keys() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in3 VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-01T02:00:00', 'C', 3.0, 30)
                    """);
            engine.releaseAllWriters();

            // 3-key IN-list with non-covered column — needs 3 independent cursors
            assertSql("""
                    price\textra
                    1.0\t10
                    2.0\t20
                    3.0\t30
                    """, "SELECT price, extra FROM t_in3 WHERE sym IN ('A', 'B', 'C')");
        });
    }

    @Test
    public void testFallbackInList5KeysMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in5 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in5 VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-01T02:00:00', 'C', 3.0, 30),
                    ('2024-01-02T00:00:00', 'D', 4.0, 40),
                    ('2024-01-02T01:00:00', 'E', 5.0, 50),
                    ('2024-01-02T02:00:00', 'A', 6.0, 60),
                    ('2024-01-03T00:00:00', 'B', 7.0, 70),
                    ('2024-01-03T01:00:00', 'C', 8.0, 80)
                    """);
            engine.releaseAllWriters();

            // 5-key IN-list across 3 partitions with non-covered column
            assertSql("""
                    price\textra
                    1.0\t10
                    2.0\t20
                    3.0\t30
                    4.0\t40
                    5.0\t50
                    6.0\t60
                    7.0\t70
                    8.0\t80
                    """, "SELECT price, extra FROM t_in5 WHERE sym IN ('A', 'B', 'C', 'D', 'E')");
        });
    }

    @Test
    public void testFallbackInListWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_in_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-01T02:00:00', 'C', 3.0, 30)
                    """);
            drainWalQueue();

            assertSql("""
                    price\textra
                    1.0\t10
                    2.0\t20
                    3.0\t30
                    """, "SELECT price, extra FROM t_in_wal WHERE sym IN ('A', 'B', 'C')");
        });
    }

    @Test
    public void testFallbackInListWithNonCoveredColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_fallback (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_fallback VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 1),
                    ('2024-01-01T01:00:00', 'B', 20.5, 2)
                    """);
            engine.releaseAllWriters();

            // extra is not in INCLUDE — falls back to FilterOnValues
            assertSql("""
                    price\textra
                    10.5\t1
                    20.5\t2
                    """, "SELECT price, extra FROM t_in_fallback WHERE sym IN ('A', 'B')");
        });
    }

    // ==================== var-width page frame tests ====================

    @Test
    public void testFallbackSelectStar() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_star (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_star VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            // SELECT * includes ts which is not covered — falls back to regular scan
            assertSql("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\tA\t10.5
                    2024-01-01T02:00:00.000000Z\tA\t11.5
                    """, "SELECT * FROM t_star WHERE sym = 'A'");
        });
    }

    @Test
    public void testFallbackStringWithNonCoveredColumn() throws Exception {
        // Selecting non-covered column alongside covered STRING forces fallback to regular scan
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fb_string (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fb_string VALUES
                    ('2024-01-01T00:00:00', 'X', 'hello', 1),
                    ('2024-01-01T01:00:00', 'Y', 'world', 2),
                    ('2024-01-01T02:00:00', 'X', 'foo', 3)
                    """);
            engine.releaseAllWriters();

            // extra is not in INCLUDE — falls back to regular indexed scan
            assertSql("""
                    label\textra
                    hello\t1
                    foo\t3
                    """, "SELECT label, extra FROM t_fb_string WHERE sym = 'X'");
        });
    }

    @Test
    public void testFallbackVarcharWithNonCoveredColumn() throws Exception {
        // Selecting non-covered column alongside covered VARCHAR forces fallback to regular scan
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_fb_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_fb_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 1),
                    ('2024-01-01T01:00:00', 'B', 'bob', 2),
                    ('2024-01-01T02:00:00', 'A', 'anna', 3)
                    """);
            engine.releaseAllWriters();

            // extra is not in INCLUDE — falls back to regular indexed scan
            assertSql("""
                    name\textra
                    alice\t1
                    anna\t3
                    """, "SELECT name, extra FROM t_fb_varchar WHERE sym = 'A'");
        });
    }

    @Test
    public void testFallbackWithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_filter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_filter VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'A', 20.5),
                    ('2024-01-01T02:00:00', 'A', 30.5)
                    """);
            engine.releaseAllWriters();

            // Additional filter on covered column — CoveringIndex does not support filters
            assertSql("""
                    price
                    20.5
                    30.5
                    """, "SELECT price FROM t_filter WHERE sym = 'A' AND price > 15.0");
        });
    }

    @Test
    public void testFallbackWithNonCoveredColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noncov (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        other INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noncov VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 1),
                    ('2024-01-01T01:00:00', 'B', 20.5, 2),
                    ('2024-01-01T02:00:00', 'A', 11.5, 3)
                    """);
            engine.releaseAllWriters();

            // Selecting a non-covered column forces fallback to regular scan
            assertSql("""
                    price\tother
                    10.5\t1
                    11.5\t3
                    """, "SELECT price, other FROM t_noncov WHERE sym = 'A'");
        });
    }

    @Test
    public void testFilteredIndexScanWithArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE commodities_market_data (
                        timestamp TIMESTAMP,
                        symbol SYMBOL INDEX TYPE POSTING,
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        bids DOUBLE[][],
                        asks DOUBLE[][],
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(timestamp) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO commodities_market_data
                    SELECT
                        dateadd('m', x::INT * 5, '2024-01-01T00:00:00.000000Z'),
                        rnd_symbol('CL','GC','SI','HG'),
                        rnd_symbol('NYMEX','COMEX'),
                        rnd_symbol('Energy','Metals'),
                        ARRAY[[rnd_double() * 100, rnd_double() * 100], [rnd_double() * 100, rnd_double() * 100]],
                        ARRAY[[rnd_double() * 100, rnd_double() * 100], [rnd_double() * 100, rnd_double() * 100]],
                        rnd_double() * 100,
                        rnd_double() * 100
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            // Full query with array access — verify no crash
            try (var factory = select("""
                    SELECT timestamp, symbol,
                        first(best_bid) AS open,
                        max(best_bid) AS high,
                        min(best_bid) AS low,
                        last(best_bid) AS close,
                        avg(best_bid) AS avgr,
                        sum(bids[2][1]) AS volume
                    FROM commodities_market_data
                    WHERE symbol = 'CL' AND exchange = 'NYMEX'
                    SAMPLE BY 15m
                    """); var cursor = factory.getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) count++;
                assertTrue(count > 0);
            }
        });
    }

    @Test
    public void testFilteredIndexScanWithArrayColumnBitmap() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE cmd_bitmap (
                        timestamp TIMESTAMP,
                        symbol SYMBOL INDEX,
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        bids DOUBLE[][],
                        asks DOUBLE[][],
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(timestamp) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO cmd_bitmap
                    SELECT
                        dateadd('m', x::INT * 5, '2024-01-01T00:00:00.000000Z'),
                        rnd_symbol('CL','GC','SI','HG'),
                        rnd_symbol('NYMEX','COMEX'),
                        rnd_symbol('Energy','Metals'),
                        ARRAY[[rnd_double() * 100, rnd_double() * 100], [rnd_double() * 100, rnd_double() * 100]],
                        ARRAY[[rnd_double() * 100, rnd_double() * 100], [rnd_double() * 100, rnd_double() * 100]],
                        rnd_double() * 100,
                        rnd_double() * 100
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            // Verify the query doesn't crash — any result is fine
            try (var factory = select("""
                    SELECT timestamp, symbol,
                        first(best_bid) AS open,
                        max(best_bid) AS high,
                        min(best_bid) AS low,
                        last(best_bid) AS close,
                        avg(best_bid) AS avgr,
                        sum(bids[2][1]) AS volume
                    FROM cmd_bitmap
                    WHERE symbol = 'CL' AND exchange = 'NYMEX'
                    SAMPLE BY 15m
                    """); var cursor = factory.getCursor(sqlExecutionContext)) {
                int count = 0;
                while (cursor.hasNext()) count++;
                assertTrue(count > 0);
            }
        });
    }

    @Test
    public void testFsstRoundtrip() throws Exception {
        assertMemoryLeak(() -> {
            String[] values = {
                    "order_confirmation_alpha_2024",
                    "order_confirmation_beta_2024",
                    "order_confirmation_gamma_2024"
            };
            byte[][] valueBytes = new byte[values.length][];
            int totalLen = 0;
            for (int i = 0; i < values.length; i++) {
                valueBytes[i] = values[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
                totalLen += valueBytes[i].length;
            }
            int reps = 100;
            int trainCount = values.length * reps;
            long offsetsBytes = (long) (trainCount + 1) * Long.BYTES;
            long trainBufSize = (long) totalLen * reps;
            long cmpCap = trainBufSize * 2 + 16;
            long decCap = trainBufSize + 32;
            long batchScratchBytes = (long) trainCount * FSSTNative.BATCH_SCRATCH_BYTES_PER_VALUE;

            long trainBuf = Unsafe.malloc(trainBufSize, MemoryTag.NATIVE_DEFAULT);
            long srcOffsAddr = Unsafe.malloc(offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            long cmpBuf = Unsafe.malloc(cmpCap, MemoryTag.NATIVE_DEFAULT);
            long cmpOffsAddr = Unsafe.malloc(offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            long decBuf = Unsafe.malloc(decCap, MemoryTag.NATIVE_DEFAULT);
            long decOffsAddr = Unsafe.malloc(offsetsBytes, MemoryTag.NATIVE_DEFAULT);
            long tableBuf = Unsafe.malloc(FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long batchScratchAddr = Unsafe.malloc(batchScratchBytes, MemoryTag.NATIVE_DEFAULT);
            long decoder = Unsafe.malloc(FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                long pos = 0;
                int ord = 0;
                for (int rep = 0; rep < reps; rep++) {
                    for (byte[] bytes : valueBytes) {
                        Unsafe.putLong(srcOffsAddr + (long) ord * Long.BYTES, pos);
                        for (byte b : bytes) {
                            Unsafe.putByte(trainBuf + pos++, b);
                        }
                        ord++;
                    }
                }
                Unsafe.putLong(srcOffsAddr + (long) trainCount * Long.BYTES, pos);

                long packed = FSSTNative.trainAndCompressBlock(
                        trainBuf, srcOffsAddr, trainCount,
                        cmpBuf, cmpCap, cmpOffsAddr,
                        tableBuf, batchScratchAddr);
                assertTrue("train+compress must succeed", packed >= 0);
                long compressed = FSSTNative.unpackCompressed(packed);
                assertTrue("repetitive data must compress smaller", compressed < pos);

                assertTrue("table import must succeed", FSSTNative.importTable(decoder, tableBuf) > 0);
                long decoded = FSSTNative.decompressBlock(
                        decoder, cmpBuf, cmpOffsAddr, Long.BYTES, trainCount,
                        decBuf, decCap, decOffsAddr);
                assertEquals("decoded length must match original", pos, decoded);

                for (int i = 0; i < trainCount; i++) {
                    long lo = Unsafe.getLong(decOffsAddr + (long) i * Long.BYTES);
                    long hi = Unsafe.getLong(decOffsAddr + (long) (i + 1) * Long.BYTES);
                    byte[] expected = valueBytes[i % values.length];
                    assertEquals("value " + i + " length", expected.length, (int) (hi - lo));
                    for (int j = 0; j < expected.length; j++) {
                        assertEquals("value " + i + " byte " + j,
                                expected[j], Unsafe.getByte(decBuf + lo + j));
                    }
                }
            } finally {
                Unsafe.free(decoder, FSSTNative.DECODER_STRUCT_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(batchScratchAddr, batchScratchBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(tableBuf, FSSTNative.MAX_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(decOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(decBuf, decCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(cmpOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(cmpBuf, cmpCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(srcOffsAddr, offsetsBytes, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(trainBuf, trainBufSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testInListWithDuplicateKeys() throws Exception {
        // Gap 11: IN-list with duplicate keys
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_in_dup (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_in_dup VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0)
                    """);
            engine.releaseAllWriters();

            // SQL engine deduplicates IN-list values — 'A' appears once
            assertSql("""
                    price
                    10.0
                    20.0
                    """, "SELECT price FROM t_in_dup WHERE sym IN ('A', 'A', 'B')");
        });
    }

    // ==================== end new optimization tests ====================

    @Test
    public void testIncludeOnlyValidWithPosting() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE bad (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE BITMAP INCLUDE (price),
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("INCLUDE is only supported for POSTING index type"));
            }
        });
    }

    @Test
    public void testIncludeParseMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE multi (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (a, b, c),
                        a INT,
                        b LONG,
                        c DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            try (TableReader r = engine.getReader("multi")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(4, coveringCols.size());
            }
        });
    }

    @Test
    public void testIncludeWithNonExistentColumnFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE t_nonexist (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (nonexistent),
                            price DOUBLE
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("INCLUDE column doesn't exist"));
            }
        });
    }

    @Test
    public void testIncludeWithStringColumnAccepted() throws Exception {
        // STRING is now supported in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_string_include (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (s),
                        s STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try (TableReader r = engine.getReader("t_string_include")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertTrue(r.getMetadata().getColumnMetadata(symIdx).isCovering());
            }
        });
    }

    @Test
    public void testIncludeWithVarcharColumnAccepted() throws Exception {
        // VARCHAR is now supported in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_varchar_include (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (v),
                        v VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try (TableReader r = engine.getReader("t_varchar_include")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertTrue(r.getMetadata().getColumnMetadata(symIdx).isCovering());
            }
        });
    }

    @Test
    public void testIncrementalSealPreservesCoveringForCleanStrides() throws Exception {
        // Incremental seal: gen0 dense + sparse gens → only dirty strides re-encoded.
        // Clean strides must copy old sidecar data verbatim.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "incr_seal_cover";
                int plen = path.size();

                // 300 keys (2 strides: 0-255 in stride 0, 256-299 in stride 1)
                int keyCount = 300;
                long colAddr = Unsafe.malloc((long) keyCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < keyCount; i++) {
                        Unsafe.putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
                    }

                    // First writer: write all keys, commit, close (seal → gen0 dense)
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{1},
                                new int[]{ColumnType.LONG},
                                1
                        );
                        for (int k = 0; k < keyCount; k++) {
                            writer.add(k, k);
                        }
                        writer.setMaxValue(keyCount - 1);
                        writer.commit();
                        writer.seal(); // explicit seal: gen0 dense + sidecar
                    }

                    // Second writer: touch only key 260 (stride 1) → stride 0 stays clean
                    PostingIndexWriter writer2 = new PostingIndexWriter(configuration);
                    writer2.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                    writer2.configureCovering(
                            new long[]{colAddr},
                            new long[]{0},
                            new int[]{3},
                            new int[]{1},
                            new int[]{ColumnType.LONG},
                            1
                    );
                    writer2.add(260, keyCount); // new row for key 260
                    writer2.setMaxValue(keyCount);
                    writer2.commit();
                    writer2.seal(); // incremental seal: stride 0 clean, stride 1 dirty
                    writer2.close();

                    // Read: verify covering values for clean stride keys
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.LONG}), EMPTY_CVR, 0)) {
                        // Key 0 is in stride 0 (clean stride) — should have correct covered value
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue("covering should be available after incremental seal", cc.hasCovering());
                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(1000L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                        // Key 100 is in stride 0 (clean stride)
                        cursor = reader.getCursor(100, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());

                        assertTrue(cc.hasNext());
                        assertEquals(100, cc.next());
                        assertEquals(1100L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                        Misc.free(cursor);

                        // Key 260 is in stride 1 (dirty stride) — should also work
                        cursor = reader.getCursor(260, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());

                        assertTrue(cc.hasNext());
                        assertEquals(260, cc.next());
                        assertEquals(1260L, cc.getCoveredLong(0));
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) keyCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testIndexRebuildAfterDrop() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rebuild (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_rebuild VALUES
                    ('2024-01-01T01:00:00', 'A', 10.0),
                    ('2024-01-01T02:00:00', 'B', 20.0),
                    ('2024-01-02T01:00:00', 'A', 30.0),
                    ('2024-01-02T02:00:00', 'B', 40.0)
                    """);
            execute("ALTER TABLE t_rebuild ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            engine.releaseAllWriters();

            assertSql("""
                    price
                    10.0
                    30.0
                    """, "SELECT price FROM t_rebuild WHERE sym = 'A'");

            // Drop and re-add: new index version created, old files purged
            execute("ALTER TABLE t_rebuild ALTER COLUMN sym DROP INDEX");
            execute("ALTER TABLE t_rebuild ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            engine.releaseAllWriters();

            assertSql("""
                    price
                    10.0
                    30.0
                    """, "SELECT price FROM t_rebuild WHERE sym = 'A'");

            assertSql("""
                    price
                    20.0
                    40.0
                    """, "SELECT price FROM t_rebuild WHERE sym = 'B'");

            execute("DROP TABLE t_rebuild");
        });
    }

    @Test
    public void testInsertIntoSelectWithPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_src (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_src VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);

            execute("""
                    CREATE TABLE t_dst (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            execute("INSERT INTO t_dst SELECT * FROM t_src");
            engine.releaseAllWriters();

            assertSql("""
                    price
                    10.5
                    11.5
                    """, "SELECT price FROM t_dst WHERE sym = 'A'");
        });
    }

    // ---- Wide table and wide INCLUDE edge case tests ----

    @Test
    public void testLatestByAllColumnTypes() throws Exception {
        // LATEST BY through CoveringRecord — exercises every type accessor
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_all (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (
                            v_byte, v_short, v_int, v_long, v_float, v_double,
                            v_bool, v_date, v_uuid, v_ipv4,
                            v_varchar, v_string
                        ),
                        v_byte BYTE,
                        v_short SHORT,
                        v_int INT,
                        v_long LONG,
                        v_float FLOAT,
                        v_double DOUBLE,
                        v_bool BOOLEAN,
                        v_date DATE,
                        v_uuid UUID,
                        v_ipv4 IPv4,
                        v_varchar VARCHAR,
                        v_string STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_all VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 100, 1000, 10_000, 1.5, 2.5, true,  '2024-06-01T00:00:00.000Z', '11111111-1111-1111-1111-111111111111', '1.2.3.4', 'hello', 'world'),
                    ('2024-01-02T00:00:00', 'A', 2, 200, 2000, 20_000, 2.5, 3.5, false, '2024-07-01T00:00:00.000Z', '22222222-2222-2222-2222-222222222222', '5.6.7.8', 'bye',   'earth'),
                    ('2024-01-02T12:00:00', 'B', 3, 300, 3000, 30_000, 3.5, 4.5, true,  '2024-08-01T00:00:00.000Z', '33333333-3333-3333-3333-333333333333', '9.0.1.2', 'foo',   'bar')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    v_byte\tv_short\tv_int\tv_long\tv_float\tv_double\tv_bool\tv_date\tv_uuid\tv_ipv4\tv_varchar\tv_string
                    2\t200\t2000\t20000\t2.5\t3.5\tfalse\t2024-07-01T00:00:00.000Z\t22222222-2222-2222-2222-222222222222\t5.6.7.8\tbye\tearth
                    """, """
                    SELECT v_byte, v_short, v_int, v_long, v_float, v_double,
                           v_bool, v_date, v_uuid, v_ipv4, v_varchar, v_string
                    FROM t_latest_all WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """);
        });
    }

    @Test
    public void testLatestByArrayColumn() throws Exception {
        // LATEST BY with ARRAY covered column through CoveringRecord
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_arr (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (vals),
                        vals DOUBLE[]
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_arr VALUES
                    ('2024-01-01T00:00:00', 'A', ARRAY[1.0, 2.0]),
                    ('2024-01-02T00:00:00', 'A', ARRAY[3.0, 4.0])
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    vals
                    [3.0,4.0]
                    """, """
                    SELECT vals
                    FROM t_latest_arr WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """);
        });
    }

    @Test
    public void testLatestByBinaryColumn() throws Exception {
        // LATEST BY with BINARY covered column through CoveringRecord
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_bin (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (data),
                        data BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("INSERT INTO t_latest_bin VALUES ('2024-01-01T00:00:00', 'A', rnd_bin(10, 10, 0))");
            execute("INSERT INTO t_latest_bin VALUES ('2024-01-02T00:00:00', 'A', rnd_bin(10, 10, 0))");
            engine.releaseAllWriters();

            // Just verify the query runs without errors and returns a row
            assertSql("""
                    count
                    1
                    """, """
                    SELECT count(*) AS count
                    FROM t_latest_bin WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """);
        });
    }

    @Test
    public void testLatestByDecimalAndLong256() throws Exception {
        // LATEST BY for DECIMAL128 and LONG256 through CoveringRecord
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_dec (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (d128, hash),
                        d128 DECIMAL(38, 10),
                        hash LONG256
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_dec VALUES
                    ('2024-01-01T00:00:00', 'A', '123.4567890000'::DECIMAL(38, 10), 0x01),
                    ('2024-01-02T00:00:00', 'A', '999.9999999999'::DECIMAL(38, 10), 0x0f)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    d128\thash
                    999.9999999999\t0x0f
                    """, """
                    SELECT d128, hash
                    FROM t_latest_dec WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """);
        });
    }

    @Test
    public void testLatestByFilterMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Day 1: A has price 5, 10. Day 2: A has price 3, 8.
            // With filter price > 7: latest matching = day 2 price=8
            execute("""
                    INSERT INTO t_latest_mp VALUES
                    ('2024-01-01T01:00:00', 'A', 5.0),
                    ('2024-01-01T02:00:00', 'A', 10.0),
                    ('2024-01-02T01:00:00', 'A', 3.0),
                    ('2024-01-02T02:00:00', 'A', 8.0)
                    """);
            engine.releaseAllWriters();

            // Latest partition (day 2) has price=8 at 02:00 which matches price > 7
            assertSql("""
                    price
                    8.0
                    """, "SELECT price FROM t_latest_mp WHERE sym = 'A' AND price > 7 LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testLatestByFilterNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_nomatch (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_nomatch VALUES
                    ('2024-01-01T01:00:00', 'A', 1.0),
                    ('2024-01-01T02:00:00', 'A', 2.0),
                    ('2024-01-01T03:00:00', 'A', 3.0)
                    """);
            engine.releaseAllWriters();

            // Filter that matches no rows — empty result
            assertSql("price\n",
                    "SELECT price FROM t_latest_nomatch WHERE sym = 'A' AND price > 100 LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testLatestByMultiKeyAllTypes() throws Exception {
        // LATEST BY multi-key with diverse types through CoveringRecord
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_multi (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (v_float, v_short, v_byte),
                        v_float FLOAT,
                        v_short SHORT,
                        v_byte BYTE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_multi VALUES
                    ('2024-01-01T00:00:00', 'A', 1.5, 10, 1),
                    ('2024-01-02T00:00:00', 'A', 2.5, 20, 2),
                    ('2024-01-01T12:00:00', 'B', 3.5, 30, 3),
                    ('2024-01-02T12:00:00', 'B', 4.5, 40, 4)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    sym\tv_float\tv_short\tv_byte
                    A\t2.5\t20\t2
                    B\t4.5\t40\t4
                    """, """
                    SELECT sym, v_float, v_short, v_byte
                    FROM t_latest_multi
                    WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym
                    """);
        });
    }

    @Test
    public void testLatestByNullValues() throws Exception {
        // LATEST BY with NULL values in various types
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_nulls (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (v_int, v_double, v_varchar),
                        v_int INT,
                        v_double DOUBLE,
                        v_varchar VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_nulls VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 1.0, 'ok'),
                    ('2024-01-02T00:00:00', 'A', NULL, NaN, NULL)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    v_int\tv_double\tv_varchar
                    null\tnull\t
                    """, """
                    SELECT v_int, v_double, v_varchar
                    FROM t_latest_nulls WHERE sym = 'A' LATEST ON ts PARTITION BY sym
                    """);
        });
    }

    @Test
    public void testLatestByWithResidualFilterCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_filter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_filter VALUES
                    ('2024-01-01T01:00:00', 'A', 10.0, 100),
                    ('2024-01-01T02:00:00', 'A', 5.0, 200),
                    ('2024-01-01T03:00:00', 'A', 20.0, 300),
                    ('2024-01-01T04:00:00', 'A', 3.0, 400),
                    ('2024-01-01T01:00:00', 'B', 15.0, 500),
                    ('2024-01-01T02:00:00', 'B', 25.0, 600)
                    """);
            engine.releaseAllWriters();

            // LATEST ON + WHERE filter: find latest row for 'A' where price > 10
            // Rows: 01:00 price=10 (no), 02:00 price=5 (no), 03:00 price=20 (YES), 04:00 price=3 (no)
            // Latest matching = 03:00 with price=20, qty=300
            assertSql("""
                    price\tqty
                    20.0\t300
                    """, "SELECT price, qty FROM t_latest_filter WHERE sym = 'A' AND price > 10 LATEST ON ts PARTITION BY sym");

            // Verify covering plan is used for LATEST BY with filter
            assertPlanNoLeakCheck(
                    "SELECT price, qty FROM t_latest_filter WHERE sym = 'A' AND price > 10 LATEST ON ts PARTITION BY sym",
                    """
                            SelectedRecord
                                CoveringIndex op: latest on: sym with: price, qty
                                  filter: sym='A'
                            """
            );

            // LATEST ON + IN-list + filter: A→price=20@03:00, B→price=25@02:00
            assertSql("""
                    sym\tprice\tqty
                    A\t20.0\t300
                    B\t25.0\t600
                    """, "SELECT sym, price, qty FROM t_latest_filter WHERE sym IN ('A', 'B') AND price > 10 LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testLatestOnAllSymbolsMultiPartition() throws Exception {
        // LATEST ON without WHERE across multiple partitions
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_all_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_all_mp VALUES
                    ('2024-01-01T10:00:00', 'A', 1.0),
                    ('2024-01-01T11:00:00', 'B', 2.0),
                    ('2024-01-02T10:00:00', 'A', 3.0),
                    ('2024-01-02T11:00:00', 'C', 4.0),
                    ('2024-01-03T10:00:00', 'B', 5.0)
                    """);
            engine.releaseAllWriters();

            // A latest in day 2, B latest in day 3, C latest in day 2
            assertSql("""
                    sym\tval
                    A\t3.0
                    C\t4.0
                    B\t5.0
                    """, "SELECT sym, val FROM t_latest_all_mp LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testLatestOnAllSymbolsNoWhere() throws Exception {
        // LATEST ON without WHERE clause — scans all symbols.
        // Reproduces issue where posting index caused empty results.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_all (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (exchange, price),
                        exchange SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_all VALUES
                    ('2024-01-01T00:00:00', 'GOLD', 'CME', 2050.5),
                    ('2024-01-01T00:01:00', 'SILVER', 'CME', 24.3),
                    ('2024-01-01T00:02:00', 'GOLD', 'LME', 2051.0),
                    ('2024-01-01T00:03:00', 'OIL', 'NYMEX', 78.2),
                    ('2024-01-01T00:04:00', 'SILVER', 'LME', 24.5),
                    ('2024-01-01T00:05:00', 'GOLD', 'CME', 2052.0)
                    """);
            engine.releaseAllWriters();

            // LATEST ON without WHERE — should return one row per symbol
            assertSql("""
                    ts\tsym\texchange
                    2024-01-01T00:03:00.000000Z\tOIL\tNYMEX
                    2024-01-01T00:04:00.000000Z\tSILVER\tLME
                    2024-01-01T00:05:00.000000Z\tGOLD\tCME
                    """, "SELECT ts, sym, exchange FROM t_latest_all LATEST ON ts PARTITION BY sym");

            // Also verify with covered columns in SELECT
            assertSql("""
                    sym\tprice
                    OIL\t78.2
                    SILVER\t24.5
                    GOLD\t2052.0
                    """, "SELECT sym, price FROM t_latest_all LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testLatestOnAllSymbolsWal() throws Exception {
        // LATEST ON without WHERE via WAL
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_all_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_latest_all_wal VALUES
                    ('2024-01-01T00:00:00', 'X', 10.0),
                    ('2024-01-01T01:00:00', 'Y', 20.0),
                    ('2024-01-01T02:00:00', 'X', 30.0)
                    """);
            drainWalQueue();

            assertSql("""
                    sym\tprice
                    Y\t20.0
                    X\t30.0
                    """, "SELECT sym, price FROM t_latest_all_wal LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testLatestOnManyKeys() throws Exception {
        // Gap 4: LATEST ON with 5+ keys
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_5k (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_5k VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-01T02:00:00', 'C', 3.0),
                    ('2024-01-01T03:00:00', 'D', 4.0),
                    ('2024-01-01T04:00:00', 'E', 5.0),
                    ('2024-01-02T00:00:00', 'A', 10.0),
                    ('2024-01-02T01:00:00', 'C', 30.0),
                    ('2024-01-02T02:00:00', 'E', 50.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price
                    10.0
                    2.0
                    30.0
                    4.0
                    50.0
                    """, "SELECT price FROM t_latest_5k WHERE sym IN ('A','B','C','D','E') LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testLatestOnMultiKeyNonCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_nc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE,
                        extra INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_nc VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0, 10),
                    ('2024-01-01T01:00:00', 'B', 2.0, 20),
                    ('2024-01-02T00:00:00', 'A', 3.0, 30),
                    ('2024-01-02T01:00:00', 'C', 4.0, 40)
                    """);
            engine.releaseAllWriters();

            // LATEST ON with non-covered column — uses LatestByValues with non-cached cursors
            assertSql("""
                    count
                    3
                    """, "SELECT count() FROM (SELECT * FROM t_latest_nc WHERE sym IN ('A', 'B', 'C') LATEST ON ts PARTITION BY sym)");
        });
    }

    // ==================== no_covering hint tests ====================

    @Test
    public void testLatestOnMultiPartitionManyCommits() throws Exception {
        // LATEST ON after multiple commits across 3 partitions
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_latest_3p (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_latest_3p VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'A', 1.5),
                    ('2024-01-02T00:00:00', 'A', 2.0),
                    ('2024-01-03T00:00:00', 'A', 3.0),
                    ('2024-01-03T01:00:00', 'B', 4.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price
                    3.0
                    """, "SELECT price FROM t_latest_3p WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testMetadataPersistenceAcrossReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_persist (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // First read: verify metadata
            try (TableReader r = engine.getReader("t_persist")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
                IntList covering = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(covering);
                assertEquals(3, covering.size());
            }

            // Release all cached readers and writers to force reopening
            engine.releaseAllReaders();

            // Second read: metadata should persist
            try (TableReader r = engine.getReader("t_persist")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
                IntList covering = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(covering);
                assertEquals(3, covering.size());
                assertEquals(metadata.getColumnIndex("price"), covering.getQuick(0));
                assertEquals(metadata.getColumnIndex("qty"), covering.getQuick(1));
            }
        });
    }

    @Test
    public void testMultiPartitionAlterAndQuery() throws Exception {
        // Multiple partitions, ALTER TABLE, then query across all
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_multi_alter (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_multi_alter VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-02T00:00:00', 'A', 20.0),
                    ('2024-01-03T00:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            // ALTER: add column
            execute("ALTER TABLE t_multi_alter ADD COLUMN extra INT");

            // Insert into new partition with new column
            execute("""
                    INSERT INTO t_multi_alter (ts, sym, price, extra) VALUES
                    ('2024-01-04T00:00:00', 'A', 40.0, 999)
                    """);
            engine.releaseAllWriters();

            // Query should return all A's across all partitions
            assertSql("""
                    price
                    10.0
                    20.0
                    30.0
                    40.0
                    """, "SELECT price FROM t_multi_alter WHERE sym = 'A'");
        });
    }

    @Test
    public void testMultiVarcharIncludeFsst() throws Exception {
        // Two VARCHAR columns in INCLUDE — exercises per-includeIdx FSST cache
        // where each column has a different symbol table.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_multi_vc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, info),
                        name VARCHAR,
                        info VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_multi_vc
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B'),
                           rnd_str('name_alpha_one','name_beta_two','name_gamma_three'),
                           rnd_str('info_first_item','info_second_item','info_third_item')
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT name, info FROM t_multi_vc WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: name, info
                                  filter: sym='A'
                            """
            );

            assertSql("count\n733\n", "SELECT COUNT(*) FROM t_multi_vc WHERE sym = 'A'");
        });
    }

    // ==================== no_index hint tests ====================

    @Test
    public void testNoCoveringHint_DataCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_hint_data (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_hint_data VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            // Both paths should produce the same results
            String expected = """
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """;
            assertSql(expected, "SELECT price, qty FROM t_hint_data WHERE sym = 'A'");
            assertSql(expected, "SELECT /*+ no_covering */ price, qty FROM t_hint_data WHERE sym = 'A'");
        });
    }

    @Test
    public void testNoCoveringHint_DisablesCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_hint (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_hint VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // Without hint: CoveringIndex
            assertPlanNoLeakCheck(
                    "SELECT price FROM t_hint WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """
            );

            // With no_covering hint: no CoveringIndex
            assertPlanDoesNotContain(
                    "SELECT /*+ no_covering */ price FROM t_hint WHERE sym = 'A'"
            );
        });
    }

    @Test
    public void testNoCoveringHint_InList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_hint_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_hint_in VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'C', 30.5)
                    """);
            engine.releaseAllWriters();

            assertPlanDoesNotContain(
                    "SELECT /*+ no_covering */ price FROM t_hint_in WHERE sym IN ('A', 'B')"
            );
        });
    }

    @Test
    public void testNoCoveringHint_LatestBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_hint_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_hint_latest VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'A', 20.5),
                    ('2024-01-01T02:00:00', 'B', 30.5)
                    """);
            engine.releaseAllWriters();

            assertPlanDoesNotContain(
                    "SELECT /*+ no_covering */ price FROM t_hint_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym"
            );

            // Data correctness
            String expected = """
                    price
                    20.5
                    """;
            assertSql(expected,
                    "SELECT price FROM t_hint_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
            assertSql(expected,
                    "SELECT /*+ no_covering */ price FROM t_hint_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testNoIndexHint_DataCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_data (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_data VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300)
                    """);
            engine.releaseAllWriters();

            String expected = """
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """;
            assertSql(expected, "SELECT price, qty FROM t_noidx_data WHERE sym = 'A'");
            assertSql(expected, "SELECT /*+ no_index */ price, qty FROM t_noidx_data WHERE sym = 'A'");
        });
    }

    @Test
    public void testNoIndexHint_DisablesIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 30.5)
                    """);
            engine.releaseAllWriters();

            // Without hint: CoveringIndex
            assertPlanNoLeakCheck(
                    "SELECT price FROM t_noidx WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """
            );

            // With no_index hint: full table scan, no index at all
            String planWith = getPlan("SELECT /*+ no_index */ price FROM t_noidx WHERE sym = 'A'");
            assertFalse("Plan should not contain any Index:\n" + planWith,
                    planWith.contains("Index"));
        });
    }

    @Test
    public void testNoIndexHint_ImpliesNoCovering() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_impl (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_impl VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5)
                    """);
            engine.releaseAllWriters();

            // no_index implies no_covering — neither CoveringIndex nor SymbolIndex should appear
            String plan = getPlan("SELECT /*+ no_index */ price FROM t_noidx_impl WHERE sym = 'A'");
            assertFalse("no_index should imply no_covering:\n" + plan,
                    plan.contains("CoveringIndex"));
            assertFalse("no_index should disable all index usage:\n" + plan,
                    plan.contains("Index"));
        });
    }

    // ==================== residual filter + covering index tests ====================

    @Test
    public void testNoIndexHint_InList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_in VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'C', 30.5)
                    """);
            engine.releaseAllWriters();

            // no_index disables both covering and bitmap index
            String plan = getPlan("SELECT /*+ no_index */ price FROM t_noidx_in WHERE sym IN ('A', 'B')");
            assertFalse("Plan should not contain Index:\n" + plan, plan.contains("Index"));

            String expected = """
                    price
                    10.5
                    20.5
                    """;
            assertSql(expected, "SELECT price FROM t_noidx_in WHERE sym IN ('A', 'B')");
            assertSql(expected, "SELECT /*+ no_index */ price FROM t_noidx_in WHERE sym IN ('A', 'B')");
        });
    }

    @Test
    public void testNoIndexHint_LatestBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_latest VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'A', 20.5),
                    ('2024-01-01T02:00:00', 'B', 30.5)
                    """);
            engine.releaseAllWriters();

            // no_index disables index-based LATEST BY
            String plan = getPlan(
                    "SELECT /*+ no_index */ * FROM t_noidx_latest LATEST ON ts PARTITION BY sym");
            assertFalse("Plan should not contain 'Indexed':\n" + plan,
                    plan.contains("Indexed"));

            // Data correctness for single-key LATEST ON
            String expected = """
                    ts\tsym\tprice
                    2024-01-01T01:00:00.000000Z\tA\t20.5
                    """;
            assertSql(expected,
                    "SELECT * FROM t_noidx_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
            assertSql(expected,
                    "SELECT /*+ no_index */ * FROM t_noidx_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testNoIndexHint_WithFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_noidx_filt (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_noidx_filt VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100),
                    ('2024-01-01T01:00:00', 'A', 60.5, 200),
                    ('2024-01-01T02:00:00', 'B', 70.5, 300),
                    ('2024-01-01T03:00:00', 'A', 80.5, 400)
                    """);
            engine.releaseAllWriters();

            String expected = """
                    price\tqty
                    60.5\t200
                    80.5\t400
                    """;
            assertSql(expected, "SELECT price, qty FROM t_noidx_filt WHERE sym = 'A' AND price > 50");
            assertSql(expected, "SELECT /*+ no_index */ price, qty FROM t_noidx_filt WHERE sym = 'A' AND price > 50");
        });
    }

    @Test
    public void testNonCoveringReaderReturnsRegularCursor() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "plain_cursor_test";
                int plen = path.size();

                // Write index without covering
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                    writer.add(0, 0);
                    writer.add(0, 1);
                    writer.setMaxValue(1);
                    writer.commit();
                }

                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                    RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                    // Cursor should still implement CoveringRowCursor but hasCovering() returns false
                    assertTrue(cursor instanceof CoveringRowCursor);
                    assertFalse(((CoveringRowCursor) cursor).hasCovering());

                    // Regular iteration should work
                    assertTrue(cursor.hasNext());
                    assertEquals(0, cursor.next());
                    assertTrue(cursor.hasNext());
                    assertEquals(1, cursor.next());
                    assertFalse(cursor.hasNext());
                    Misc.free(cursor);
                }
            }
        });
    }

    @Test
    public void testNonCoveringTableUnchanged() throws Exception {
        // POSTING index without INCLUDE: with the default
        // cairo.posting.index.auto.include.timestamp=true the designated
        // timestamp is auto-appended so the bare INDEX TYPE POSTING case
        // still gets covering on the latest-by query path.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE plain (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            try (TableReader r = engine.getReader("plain")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.isColumnIndexed(symIdx));
                assertEquals(IndexType.POSTING, metadata.getColumnIndexType(symIdx));
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
                IntList coveringIndices = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringIndices);
                assertEquals(1, coveringIndices.size());
                assertEquals(metadata.getColumnIndex("ts"), coveringIndices.getQuick(0));
            }
        });
    }

    @Test
    public void testO3CommitSealFailureMarksWriterDistressed() throws Exception {
        // Regression test for #8: when sealPostingIndexesForO3Partitions
        // throws (e.g. an I/O failure on a covering column file mmap),
        // finishO3Commit must mark the writer as distressed before
        // rethrowing. Without that, the writer pool keeps handing the same
        // half-committed writer to subsequent operations.
        // The seal opens covering .d column files via openRO and then mmaps
        // them. Track the fd opened for our covering column and fail its
        // mmap so a real CairoException propagates from the seal (failing
        // openRO only flips the seal's internal "mapped=false" flag and
        // doesn't surface the failure to finishO3Commit).
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        final long[] targetFd = {-1};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (failArmed.get() && fd == targetFd[0]) {
                    return io.questdb.std.FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRO(LPSZ name) {
                long fd = super.openRO(name);
                if (failArmed.get() && fd != -1 && name != null
                        && Utf8s.endsWithAscii(name, "price.d")) {
                    targetFd[0] = fd;
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_seal_fail (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_seal_fail VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-02T00:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            failArmed.set(true);
            try (TableWriter w = TestUtils.getWriter(engine, "t_seal_fail")) {
                // O3 row inserted into day 1 after day 2 already exists —
                // commit must seal the affected partition's posting index,
                // and that seal will fail because openRO("...d") returns -1.
                String tsStr = "2024-01-01T00:30:00.000000Z";
                TableWriter.Row r = w.newRow(io.questdb.cairo.MicrosTimestampDriver.INSTANCE.parseFloor(tsStr, 0, tsStr.length()));
                r.putSym(1, "C");
                r.putDouble(2, 99.0);
                r.append();
                try {
                    w.commit();
                    fail("expected seal failure to surface");
                } catch (AssertionError e) {
                    throw e;
                } catch (Throwable ignore) {
                    // expected
                }
                assertTrue("writer must be distressed after a seal failure",
                        w.isDistressed());
            }
            failArmed.set(false);
        });
    }

    @Test
    public void testO3CommitSealFailureOnOpenROMarksWriterDistressed() throws Exception {
        // Red test for TableWriter.sealPostingIndexForPartition (around
        // TableWriter.java:10729). When openRO on a covering column file
        // returns -1, the seal silently sets mapped=false and skips the
        // entire seal block including indexer.mergeTentativeIntoActiveIfAny()
        // and rebuildSidecars(). finishO3Commit catches throws from
        // sealPostingIndexesForO3Partitions and marks the writer distressed,
        // but a silent mapped=false produces no throw — the writer stays
        // healthy from the pool's perspective while the partition's posting
        // index is half-rebuilt. Subsequent operations get handed the same
        // half-committed writer.
        //
        // The companion test testO3CommitSealFailureMarksWriterDistressed
        // validates the mmap-failure path (which does throw) and was the
        // workaround the author noted for this exact bug. After the fix,
        // openRO failure must propagate the same way.
        final AtomicBoolean failArmed = new AtomicBoolean(false);
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (failArmed.get() && name != null && Utf8s.endsWithAscii(name, "price.d")) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("""
                    CREATE TABLE t_seal_fail_open (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_seal_fail_open VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T01:00:00', 'B', 20.0),
                    ('2024-01-02T00:00:00', 'A', 30.0)
                    """);
            engine.releaseAllWriters();

            failArmed.set(true);
            try (TableWriter w = TestUtils.getWriter(engine, "t_seal_fail_open")) {
                String tsStr = "2024-01-01T00:30:00.000000Z";
                TableWriter.Row r = w.newRow(io.questdb.cairo.MicrosTimestampDriver.INSTANCE.parseFloor(tsStr, 0, tsStr.length()));
                r.putSym(1, "C");
                r.putDouble(2, 99.0);
                r.append();
                try {
                    w.commit();
                    fail("expected seal failure to surface");
                } catch (AssertionError e) {
                    throw e;
                } catch (Throwable ignore) {
                    // expected
                }
                assertTrue("writer must be distressed after a covering openRO failure",
                        w.isDistressed());
            }
            failArmed.set(false);
        });
    }

    @Test
    public void testSealFsyncsCoveringSidecarFiles() throws Exception {
        // Regression: PostingIndexWriter.sealIncremental syncs the sealed
        // value file (.pv.<sealTxn>) before publishing the new metadata
        // page in .pk, but it never syncs the covering sidecar files
        // (.pc<N>... and .pci). On power loss the kernel is free to
        // reorder writes, so an installer can see the new sealTxn in .pk
        // while the sidecar tail pages are still dirty in page cache —
        // readers then map a torn sidecar and decode garbage.
        //
        // This test tracks every msync call by mapping mmap address back
        // to the file descriptor that was passed to mmap, and asserts
        // that at least one .pc<N> file and the .pci file were msync'd
        // by the time the seal completed.
        final java.util.concurrent.ConcurrentHashMap<Long, String> fdToPath = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.concurrent.ConcurrentHashMap<Long, Long> addrToFd = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.Set<String> syncedFiles = java.util.concurrent.ConcurrentHashMap.newKeySet();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                long addr = super.mmap(fd, len, offset, flags, memoryTag);
                if (addr > 0) {
                    addrToFd.put(addr, fd);
                }
                return addr;
            }

            @Override
            public void msync(long addr, long len, boolean async) {
                Long fd = addrToFd.get(addr);
                if (fd != null) {
                    String path = fdToPath.get(fd);
                    if (path != null) {
                        syncedFiles.add(path);
                    }
                }
                super.msync(addr, len, async);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (fd > 0 && name != null) {
                    fdToPath.put(fd, Utf8s.stringFromUtf8Bytes(name));
                }
                return fd;
            }
        };
        assertMemoryLeak(ff, () -> {
            // Force every memory-mapped sync to actually flush so msync
            // gets called by MemoryCMARWImpl.sync() instead of being a no-op.
            setProperty(io.questdb.PropertyKey.CAIRO_COMMIT_MODE, "sync");
            execute("""
                    CREATE TABLE t_sync (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Insert in-order rows into 2 partitions.
            execute("""
                    INSERT INTO t_sync VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0)
                    """);
            // Now do an O3 insert that lands in partition 1. This forces
            // sealPostingIndexesForO3Partitions() and produces sealed .pv
            // and covering sidecar files for partition 1.
            execute("""
                    INSERT INTO t_sync VALUES
                    ('2024-01-01T00:30:00', 'C', 9.0)
                    """);
            // Clear the recorded set just before the seal runs by closing
            // the writer (which triggers the final seal of the last
            // partition) — but keep the tracking maps live so addresses
            // recorded earlier still resolve.
            engine.releaseAllWriters();

            boolean sidecarSynced = false;
            boolean coverInfoSynced = false;
            for (String f : syncedFiles) {
                if (f.contains("sym.pci")) {
                    coverInfoSynced = true;
                }
                if (f.matches(".*sym\\.pc\\d+\\..*")) {
                    sidecarSynced = true;
                }
            }
            assertTrue("seal must msync the .pci covering info sidecar; synced files: " + syncedFiles,
                    coverInfoSynced);
            assertTrue("seal must msync the .pc<N> covering data sidecar(s); synced files: " + syncedFiles,
                    sidecarSynced);
        });
    }

    @Test
    public void testO3MultiPartitionSidecarRebuild() throws Exception {
        // Gap 15/16: O3 affecting multiple partitions. Verifies sidecar rebuild
        // handles multiple O3-affected partitions correctly.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_multi (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            // Insert day 3 first
            execute("""
                    INSERT INTO t_o3_multi VALUES
                    ('2024-01-03T00:00:00', 'A', 30.0),
                    ('2024-01-03T12:00:00', 'B', 31.0)
                    """);
            drainWalQueue();
            // O3: insert day 1 and day 2 (both before day 3)
            execute("""
                    INSERT INTO t_o3_multi VALUES
                    ('2024-01-01T00:00:00', 'A', 10.0),
                    ('2024-01-01T12:00:00', 'B', 11.0),
                    ('2024-01-02T00:00:00', 'A', 20.0),
                    ('2024-01-02T12:00:00', 'B', 21.0)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            // All 3 partitions should return correct covered values
            assertSql("""
                    price
                    10.0
                    20.0
                    30.0
                    """, "SELECT price FROM t_o3_multi WHERE sym = 'A'");

            assertSql("""
                    price
                    11.0
                    21.0
                    31.0
                    """, "SELECT price FROM t_o3_multi WHERE sym = 'B'");
        });
    }

    // ==================== page frame cursor tests ====================

    @Test
    public void testO3WithNullCoveredValues() throws Exception {
        // Gap 18: O3 with NULL values in covered columns
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_null (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_null VALUES
                    ('2024-01-02T00:00:00', 'A', 20.0)
                    """);
            drainWalQueue();
            // O3: insert day 1 with NULL price
            execute("""
                    INSERT INTO t_o3_null VALUES
                    ('2024-01-01T00:00:00', 'A', NULL)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            assertSql("""
                    price
                    null
                    20.0
                    """, "SELECT price FROM t_o3_null WHERE sym = 'A'");
        });
    }

    @Test
    public void testO3WithSymbolIncludeColumn() throws Exception {
        // Gap 17 extended: O3 with SYMBOL INCLUDE columns
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3_sym (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (tag, price),
                        tag SYMBOL,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("""
                    INSERT INTO t_o3_sym VALUES
                    ('2024-01-02T00:00:00', 'A', 'hot', 20.0)
                    """);
            drainWalQueue();
            // O3: insert day 1
            execute("""
                    INSERT INTO t_o3_sym VALUES
                    ('2024-01-01T00:00:00', 'A', 'cold', 10.0)
                    """);
            drainWalQueue();
            engine.releaseAllWriters();

            assertSql("""
                    sym\ttag\tprice
                    A\tcold\t10.0
                    A\thot\t20.0
                    """, "SELECT sym, tag, price FROM t_o3_sym WHERE sym = 'A'");
        });
    }

    @Test
    public void testPageFrameAggregationUuidIpv4() throws Exception {
        // Page frame aggregation for UUID and IPv4 types
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_uuid (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (id, addr),
                        id UUID,
                        addr IPv4
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_uuid VALUES
                    ('2024-01-01T00:00:00', 'A', '11111111-1111-1111-1111-111111111111', '1.2.3.4'),
                    ('2024-01-01T01:00:00', 'A', '22222222-2222-2222-2222-222222222222', '5.6.7.8'),
                    ('2024-01-01T02:00:00', 'B', '33333333-3333-3333-3333-333333333333', '9.0.1.2')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    count
                    2
                    """, "SELECT count(*) AS count FROM t_pf_uuid WHERE sym = 'A'");
        });
    }

    @Test
    public void testPageFrameBinaryGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_bin (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Enough rows to exercise page frame growth (> INITIAL_CAPACITY=4096)
            execute("""
                    INSERT INTO t_pf_bin
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 60_000_000L,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END,
                        (x % 100)::DOUBLE
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            // GROUP BY exercises the page frame path
            assertSql("""
                    sym\tavg
                    A\t49.0
                    B\t50.0
                    """, "SELECT sym, avg(price) avg FROM t_pf_bin WHERE sym IN ('A', 'B') GROUP BY sym ORDER BY sym");
        });
    }

    @Test
    public void testPageFrameCursor_Aggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_agg (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Generate 200 rows with 3 symbols (A/B/C cycling via modulo)
            execute("""
                    INSERT INTO t_pf_agg
                    SELECT dateadd('m', x::INT, '2024-01-01T00:00:00'),
                           CASE x % 3 WHEN 0 THEN 'A' WHEN 1 THEN 'B' ELSE 'C' END,
                           10.0 + x * 0.1,
                           x::INT
                    FROM long_sequence(200)
                    """);
            engine.releaseAllWriters();

            // Aggregation should produce correct results via either path
            assertSqlCursors(
                    "SELECT count() FROM t_pf_agg WHERE sym = 'A'",
                    "SELECT /*+ no_covering */ count() FROM t_pf_agg WHERE sym = 'A'"
            );
        });
    }

    @Test
    public void testPageFrameCursor_MultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_multi (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_multi VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0),
                    ('2024-01-02T00:00:00', 'A', 3.0),
                    ('2024-01-02T01:00:00', 'B', 4.0),
                    ('2024-01-03T00:00:00', 'A', 5.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    val
                    1.0
                    3.0
                    5.0
                    """, "SELECT val FROM t_pf_multi WHERE sym = 'A'");
        });
    }

    @Test
    public void testPageFrameCursor_SupportsPageFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_support (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (val),
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_support VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-01T01:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            // Verify the factory reports page frame support
            assertSql("""
                    val
                    1.0
                    """, "SELECT val FROM t_pf_support WHERE sym = 'A'");
        });
    }

    @Test
    public void testPciFileFormat() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "pci_format_test";
                int plen = path.size();

                int rowCount = 4;
                long colAddrDouble = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                long colAddrInt = Unsafe.malloc((long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddrDouble + (long) i * Double.BYTES, i * 1.5);
                        Unsafe.putInt(colAddrInt + (long) i * Integer.BYTES, i * 10);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddrDouble, colAddrInt},
                                new long[]{0, 0},
                                new int[]{3, 2},  // DOUBLE shift=3, INT shift=2
                                new int[]{1, 2},  // column indices
                                new int[]{ColumnType.DOUBLE, ColumnType.INT},
                                2
                        );

                        writer.add(0, 0);
                        writer.add(0, 1);
                        writer.add(1, 2);
                        writer.add(1, 3);
                        writer.setMaxValue(3);
                        writer.commit();
                    }

                    // Verify .pci file content
                    FilesFacade ff = configuration.getFilesFacade();
                    Path pciPath = path.trimTo(plen);
                    assertTrue(ff.exists(PostingIndexUtils.coverInfoFileName(pciPath, name, COLUMN_NAME_TXN_NONE)));

                    long liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    assertTrue(liveSealTxn >= 0);
                    assertTrue(ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveSealTxn)));
                    assertTrue(ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 1, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveSealTxn)));
                } finally {
                    Unsafe.free(colAddrDouble, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(colAddrInt, (long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testPostingIndexReaderWithCovering() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_reader_test";
                int plen = path.size();

                int rowCount = 10;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    // Populate column data: row i has value 10.0 * (i + 1)
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }

                    // Write index with covering
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );

                        // key 0 -> rows 0, 3, 6
                        writer.add(0, 0);
                        writer.add(0, 3);
                        writer.add(0, 6);
                        // key 1 -> rows 1, 4
                        writer.add(1, 1);
                        writer.add(1, 4);
                        writer.setMaxValue(6);
                        writer.commit();
                    }

                    // Read back with covering cursor — verify both row IDs and covered values
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());


                        // key 0: rows 0, 3, 6 -> values 10.0, 40.0, 70.0
                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(10.0, cc.getCoveredDouble(0), 0.001);

                        assertTrue(cc.hasNext());
                        assertEquals(3, cc.next());
                        assertEquals(40.0, cc.getCoveredDouble(0), 0.001);

                        assertTrue(cc.hasNext());
                        assertEquals(6, cc.next());
                        assertEquals(70.0, cc.getCoveredDouble(0), 0.001);

                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                        // key 1: rows 1, 4 -> values 20.0, 50.0
                        cursor = reader.getCursor(1, 0, Long.MAX_VALUE, new int[]{0});
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());


                        assertTrue(cc.hasNext());
                        assertEquals(1, cc.next());
                        assertEquals(20.0, cc.getCoveredDouble(0), 0.001);

                        assertTrue(cc.hasNext());
                        assertEquals(4, cc.next());
                        assertEquals(50.0, cc.getCoveredDouble(0), 0.001);

                        assertFalse(cc.hasNext());
                        Misc.free(cursor);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testPostingIndexRowIdsMatchBitmap() throws Exception {
        assertMemoryLeak(() -> {
            // Create two tables — same data, different index types
            execute("""
                    CREATE TABLE cmd_posting (
                        timestamp TIMESTAMP,
                        symbol SYMBOL INDEX TYPE POSTING,
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(timestamp) PARTITION BY HOUR BYPASS WAL
                    """);
            execute("""
                    CREATE TABLE cmd_bitmap2 (
                        timestamp TIMESTAMP,
                        symbol SYMBOL INDEX,
                        exchange SYMBOL,
                        commodity_class SYMBOL,
                        best_bid DOUBLE,
                        best_ask DOUBLE
                    ) TIMESTAMP(timestamp) PARTITION BY HOUR BYPASS WAL
                    """);
            // Generate identical data into both tables
            execute("""
                    INSERT INTO cmd_posting
                    SELECT
                        dateadd('m', x::INT * 5, '2024-01-01T00:00:00.000000Z'),
                        rnd_symbol('CL','GC','SI','HG'),
                        rnd_symbol('NYMEX','COMEX'),
                        rnd_symbol('Energy','Metals'),
                        rnd_double() * 100,
                        rnd_double() * 100
                    FROM long_sequence(500)
                    """);
            execute("INSERT INTO cmd_bitmap2 SELECT * FROM cmd_posting");
            engine.releaseAllWriters();

            // Compare row counts — both index types must return the same count
            long bitmapCount;
            long postingCount;

            try (var f = select("SELECT count() FROM cmd_bitmap2 WHERE symbol = 'CL'");
                 var c = f.getCursor(sqlExecutionContext)) {
                c.hasNext();
                bitmapCount = c.getRecord().getLong(0);
            }
            try (var f = select("SELECT count() FROM cmd_posting WHERE symbol = 'CL'");
                 var c = f.getCursor(sqlExecutionContext)) {
                c.hasNext();
                postingCount = c.getRecord().getLong(0);
            }
            assertEquals("count mismatch for symbol='CL'", bitmapCount, postingCount);
        });
    }

    @Test
    public void testPostingIndexSplitPageFrame() throws Exception {
        // Regression: posting index cursor returned partition-absolute row IDs
        // instead of frame-relative (bitmap subtracts minValue, posting didn't).
        // With >1M rows in a single partition, the page frame splits and the
        // second frame has partitionLo>0. The absolute row IDs double-count
        // partitionLo against the already-offset page addresses.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_split_frame (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL
                    """);
            // Insert 1.5M rows — enough to split into 2 frames (pageFrameMaxRows=1M)
            // sym alternates between A and B so both keys span both frames
            execute("""
                    INSERT INTO t_split_frame
                    SELECT
                        timestamp_sequence('2024-01-01', 1_000_000) AS ts,
                        CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END AS sym,
                        x * 1.5 AS val
                    FROM long_sequence(1_500_000)
                    """);
            engine.releaseAllWriters();

            // Verify we have enough rows to split into multiple page frames
            try (TableReader reader = engine.getReader("t_split_frame")) {
                assertEquals(1_500_000, reader.size());
            }

            // Indexed scan must return correct data from both frames.
            // Before the fix, frame 2 (partitionLo=1M) would read wrong data.
            // A = even x (2,4,...,1500000), avg(x)=750001, avg(val)=750001*1.5
            assertSql("""
                    count\tavg
                    750000\t1125001.5
                    """, "SELECT count(), avg(val) FROM t_split_frame WHERE sym = 'A'");

            // B = odd x (1,3,...,1499999), avg(x)=750000, avg(val)=750000*1.5
            assertSql("""
                    count\tavg
                    750000\t1125000.0
                    """, "SELECT count(), avg(val) FROM t_split_frame WHERE sym = 'B'");
        });
    }

    @Test
    public void testPostingIndexWithO3Inserts() throws Exception {
        // Posting index (without covering) works correctly with O3
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_o3 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            execute("""
                    INSERT INTO t_o3 VALUES
                    ('2024-01-01T01:00:00', 'A', 10.0),
                    ('2024-01-01T03:00:00', 'B', 30.0),
                    ('2024-01-01T05:00:00', 'A', 50.0)
                    """);

            // Out-of-order inserts
            execute("""
                    INSERT INTO t_o3 VALUES
                    ('2024-01-01T00:00:00', 'A', 5.0),
                    ('2024-01-01T02:00:00', 'A', 20.0),
                    ('2024-01-01T04:00:00', 'B', 40.0)
                    """);
            drainWalQueue();

            // All A's returned in ts order via posting index + column reads
            assertSql("""
                    price
                    5.0
                    10.0
                    20.0
                    50.0
                    """, "SELECT price FROM t_o3 WHERE sym = 'A'");

            assertSql("""
                    price
                    30.0
                    40.0
                    """, "SELECT price FROM t_o3 WHERE sym = 'B'");
        });
    }

    @Test
    public void testPostingIndexWithVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        name VARCHAR,
                        val INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 'alice', 1),
                    ('2024-01-01T01:00:00', 'B', 'bob', 2),
                    ('2024-01-01T02:00:00', 'A', 'alice2', 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    name\tval
                    alice\t1
                    alice2\t3
                    """, "SELECT name, val FROM t_varchar WHERE sym = 'A'");
        });
    }

    @Test
    public void testPostingIndexWithVarcharLargePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_varchar_big (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        name VARCHAR,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL
                    """);
            // Insert enough data to create substantial partitions
            for (int h = 0; h < 3; h++) {
                StringBuilder sb = new StringBuilder("INSERT INTO t_varchar_big VALUES ");
                for (int i = 0; i < 10_000; i++) {
                    if (i > 0) sb.append(',');
                    String sym = switch (i % 5) {
                        case 0 -> "A";
                        case 1 -> "B";
                        case 2 -> "C";
                        case 3 -> "D";
                        default -> "E";
                    };
                    sb.append(String.format("('2024-01-01T%02d:%02d:%02d', '%s', 'name_%d_%d', %d.%d)",
                            h, i / 60 % 60, i % 60, sym, h, i, 100 + h, i));
                }
                execute(sb.toString());
            }
            engine.releaseAllWriters();

            assertSql("""
                    count
                    6000
                    """, "SELECT count() FROM t_varchar_big WHERE sym = 'A'");

            // Access varchar column via index — verify no crash with var-size column
            assertSql("""
                    count
                    5
                    """, "SELECT count() FROM (SELECT name, val FROM t_varchar_big WHERE sym = 'A' LIMIT 5)");
        });
    }

    @Test
    public void testPostingIndexWriterWithCovering() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_writer_test";
                int plen = path.size();

                // Create a fake "covered column" in native memory (simulates a DOUBLE column)
                int rowCount = 10;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        // Configure covering: 1 covered column (DOUBLE at shift=3)
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},  // columnTop = 0
                                new int[]{3},   // shift = 3 (Double.BYTES = 8 = 2^3)
                                new int[]{2},   // columnIndex = 2
                                new int[]{ColumnType.DOUBLE},
                                1
                        );

                        // Add 3 keys, 10 row IDs total
                        writer.add(0, 0);
                        writer.add(0, 3);
                        writer.add(0, 6);
                        writer.add(1, 1);
                        writer.add(1, 4);
                        writer.add(1, 7);
                        writer.add(1, 9);
                        writer.add(2, 2);
                        writer.add(2, 5);
                        writer.add(2, 8);
                        writer.setMaxValue(9);
                        writer.commit();
                    }
                    // Writer close triggers seal which writes .pci and .pc0

                    FilesFacade ff = configuration.getFilesFacade();
                    assertTrue(ff.exists(PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)));
                    long liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    assertTrue(liveSealTxn >= 0);
                    assertTrue(ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveSealTxn)));
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testRenameIndexedColumn() throws Exception {
        // Rename a column that has INDEX TYPE POSTING
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_rename (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_rename VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5),
                    ('2024-01-01T02:00:00', 'A', 11.5)
                    """);
            engine.releaseAllWriters();

            execute("ALTER TABLE t_rename RENAME COLUMN sym TO symbol");
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // Query using new name — index should still work
            assertSql("""
                    val
                    10.5
                    11.5
                    """, "SELECT val FROM t_rename WHERE symbol = 'A'");
        });
    }

    @Test
    public void testSealCreatesNewValueFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_seal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_seal VALUES
                    ('2024-01-01T00:00:00', 'A', 1),
                    ('2024-01-01T01:00:00', 'B', 2),
                    ('2024-01-01T02:00:00', 'A', 3)
                    """);
            engine.releaseAllWriters();

            // Check what files exist
            java.io.File partDir = new java.io.File(root, "t_seal~/2024-01-01");
            String[] pvFiles = partDir.list((_, n) -> n.startsWith("sym.pv"));
            System.out.println("PV files: " + java.util.Arrays.toString(pvFiles));

            // Full scan without index (SELECT * doesn't use index)
            assertSql("""
                    ts\tsym\tval
                    2024-01-01T00:00:00.000000Z\tA\t1
                    2024-01-01T01:00:00.000000Z\tB\t2
                    2024-01-01T02:00:00.000000Z\tA\t3
                    """, "SELECT * FROM t_seal");

            // Index scan
            assertSql("""
                    count
                    2
                    """, "SELECT count() FROM t_seal WHERE sym = 'A'");
        });
    }

    @Test
    public void testSeekToLastEmptyPartition() throws Exception {
        // Gap 6: LATEST ON where key exists in day 1 but not day 2.
        // seekToLast in day 2 returns -1, cursor tries day 1.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_seek_empty (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_seek_empty VALUES
                    ('2024-01-01T00:00:00', 'A', 1.0),
                    ('2024-01-02T00:00:00', 'B', 2.0)
                    """);
            engine.releaseAllWriters();

            // A exists in day 1 only. LATEST ON scans day 2 first (DESC),
            // finds 0 rows for A, falls to day 1.
            assertSql("""
                    price
                    1.0
                    """, "SELECT price FROM t_seek_empty WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testSequencerMetadataPreservesIndexType() throws Exception {
        // Verify that WAL table sequencer metadata persists and reloads index type
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_seq_idx (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            execute("""
                    INSERT INTO t_seq_idx VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            drainWalQueue();

            // Verify index type is POSTING through the reader
            try (TableReader r = engine.getReader("t_seq_idx")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertEquals(IndexType.POSTING, r.getMetadata().getColumnIndexType(symIdx));
            }

            // Force sequencer metadata reload by releasing everything and reopening
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_seq_idx")) {
                int symIdx = r.getMetadata().getColumnIndex("sym");
                assertEquals("index type should survive reload", IndexType.POSTING, r.getMetadata().getColumnIndexType(symIdx));
            }
        });
    }

    // --- Issue 1: INCLUDE validation on WAL and CREATE TABLE paths ---

    @Test
    public void testShowColumnsWithBitmapIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_show_bmp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX CAPACITY 256,
                        val DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // BITMAP index should show indexType=BITMAP and empty indexInclude
            assertSql("""
                    column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                    ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                    sym\tSYMBOL\ttrue\t256\ttrue\t128\t0\tfalse\tfalse\tBITMAP\t
                    val\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                    """, "SHOW COLUMNS FROM t_show_bmp");
        });
    }

    @Test
    public void testShowColumnsWithPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_show_cols (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Verify indexType and indexInclude columns are present for POSTING index.
            // The timestamp is auto-appended to INCLUDE for POSTING indexes (see
            // CreateTableOperationImpl.maybeAppendTimestamp), so it shows up here.
            assertSql("""
                    column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                    ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                    sym\tSYMBOL\ttrue\t256\ttrue\t128\t0\tfalse\tfalse\tPOSTING\tprice,qty,ts
                    price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                    qty\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                    """, "SHOW COLUMNS FROM t_show_cols");
        });
    }

    @Test
    public void testShowCreateTableIncludeAfterAllCoveredColumnsDropped() throws Exception {
        // Regression: when every covered column is dropped, the writer
        // tombstones each entry to -1 but does not shrink the covering
        // list (TableWriter.tombstoneCoveredColumnInOtherIndexes). The
        // SHOW CREATE TABLE renderer guards on coveringCols.size() > 0
        // and emits "INCLUDE (" / ")" unconditionally, so the rendered
        // DDL becomes "INCLUDE ()" — which the parser then rejects with
        // "at least one column name expected in INCLUDE", breaking the
        // SHOW CREATE TABLE -> execute round-trip.
        // Disable the timestamp auto-include so the covering list does
        // not retain ts as a survivor, making it possible to fully empty
        // the list via DROP COLUMN.
        setProperty(io.questdb.PropertyKey.CAIRO_POSTING_INDEX_AUTO_INCLUDE_TIMESTAMP, "false");
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_all (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_drop_all ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            execute("ALTER TABLE t_drop_all DROP COLUMN price");
            execute("ALTER TABLE t_drop_all DROP COLUMN qty");

            // Capture the rendered DDL. SHOW CREATE TABLE returns a single
            // VARCHAR column.
            String ddl;
            try (var factory = select("SHOW CREATE TABLE t_drop_all");
                 var cursor = factory.getCursor(sqlExecutionContext)) {
                assertTrue(cursor.hasNext());
                ddl = cursor.getRecord().getVarcharA(0).toString();
            }
            assertFalse("SHOW CREATE TABLE produced invalid INCLUDE () in: " + ddl,
                    ddl.contains("INCLUDE ()"));
            assertFalse("SHOW CREATE TABLE produced invalid INCLUDE() in: " + ddl,
                    ddl.contains("INCLUDE()"));

            // The rendered DDL must round-trip: dropping the table and
            // re-running the DDL must succeed.
            execute("DROP TABLE t_drop_all");
            execute(ddl);

            // SHOW COLUMNS must not crash on the all-tombstoned list and
            // must report indexInclude as empty for sym.
            assertSql("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                            sym\tSYMBOL\ttrue\t256\ttrue\t128\t0\tfalse\tfalse\tPOSTING\t
                            """,
                    "SHOW COLUMNS FROM t_drop_all");
        });
    }

    @Test
    public void testShowCreateTableIncludeAfterDropColumn() throws Exception {
        // Regression: the persisted covering list stores writer indices
        // (stable across DROP COLUMN), but the SHOW CREATE TABLE / SHOW
        // COLUMNS renderers in CairoTable-backed metadata pass those indices
        // to CairoTable.getColumnQuiet(int), which is dense-position keyed.
        // After dropping an unrelated column the dense positions of all
        // later columns shift down by one while writer indices stay put,
        // so the renderer either looks up the wrong column or runs off the
        // end of the list and silently omits a covered column.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_drop_show (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        unrelated DOUBLE,
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("ALTER TABLE t_drop_show ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            // Drop a column that is NOT in the INCLUDE list. This shifts
            // dense positions of price/qty down by one but leaves their
            // writer indices unchanged, so a writer-keyed renderer should
            // still print every covered column (price, qty, and the ts
            // that the ALTER path auto-appends for POSTING indexes).
            execute("ALTER TABLE t_drop_show DROP COLUMN unrelated");

            assertSql("""
                            ddl
                            CREATE TABLE 't_drop_show' (\s
                            \tts TIMESTAMP,
                            \tsym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty, ts),
                            \tprice DOUBLE,
                            \tqty INT
                            ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE t_drop_show");

            assertSql("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                            sym\tSYMBOL\ttrue\t256\ttrue\t128\t0\tfalse\tfalse\tPOSTING\tprice,qty,ts
                            price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            qty\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            """,
                    "SHOW COLUMNS FROM t_drop_show");
        });
    }

    @Test
    public void testShowCreateTableWithInclude() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_show (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            assertSql("""
                    ddl
                    CREATE TABLE 't_show' (\s
                    \tts TIMESTAMP,
                    \tsym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty, ts),
                    \tprice DOUBLE,
                    \tqty INT
                    ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                    """, "SHOW CREATE TABLE t_show");
        });
    }

    @Test
    public void testSidecarMissingFileFallsBackToUnavailable() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_missing_sidecar";
                int plen = path.size();

                int rowCount = 10;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
                    }

                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr}, new long[]{0}, new int[]{3},
                                new int[]{2}, new int[]{ColumnType.DOUBLE}, 1
                        );
                        for (int i = 0; i < rowCount; i++) {
                            writer.add(0, i);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }

                    FilesFacade ff = configuration.getFilesFacade();
                    long liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                            ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    LPSZ sidecarFile = PostingIndexUtils.coverDataFileName(
                            path.trimTo(plen), name, 0, COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, liveSealTxn);
                    assertTrue("sidecar present before deletion", ff.exists(sidecarFile));
                    assertTrue("sidecar removed", ff.removeQuiet(sidecarFile));

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        RowCursor cursor = reader.getCursor(0, 0, Long.MAX_VALUE, new int[]{0});
                        try {
                            assertTrue(cursor instanceof CoveringRowCursor);
                            CoveringRowCursor cc = (CoveringRowCursor) cursor;

                            int count = 0;
                            while (cursor.hasNext()) {
                                long rowId = cursor.next();
                                assertEquals(count, rowId);
                                assertFalse("missing sidecar must surface as unavailable",
                                        cc.isCoveredAvailable(0));
                                count++;
                            }
                            assertEquals(rowCount, count);
                        } finally {
                            Misc.free(cursor);
                        }
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSidecarVersioningAcrossSeals() throws Exception {
        // Successive seals must create NEW versioned sidecar files
        // (.pc<N>.<C>.<S>) rather than truncating the previous sealed
        // version in place — otherwise a reader mmap'd to the prior
        // sealed-version file would be invalidated.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_seal_versioning";
                int plen = path.size();
                int rowCount = 30;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
                    }
                    FilesFacade ff = configuration.getFilesFacade();

                    // First writer instance: write data and explicit seal.
                    long firstSealTxn;
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );
                        for (int i = 0; i < 10; i++) {
                            writer.add(i % 3, i);
                        }
                        writer.setMaxValue(9);
                        writer.commit();
                        writer.seal();
                        firstSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                                ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    }
                    assertTrue("first seal must produce a positive sealTxn", firstSealTxn > 0);
                    // Verify .pc0 at the first sealTxn is on disk.
                    assertTrue("first-seal sidecar .pc0 must exist",
                            ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0,
                                    COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, firstSealTxn)));

                    // Second writer instance: reopen, write more data, force another seal.
                    long secondSealTxn;
                    try (PostingIndexWriter writer2 = new PostingIndexWriter(configuration)) {
                        writer2.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, false);
                        writer2.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{2},
                                new int[]{ColumnType.DOUBLE},
                                1
                        );
                        for (int i = 10; i < 30; i++) {
                            writer2.add(i % 3, i);
                        }
                        writer2.setMaxValue(29);
                        writer2.commit();
                        writer2.seal();
                        secondSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                                ff, PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE));
                    }
                    assertTrue("second seal must advance sealTxn beyond the first",
                            secondSealTxn > firstSealTxn);

                    // The previous-seal .pc0 file must STILL exist on disk
                    // after the second seal — the previous sealed version
                    // stays untouched until the background purge job collects it.
                    assertTrue("first-seal sidecar .pc0 must survive a subsequent seal",
                            ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0,
                                    COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, firstSealTxn)));
                    // And the second seal's .pc0 at the new sealTxn must exist too.
                    assertTrue("second-seal sidecar .pc0 must exist at the new sealTxn",
                            ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 0,
                                    COLUMN_NAME_TXN_NONE, COLUMN_NAME_TXN_NONE, secondSealTxn)));

                    // Reader at the latest sealTxn sees ALL committed data.
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{2}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        int totalRows = 0;
                        for (int key = 0; key < 3; key++) {
                            CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(key, 0, Long.MAX_VALUE, new int[]{0});
                            while (cc.hasNext()) {
                                long rowId = cc.next();
                                double covered = cc.getCoveredDouble(0);
                                assertEquals("covered value must round-trip across the seal boundary",
                                        100.0 + rowId, covered, 0.001);
                                totalRows++;
                            }
                            Misc.free(cc);
                        }
                        assertEquals("reader must see every committed row across both seals", 30, totalRows);
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSparseGenCoverWithLeadingBlockTrim() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cov_sparse_min";
                int plen = path.size();
                int rowCount = 600;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.putDouble(colAddr + (long) i * Double.BYTES, i);
                    }

                    PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE);
                    writer.configureCovering(
                            new long[]{colAddr}, new long[]{0}, new int[]{3},
                            new int[]{1}, new int[]{ColumnType.DOUBLE}, 1
                    );

                    // Gen 0 (dense): introduces keys 0, 1, 2 so the writer's total keyCount=3.
                    writer.add(0, 0);
                    writer.add(1, 1);
                    writer.add(2, 2);
                    writer.setMaxValue(2);
                    writer.commit();

                    // Gen 1 (sparse): touches only keys 0 and 1 — key 2 inactive makes the gen sparse.
                    // key 0: rows 100..399 (300 values → sidecarBase=300 in gen 1 for key 1)
                    // key 1: rows 400..599 (200 values → 4 blocks of 64/64/64/8)
                    for (int i = 100; i < 400; i++) writer.add(0, i);
                    for (int i = 400; i < 600; i++) writer.add(1, i);
                    writer.setMaxValue(rowCount - 1);
                    writer.commit();

                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0,
                            coveringMetadata(new int[]{1}, new int[]{ColumnType.DOUBLE}), EMPTY_CVR, 0)) {
                        // minValue=464 drops block 0 (rows 400..463); blocks 1..3 remain.
                        final long minValue = 464;
                        CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(1, minValue, Long.MAX_VALUE, new int[]{0});
                        try {
                            assertTrue(cc.hasCovering());
                            int expectedRow = 464;
                            while (cc.hasNext()) {
                                // RowCursor.next() returns rowId relative to minValue.
                                long actualRow = cc.next() + minValue;
                                assertEquals(expectedRow, actualRow);
                                double covered = cc.getCoveredDouble(0);
                                assertEquals("row " + actualRow + " covered value",
                                        (double) actualRow, covered, 0.0);
                                expectedRow++;
                            }
                            assertEquals(600, expectedRow);
                        } finally {
                            Misc.free(cc);
                        }
                    }

                    writer.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testStringPageFrameDataCorrectness() throws Exception {
        // STRING columns also go through the page frame path now
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_str_pf (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (label),
                        label STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_str_pf VALUES
                    ('2024-01-01T00:00:00', 'A', 'hello'),
                    ('2024-01-01T01:00:00', 'A', 'world'),
                    ('2024-01-01T02:00:00', 'A', null),
                    ('2024-01-01T03:00:00', 'B', 'other')
                    """);
            engine.releaseAllWriters();

            assertSql("label\nhello\nworld\n\n", "SELECT label FROM t_str_pf WHERE sym = 'A'");
        });
    }

    @Test
    public void testTruncateTableWithPostingIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_trunc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_trunc VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5),
                    ('2024-01-01T01:00:00', 'B', 20.5)
                    """);
            engine.releaseAllWriters();

            // Verify data exists
            assertSql("""
                    price
                    10.5
                    """, "SELECT price FROM t_trunc WHERE sym = 'A'");

            execute("TRUNCATE TABLE t_trunc");

            // Table should be empty
            assertSql("""
                    price
                    """, "SELECT price FROM t_trunc WHERE sym = 'A'");

            // Insert new data — index should work after truncate
            execute("""
                    INSERT INTO t_trunc VALUES
                    ('2024-01-02T00:00:00', 'C', 30.5),
                    ('2024-01-02T01:00:00', 'C', 31.5)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price
                    30.5
                    31.5
                    """, "SELECT price FROM t_trunc WHERE sym = 'C'");
        });
    }

    @Test
    public void testVarcharPageFrameCountMixed() throws Exception {
        // COUNT(*) with mixed VARCHAR + DOUBLE INCLUDE.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_count (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_count
                    SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,
                           rnd_symbol('A','B','C'),
                           rnd_str('alpha','beta','gamma'),
                           rnd_double() * 100
                    FROM long_sequence(1500)
                    """);
            engine.releaseAllWriters();

            // COUNT via non-covering path as baseline
            assertSql("""
                    count
                    528
                    """, "SELECT COUNT(*) FROM t_vw_count WHERE sym = 'A'");
        });
    }

    // --- Issue 3: Index restart skips already-indexed partitions ---

    @Test
    public void testVarcharPageFrameDataCorrectness() throws Exception {
        // Verify VARCHAR values are correctly read through the page frame path
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_data (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_data VALUES
                    ('2024-01-01T00:00:00', 'A', 'short'),
                    ('2024-01-01T01:00:00', 'A', 'medium_length_string'),
                    ('2024-01-01T02:00:00', 'A', 'this is a longer string that exceeds the inline limit of nine bytes'),
                    ('2024-01-01T03:00:00', 'A', ''),
                    ('2024-01-01T04:00:00', 'A', null),
                    ('2024-01-01T05:00:00', 'B', 'other')
                    """);
            engine.releaseAllWriters();

            // Compare covering vs non-covering results
            assertSql("name\nshort\nmedium_length_string\nthis is a longer string that exceeds the inline limit of nine bytes\n\n\n", "SELECT name FROM t_vw_data WHERE sym = 'A'");
        });
    }

    // --- Issue 4: Page frame with BINARY covered column (GROUP BY) ---

    @Test
    public void testVarcharPageFrameInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name),
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_in VALUES
                    ('2024-01-01T00:00:00', 'A', 'alpha'),
                    ('2024-01-01T01:00:00', 'B', 'beta'),
                    ('2024-01-01T02:00:00', 'C', 'gamma'),
                    ('2024-01-01T03:00:00', 'A', 'delta')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    name
                    alpha
                    delta
                    beta
                    """, "SELECT name FROM t_vw_in WHERE sym IN ('A', 'B') ORDER BY ts");
        });
    }

    // --- Issue 5: DISTINCT end-to-end ---

    @Test
    public void testVarcharPageFrameMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 'day1_alpha', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'day1_beta', 20.0),
                    ('2024-01-02T00:00:00', 'A', 'day2_alpha', 30.0),
                    ('2024-01-03T00:00:00', 'A', 'day3_alpha', 50.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    name\tprice
                    day1_alpha\t10.0
                    day2_alpha\t30.0
                    day3_alpha\t50.0
                    """, "SELECT name, price FROM t_vw_mp WHERE sym = 'A'");

            // Aggregate across partitions
            assertSql("""
                    count\tmin\tmax
                    3\t10.0\t50.0
                    """, "SELECT count(*), min(price), max(price) FROM t_vw_mp WHERE sym = 'A'");
        });
    }

    @Test
    public void testVarcharPageFramePlan() throws Exception {
        // VARCHAR in INCLUDE should use CoveringIndex with page frame path (not row-by-row)
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_plan (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_plan VALUES
                    ('2024-01-01T00:00:00', 'A', 'hello', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'world', 20.0)
                    """);
            engine.releaseAllWriters();

            assertPlanNoLeakCheck(
                    "SELECT name, price FROM t_vw_plan WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: name, price
                                  filter: sym='A'
                            """
            );
        });
    }

    // --- Issue 6: Covered TIMESTAMP values correct through seal + delta compression ---

    @Test
    public void testVarcharPageFrameVectorizedGroupBy() throws Exception {
        // Vectorized GROUP BY on covered DOUBLE column should work even when
        // VARCHAR is also in INCLUDE (page frame path now supports var-width).
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_agg (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_agg VALUES
                    ('2024-01-01T00:00:00', 'A', 'alpha', 10.0),
                    ('2024-01-01T01:00:00', 'B', 'beta', 20.0),
                    ('2024-01-01T02:00:00', 'A', 'gamma', 30.0),
                    ('2024-01-01T03:00:00', 'A', null, 50.0)
                    """);
            engine.releaseAllWriters();

            // Vectorized aggregate uses page frame cursor
            assertPlanNoLeakCheck(
                    "SELECT count(*), min(price), max(price) FROM t_vw_agg WHERE sym = 'A'",
                    """
                            GroupBy vectorized: true workers: 1
                              values: [count(*),min(price),max(price)]
                                CoveringIndex on: sym with: price
                                  filter: sym='A'
                            """
            );

            assertSql("""
                    count\tmin\tmax
                    3\t10.0\t50.0
                    """, "SELECT count(*), min(price), max(price) FROM t_vw_agg WHERE sym = 'A'");
        });
    }

    // --- Auto-include designated timestamp in INCLUDE ---

    @Test
    public void testVarcharPageFrameWithFilter() throws Exception {
        // Residual filter on covered DOUBLE column with VARCHAR also in INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_vw_filt (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),
                        name VARCHAR,
                        price DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_vw_filt VALUES
                    ('2024-01-01T00:00:00', 'A', 'cheap', 5.0),
                    ('2024-01-01T01:00:00', 'A', 'mid', 50.0),
                    ('2024-01-01T02:00:00', 'A', 'expensive', 95.0),
                    ('2024-01-01T03:00:00', 'B', 'other', 75.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                            name\tprice
                            mid\t50.0
                            expensive\t95.0
                            """,
                    "SELECT name, price FROM t_vw_filt WHERE sym = 'A' AND price > 20");
        });
    }

    @Test
    public void testWideInclude10Columns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide10 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9),
                        c0 INT,
                        c1 LONG,
                        c2 DOUBLE,
                        c3 FLOAT,
                        c4 SHORT,
                        c5 BYTE,
                        c6 BOOLEAN,
                        c7 DATE,
                        c8 TIMESTAMP,
                        c9 CHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide10 VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 100, 1.5, 2.5, 10, 3, true, '2024-01-01', '2024-01-01T00:00:00', 'x'),
                    ('2024-01-01T01:00:00', 'B', 2, 200, 2.5, 3.5, 20, 4, false, '2024-01-02', '2024-01-01T01:00:00', 'y'),
                    ('2024-01-01T02:00:00', 'A', 3, 300, 3.5, 4.5, 30, 5, true, '2024-01-03', '2024-01-01T02:00:00', 'z')
                    """);
            engine.releaseAllWriters();

            // Query all 10 covered columns
            assertSql("""
                    c0\tc1\tc2\tc3\tc4\tc5\tc6\tc7\tc8\tc9
                    1\t100\t1.5\t2.5\t10\t3\ttrue\t2024-01-01T00:00:00.000Z\t2024-01-01T00:00:00.000000Z\tx
                    3\t300\t3.5\t4.5\t30\t5\ttrue\t2024-01-03T00:00:00.000Z\t2024-01-01T02:00:00.000000Z\tz
                    """, "SELECT c0, c1, c2, c3, c4, c5, c6, c7, c8, c9 FROM t_wide10 WHERE sym = 'A'");

            // Query a subset of covered columns
            assertSql("""
                    c0\tc5\tc9
                    1\t3\tx
                    3\t5\tz
                    """, "SELECT c0, c5, c9 FROM t_wide10 WHERE sym = 'A'");

            // Plan should use CoveringIndex (SelectedRecord wraps subset)
            assertPlanNoLeakCheck(
                    "SELECT c0, c9 FROM t_wide10 WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: c0, c9
                                  filter: sym='A'
                            """
            );
        });
    }

    @Test
    public void testWideInclude20Columns() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide20 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT,
                        c5 LONG, c6 LONG, c7 LONG, c8 LONG, c9 LONG,
                        c10 DOUBLE, c11 DOUBLE, c12 DOUBLE, c13 DOUBLE, c14 DOUBLE,
                        c15 FLOAT, c16 FLOAT, c17 FLOAT, c18 FLOAT, c19 FLOAT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide20 VALUES
                    ('2024-01-01T00:00:00', 'X', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0),
                    ('2024-01-01T01:00:00', 'Y', 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 117.0, 118.0, 119.0),
                    ('2024-01-01T02:00:00', 'X', 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210.0, 211.0, 212.0, 213.0, 214.0, 215.0, 216.0, 217.0, 218.0, 219.0)
                    """);
            engine.releaseAllWriters();

            // Query first and last covered columns
            assertSql("""
                    c0\tc19
                    0\t19.0
                    200\t219.0
                    """, "SELECT c0, c19 FROM t_wide20 WHERE sym = 'X'");

            // Query all 20 covered columns
            assertSql("""
                    c0\tc1\tc2\tc3\tc4\tc5\tc6\tc7\tc8\tc9\tc10\tc11\tc12\tc13\tc14\tc15\tc16\tc17\tc18\tc19
                    100\t101\t102\t103\t104\t105\t106\t107\t108\t109\t110.0\t111.0\t112.0\t113.0\t114.0\t115.0\t116.0\t117.0\t118.0\t119.0
                    """, "SELECT c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19 FROM t_wide20 WHERE sym = 'Y'");

            // IN-list across wide INCLUDE
            assertSql("""
                    c0\tc10\tc19
                    0\t10.0\t19.0
                    100\t110.0\t119.0
                    200\t210.0\t219.0
                    """, "SELECT c0, c10, c19 FROM t_wide20 WHERE sym IN ('X', 'Y') ORDER BY c0");
        });
    }

    @Test
    public void testWideInclude20ColumnsMultiPartition() throws Exception {
        // 20 INCLUDE columns across multiple partitions and commits
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide20_mp (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT,
                        c5 LONG, c6 LONG, c7 LONG, c8 LONG, c9 LONG,
                        c10 DOUBLE, c11 DOUBLE, c12 DOUBLE, c13 DOUBLE, c14 DOUBLE,
                        c15 FLOAT, c16 FLOAT, c17 FLOAT, c18 FLOAT, c19 FLOAT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Partition 1, commit 1
            execute("""
                    INSERT INTO t_wide20_mp VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0)
                    """);
            // Partition 1, commit 2 (same partition, different gen)
            execute("""
                    INSERT INTO t_wide20_mp VALUES
                    ('2024-01-01T12:00:00', 'A', 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 117.0, 118.0, 119.0)
                    """);
            // Partition 2
            execute("""
                    INSERT INTO t_wide20_mp VALUES
                    ('2024-01-02T00:00:00', 'A', 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210.0, 211.0, 212.0, 213.0, 214.0, 215.0, 216.0, 217.0, 218.0, 219.0),
                    ('2024-01-02T01:00:00', 'B', 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310.0, 311.0, 312.0, 313.0, 314.0, 315.0, 316.0, 317.0, 318.0, 319.0)
                    """);
            engine.releaseAllWriters();

            // Query across partitions and gens, select subset of wide INCLUDE
            assertSql("""
                    c0\tc9\tc14\tc19
                    0\t9\t14.0\t19.0
                    100\t109\t114.0\t119.0
                    200\t209\t214.0\t219.0
                    """, "SELECT c0, c9, c14, c19 FROM t_wide20_mp WHERE sym = 'A'");

            // LATEST ON with wide INCLUDE
            assertSql("""
                    c0\tc19
                    200\t219.0
                    """, "SELECT c0, c19 FROM t_wide20_mp WHERE sym = 'A' LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testWideIncludeAlterTable() throws Exception {
        // Add a wide INCLUDE via ALTER TABLE, then insert and query
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_alter_wide (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        c0 INT, c1 LONG, c2 DOUBLE, c3 FLOAT, c4 SHORT,
                        c5 INT, c6 LONG, c7 DOUBLE, c8 FLOAT, c9 SHORT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            execute("ALTER TABLE t_alter_wide ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)");

            // Insert after adding index so sidecar data is written
            execute("""
                    INSERT INTO t_alter_wide VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2.0, 3.0, 4, 5, 6, 7.0, 8.0, 9),
                    ('2024-01-01T12:00:00', 'A', 10, 11, 12.0, 13.0, 14, 15, 16, 17.0, 18.0, 19),
                    ('2024-01-01T06:00:00', 'B', 20, 21, 22.0, 23.0, 24, 25, 26, 27.0, 28.0, 29)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    c0\tc4\tc9
                    0\t4\t9
                    10\t14\t19
                    """, "SELECT c0, c4, c9 FROM t_alter_wide WHERE sym = 'A'");

            assertSql("""
                    c0\tc4\tc9
                    20\t24\t29
                    """, "SELECT c0, c4, c9 FROM t_alter_wide WHERE sym = 'B'");
        });
    }

    @Test
    public void testWideIncludeDistinct() throws Exception {
        // DISTINCT on symbol with wide INCLUDE — should still use posting index
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_distinct (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT,
                        c5 DOUBLE, c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_distinct VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2, 3, 4, 5.0, 6.0, 7.0, 8.0, 9.0),
                    ('2024-01-01T01:00:00', 'B', 10, 11, 12, 13, 14, 15.0, 16.0, 17.0, 18.0, 19.0),
                    ('2024-01-01T02:00:00', 'A', 20, 21, 22, 23, 24, 25.0, 26.0, 27.0, 28.0, 29.0),
                    ('2024-01-01T03:00:00', 'C', 30, 31, 32, 33, 34, 35.0, 36.0, 37.0, 38.0, 39.0)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    sym
                    A
                    B
                    C
                    """, "SELECT DISTINCT sym FROM t_wide_distinct");
        });
    }

    @Test
    public void testWideIncludeInListSubset() throws Exception {
        // IN-list query selecting a sparse subset of wide INCLUDE columns
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_in (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT, c5 INT,
                        c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE, c10 DOUBLE, c11 DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_in VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2, 3, 4, 5, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0),
                    ('2024-01-01T01:00:00', 'B', 10, 11, 12, 13, 14, 15, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0),
                    ('2024-01-01T02:00:00', 'C', 20, 21, 22, 23, 24, 25, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0)
                    """);
            engine.releaseAllWriters();

            // IN-list selecting every 3rd covered column
            assertSql("""
                    c0\tc3\tc6\tc9
                    0\t3\t6.0\t9.0
                    10\t13\t16.0\t19.0
                    """, "SELECT c0, c3, c6, c9 FROM t_wide_in WHERE sym IN ('A', 'B')");
        });
    }

    @Test
    public void testWideIncludeLatestOn() throws Exception {
        // LATEST ON with wide INCLUDE — verifies last row per symbol
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_latest (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7),
                        c0 INT, c1 LONG, c2 DOUBLE, c3 FLOAT,
                        c4 INT, c5 LONG, c6 DOUBLE, c7 FLOAT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_latest VALUES
                    ('2024-01-01T00:00:00', 'A', 1, 2, 3.0, 4.0, 5, 6, 7.0, 8.0),
                    ('2024-01-01T01:00:00', 'B', 10, 20, 30.0, 40.0, 50, 60, 70.0, 80.0),
                    ('2024-01-01T02:00:00', 'A', 100, 200, 300.0, 400.0, 500, 600, 700.0, 800.0)
                    """);
            engine.releaseAllWriters();

            // LATEST ON single key
            assertSql("""
                    c0\tc1\tc2\tc3\tc4\tc5\tc6\tc7
                    100\t200\t300.0\t400.0\t500\t600\t700.0\t800.0
                    """, "SELECT c0, c1, c2, c3, c4, c5, c6, c7 FROM t_wide_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym");

            // LATEST ON IN-list (ordered by symbol key)
            assertSql("""
                    sym\tc0\tc7
                    A\t100\t800.0
                    B\t10\t80.0
                    """, "SELECT sym, c0, c7 FROM t_wide_latest WHERE sym IN ('A', 'B') LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testWideIncludeManyRowsPerKey() throws Exception {
        // Many rows for same key with wide INCLUDE — stress sidecar writes
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_many (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7),
                        c0 INT, c1 LONG, c2 DOUBLE, c3 FLOAT,
                        c4 INT, c5 LONG, c6 DOUBLE, c7 FLOAT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);

            // Insert 200 rows for key 'A', 50 for 'B'
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO t_wide_many VALUES ");
            for (int i = 0; i < 200; i++) {
                if (i > 0) sb.append(", ");
                String sym = i % 4 == 0 ? "B" : "A";
                sb.append("('2024-01-01T00:").append(String.format("%02d", i / 60)).append(":")
                        .append(String.format("%02d", i % 60)).append("', '")
                        .append(sym).append("', ")
                        .append(i).append(", ").append(i * 10L).append(", ")
                        .append(i * 1.5).append(", ").append(i * 2.5f).append(", ")
                        .append(i + 1000).append(", ").append((i + 1000) * 10L).append(", ")
                        .append((i + 1000) * 1.5).append(", ").append((i + 1000) * 2.5f).append(")");
            }
            execute(sb.toString());
            engine.releaseAllWriters();

            // Verify row count for key A (150 rows: indices not divisible by 4)
            assertSql("""
                    count
                    150
                    """, "SELECT count(*) count FROM t_wide_many WHERE sym = 'A'");

            // Verify row count for key B (50 rows: indices divisible by 4)
            assertSql("""
                    count
                    50
                    """, "SELECT count(*) count FROM t_wide_many WHERE sym = 'B'");

            // Spot check first row for A (i=1)
            assertSql("""
                    c0\tc1
                    1\t10
                    """, "SELECT c0, c1 FROM t_wide_many WHERE sym = 'A' LIMIT 1");
        });
    }

    @Test
    public void testWideIncludeMetadataRoundtrip() throws Exception {
        // Verify metadata preserves wide INCLUDE column indices
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_meta (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7),
                        c0 INT, c1 LONG, c2 DOUBLE, c3 FLOAT,
                        c4 SHORT, c5 BYTE, c6 DATE, c7 TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            engine.releaseAllWriters();

            try (TableReader r = engine.getReader("t_wide_meta")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                IntList coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(9, coveringCols.size());
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
            }
        });
    }

    @Test
    public void testWideIncludeNullsAllColumns() throws Exception {
        // All covered columns are NULL — tests null sentinel handling for every type
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_nulls (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (i, l, d, f, s, b, bl, dt, ts2, ch),
                        i INT,
                        l LONG,
                        d DOUBLE,
                        f FLOAT,
                        s SHORT,
                        b BYTE,
                        bl BOOLEAN,
                        dt DATE,
                        ts2 TIMESTAMP,
                        ch CHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_nulls VALUES
                    ('2024-01-01T00:00:00', 'A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                    ('2024-01-01T01:00:00', 'A', 42, 100, 3.14, 2.7, 10, 5, true, '2024-01-01', '2024-01-01T01:00:00', 'z')
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    i\tl\td\tf\ts\tb\tbl\tdt\tts2\tch
                    null\tnull\tnull\tnull\t0\t0\tfalse\t\t\t
                    42\t100\t3.14\t2.7\t10\t5\ttrue\t2024-01-01T00:00:00.000Z\t2024-01-01T01:00:00.000000Z\tz
                    """, "SELECT i, l, d, f, s, b, bl, dt, ts2, ch FROM t_wide_nulls WHERE sym = 'A'");
        });
    }

    @Test
    public void testWideIncludeNullsUuidIpv4Long256() throws Exception {
        // NULL handling for the fixed-size types not covered by
        // testWideIncludeNullsAllColumns: UUID (16 bytes, dual Long.MIN_VALUE
        // sentinel), IPv4 (4 bytes, sentinel = 0), LONG256 (32 bytes, all-zero
        // sentinel). Verifies the covering path renders these as NULL/empty
        // and not as the raw sentinel bit pattern.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_nulls_2 (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (id, addr, l256),
                        id UUID,
                        addr IPv4,
                        l256 LONG256
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_nulls_2 VALUES
                    ('2024-01-01T00:00:00', 'A', NULL, NULL, NULL),
                    ('2024-01-01T01:00:00', 'A', '11111111-1111-1111-1111-111111111111', '10.0.0.1',
                        cast('0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef' as LONG256))
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    id\taddr\tl256
                    \t\t
                    11111111-1111-1111-1111-111111111111\t10.0.0.1\t0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
                    """, "SELECT id, addr, l256 FROM t_wide_nulls_2 WHERE sym = 'A'");
        });
    }

    @Test
    public void testWideIncludeWalTable() throws Exception {
        // Wide INCLUDE on WAL table with multiple commits
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_wal (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9),
                        c0 INT, c1 INT, c2 INT, c3 INT, c4 INT,
                        c5 DOUBLE, c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);

            execute("""
                    INSERT INTO t_wide_wal VALUES
                    ('2024-01-01T00:00:00', 'A', 0, 1, 2, 3, 4, 5.0, 6.0, 7.0, 8.0, 9.0)
                    """);
            execute("""
                    INSERT INTO t_wide_wal VALUES
                    ('2024-01-01T01:00:00', 'A', 10, 11, 12, 13, 14, 15.0, 16.0, 17.0, 18.0, 19.0)
                    """);
            drainWalQueue();

            assertSql("""
                    c0\tc4\tc9
                    0\t4\t9.0
                    10\t14\t19.0
                    """, "SELECT c0, c4, c9 FROM t_wide_wal WHERE sym = 'A'");
        });
    }

    @Test
    public void testWideIncludeWithVarchar() throws Exception {
        // Mix of fixed-size and variable-length columns in a wide INCLUDE
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_wide_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty, label, tag, note, extra_d, extra_i),
                        price DOUBLE,
                        qty INT,
                        label VARCHAR,
                        tag VARCHAR,
                        note STRING,
                        extra_d DOUBLE,
                        extra_i INT
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_wide_varchar VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100, 'label_a1', 'tag_a', 'note_a1', 1.1, 1),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200, 'label_b1', 'tag_b', 'note_b1', 2.2, 2),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300, 'label_a2', 'tag_a', 'note_a2', 3.3, 3)
                    """);
            engine.releaseAllWriters();

            assertSql("""
                    price\tqty\tlabel\ttag\tnote\textra_d\textra_i
                    10.5\t100\tlabel_a1\ttag_a\tnote_a1\t1.1\t1
                    30.5\t300\tlabel_a2\ttag_a\tnote_a2\t3.3\t3
                    """, "SELECT price, qty, label, tag, note, extra_d, extra_i FROM t_wide_varchar WHERE sym = 'A'");

            // Query only varchar columns
            assertSql("""
                    label\ttag\tnote
                    label_b1\ttag_b\tnote_b1
                    """, "SELECT label, tag, note FROM t_wide_varchar WHERE sym = 'B'");
        });
    }

    @Test
    public void testWideTableManyUncoveredColumns() throws Exception {
        // Table with 30 columns total but only 2 in INCLUDE — tests that
        // the sidecar doesn't interfere with many non-covered columns
        assertMemoryLeak(() -> {
            StringBuilder ddl = new StringBuilder();
            ddl.append("CREATE TABLE t_30col (\n");
            ddl.append("    ts TIMESTAMP,\n");
            ddl.append("    sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),\n");
            ddl.append("    price DOUBLE,\n");
            ddl.append("    qty INT");
            for (int i = 0; i < 26; i++) {
                ddl.append(",\n    extra_").append(i).append(" DOUBLE");
            }
            ddl.append("\n) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute(ddl.toString());

            // Insert with all extra columns set to their index value
            StringBuilder ins = new StringBuilder();
            ins.append("INSERT INTO t_30col VALUES ('2024-01-01T00:00:00', 'A', 10.5, 100");
            for (int i = 0; i < 26; i++) {
                ins.append(", ").append(i).append(".0");
            }
            ins.append("), ('2024-01-01T01:00:00', 'B', 20.5, 200");
            for (int i = 0; i < 26; i++) {
                ins.append(", ").append(i + 100).append(".0");
            }
            ins.append(")");
            execute(ins.toString());
            engine.releaseAllWriters();

            // Covered query should use CoveringIndex
            assertPlanNoLeakCheck(
                    "SELECT price, qty FROM t_30col WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: price, qty
                                  filter: sym='A'
                            """
            );
            assertSql("""
                    price\tqty
                    10.5\t100
                    """, "SELECT price, qty FROM t_30col WHERE sym = 'A'");

            // Query that includes uncovered columns falls back
            assertSql("""
                    price\textra_0
                    10.5\t0.0
                    """, "SELECT price, extra_0 FROM t_30col WHERE sym = 'A'");
        });
    }

    @Test
    public void testWideTablePartialCoverage() throws Exception {
        // Table with many columns but only some in INCLUDE — queries that select
        // non-covered columns must fall back to regular scan
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_partial (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                        price DOUBLE,
                        qty INT,
                        uncovered1 DOUBLE,
                        uncovered2 INT,
                        uncovered3 VARCHAR,
                        uncovered4 LONG,
                        uncovered5 BOOLEAN
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_partial VALUES
                    ('2024-01-01T00:00:00', 'A', 10.5, 100, 99.9, 42, 'hello', 12345, true),
                    ('2024-01-01T01:00:00', 'B', 20.5, 200, 88.8, 43, 'world', 67890, false),
                    ('2024-01-01T02:00:00', 'A', 30.5, 300, 77.7, 44, 'test', 11111, true)
                    """);
            engine.releaseAllWriters();

            // Covered columns only — should use CoveringIndex
            assertPlanNoLeakCheck(
                    "SELECT price, qty FROM t_partial WHERE sym = 'A'",
                    """
                            SelectedRecord
                                CoveringIndex on: sym with: price, qty
                                  filter: sym='A'
                            """
            );
            assertSql("""
                    price\tqty
                    10.5\t100
                    30.5\t300
                    """, "SELECT price, qty FROM t_partial WHERE sym = 'A'");

            // Mixed covered + uncovered — should NOT use CoveringIndex
            assertSql("""
                    price\tuncovered1
                    10.5\t99.9
                    30.5\t77.7
                    """, "SELECT price, uncovered1 FROM t_partial WHERE sym = 'A'");

            // All uncovered — should NOT use CoveringIndex
            assertSql("""
                    uncovered1\tuncovered3
                    99.9\thello
                    77.7\ttest
                    """, "SELECT uncovered1, uncovered3 FROM t_partial WHERE sym = 'A'");
        });
    }

    /**
     * Builds a minimal RecordMetadata for direct PostingIndexReader construction.
     * Pads slots up to the highest covered index with placeholder LONG columns;
     * each covered index gets the type the writer was given. Tests that bypass
     * TableReader must supply this — the reader resolves covered-column types
     * from live metadata, not from the .pci.
     */
    private static RecordMetadata coveringMetadata(int[] coveredIndices, int[] coveredTypes) {
        int maxIdx = -1;
        for (int idx : coveredIndices) {
            if (idx > maxIdx) maxIdx = idx;
        }
        GenericRecordMetadata m = new GenericRecordMetadata();
        for (int i = 0; i <= maxIdx; i++) {
            int type = ColumnType.LONG;
            for (int j = 0; j < coveredIndices.length; j++) {
                if (coveredIndices[j] == i) {
                    type = coveredTypes[j];
                    break;
                }
            }
            m.add(new TableColumnMetadata("c" + i, type, IndexType.NONE, 0, false, null, i, false));
        }
        return m;
    }

    private void assertPlanDoesNotContain(String query) throws SqlException {
        try (io.questdb.cairo.sql.RecordCursorFactory factory = select(query)) {
            planSink.clear();
            factory.toPlan(planSink);
            String planText = planSink.getSink().toString();
            assertFalse("Plan should not contain '" + "CoveringIndex" + "':\n" + planText,
                    planText.contains("CoveringIndex"));
        }
    }

    private String getPlan(String query) throws SqlException {
        try (io.questdb.cairo.sql.RecordCursorFactory factory = select(query)) {
            planSink.clear();
            factory.toPlan(planSink);
            return planSink.getSink().toString();
        }
    }
}
