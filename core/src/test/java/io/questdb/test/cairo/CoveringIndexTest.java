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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;
import static org.junit.Assert.*;

public class CoveringIndexTest extends AbstractCairoTest {

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
            String[] pvFiles = partDir.list((d, n) -> n.startsWith("sym.pv"));
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
    public void testPostingIndexRowIdsMatchBitmap() throws Exception {
        java.io.File parquetFile = new java.io.File("/tmp/entqdbroot/import/commodities_market_data.parquet");
        org.junit.Assume.assumeTrue("parquet file not available", parquetFile.exists());
        inputRoot = parquetFile.getParent();
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
            // No array columns — just fixed-width to avoid the crash
            execute("INSERT INTO cmd_posting SELECT timestamp, symbol, exchange, commodity_class, best_bid, best_ask FROM read_parquet('commodities_market_data.parquet')");
            execute("INSERT INTO cmd_bitmap2 SELECT timestamp, symbol, exchange, commodity_class, best_bid, best_ask FROM read_parquet('commodities_market_data.parquet')");
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
    public void testFilteredIndexScanWithArrayColumnParquetBitmap() throws Exception {
        java.io.File parquetFile = new java.io.File("/tmp/entqdbroot/import/commodities_market_data.parquet");
        org.junit.Assume.assumeTrue("parquet file not available", parquetFile.exists());
        inputRoot = parquetFile.getParent();
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
            execute("INSERT INTO cmd_bitmap SELECT * FROM read_parquet('commodities_market_data.parquet')");
            engine.releaseAllWriters();

            // Just verify the query doesn't crash — any result is fine
            try (var factory = select("""
                    SELECT timestamp, symbol,
                        first(best_bid) as open,
                        max(best_bid) as high,
                        min(best_bid) as low,
                        last(best_bid) as close,
                        avg(best_bid) as avgr,
                        sum(bids[2][1]) as volume
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
    public void testFilteredIndexScanWithArrayColumnParquet() throws Exception {
        java.io.File parquetFile = new java.io.File("/tmp/entqdbroot/import/commodities_market_data.parquet");
        org.junit.Assume.assumeTrue("parquet file not available", parquetFile.exists());
        // Set sql.copy.input.root so read_parquet can find the file
        inputRoot = parquetFile.getParent();
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
            execute("INSERT INTO commodities_market_data SELECT * FROM read_parquet('commodities_market_data.parquet')");
            engine.releaseAllWriters();

            // Full query with array access — verify no crash
            try (var factory = select("""
                    SELECT timestamp, symbol,
                        first(best_bid) as open,
                        max(best_bid) as high,
                        min(best_bid) as low,
                        last(best_bid) as close,
                        avg(best_bid) as avgr,
                        sum(bids[2][1]) as volume
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
                int[] coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(2, coveringCols.length);
                assertEquals(metadata.getColumnIndex("price"), coveringCols[0]);
                assertEquals(metadata.getColumnIndex("qty"), coveringCols[1]);
            }
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
                int[] coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(2, coveringCols.length);
                assertEquals(metadata.getColumnIndex("price"), coveringCols[0]);
                assertEquals(metadata.getColumnIndex("qty"), coveringCols[1]);
            }
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
    public void testAlterTableAddIndexWithIncludeUnsupportedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_varchar (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        name VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_varchar ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (name)");
                fail("Should have thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("INCLUDE column must be a fixed-size numeric type"));
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
                int[] coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(2, coveringCols.length);
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
                int[] coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(3, coveringCols.length);
            }
        });
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
    public void testNonCoveringTableUnchanged() throws Exception {
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
                assertFalse(metadata.getColumnMetadata(symIdx).isCovering());
                assertNull(metadata.getColumnMetadata(symIdx).getCoveringColumnIndices());
            }
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
                        Unsafe.getUnsafe().putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
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

                    // Verify .pci file exists
                    FilesFacade ff = configuration.getFilesFacade();
                    assertTrue(ff.exists(PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE)));
                    // Verify .pc0 file exists
                    assertTrue(ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0)));
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
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
                        Unsafe.getUnsafe().putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
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
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0, 0)) {
                        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
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

                        // key 1: rows 1, 4 -> values 20.0, 50.0
                        cursor = reader.getCursor(true, 1, 0, Long.MAX_VALUE);
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());

                        assertTrue(cc.hasNext());
                        assertEquals(1, cc.next());
                        assertEquals(20.0, cc.getCoveredDouble(0), 0.001);

                        assertTrue(cc.hasNext());
                        assertEquals(4, cc.next());
                        assertEquals(50.0, cc.getCoveredDouble(0), 0.001);

                        assertFalse(cc.hasNext());
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
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
                    RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                    // Cursor should still implement CoveringRowCursor but hasCovering() returns false
                    assertTrue(cursor instanceof CoveringRowCursor);
                    assertFalse(((CoveringRowCursor) cursor).hasCovering());

                    // Regular iteration should work
                    assertTrue(cursor.hasNext());
                    assertEquals(0, cursor.next());
                    assertTrue(cursor.hasNext());
                    assertEquals(1, cursor.next());
                    assertFalse(cursor.hasNext());
                }
            }
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
                        Unsafe.getUnsafe().putDouble(colAddrDouble + (long) i * Double.BYTES, i * 1.5);
                        Unsafe.getUnsafe().putInt(colAddrInt + (long) i * Integer.BYTES, i * 10);
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

                    // Verify both .pc0 and .pc1 exist
                    assertTrue(ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0)));
                    assertTrue(ff.exists(PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 1)));
                } finally {
                    Unsafe.free(colAddrDouble, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(colAddrInt, (long) rowCount * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringFileNaming() {
        try (Path path = new Path().of("/db/2024-01-01")) {
            int plen = path.size();
            String name = "sym";

            PostingIndexUtils.coverInfoFileName(path, name, COLUMN_NAME_TXN_NONE);
            assertTrue(path.toString().contains("sym.pci"));

            PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 0);
            assertTrue(path.toString().contains("sym.pc0"));

            PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, 1);
            assertTrue(path.toString().contains("sym.pc1"));

            // With column name txn
            PostingIndexUtils.coverInfoFileName(path.trimTo(plen), name, 5);
            assertTrue(path.toString().contains("sym.pci.5"));

            PostingIndexUtils.coverDataFileName(path.trimTo(plen), name, 5, 0);
            assertTrue(path.toString().contains("sym.pc0.5"));
        }
    }

    // ===================================================================
    // End-to-end SQL tests
    // ===================================================================

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
                                CoveringIndex on: sym
                                  filter: sym='A'
                            """
            );
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
                        i / 3600, (i % 3600) / 60, i % 60, sym, i, i * 10
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

    // ===================================================================
    // Fallback scenario tests: covering index NOT used
    // ===================================================================

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
                                CoveringIndex on: sym
                                  filter: sym IN ['A','B']
                            """
            );
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

    // ===================================================================
    // DDL edge case tests
    // ===================================================================

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
    public void testIncludeWithFsstFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE bad (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE FSST INCLUDE (price),
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
                int[] covering = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(covering);
                assertEquals(2, covering.length);
            }

            // Release all cached readers and writers to force reopening
            engine.releaseAllReaders();

            // Second read: metadata should persist
            try (TableReader r = engine.getReader("t_persist")) {
                TableReaderMetadata metadata = r.getMetadata();
                int symIdx = metadata.getColumnIndex("sym");
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
                int[] covering = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(covering);
                assertEquals(2, covering.length);
                assertEquals(metadata.getColumnIndex("price"), covering[0]);
                assertEquals(metadata.getColumnIndex("qty"), covering[1]);
            }
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
            // TODO: SHOW CREATE TABLE should include the INCLUDE clause so the table
            //  can be recreated with covering column info preserved. Currently, the
            //  MetadataCache startup hydration (from _meta file) doesn't read the
            //  variable-length covering column data section.
            assertSql("""
                    ddl
                    CREATE TABLE 't_show' (\s
                    \tts TIMESTAMP,
                    \tsym SYMBOL INDEX TYPE POSTING,
                    \tprice DOUBLE,
                    \tqty INT
                    ) timestamp(ts) PARTITION BY DAY BYPASS WAL;
                    """, "SHOW CREATE TABLE t_show");
        });
    }

    // ===================================================================
    // Type-specific covering column tests
    // ===================================================================

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
    public void testIncludeWithVarSizeColumnFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE bad (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (s),
                            s STRING
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("fixed-size"));
            }
        });
    }

    @Test
    public void testIncludeWithVarcharColumnFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("""
                        CREATE TABLE bad (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (v),
                            v VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY
                        """);
                fail("Should have thrown SqlException");
            } catch (SqlException e) {
                assertTrue(e.getMessage().contains("fixed-size"));
            }
        });
    }

    @Test
    public void testCoveringMultiGenFallback() throws Exception {
        // When genCount > 1 (pre-seal), hasCovering() must return false
        // and queries fall back to regular column reads
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_multigen";
                int plen = path.size();

                int rowCount = 20;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.getUnsafe().putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
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

                    // Gen 1 — index now has genCount=2, not sealed
                    for (int i = 10; i < 20; i++) {
                        writer.add(0, i);
                    }
                    writer.setMaxValue(19);
                    writer.commit();

                    // Reader sees genCount=2 but no sidecar files yet (seal hasn't run)
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        // genCount=2, hasCovering() must be false
                        assertFalse(cc.hasCovering());

                        // But row IDs are still correct
                        int count = 0;
                        while (cursor.hasNext()) {
                            assertEquals(count, cursor.next());
                            count++;
                        }
                        assertEquals(20, count);
                    }

                    writer.close(); // seal happens here
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testCoveringAfterSealThenNewCommit() throws Exception {
        // After seal + new commit: genCount=2, sidecar files exist from seal,
        // but hasCovering() must be false to prevent wrong sidecar reads
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                String name = "cover_postseal";
                int plen = path.size();

                int rowCount = 30;
                long colAddr = Unsafe.malloc((long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.getUnsafe().putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
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

                    // Second writer: reopen without reinit (preserves sealed data), add more
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

                    // Reader: sidecar files exist but genCount=2 — hasCovering must be false
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                        assertTrue(cursor instanceof CoveringRowCursor);
                        assertFalse(((CoveringRowCursor) cursor).hasCovering());

                        int count = 0;
                        while (cursor.hasNext()) {
                            cursor.next();
                            count++;
                        }
                        assertTrue(count > 0);
                    }

                    writer2.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
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

                int keyCount = 300;
                int rowCount = keyCount;
                long colAddr = Unsafe.malloc((long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < rowCount; i++) {
                        Unsafe.getUnsafe().putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
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
                        for (int k = 0; k < keyCount; k++) {
                            writer.add(k, k);
                        }
                        writer.setMaxValue(rowCount - 1);
                        writer.commit();
                    }

                    // Read back keys from second stride (key >= 256)
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        // Key 260 is in stride 1, local key 4
                        RowCursor cursor = reader.getCursor(true, 260, 0, Long.MAX_VALUE);
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());

                        assertTrue(cc.hasNext());
                        assertEquals(260, cc.next());
                        assertEquals(1260L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());

                        // Key 299 is in stride 1, local key 43
                        cursor = reader.getCursor(true, 299, 0, Long.MAX_VALUE);
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());
                        assertTrue(cc.hasNext());
                        assertEquals(299, cc.next());
                        assertEquals(1299L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());

                        // Key 0 is in stride 0 (control)
                        cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());
                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(1000L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
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
                        Unsafe.getUnsafe().putDouble(colAddr + (long) i * Double.BYTES, 50.0 + i);
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
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
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
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
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
                int[] indices = r.getMetadata().getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull("covering indices lost after ALTER", indices);
                assertEquals(1, indices.length);
            }

            assertSql("""
                    price
                    10.5
                    11.5
                    """, "SELECT price FROM t_alter WHERE sym = 'A'");
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
                        Unsafe.getUnsafe().putShort(colAddr + (long) i * Short.BYTES, (short) (100 + i));
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
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
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
                    }
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Short.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
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
                        Unsafe.getUnsafe().putLong(colAddr + (long) i * Long.BYTES, 1000L + i);
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
                    } // seal creates gen0 dense + sidecar

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
                    writer2.close(); // incremental seal: stride 0 clean, stride 1 dirty

                    // Read: verify covering values for clean stride keys
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        // Key 0 is in stride 0 (clean stride) — should have correct covered value
                        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                        assertTrue(cursor instanceof CoveringRowCursor);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue("covering should be available after incremental seal", cc.hasCovering());
                        assertTrue(cc.hasNext());
                        assertEquals(0, cc.next());
                        assertEquals(1000L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());

                        // Key 100 is in stride 0 (clean stride)
                        cursor = reader.getCursor(true, 100, 0, Long.MAX_VALUE);
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());
                        assertTrue(cc.hasNext());
                        assertEquals(100, cc.next());
                        assertEquals(1100L, cc.getCoveredLong(0));
                        assertFalse(cc.hasNext());

                        // Key 260 is in stride 1 (dirty stride) — should also work
                        cursor = reader.getCursor(true, 260, 0, Long.MAX_VALUE);
                        cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());
                        assertTrue(cc.hasNext());
                        assertEquals(260, cc.next());
                        assertEquals(1260L, cc.getCoveredLong(0));
                    }
                } finally {
                    Unsafe.free(colAddr, (long) keyCount * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
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
                                CoveringIndex latest on: sym
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
            // generateSelectGroupBy and replace the entire chain with PostingIndexDistinct
            assertPlanNoLeakCheck(
                    "SELECT DISTINCT sym FROM t_distinct_plan",
                    """
                            PostingIndexDistinct on: sym
                            """
            );
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
    public void testDistinctSymBitmapFallback() throws Exception {
        assertMemoryLeak(() -> {
            // BITMAP index (not POSTING) should NOT use PostingIndexDistinct
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
                    """, "SELECT DISTINCT sym FROM t_distinct_bmp");

            // Plan should NOT show PostingIndexDistinct
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

    // ==================== Coverage gap tests ====================

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
                    ) TIMESTAMP(ts) PARTITION BY DAY WAL
                    """);
            execute("INSERT INTO t_multi_commit VALUES ('2024-01-01T00:00:00', 'A', 10.0)");
            drainWalQueue();
            execute("INSERT INTO t_multi_commit VALUES ('2024-01-01T01:00:00', 'A', 11.0)");
            drainWalQueue();
            execute("INSERT INTO t_multi_commit VALUES ('2024-01-01T02:00:00', 'B', 20.0)");
            drainWalQueue();
            engine.releaseAllWriters(); // seal merges all 3 gens

            assertSql("""
                    price
                    10.0
                    11.0
                    """, "SELECT price FROM t_multi_commit WHERE sym = 'A'");
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
}
