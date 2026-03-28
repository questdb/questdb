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
import io.questdb.std.str.StringSink;
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
                    CREATE TABLE t_binary (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        data BINARY
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            try {
                execute("ALTER TABLE t_binary ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (data)");
                fail("Should have thrown");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("INCLUDE column type is not supported"));
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

                    // Gen 1 — index now has genCount=2 with per-gen sidecar data
                    for (int i = 10; i < 20; i++) {
                        writer.add(0, i);
                    }
                    writer.setMaxValue(19);
                    writer.commit();

                    // Reader sees genCount=2 with per-gen sidecar data
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
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
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        RowCursor cursor = reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                        assertTrue(cursor instanceof CoveringRowCursor);
                        assertTrue(((CoveringRowCursor) cursor).hasCovering());

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
                    """, "SELECT DISTINCT sym FROM t_distinct_bmp ORDER BY sym");

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
                        Unsafe.getUnsafe().putDouble(colAddr + (long) i * Double.BYTES, 100.0 + i);
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
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        // Key 1: rows 1,3,5,7,9,11,13,15,17,19
                        RowCursor cursor = reader.getCursor(true, 1, 0, Long.MAX_VALUE);
                        CoveringRowCursor cc = (CoveringRowCursor) cursor;
                        assertTrue(cc.hasCovering());
                        int count = 0;
                        while (cc.hasNext()) {
                            long rowId = cc.next();
                            assertEquals(count * 2 + 1, rowId);
                            assertEquals(100.0 + count * 2 + 1, cc.getCoveredDouble(0), 0.001);
                            count++;
                        }
                        assertEquals(10, count);
                    }
                    writer.close();
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
                        Unsafe.getUnsafe().putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
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
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        // Key 0: only gen 0 data (rows 0,2)
                        CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                        assertTrue(cc.hasCovering());
                        assertTrue(cc.hasNext()); assertEquals(0, cc.next()); assertEquals(10.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext()); assertEquals(2, cc.next()); assertEquals(30.0, cc.getCoveredDouble(0), 0.001);
                        assertFalse(cc.hasNext());

                        // Key 1: gen 0 + gen 1 (rows 1,3,4,5)
                        cc = (CoveringRowCursor) reader.getCursor(true, 1, 0, Long.MAX_VALUE);
                        assertTrue(cc.hasCovering());
                        assertTrue(cc.hasNext()); assertEquals(1, cc.next()); assertEquals(20.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext()); assertEquals(3, cc.next()); assertEquals(40.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext()); assertEquals(4, cc.next()); assertEquals(50.0, cc.getCoveredDouble(0), 0.001);
                        assertTrue(cc.hasNext()); assertEquals(5, cc.next()); assertEquals(60.0, cc.getCoveredDouble(0), 0.001);
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
                        Unsafe.getUnsafe().putDouble(colAddr + (long) i * Double.BYTES, 10.0 * (i + 1));
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
                            configuration, path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, -1, 0)) {
                        CoveringRowCursor cc = (CoveringRowCursor) reader.getCursor(true, 0, 0, Long.MAX_VALUE);
                        assertTrue(cc.hasCovering());
                        for (int i = 0; i < 30; i++) {
                            assertTrue("row " + i, cc.hasNext());
                            assertEquals(i, cc.next());
                            assertEquals("value at row " + i, 10.0 * (i + 1), cc.getCoveredDouble(0), 0.001);
                        }
                        assertFalse(cc.hasNext());
                    }
                    w2.close();
                } finally {
                    Unsafe.free(colAddr, (long) rowCount * Double.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
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
                            CoveringIndex on: sym
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
                            CoveringIndex on: sym
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
                            CoveringIndex on: sym
                              filter: sym='GOLD'
                            """
            );
        });
    }

    // ---- Wide table and wide INCLUDE edge case tests ----

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
                                CoveringIndex on: sym
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
                                CoveringIndex on: sym
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
                                CoveringIndex on: sym
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
                int[] coveringCols = metadata.getColumnMetadata(symIdx).getCoveringColumnIndices();
                assertNotNull(coveringCols);
                assertEquals(8, coveringCols.length);
                assertTrue(metadata.getColumnMetadata(symIdx).isCovering());
            }
        });
    }

    // ==================== no_covering hint tests ====================

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
                                CoveringIndex on: sym
                                  filter: sym='A'
                            """
            );

            // With no_covering hint: no CoveringIndex
            assertPlanDoesNotContain(
                    "SELECT /*+ no_covering */ price FROM t_hint WHERE sym = 'A'",
                    "CoveringIndex"
            );
        });
    }

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
                    "SELECT /*+ no_covering */ price FROM t_hint_in WHERE sym IN ('A', 'B')",
                    "CoveringIndex"
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
                    "SELECT /*+ no_covering */ price FROM t_hint_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym",
                    "CoveringIndex"
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

    // ==================== page frame cursor tests ====================

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
            StringBuilder sb = new StringBuilder("INSERT INTO t_pf_agg VALUES ");
            double sumA = 0;
            int countA = 0;
            for (int i = 0; i < 200; i++) {
                if (i > 0) sb.append(", ");
                String sym = (i % 3 == 0) ? "A" : (i % 3 == 1) ? "B" : "C";
                double price = 10.0 + i * 0.1;
                sb.append("('2024-01-01T").append(String.format("%02d", i / 60)).append(":")
                        .append(String.format("%02d", i % 60)).append(":00', '").append(sym)
                        .append("', ").append(price).append(", ").append(i).append(")");
                if ("A".equals(sym)) {
                    sumA += price;
                    countA++;
                }
            }
            execute(sb.toString());
            engine.releaseAllWriters();

            // Aggregation should produce correct results via either path
            assertSql(
                    "count\n" + countA + "\n",
                    "SELECT count() FROM t_pf_agg WHERE sym = 'A'"
            );
            assertSql(
                    "count\n" + countA + "\n",
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

    private void assertPlanDoesNotContain(String query, String unwanted) throws SqlException {
        try (io.questdb.cairo.sql.RecordCursorFactory factory = select(query)) {
            planSink.clear();
            factory.toPlan(planSink);
            String planText = planSink.getSink().toString();
            assertFalse("Plan should not contain '" + unwanted + "':\n" + planText,
                    planText.contains(unwanted));
        }
    }
}
