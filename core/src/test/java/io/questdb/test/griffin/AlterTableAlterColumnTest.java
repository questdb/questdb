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

package io.questdb.test.griffin;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.griffin.model.CreateTableColumnModel;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AlterTableAlterColumnTest extends AbstractCairoTest {

    @Test
    public void testAddIndexColumnWithCapacity_capacityCanBeReadByWriter() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();

                    execute("alter table x alter column ik add index capacity 1024");

                    try (TableWriter writer = getWriter("x")) {
                        int blockCapacity = writer.getMetadata().getIndexValueBlockCapacity("ik");
                        Assert.assertEquals(1024, blockCapacity);
                    }
                }
        );
    }

    @Test
    public void testAddIndexColumns() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();
                    try (TableReader reader = getReader("x")) {
                        try {
                            reader.getBitmapIndexReader(0, reader.getMetadata().getColumnIndex("ik"), BitmapIndexReader.DIR_FORWARD);
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }
                    }

                    execute("alter table x alter column ik add index");

                    try (TableReader reader = getReader("x")) {
                        Assert.assertNotNull(reader.getBitmapIndexReader(0, reader.getMetadata().getColumnIndex("ik"), BitmapIndexReader.DIR_FORWARD));
                    }
                }
        );
    }

    @Test
    public void testAlterExpectCapacityKeyword() throws Exception {
        assertFailure("alter table x alter column c add index a", 39, "'capacity' expected");
    }

    @Test
    public void testAlterExpectCapacityValue() throws Exception {
        assertFailure("alter table x alter column c add index capacity ", 48, "capacity value expected");
    }

    @Test
    public void testAlterExpectCapacityValueIsInteger() throws Exception {
        assertFailure("alter table x alter column c add index capacity qwe", 48, "positive integer literal expected as index capacity");
    }

    @Test
    public void testAlterExpectCapacityValueIsPositiveInteger() throws Exception {
        assertFailure("alter table x alter column c add index capacity -123", 48, "positive integer literal expected as index capacity");
    }

    @Test
    public void testAlterExpectColumnKeyword() throws Exception {
        assertFailure("alter table x alter", 19, "'column' expected");
    }

    @Test
    public void testAlterExpectColumnName() throws Exception {
        assertFailure("alter table x alter column", 26, "column name expected");
    }

    @Test
    public void testBusyTable() throws Exception {
        assertMemoryLeak(() -> {
            CountDownLatch allHaltLatch = new CountDownLatch(1);
            try {
                createX();
                AtomicInteger errorCounter = new AtomicInteger();

                // start a thread that would lock table we
                // about to alter
                CyclicBarrier startBarrier = new CyclicBarrier(2);
                CountDownLatch haltLatch = new CountDownLatch(1);
                new Thread(() -> {
                    try (TableWriter ignore = getWriter("x")) {
                        // make sure writer is locked before test begins
                        startBarrier.await();
                        // make sure we don't release writer until main test finishes
                        Assert.assertTrue(haltLatch.await(5, TimeUnit.SECONDS));
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errorCounter.incrementAndGet();
                    } finally {
                        engine.clear();
                        allHaltLatch.countDown();
                    }
                }).start();

                startBarrier.await();
                try {
                    execute("alter table x alter column ik add index", sqlExecutionContext);
                    Assert.fail();
                } finally {
                    haltLatch.countDown();
                }
            } catch (EntryUnavailableException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table busy");
            }
            Assert.assertTrue(allHaltLatch.await(2, TimeUnit.SECONDS));
        });
    }

    @Test
    public void testCreateTableParquetCompressionPersistence() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT," +
                            " b DOUBLE PARQUET(default, ZSTD(3))," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("b");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DEFAULT, TableUtils.getParquetConfigEncoding(config));
                Assert.assertEquals(ParquetCompression.COMPRESSION_ZSTD, TableUtils.getParquetConfigCompression(config) - 1);
                Assert.assertEquals(3, TableUtils.getParquetConfigCompressionLevel(config) - 1);
            }
        });
    }

    @Test
    public void testCreateTableParquetConfigSurvivesReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3))," +
                            " b DOUBLE," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
            }

            engine.releaseAllWriters();

            try (TableReader reader = getReader("y")) {
                int colIndex = reader.getMetadata().getColumnIndex("a");
                int config = reader.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, TableUtils.getParquetConfigEncoding(config));
                Assert.assertEquals(ParquetCompression.COMPRESSION_ZSTD, TableUtils.getParquetConfigCompression(config) - 1);
                Assert.assertEquals(3, TableUtils.getParquetConfigCompressionLevel(config) - 1);
            }
        });
    }

    @Test
    public void testCreateTableParquetEncodingAndCompressionPersistence() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3))," +
                            " b DOUBLE," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertEquals(4, TableUtils.getParquetConfigEncoding(config));
                Assert.assertEquals(5, TableUtils.getParquetConfigCompression(config));
                Assert.assertEquals(4, TableUtils.getParquetConfigCompressionLevel(config));
            }
        });
    }

    @Test
    public void testCreateTableParquetEncodingPersistence() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT PARQUET(DELTA_BINARY_PACKED)," +
                            " b DOUBLE," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertEquals(4, TableUtils.getParquetConfigEncoding(config));
            }
        });
    }

    @Test
    public void testCreateTableParquetBloomFilterOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a VARCHAR PARQUET(BLOOM_FILTER)," +
                            " b INT," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DEFAULT, TableUtils.getParquetConfigEncoding(config));

                int bIndex = writer.getMetadata().getColumnIndex("b");
                int bConfig = writer.getMetadata().getColumnMetadata(bIndex).getParquetEncodingConfig();
                Assert.assertFalse(TableUtils.isParquetConfigBloomFilter(bConfig));
            }
        });
    }

    @Test
    public void testCreateTableParquetBloomFilterWithEncoding() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT PARQUET(DELTA_BINARY_PACKED, BLOOM_FILTER)," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, TableUtils.getParquetConfigEncoding(config));
            }
        });
    }

    @Test
    public void testCreateTableParquetBloomFilterWithEncodingAndCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3), BLOOM_FILTER)," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, TableUtils.getParquetConfigEncoding(config));
                Assert.assertEquals(ParquetCompression.COMPRESSION_ZSTD, TableUtils.getParquetConfigCompression(config) - 1);
                Assert.assertEquals(3, TableUtils.getParquetConfigCompressionLevel(config) - 1);
            }
        });
    }

    @Test
    public void testCreateTableParquetBloomFilterSurvivesReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT PARQUET(DELTA_BINARY_PACKED, BLOOM_FILTER)," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
            }

            engine.releaseAllWriters();

            try (TableReader reader = getReader("y")) {
                int colIndex = reader.getMetadata().getColumnIndex("a");
                int config = reader.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, TableUtils.getParquetConfigEncoding(config));
            }
        });
    }

    @Test
    public void testDropInvalidKeyword() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d DROP nonsense",
                34,
                "'index' expected"
        );
    }

    @Test
    public void testDropParquetCompressionOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3))," +
                            " b DOUBLE," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            execute("ALTER TABLE y ALTER COLUMN a SET PARQUET(DELTA_BINARY_PACKED)");

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                // Setting encoding only should keep encoding but clear compression
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                int encoding = TableUtils.getParquetConfigEncoding(config);
                Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, encoding);
                Assert.assertEquals(0, TableUtils.getParquetConfigCompression(config));
                Assert.assertEquals(0, TableUtils.getParquetConfigCompressionLevel(config));
            }
        });
    }

    @Test
    public void testResetParquetEncodingShowCreateTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE y (a INT, b DOUBLE, t TIMESTAMP NOT NULL) TIMESTAMP(t) PARTITION BY DAY");

            execute("ALTER TABLE y ALTER COLUMN a SET PARQUET(DELTA_BINARY_PACKED, ZSTD(3))");

            assertSql("""
                            ddl
                            CREATE TABLE 'y' (\s
                            \ta INT PARQUET(delta_binary_packed, zstd(3)),
                            \tb DOUBLE,
                            \tt TIMESTAMP NOT NULL
                            ) timestamp(t) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE y");

            execute("ALTER TABLE y ALTER COLUMN a SET PARQUET(default)");

            assertSql("""
                            ddl
                            CREATE TABLE 'y' (\s
                            \ta INT,
                            \tb DOUBLE,
                            \tt TIMESTAMP NOT NULL
                            ) timestamp(t) PARTITION BY DAY BYPASS WAL;
                            """,
                    "SHOW CREATE TABLE y");
        });
    }

    @Test
    public void testResetParquetEncoding() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3))," +
                            " b DOUBLE," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertNotEquals(0, config);
            }

            execute("ALTER TABLE y ALTER COLUMN a SET PARQUET(default)");

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertEquals(0, config);
            }
        });
    }

    @Test
    public void testResetParquetEncodingKeepCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE y (" +
                            "a INT PARQUET(DELTA_BINARY_PACKED, ZSTD(3))," +
                            " b DOUBLE," +
                            " t TIMESTAMP NOT NULL" +
                            ") TIMESTAMP(t) PARTITION BY DAY"
            );

            execute("ALTER TABLE y ALTER COLUMN a SET PARQUET(default, ZSTD(3))");

            try (TableWriter writer = getWriter("y")) {
                int colIndex = writer.getMetadata().getColumnIndex("a");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                // Setting default encoding with compression should clear encoding but keep compression
                int encoding = TableUtils.getParquetConfigEncoding(config);
                Assert.assertEquals(0, encoding);
                int compression = TableUtils.getParquetConfigCompression(config);
                Assert.assertEquals(ParquetCompression.COMPRESSION_ZSTD, compression - 1);
                Assert.assertEquals(3, TableUtils.getParquetConfigCompressionLevel(config) - 1);
            }
        });
    }

    @Test
    public void testResetParquetEncodingThenConvert() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "val INT PARQUET(DELTA_BINARY_PACKED)," +
                    " ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");

            execute("INSERT INTO x SELECT" +
                    " CASE WHEN x % 2 = 0 THEN rnd_int() ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
                    " FROM long_sequence(1000)");

            // seal the partition by inserting into the next day
            execute("INSERT INTO x VALUES (42, '2015-01-02T00:00:00.000000Z')");

            execute("ALTER TABLE x ALTER COLUMN val SET PARQUET(default)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                assertSqlCursors(
                        "SELECT * FROM x WHERE ts IN '2015-01-01'",
                        "SELECT * FROM read_parquet('x.parquet')"
                );
            }
        });
    }

    @Test
    public void testExpectActionKeyword() throws Exception {
        assertFailure("alter table x", 13, SqlCompilerImpl.ALTER_TABLE_EXPECTED_TOKEN_DESCR);
    }

    @Test
    public void testExpectTableKeyword() throws Exception {
        assertFailure("alter x", 6, "'table' or 'materialized' or 'view' expected");
    }

    @Test
    public void testExpectTableKeyword2() throws Exception {
        assertFailure("alter", 5, "'table' or 'materialized' or 'view' expected");
    }

    @Test
    public void testExpectTableName() throws Exception {
        assertFailure("alter table", 11, "table name expected");
    }

    @Test
    public void testInvalidColumnName() throws Exception {
        assertFailure("alter table x alter column y add index", 27, "column 'y' does not exist in table 'x'");
    }

    @Test
    public void testInvalidTableName() throws Exception {
        assertFailure("alter table z alter column y add index", 12, "table does not exist [table=z]");
    }

    @Test
    public void testQuotedColumnNameAddDropIndex() throws Exception {
        assertMemoryLeak(
                () -> {
                    execute("CREATE TABLE test_quoted (\"MY_COL\" SYMBOL, ts TIMESTAMP NOT NULL) TIMESTAMP (ts)");

                    execute("ALTER TABLE test_quoted ALTER COLUMN \"MY_COL\" ADD INDEX");
                    try (TableReader reader = getReader("test_quoted")) {
                        int colIndex = reader.getMetadata().getColumnIndex("MY_COL");
                        Assert.assertTrue(reader.getMetadata().isColumnIndexed(colIndex));
                    }

                    // single-quoted column name should also work
                    execute("ALTER TABLE test_quoted ALTER COLUMN 'MY_COL' DROP INDEX");
                    try (TableReader reader = getReader("test_quoted")) {
                        int colIndex = reader.getMetadata().getColumnIndex("MY_COL");
                        Assert.assertFalse(reader.getMetadata().isColumnIndexed(colIndex));
                    }
                }
        );
    }

    @Test
    public void testQuotedColumnNameAlterType() throws Exception {
        assertMemoryLeak(
                () -> {
                    execute("CREATE TABLE test_quoted (\"MY_COL\" LONG, ts TIMESTAMP NOT NULL) TIMESTAMP (ts) PARTITION BY DAY");
                    execute("INSERT INTO test_quoted VALUES (123456789, '2021-01-01T00:00:00.000000Z')");

                    execute("ALTER TABLE test_quoted ALTER COLUMN \"MY_COL\" TYPE TIMESTAMP_NS");

                    assertQueryNoLeakCheck(
                            """
                                    MY_COL\tts
                                    1970-01-01T00:00:00.123456789Z\t2021-01-01T00:00:00.000000Z
                                    """,
                            "SELECT * FROM test_quoted",
                            "ts",
                            true,
                            true
                    );
                }
        );
    }

    @Test
    public void testQuotedColumnNameCacheNocache() throws Exception {
        assertMemoryLeak(
                () -> {
                    execute("CREATE TABLE test_quoted (\"MY_COL\" SYMBOL NOCACHE, ts TIMESTAMP NOT NULL) TIMESTAMP (ts)");

                    execute("ALTER TABLE test_quoted ALTER COLUMN \"MY_COL\" CACHE");
                    engine.releaseAllReaders();
                    try (TableReader reader = getReader("test_quoted")) {
                        int colIndex = reader.getMetadata().getColumnIndex("MY_COL");
                        Assert.assertTrue(reader.getSymbolMapReader(colIndex).isCached());
                    }

                    execute("ALTER TABLE test_quoted ALTER COLUMN \"MY_COL\" NOCACHE");
                    engine.releaseAllReaders();
                    try (TableReader reader = getReader("test_quoted")) {
                        int colIndex = reader.getMetadata().getColumnIndex("MY_COL");
                        Assert.assertFalse(reader.getSymbolMapReader(colIndex).isCached());
                    }
                }
        );
    }

    @Test
    public void testQuotedColumnNameNonExistent() throws Exception {
        assertMemoryLeak(
                () -> {
                    execute("CREATE TABLE test_quoted (col SYMBOL, ts TIMESTAMP NOT NULL) TIMESTAMP (ts)");
                    assertExceptionNoLeakCheck(
                            "ALTER TABLE test_quoted ALTER COLUMN \"nonexistent\" ADD INDEX",
                            37,
                            "column 'nonexistent' does not exist"
                    );
                }
        );
    }

    @Test
    public void testQuotedColumnNameSpecialCharacters() throws Exception {
        assertMemoryLeak(
                () -> {
                    // space in column name
                    execute("CREATE TABLE test_space (\"my col\" SYMBOL, ts TIMESTAMP NOT NULL) TIMESTAMP (ts)");
                    execute("ALTER TABLE test_space ALTER COLUMN \"my col\" ADD INDEX");
                    try (TableReader reader = getReader("test_space")) {
                        int colIndex = reader.getMetadata().getColumnIndex("my col");
                        Assert.assertTrue(reader.getMetadata().isColumnIndexed(colIndex));
                    }

                    // SQL keyword as column name
                    execute("CREATE TABLE test_keyword (\"select\" SYMBOL, ts TIMESTAMP NOT NULL) TIMESTAMP (ts)");
                    execute("ALTER TABLE test_keyword ALTER COLUMN \"select\" ADD INDEX");
                    try (TableReader reader = getReader("test_keyword")) {
                        int colIndex = reader.getMetadata().getColumnIndex("select");
                        Assert.assertTrue(reader.getMetadata().isColumnIndexed(colIndex));
                    }

                    // mixed case column name
                    execute("CREATE TABLE test_mixed (\"MyColumn\" SYMBOL, ts TIMESTAMP NOT NULL) TIMESTAMP (ts)");
                    execute("ALTER TABLE test_mixed ALTER COLUMN \"MyColumn\" ADD INDEX");
                    try (TableReader reader = getReader("test_mixed")) {
                        int colIndex = reader.getMetadata().getColumnIndex("MyColumn");
                        Assert.assertTrue(reader.getMetadata().isColumnIndexed(colIndex));
                    }
                }
        );
    }

    @Test
    public void testSetInvalidKeyword() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET nonsense",
                33,
                "'parquet' expected"
        );
    }

    @Test
    public void testSetParquetByteStreamSplitRejectedForFloat() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN e SET PARQUET(BYTE_STREAM_SPLIT)",
                41,
                "encoding 'BYTE_STREAM_SPLIT' is not valid for column type"
        );
    }

    @Test
    public void testSetParquetByteStreamSplitRejectedForInt() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN i SET PARQUET(BYTE_STREAM_SPLIT)",
                41,
                "encoding 'BYTE_STREAM_SPLIT' is not valid for column type"
        );
    }

    @Test
    public void testSetParquetCompression() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN d SET PARQUET(default, ZSTD(3))");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("d");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                // compression = ZSTD (4) + 1 = 5 in packed form
                Assert.assertEquals(5, TableUtils.getParquetConfigCompression(config));
                Assert.assertEquals(4, TableUtils.getParquetConfigCompressionLevel(config));
            }
        });
    }

    @Test
    public void testSetParquetCompressionLevelMissingCloseParen() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(default, ZSTD(3 extra",
                57,
                "')' expected"
        );
    }

    @Test
    public void testSetParquetCompressionLevelNotANumber() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(default, ZSTD(abc))",
                55,
                "compression level must be a number"
        );
    }

    @Test
    public void testSetParquetCompressionLevelTooHigh() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(default, ZSTD(30))",
                55,
                "ZSTD compression level must be between 1 and 22"
        );
    }

    @Test
    public void testSetParquetCompressionLevelTooLow() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(default, ZSTD(0))",
                55,
                "ZSTD compression level must be between 1 and 22"
        );
    }

    @Test
    public void testSetParquetCompressionLevelTruncated() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(PLAIN, ZSTD(",
                53,
                "compression level expected"
        );
    }

    @Test
    public void testSetParquetCompressionUncompressed() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN d SET PARQUET(default, UNCOMPRESSED)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("d");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                // compression = UNCOMPRESSED (0) + 1 = 1 in packed form
                Assert.assertEquals(1, TableUtils.getParquetConfigCompression(config));
            }
        });
    }

    @Test
    public void testSetParquetDeltaBinaryPackedForShort() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN f SET PARQUET(DELTA_BINARY_PACKED)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("f");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, TableUtils.getParquetConfigEncoding(config));
            }
        });
    }

    @Test
    public void testSetParquetBloomFilter() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN c SET PARQUET(BLOOM_FILTER)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("c");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
            }
        });
    }

    @Test
    public void testSetParquetBloomFilterWithEncoding() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN i SET PARQUET(DELTA_BINARY_PACKED, BLOOM_FILTER)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("i");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, TableUtils.getParquetConfigEncoding(config));
            }
        });
    }

    @Test
    public void testSetParquetBloomFilterWithEncodingAndCompression() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN i SET PARQUET(DELTA_BINARY_PACKED, ZSTD(3), BLOOM_FILTER)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("i");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, TableUtils.getParquetConfigEncoding(config));
                Assert.assertEquals(ParquetCompression.COMPRESSION_ZSTD, TableUtils.getParquetConfigCompression(config) - 1);
                Assert.assertEquals(3, TableUtils.getParquetConfigCompressionLevel(config) - 1);
            }
        });
    }

    @Test
    public void testSetParquetClearsBloomFilter() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN i SET PARQUET(DELTA_BINARY_PACKED, BLOOM_FILTER)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("i");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
            }

            // SET without BLOOM_FILTER clears the flag
            execute("ALTER TABLE x ALTER COLUMN i SET PARQUET(PLAIN)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("i");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertFalse(TableUtils.isParquetConfigBloomFilter(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_PLAIN, TableUtils.getParquetConfigEncoding(config));
            }
        });
    }

    @Test
    public void testSetParquetEmptyParens() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET()",
                41,
                "invalid parquet encoding ')'"
        );
    }

    @Test
    public void testSetParquetEncoding() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN i SET PARQUET(DELTA_BINARY_PACKED)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("i");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, TableUtils.getParquetConfigEncoding(config));
            }
        });
    }

    @Test
    public void testSetParquetEncodingAndCompression() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN i SET PARQUET(DELTA_BINARY_PACKED, ZSTD(3))");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("i");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertEquals(4, TableUtils.getParquetConfigEncoding(config));
                Assert.assertEquals(5, TableUtils.getParquetConfigCompression(config));
                Assert.assertEquals(4, TableUtils.getParquetConfigCompressionLevel(config));
            }
        });
    }

    @Test
    public void testSetParquetBloomFilterBeforeEncoding() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN i SET PARQUET(BLOOM_FILTER, PLAIN)",
                53,
                "')' expected"
        );
    }

    @Test
    public void testSetParquetBloomFilterDuplicate() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN i SET PARQUET(PLAIN, BLOOM_FILTER, BLOOM_FILTER)",
                60,
                "')' expected"
        );
    }

    @Test
    public void testCreateTableParquetBloomFilterBeforeEncoding() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute(
                        "CREATE TABLE y (" +
                                "a INT PARQUET(BLOOM_FILTER, PLAIN)," +
                                " t TIMESTAMP NOT NULL" +
                                ") TIMESTAMP(t) PARTITION BY DAY"
                );
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "')' expected");
            }
        });
    }

    @Test
    public void testCreateTableParquetBloomFilterBeforeCompression() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute(
                        "CREATE TABLE y (" +
                                "a INT PARQUET(BLOOM_FILTER, ZSTD)," +
                                " t TIMESTAMP NOT NULL" +
                                ") TIMESTAMP(t) PARTITION BY DAY"
                );
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "')' expected");
            }
        });
    }

    @Test
    public void testCreateTableParquetBloomFilterDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute(
                        "CREATE TABLE y (" +
                                "a INT PARQUET(PLAIN, BLOOM_FILTER, BLOOM_FILTER)," +
                                " t TIMESTAMP NOT NULL" +
                                ") TIMESTAMP(t) PARTITION BY DAY"
                );
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "')' expected");
            }
        });
    }

    @Test
    public void testParquetEncodingConfigRoundTrip() throws Exception {
        int packed = TableUtils.packParquetConfig(
                ParquetEncoding.ENCODING_DELTA_BINARY_PACKED,
                ParquetCompression.COMPRESSION_ZSTD + 1,
                4,
                true
        );

        CreateTableColumnModel model = CreateTableColumnModel.FACTORY.newInstance();
        model.setParquetEncodingConfig(packed);

        Assert.assertTrue(model.isParquetBloomFilter());
        Assert.assertEquals(ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, model.getParquetEncoding());
        Assert.assertEquals(ParquetCompression.COMPRESSION_ZSTD, model.getParquetCompression());
        Assert.assertEquals(3, model.getParquetCompressionLevel());

        int repacked = model.getParquetEncodingConfig();
        Assert.assertEquals(packed, repacked);
    }

    @Test
    public void testSetParquetBloomFilterBeforeCompression() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN i SET PARQUET(BLOOM_FILTER, ZSTD)",
                53,
                "')' expected"
        );
    }

    @Test
    public void testSetParquetBloomFilterBeforeCompressionAfterDefault() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN i SET PARQUET(default, BLOOM_FILTER, ZSTD)",
                62,
                "')' expected"
        );
    }

    @Test
    public void testSetParquetBloomFilterCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN c SET PARQUET(bloom_filter)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("c");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
            }
        });
    }

    @Test
    public void testSetParquetBloomFilterWithDefaultEncoding() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN i SET PARQUET(default, BLOOM_FILTER)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("i");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DEFAULT, TableUtils.getParquetConfigEncoding(config));
            }
        });
    }

    @Test
    public void testSetParquetBloomFilterWithDefaultEncodingAndCompression() throws Exception {
        assertMemoryLeak(() -> {
            createX();

            execute("ALTER TABLE x ALTER COLUMN i SET PARQUET(default, UNCOMPRESSED, BLOOM_FILTER)");

            try (TableWriter writer = getWriter("x")) {
                int colIndex = writer.getMetadata().getColumnIndex("i");
                int config = writer.getMetadata().getColumnMetadata(colIndex).getParquetEncodingConfig();
                Assert.assertTrue(TableUtils.isParquetConfigExplicit(config));
                Assert.assertTrue(TableUtils.isParquetConfigBloomFilter(config));
                Assert.assertEquals(ParquetEncoding.ENCODING_DEFAULT, TableUtils.getParquetConfigEncoding(config));
                Assert.assertEquals(ParquetCompression.COMPRESSION_UNCOMPRESSED, TableUtils.getParquetConfigCompression(config) - 1);
            }
        });
    }

    @Test
    public void testSetParquetBloomFilterJunkAfter() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN i SET PARQUET(PLAIN, BLOOM_FILTER, junk)",
                60,
                "')' expected"
        );
    }

    @Test
    public void testSetParquetEncodingInvalidForType() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(DELTA_LENGTH_BYTE_ARRAY)",
                41,
                "encoding 'DELTA_LENGTH_BYTE_ARRAY' is not valid for column type"
        );
    }

    @Test
    public void testSetParquetEncodingInvalidName() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(INVALID_ENCODING)",
                41,
                "invalid parquet encoding 'INVALID_ENCODING', supported values: plain, rle_dictionary"
        );
    }

    @Test
    public void testSetParquetEncodingCommaTruncated() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(PLAIN,",
                47,
                "compression codec name or BLOOM_FILTER expected"
        );
    }

    @Test
    public void testSetParquetEncodingTruncated() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(PLAIN",
                46,
                "',' or ')' expected"
        );
    }

    @Test
    public void testSetParquetEncodingThenConvert() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x2 (" +
                    "val LONG," +
                    " ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");

            execute("INSERT INTO x2 SELECT" +
                    " CASE WHEN x % 2 = 0 THEN rnd_long() ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
                    " FROM long_sequence(1000)");

            // seal the partition by inserting into the next day
            execute("INSERT INTO x2 VALUES (42, '2015-01-02T00:00:00.000000Z')");

            execute("ALTER TABLE x2 ALTER COLUMN val SET PARQUET(DELTA_BINARY_PACKED, ZSTD(3))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x2")
            ) {
                path.of(root).concat("x2.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                assertSqlCursors(
                        "SELECT * FROM x2 WHERE ts IN '2015-01-01'",
                        "SELECT * FROM read_parquet('x2.parquet')"
                );
            }
        });
    }

    @Test
    public void testSetParquetInvalidCompressionName() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(default, INVALID_CODEC)",
                50,
                "invalid parquet compression codec 'INVALID_CODEC', supported values: uncompressed, snappy, gzip, brotli, zstd, lz4_raw"
        );
    }

    @Test
    public void testSetParquetMissingEncodingOrCompression() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET",
                40,
                "'(' expected"
        );
    }

    @Test
    public void testSetParquetNotOpenParen() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET PLAIN",
                41,
                "'(' expected"
        );
    }

    @Test
    public void testSetParquetPlainRejectedForSymbol() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN sym SET PARQUET(PLAIN)",
                43,
                "encoding 'PLAIN' is not valid for column type"
        );
    }

    @Test
    public void testSetParquetTrailingJunk() throws Exception {
        assertFailure(
                "ALTER TABLE x ALTER COLUMN d SET PARQUET(PLAIN junk)",
                47,
                "')' expected"
        );
    }

    private void assertFailure(String sql, int position, String message) throws Exception {
        assertMemoryLeak(() -> {
            try {
                createX();
                select(sql).close();
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), message);
                Assert.assertEquals(position, e.getPosition());
            }
        });
    }

    private void createX() throws SqlException {
        execute(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol('msft','ibm', 'googl') sym," +
                        " round(rnd_double(0)*100, 3) amt," +
                        " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                        " rnd_boolean() b," +
                        " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) ik," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1_000_000_000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(10)" +
                        ") timestamp (timestamp)"
        );
    }
}
