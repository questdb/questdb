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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ParquetRowGroupPruningTest extends AbstractCairoTest {
    @Before
    public void setUp() {
        ParquetRowGroupFilter.resetRowGroupsSkipped();
        super.setUp();
    }

    @Test
    public void testRowGroupPruningSurvivesEmptyMetadataCacheWindow() throws Exception {
        // Regression for the parquet pruning probe
        // (AbstractPartitionFrameCursorFactory#hasParquetFormatPartitions). The table
        // token is resolved from the synchronously loaded registry, but the metadata
        // cache hydrates lazily (async at startup, or after a clearCache()). The probe
        // must hydrate the table on demand before reading the cache; otherwise it sees
        // the table as missing, returns false, and row-group pruning is silently skipped
        // during the hydration window. Pruning is an optimization, not a correctness
        // feature, so the regression does NOT change query results - it can only be
        // caught by asserting on the pruning signal (rowGroupsSkipped).
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(5000)
                    """);
            // Second partition makes 2024-01-01 a non-active partition so it converts.
            execute("""
                    INSERT INTO x VALUES
                    (8000, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            // Reproduce the registered-but-not-yet-cached window: evict the table from
            // the metadata cache so the query below is the first reader to need it.
            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.clearCache();
            }
            // Precondition: the table really is absent from the cache. getTable() reads
            // the map without hydrating, so it stays null until the pruning probe
            // hydrates on demand.
            final TableToken token = engine.getTableTokenIfExists("x");
            try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                Assert.assertNull(metadataRO.getTable(token));
            }

            // The filtered query must still skip row groups: hasParquetFormatPartitions()
            // hydrates the table on demand, so pruning is applied even though the cache
            // started empty. Without the on-demand hydrate this assertion fails (0 skipped).
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -991 ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
        });
    }

    @Test
    public void testBloomFilterBackwardScan() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 100_000)
                    FROM long_sequence(5000)
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (8000, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = -991 ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 42 ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("""
                            val
                            42
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IN (-1, -2, -3) ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IN (1, 2001) ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("""
                            val
                            2001
                            1
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", -991);
            assertQuery("SELECT val FROM x WHERE val = :v ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", 42);
            assertQuery("SELECT val FROM x WHERE val = :v ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("val\n42\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
        });
    }

    @Test
    public void testBloomFilterByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (10, '2024-01-01T01:00:00.000000Z'),
                    (100, '2024-01-01T02:00:00.000000Z'),
                    (101, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");
            assertQuery("SELECT val FROM x WHERE val = 50::byte")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 10::byte")
                    .noLeakCheck()
                    .returns("""
                            val
                            10
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 50::SHORT")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 10::SHORT")
                    .noLeakCheck()
                    .returns("val\n10\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 50")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 10")
                    .noLeakCheck()
                    .returns("val\n10\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 50::LONG")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 10::LONG")
                    .noLeakCheck()
                    .returns("val\n10\n");
        });
    }

    @Test
    public void testBloomFilterChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('A', '2024-01-01T00:00:00.000000Z'),
                    ('M', '2024-01-01T01:00:00.000000Z'),
                    ('Z', '2024-01-01T02:00:00.000000Z'),
                    ('X', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 'G'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 'M'")
                    .noLeakCheck()
                    .returns("""
                            val
                            M
                            """);
        });
    }

    @Test
    public void testBloomFilterDate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01'::DATE, '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-15'::DATE, '2024-01-01T01:00:00.000000Z'),
                    ('2020-10-31'::DATE, '2024-01-01T02:00:00.000000Z'),
                    ('2020-12-01'::DATE, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '2020-03-15'::DATE")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '2020-06-15'::DATE")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-15T00:00:00.000Z
                            """);
            assertQuery("SELECT val FROM x WHERE val = '2020-06-15T00:00:00.000000001Z'")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-15T00:00:00.000Z
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(30,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999999999.98', '2024-01-01T02:00:00.000000Z'),
                    ('9999999999999.99', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '2500000000000.25'::DECIMAL(30,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '5000000000000.50'::DECIMAL(30,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000000000.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal16() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(4,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('10.10', '2024-01-01T00:00:00.000000Z'),
                    ('50.50', '2024-01-01T01:00:00.000000Z'),
                    ('99.99', '2024-01-01T02:00:00.000000Z'),
                    ('10.11', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '30.30'::DECIMAL(4,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '50.50'::DECIMAL(4,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            50.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(50,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('100000000000000000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('500000000000000000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('999999999999999999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('999999999999999999999.98', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '250000000000000000000.25'::DECIMAL(50,2)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '500000000000000000000.50'::DECIMAL(50,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            500000000000000000000.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal32() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(8,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                     INSERT INTO x VALUES
                     ('1000.10', '2024-01-01T00:00:00.000000Z'),
                     ('50000.50', '2024-01-01T01:00:00.000000Z'),
                     ('99999.99', '2024-01-01T02:00:00.000000Z'),
                     ('99998.99', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '25000.25'::DECIMAL(8,2)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '50000.50'::DECIMAL(8,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            50000.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '2500000.25'::DECIMAL(15,2)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '5000000.50'::DECIMAL(15,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
        });
    }

    @Test
    public void testBloomFilterDecimal8() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(2,1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1', '2024-01-01T00:00:00.000000Z'),
                    ('5.5', '2024-01-01T01:00:00.000000Z'),
                    ('9.9', '2024-01-01T02:00:00.000000Z'),
                    ('7.9', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '3.3'::DECIMAL(2,1)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '5.5'::DECIMAL(2,1)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5.5
                            """);
        });
    }

    @Test
    public void testBloomFilterDouble() throws Exception {
        // A DOUBLE equality no longer prunes on the bloom filter at this magnitude: the bloom filter
        // hashes the exact bits of the bound, while the row-level filter keeps every row within
        // DOUBLE_TOLERANCE of it, and a group holding 3.3300000000499 would report 3.33 as absent.
        // The bound pushes as the BETWEEN spanning its tolerance band instead, which prunes on the
        // min/max stats only (both values here fall inside them). testDoubleColumnExactEqStillPrunes-
        // OnBloomFilter covers the magnitude at which the band holds the bound alone and the bloom
        // filter comes back.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.11, '2024-01-01T00:00:00.000000Z'),
                    (5.55, '2024-01-01T01:00:00.000000Z'),
                    (9.99, '2024-01-01T02:00:00.000000Z'),
                    (9.79, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 3.33")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            assertQuery("SELECT val FROM x WHERE val = 5.55")
                    .noLeakCheck()
                    .returns("""
                            val
                            5.55
                            """);

            // A bound clear of the group's min/max still prunes it, through the band.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 99.99")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterDroppedColumnO3Rewrite() throws Exception {
        // Regression: after DROP COLUMN, an O3 insert rewrites the parquet
        // partition and raw-copies the row groups the O3 data does not touch.
        // The updater extracts bloom bitsets in source-file order but writes
        // the survivors in target order; without the remap, dropping the
        // leading bloomed column shifted every bloom one slot (a's bloom onto
        // b, b's onto c), so an equality filter probed the wrong column's
        // bloom and silently pruned row groups holding matching rows.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b INT, c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Disjoint, gapped value ranges per column: a misattributed bloom
            // reports every value of its neighbor as absent. 12 rows make 3
            // row groups of 4.
            execute("""
                    INSERT INTO x
                    SELECT (1000 + 10 * x)::INT, (2000 + 10 * x)::INT, (3000 + 10 * x)::INT,
                           timestamp_sequence('2024-01-01', 3_600_000_000)
                    FROM long_sequence(12)
                    """);
            // Second partition makes 2024-01-01 a non-active partition so it converts.
            execute("INSERT INTO x VALUES (1, 9999, 9999, '2024-01-02T00:00:00.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01' WITH (bloom_filter_columns = 'a,b,c')");
            drainWalQueue();

            // Drop the leading bloomed column: survivors b, c, ts shift down one slot.
            execute("ALTER TABLE x DROP COLUMN a");
            drainWalQueue();
            // The O3 row lands in the first row group only; the schema change
            // forces a rewrite that copies row groups 1 and 2 with their blooms.
            execute("INSERT INTO x(b, c, ts) VALUES (5000, 6000, '2024-01-01T00:30:00.000000Z')");
            drainWalQueue();

            // Equality on values that live only in the copied row groups must
            // return their rows instead of pruning on a misattributed bloom.
            assertQuery("SELECT b, c FROM x WHERE b = 2100")
                    .noLeakCheck()
                    .returns("""
                            b\tc
                            2100\t3100
                            """);
            assertQuery("SELECT b, c FROM x WHERE c = 3060")
                    .noLeakCheck()
                    .returns("""
                            b\tc
                            2060\t3060
                            """);
            // The merged row group re-encodes its bloom and must find the O3 row.
            assertQuery("SELECT b, c FROM x WHERE b = 5000")
                    .noLeakCheck()
                    .returns("""
                            b\tc
                            5000\t6000
                            """);

            // A value inside a copied row group's min/max range but absent from
            // its data must still prune, proving the copied blooms stay live.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 2095")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z'),
                    (5.0, '2024-01-01T01:00:00.000000Z'),
                    (10.0, '2024-01-01T02:00:00.000000Z'),
                    (12.0, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 3.0")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 5.0::FLOAT")
                    .noLeakCheck()
                    .returns("""
                            val
                            5.0
                            """);
        });
    }

    @Test
    public void testBloomFilterIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1.1.1', '2024-01-01T00:00:00.000000Z'),
                    ('10.0.0.1', '2024-01-01T01:00:00.000000Z'),
                    (NULL, '2024-01-01T02:00:00.000000Z'),
                    ('192.168.1.1', '2024-01-01T03:00:00.000000Z'),
                    ('127.0.0.1', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '5.5.5.5'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '10.0.0.1'")
                    .noLeakCheck()
                    .returns("""
                            val
                            10.0.0.1
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val in (NULL)")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testBloomFilterInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 25_000")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 50_000")
                    .noLeakCheck()
                    .returns("""
                            val
                            50000
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000::LONG")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 50_000::LONG")
                    .noLeakCheck()
                    .returns("val\n50000\n");
        });
    }

    @Test
    public void testBloomFilterIntBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            bindVariableService.clear();
            bindVariableService.setInt("v", 25_000);
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", 50_000);
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n50000\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt(0, 25_000);
            assertQuery("SELECT val FROM x WHERE val = $1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterIntInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val IN (2, 3, 4)")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val IN (1, 25_000)")
                    .noLeakCheck()
                    .returns("""
                            val
                            1
                            """);
        });
    }

    @Test
    public void testBloomFilterLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (500_000, '2024-01-01T01:00:00.000000Z'),
                    (1_000_000, '2024-01-01T02:00:00.000000Z'),
                    (1_000_001, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 250_000")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 500_000")
                    .noLeakCheck()
                    .returns("""
                            val
                            500000
                            """);
        });
    }

    @Test
    public void testBloomFilterLong128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG128, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (to_long128(0, 1), '2024-01-01T00:00:00.000000Z'),
                    (to_long128(0, 50), '2024-01-01T01:00:00.000000Z'),
                    (to_long128(0, 100), '2024-01-01T02:00:00.000000Z'),
                    (to_long128(0, 101), '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = to_long128(0, 25)")
                    .noLeakCheck()
                    .returns("val\n");


            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = to_long128(0, 50)")
                    .noLeakCheck()
                    .returns("""
                            val
                            00000000-0000-0032-0000-000000000000
                            """);
        });
    }

    @Test
    public void testBloomFilterMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, 'aaa', '2024-01-01T00:00:00.000000Z'),
                    (50, 'mmm', '2024-01-01T01:00:00.000000Z'),
                    (100, 'zzz', '2024-01-01T02:00:00.000000Z'),
                    (101, 'xxx', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'a,b')");

            assertQuery("SELECT a, b FROM x WHERE a = 25 AND b = 'ggg'")
                    .noLeakCheck()
                    .returns("a\tb\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT a, b FROM x WHERE a = 1 AND b = 'ggg'")
                    .noLeakCheck()
                    .returns("a\tb\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT a, b FROM x WHERE a = 50 AND b = 'mmm'")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            50\tmmm
                            """);
        });
    }

    @Test
    public void testBloomFilterShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100, '2024-01-01T00:00:00.000000Z'),
                    (200, '2024-01-01T01:00:00.000000Z'),
                    (1000, '2024-01-01T02:00:00.000000Z'),
                    (1010, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 501")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 200")
                    .noLeakCheck()
                    .returns("""
                            val
                            200
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 501")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 200")
                    .noLeakCheck()
                    .returns("val\n200\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 501::LONG")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 200::LONG")
                    .noLeakCheck()
                    .returns("val\n200\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 501::SHORT")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 200::SHORT")
                    .noLeakCheck()
                    .returns("val\n200\n");
        });
    }

    @Test
    public void testBloomFilterString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('aaa', '2024-01-01T00:00:00.000000Z'),
                    ('mmm', '2024-01-01T01:00:00.000000Z'),
                    ('zzz', '2024-01-01T02:00:00.000000Z'),
                    ('xxx', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 'ggg'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 'mmm'")
                    .noLeakCheck()
                    .returns("""
                            val
                            mmm
                            """);
        });
    }

    @Test
    public void testBloomFilterSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('alpha', '2024-01-01T00:00:00.000000Z'),
                    ('gamma', '2024-01-01T01:00:00.000000Z'),
                    ('zeta', '2024-01-01T02:00:00.000000Z'),
                    ('zeta1', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 'delta'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = 'gamma'")
                    .noLeakCheck()
                    .returns("""
                            val
                            gamma
                            """);
        });
    }

    @Test
    public void testBloomFilterSymbolRenamedColumn() throws Exception {
        // Regression: the row-group bloom-filter pushdown resolved the filtered column
        // by its parquet name. Parquet column names are frozen at conversion time, so a
        // rename leaves them stale. When another column already bears the query's current
        // name, the pushdown checked the WRONG column's bloom filter and wrongly skipped
        // row groups, silently dropping valid rows. The fix resolves by stable column id.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a SYMBOL, b SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // 'gamma' lives only in column a; column b never holds it.
            execute("""
                    INSERT INTO x VALUES
                    ('gamma', 'p', '2024-01-01T00:00:00.000000Z'),
                    ('delta', 'q', '2024-01-01T01:00:00.000000Z'),
                    ('gamma', 'r', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'a,b')");

            // Swap names: free 'b', then rename a -> b. Now the live column 'b' is the
            // original column 'a' (holds 'gamma'), while the parquet still carries a
            // frozen column literally named 'b' (the original b, which never held 'gamma').
            execute("ALTER TABLE x RENAME COLUMN b TO c");
            execute("ALTER TABLE x RENAME COLUMN a TO b");

            // Equality on the renamed column must find its rows, not be pruned away.
            assertQuery("SELECT b FROM x WHERE b = 'gamma' ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            gamma
                            gamma
                            """);

            // A value genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 'nope'")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-15T12:00:00.000000Z', '2024-01-01T01:00:00.000000Z'),
                    ('2020-12-31T23:59:59.999999Z', '2024-01-01T02:00:00.000000Z'),
                    ('2020-12-30T23:59:59.999999Z', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '2020-03-15T00:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '2020-06-15T12:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-15T12:00:00.000000Z
                            """);

            assertQuery("SELECT val FROM x WHERE val = '2020-06-15T12:00:00.000000Z'::TIMESTAMP_NS")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-15T12:00:00.000000Z
                            """);

            assertQuery("SELECT val FROM x WHERE val = '2020-01-01'::Date")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testBloomFilterUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:00.000000Z'),
                    ('55555555-5555-5555-5555-555555555555', '2024-01-01T01:00:00.000000Z'),
                    ('99999999-9999-9999-9999-999999999999', '2024-01-01T02:00:00.000000Z'),
                    ('99999999-9999-9999-9999-999999999998', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = '33333333-3333-3333-3333-333333333334'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '55555555-5555-5555-5555-555555555555'")
                    .noLeakCheck()
                    .returns("""
                            val
                            55555555-5555-5555-5555-555555555555
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '33333333-3333-3333-3333-333333333334'::UUID")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT val FROM x WHERE val = '55555555-5555-5555-5555-555555555555'::UUID")
                    .noLeakCheck()
                    .returns("""
                            val
                            55555555-5555-5555-5555-555555555555
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = NULL::UUID")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IN ('33333333-3333-3333-3333-333333333334', '44444444-4444-4444-4444-444444444444')")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT val FROM x WHERE val IN ('11111111-1111-1111-1111-111111111111', '99999999-9999-9999-9999-999999999999')")
                    .noLeakCheck()
                    .returns("""
                            val
                            11111111-1111-1111-1111-111111111111
                            99999999-9999-9999-9999-999999999999
                            """);
        });
    }

    @Test
    public void testBloomFilterVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('abc', '2024-01-01T00:00:00.000000Z'),
                    ('❤️', '2024-01-01T01:00:00.000000Z'),
                    ('xyz', '2024-01-01T02:00:00.000000Z'),
                    ('xxx', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 'ghi'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT val FROM x WHERE val = '❤️'")
                    .noLeakCheck()
                    .returns("""
                            val
                            ❤️
                            """);
        });
    }

    @Test
    public void testBloomFilterVarcharBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('abc', '2024-01-01T00:00:00.000000Z'),
                    ('❤️', '2024-01-01T01:00:00.000000Z'),
                    ('xyz', '2024-01-01T02:00:00.000000Z'),
                    ('xxx', '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            bindVariableService.clear();
            bindVariableService.setStr("v", "ghi");
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setStr("v", "❤️");
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n❤️\n");
        });
    }

    @Test
    public void testBloomFilterWithColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val INT");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 10),
                    (4, '2024-01-01T03:00:00.000000Z', 100),
                    (5, '2024-01-01T04:00:00.000000Z', 1000),
                    (6, '2024-01-02T01:00:00.000000Z', 10_000)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT id, val FROM x WHERE val = 50")
                    .noLeakCheck()
                    .returns("id\tval\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT id, val FROM x WHERE val = 100")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            4\t100
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testColumnTopDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val DOUBLE");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 1.11),
                    (4, '2024-01-01T03:00:00.000000Z', 2.22),
                    (5, '2024-01-02T01:00:00.000000Z', 3.33)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT id, val FROM x WHERE val = 1.0")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT id, val FROM x WHERE val = 1.11")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            3\t1.11
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testColumnTopInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val INT");
            execute("""
                    INSERT INTO x VALUES
                    (4, '2024-01-01T03:00:00.000000Z', 100),
                    (5, '2024-01-01T04:00:00.000000Z', 200),
                    (6, '2024-01-02T01:00:00.000000Z', 300)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT id, val FROM x WHERE val = 100")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            4\t100
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\tnull
                            2\tnull
                            3\tnull
                            """);

            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
            assertQuery("SELECT id, val FROM x WHERE val = 999")
                    .noLeakCheck()
                    .returns("id\tval\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

        });
    }

    @Test
    public void testColumnTopLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val LONG");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 100_000),
                    (4, '2024-01-01T03:00:00.000000Z', 200_000),
                    (5, '2024-01-02T01:00:00.000000Z', 300_000)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT id, val FROM x WHERE val = 100_000")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            3\t100000
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\tnull
                            2\tnull
                            """);
        });
    }

    @Test
    public void testColumnTopString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val STRING");
            execute("""
                    INSERT INTO x VALUES
                    (4, '2024-01-01T03:00:00.000000Z', 'hello'),
                    (5, '2024-01-01T04:00:00.000000Z', 'world'),
                    (6, '2024-01-02T01:00:00.000000Z', 'world1')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertQuery("SELECT id, val FROM x WHERE val = 'aaa'")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQuery("SELECT id, val FROM x WHERE val = 'hello'")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            4\thello
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t
                            2\t
                            3\t
                            """);
        });
    }

    @Test
    public void testColumnTopVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x ADD COLUMN val VARCHAR");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2024-01-01T02:00:00.000000Z', 'abc'),
                    (4, '2024-01-01T03:00:00.000000Z', 'def'),
                    (5, '2024-01-02T02:00:00.000000Z', 'def2')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertQuery("SELECT id, val FROM x WHERE val = 'aaa'")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT id, val FROM x WHERE val = 'abc'")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            3\tabc
                            """);
            assertQuery("SELECT id, val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            id\tval
                            1\t
                            2\t
                            """);
        });
    }

    @Test
    public void testCombinedFilters() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 100 AND val < 120")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n19\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 10_000 AND val IS NOT NULL")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagAfterConvertBackToNative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);
            assertHasParquetPartitions("x", false);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts >= 0");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagAfterDetachPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x DETACH PARTITION WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagAfterDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x DROP PARTITION WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagAfterTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", true);

            execute("TRUNCATE TABLE x");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testHasParquetPartitionsFlagMixedPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-01'");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts = '2024-01-02'");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts = '2024-01-01'");
            assertHasParquetPartitions("x", true);

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE WHERE ts = '2024-01-02'");
            assertHasParquetPartitions("x", false);
        });
    }

    @Test
    public void testInListWithNullDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.11, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3.33, '2024-01-01T02:00:00.000000Z'),
                    (4.44, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 3.34)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testInListWithNullInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (5, '2024-01-01T03:00:00.000000Z'),
                    (6, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 3)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            3
                            """);
            assertQuery("SELECT val FROM x WHERE val IN (null)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
            assertQuery("SELECT val FROM x WHERE val IN (null, 99)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testInListWithNullLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100_000, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (300_000, '2024-01-01T02:00:00.000000Z'),
                    (400_000, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 300_000)")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            300000
                            """);
        });
    }

    @Test
    public void testInListWithNullString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('aaa', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('ccc', '2024-01-01T02:00:00.000000Z'),
                    ('ddd', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 'cccd')")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
        });
    }

    @Test
    public void testInListWithNullVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('hello', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('world', '2024-01-01T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (null, 'world')")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            world
                            """);
        });
    }

    @Test
    public void testIntervalScanStringMultiBlockPage() throws Exception {
        // Force a large row-group size so all 300 rows land in one row group (one
        // data page) regardless of execution order. The property is a static
        // override that persists across test methods (reset only in @AfterClass),
        // and many sibling tests lower it to 100; a value below 128 would split the
        // rows into single-block row groups whose length stream never spans multiple
        // blocks, silently bypassing the partial multi-block read path this guards.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
        // A STRING column read over a partial row-group range. 300 rows land in a
        // single row group whose one data page holds a DELTA_LENGTH_BYTE_ARRAY
        // length stream that spans several 128-value blocks. An interval ending
        // inside the row group makes the column read stop before the later blocks
        // (rowGroupHi < the page's value count). The data offset must still skip
        // the whole length stream; it used to under-count the unread blocks and
        // return shifted values (e.g. "v1" decoded as a string with leading NULs).
        // Existing STRING tests miss this: they filter on the value column (a
        // different, immune decode path) and use only a handful of rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT 'v' || x, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(300)
                    """);
            // A row in the next partition so 2024-01-01 is not the active partition
            // and CONVERT actually rewrites it to parquet.
            execute("INSERT INTO x VALUES ('tail', '2024-01-02T00:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE ts < '2024-01-01T00:05:00.000000Z'")
                    .noLeakCheck()
                    .returns("""
                            val
                            v1
                            v2
                            v3
                            v4
                            v5
                            """);
        });
    }

    @Test
    public void testIntervalScanStringMultiBlockPageBackward() throws Exception {
        // Force a large row-group size for the same reason as
        // testIntervalScanStringMultiBlockPage: keep all 300 rows in one multi-block
        // page so a sibling test's lowered override cannot mask the partial-read path.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
        // As testIntervalScanStringMultiBlockPage but with descending timestamp
        // order, which drives the backward page-frame cursor. It computes the same
        // partial (rowGroupHi < value count) frame and reads the STRING column over
        // it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT 'v' || x, timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(300)
                    """);
            // A row in the next partition so 2024-01-01 is not the active partition
            // and CONVERT actually rewrites it to parquet.
            execute("INSERT INTO x VALUES ('tail', '2024-01-02T00:00:00.000000Z')");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE ts < '2024-01-01T00:05:00.000000Z' ORDER BY ts DESC")
                    .noLeakCheck()
                    .returns("""
                            val
                            v5
                            v4
                            v3
                            v2
                            v1
                            """);
        });
    }

    @Test
    public void testIsNotNullAllNullsRowGroup() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT NULL, timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(100)
                    """);
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01T16:40:00', 600_000_000)
                    FROM long_sequence(50)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val IS NOT NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testIsNotNullFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (42, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IS NOT NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            42
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testIsNullFilter() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IS NULL")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testIsNullNoNullsRowGroup() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(100)
                    """);
            execute("""
                    INSERT INTO x
                    SELECT NULL, timestamp_sequence('2024-01-01T16:40:00', 600_000_000)
                    FROM long_sequence(50)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (50, '2024-01-01T03:00:00.000000Z'),
                    (100, '2024-01-01T04:00:00.000000Z'),
                    (110, '2024-01-02T03:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 2")
                    .noLeakCheck()
                    .returns("""
                            val
                            2
                            """);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningByteNegative() throws Exception {
        // BYTE is INT32-backed in parquet. Inline stats round-trip through a
        // u64 slot at INT32 physical width, so a negative min value must read
        // back as the correct i32 in the skip path. Without that, predicates
        // like val = 0 against a row group whose true min is negative drop
        // every row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (-100, '2024-01-01T00:00:00.000000Z'),
                    (-50, '2024-01-01T01:00:00.000000Z'),
                    (-1, '2024-01-01T02:00:00.000000Z'),
                    (0, '2024-01-01T03:00:00.000000Z'),
                    (10, '2024-01-01T04:00:00.000000Z'),
                    (50, '2024-01-02T03:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // val = 0 hits the row group whose true min is -100; before the
            // round-trip fix the inline min read back as 156, dropping the
            // row group. Must not skip.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 0")
                    .noLeakCheck()
                    .returns("""
                            val
                            0
                            """);

            // val = -42 falls inside [-100, 10] but not in the data: must not
            // be skipped, must return empty.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -42")
                    .noLeakCheck()
                    .returns("val\n");

            // val = -127 is outside both row groups; should skip.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -127")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('A', '2024-01-01T00:00:00.000000Z'),
                    ('B', '2024-01-01T01:00:00.000000Z'),
                    ('C', '2024-01-01T02:00:00.000000Z'),
                    ('X', '2024-01-01T03:00:00.000000Z'),
                    ('Y', '2024-01-01T04:00:00.000000Z'),
                    ('Z', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 'C'")
                    .noLeakCheck()
                    .returns("""
                            val
                            C
                            """);

            assertQuery("SELECT val FROM x WHERE val = 'c'")
                    .noLeakCheck()
                    .returns("""
                            val
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01'::DATE, '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-01'::DATE, '2024-01-01T01:00:00.000000Z'),
                    ('2020-12-31'::DATE, '2024-01-01T02:00:00.000000Z'),
                    ('2021-12-31'::DATE, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '2020-06-01'::DATE")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-01T00:00:00.000Z
                            """);
            assertQuery("SELECT val FROM x WHERE val = '2099-01-01'::DATE")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(30,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5000000000000.50'::DECIMAL(30,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000000000.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '100.10'::DECIMAL(30,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal16() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(4,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('10.10', '2024-01-01T00:00:00.000000Z'),
                    ('50.50', '2024-01-01T01:00:00.000000Z'),
                    ('99.99', '2024-01-01T02:00:00.000000Z'),
                    ('99.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '50.50'::DECIMAL(4,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            50.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '1.01'::DECIMAL(4,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(50,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('100000000000000000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('500000000000000000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('999999999999999999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('999999999999999999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '500000000000000000000.50'::DECIMAL(50,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            500000000000000000000.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '10.10'::DECIMAL(50,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal32() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(8,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000.10', '2024-01-01T00:00:00.000000Z'),
                    ('50000.50', '2024-01-01T01:00:00.000000Z'),
                    ('99999.99', '2024-01-01T02:00:00.000000Z'),
                    ('99999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '50000.50'::DECIMAL(8,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            50000.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '100.10'::DECIMAL(8,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5000000.50'::DECIMAL(15,2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
            assertQuery("SELECT val FROM x WHERE val = '100.10'::DECIMAL(15,2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimal8() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(2,1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1', '2024-01-01T00:00:00.000000Z'),
                    ('5.5', '2024-01-01T01:00:00.000000Z'),
                    ('9.9', '2024-01-01T02:00:00.000000Z'),
                    ('9.8', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5.5'::DECIMAL(2,1)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5.5
                            """);
            assertQuery("SELECT val FROM x WHERE val = '0.1'::DECIMAL(2,1)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleScaleUp() throws Exception {
        // PushdownFilterExtractor#rescaleDecimalForPushdown rebuilds the literal at
        // the column's scale when the literal scale is smaller. Pushdown still works
        // because the rescaled raw value matches the column's row group statistics.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // literal scale 1 < column scale 2, value rescales to 50000005 (scale 2)
            assertQuery("SELECT val FROM x WHERE val = '5000000.5'::DECIMAL(15, 1)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
            // out-of-range literal: rescale produces a value smaller than the row
            // group min, so the second partition's row group is pruned.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '100.1'::DECIMAL(15, 1)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleScaleDownLossless() throws Exception {
        // Literal carries trailing zeros beyond the column's scale, so the rescale
        // is lossless; pushdown keeps working with the rebuilt constant.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5000000.500'::DECIMAL(15, 3)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '100.100'::DECIMAL(15, 3)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleScaleDownLossy() throws Exception {
        // Literal has non-zero fractional digits beyond the column's scale, so
        // rescaling would lose precision and pushdown is abandoned. The runtime
        // filter still correctly returns no matching rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '5000000.501'::DECIMAL(15, 3)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleWiderLiteralFits() throws Exception {
        // Literal is DECIMAL128 but its value fits in the column's DECIMAL64
        // storage. The helper rebuilds the constant at the narrower tag so
        // pushdown's getDecimal64 dispatch produces the right value.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '5000000.50'::DECIMAL(30, 2)")
                    .noLeakCheck()
                    .returns("""
                            val
                            5000000.50
                            """);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '100.10'::DECIMAL(30, 2)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningDecimalRescaleWiderLiteralOverflows() throws Exception {
        // Literal value is larger than the column's DECIMAL64 storage can hold,
        // so the helper abandons pushdown. The runtime filter still produces the
        // correct (full) result for `c <= huge_literal`.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DECIMAL(15,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1000000.10', '2024-01-01T00:00:00.000000Z'),
                    ('5000000.50', '2024-01-01T01:00:00.000000Z'),
                    ('9999999.99', '2024-01-01T02:00:00.000000Z'),
                    ('9999999.98', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= '99999999999999999999999999.99'::DECIMAL(30, 2) ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1000000.10
                            5000000.50
                            9999999.98
                            9999999.99
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testMinMaxPruningDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.11, '2024-01-01T00:00:00.000000Z'),
                    (2.22, '2024-01-01T01:00:00.000000Z'),
                    (3.33, '2024-01-01T02:00:00.000000Z'),
                    (4.44, '2024-01-01T03:00:00.000000Z'),
                    (5.55, '2024-01-01T04:00:00.000000Z'),
                    (5.56, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 3.33")
                    .noLeakCheck()
                    .returns("""
                            val
                            3.33
                            """);
            assertQuery("SELECT val FROM x WHERE val = 99.99")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.5, '2024-01-01T00:00:00.000000Z'),
                    (2.5, '2024-01-01T01:00:00.000000Z'),
                    (3.5, '2024-01-01T02:00:00.000000Z'),
                    (4.5, '2024-01-01T03:00:00.000000Z'),
                    (5.5, '2024-01-01T04:00:00.000000Z'),
                    (5.6, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 3.5::FLOAT")
                    .noLeakCheck()
                    .returns("""
                            val
                            3.5
                            """);
            assertQuery("SELECT val FROM x WHERE val = 99.9::FLOAT")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningGeoByte() throws Exception {
        // GeoByte rides the same INT32-physical inline path as BYTE: the _pm
        // slot now holds parquet i32 stats verbatim instead of a 1-byte
        // narrow encoding. The skip path doesn't currently push geohash
        // equality through the row-group filter, but values must still
        // round-trip correctly through the parquet conversion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val GEOHASH(1c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (#0, '2024-01-01T00:00:00.000000Z'),
                    (#1, '2024-01-01T01:00:00.000000Z'),
                    (#2, '2024-01-01T02:00:00.000000Z'),
                    (#3, '2024-01-01T03:00:00.000000Z'),
                    (#z, '2024-01-02T03:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = #2")
                    .noLeakCheck()
                    .returns("""
                            val
                            2
                            """);

            // #y matches no row in either partition; the query must return
            // empty without misclassifying the row groups.
            assertQuery("SELECT val FROM x WHERE val = #y")
                    .noLeakCheck()
                    .returns("val\n");

            // Full scan with timestamp ordering still has to read every value
            // through the parquet reader, regardless of whether the row group
            // filter fires.
            assertQuery("SELECT val, ts FROM x ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val\tts
                            0\t2024-01-01T00:00:00.000000Z
                            1\t2024-01-01T01:00:00.000000Z
                            2\t2024-01-01T02:00:00.000000Z
                            3\t2024-01-01T03:00:00.000000Z
                            z\t2024-01-02T03:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testMinMaxPruningGeoShort() throws Exception {
        // GeoShort rides the same INT32-physical inline path as SHORT: the
        // _pm slot now holds parquet i32 stats verbatim instead of a 2-byte
        // narrow encoding. The skip path doesn't currently push geohash
        // equality through the row-group filter, but values must still
        // round-trip correctly through the parquet conversion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val GEOHASH(3c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (#000, '2024-01-01T00:00:00.000000Z'),
                    (#001, '2024-01-01T01:00:00.000000Z'),
                    (#002, '2024-01-01T02:00:00.000000Z'),
                    (#003, '2024-01-01T03:00:00.000000Z'),
                    (#zzz, '2024-01-02T03:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = #002")
                    .noLeakCheck()
                    .returns("""
                            val
                            002
                            """);

            // #yyy matches no row in either partition; the query must return
            // empty without misclassifying the row groups.
            assertQuery("SELECT val FROM x WHERE val = #yyy")
                    .noLeakCheck()
                    .returns("val\n");

            assertQuery("SELECT val, ts FROM x ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            val\tts
                            000\t2024-01-01T00:00:00.000000Z
                            001\t2024-01-01T01:00:00.000000Z
                            002\t2024-01-01T02:00:00.000000Z
                            003\t2024-01-01T03:00:00.000000Z
                            zzz\t2024-01-02T03:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testMinMaxPruningIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1.1.1', '2024-01-01T00:00:00.000000Z'),
                    ('10.0.0.1', '2024-01-01T01:00:00.000000Z'),
                    (NULL, '2024-01-01T02:00:00.000000Z'),
                    ('192.168.1.1', '2024-01-01T03:00:00.000000Z'),
                    ('192.168.1.2', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '10.0.0.1'")
                    .noLeakCheck()
                    .returns("""
                            val
                            10.0.0.1
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = '1.1.1.0'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val in (NULL)")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IN ('192.168.1.1', '192.168.1.2') ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            192.168.1.1
                            192.168.1.2
                            """);
        });
    }

    @Test
    public void testMinMaxPruningInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (10_000, '2024-01-01T00:00:00.000000Z'),
                    (20_000, '2024-01-01T01:00:00.000000Z'),
                    (30_000, '2024-01-01T02:00:00.000000Z'),
                    (40_000, '2024-01-01T03:00:00.000000Z'),
                    (50_000, '2024-01-01T04:00:00.000000Z'),
                    (60_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 30_000")
                    .noLeakCheck()
                    .returns("""
                            val
                            30000
                            """);
            assertQuery("SELECT val FROM x WHERE val = 99_999")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningIntBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (10_000, '2024-01-01T00:00:00.000000Z'),
                    (20_000, '2024-01-01T01:00:00.000000Z'),
                    (30_000, '2024-01-01T02:00:00.000000Z'),
                    (40_000, '2024-01-01T03:00:00.000000Z'),
                    (50_000, '2024-01-01T04:00:00.000000Z'),
                    (60_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            bindVariableService.clear();
            bindVariableService.setInt("v", 30_000);
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n30000\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", 99_999);
            assertQuery("SELECT val FROM x WHERE val = :v")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt(0, 99_999);
            assertQuery("SELECT val FROM x WHERE val = $1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningIntInList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (10_000, '2024-01-01T00:00:00.000000Z'),
                    (20_000, '2024-01-01T01:00:00.000000Z'),
                    (30_000, '2024-01-01T02:00:00.000000Z'),
                    (40_000, '2024-01-01T03:00:00.000000Z'),
                    (50_000, '2024-01-01T04:00:00.000000Z'),
                    (60_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val IN (10_000, 50_000)")
                    .noLeakCheck()
                    .returns("""
                            val
                            10000
                            50000
                            """);
            assertQuery("SELECT val FROM x WHERE val IN (99_998, 99_999)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100_000, '2024-01-01T00:00:00.000000Z'),
                    (200_000, '2024-01-01T01:00:00.000000Z'),
                    (300_000, '2024-01-01T02:00:00.000000Z'),
                    (400_000, '2024-01-01T03:00:00.000000Z'),
                    (500_000, '2024-01-01T04:00:00.000000Z'),
                    (600_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 300_000")
                    .noLeakCheck()
                    .returns("""
                            val
                            300000
                            """);
            assertQuery("SELECT val FROM x WHERE val = 999_999")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningLong128() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG128, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (to_long128(0, 1), '2024-01-01T00:00:00.000000Z'),
                    (to_long128(0, 50), '2024-01-01T01:00:00.000000Z'),
                    (to_long128(0, 100), '2024-01-01T02:00:00.000000Z'),
                    (to_long128(0, 101), '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = to_long128(0, 50)")
                    .noLeakCheck()
                    .returns("""
                            val
                            00000000-0000-0032-0000-000000000000
                            """);
            assertQuery("SELECT val FROM x WHERE val = to_long128(0, 999)")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100, '2024-01-01T00:00:00.000000Z'),
                    (200, '2024-01-01T01:00:00.000000Z'),
                    (300, '2024-01-01T02:00:00.000000Z'),
                    (400, '2024-01-01T03:00:00.000000Z'),
                    (500, '2024-01-01T04:00:00.000000Z'),
                    (600, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 300")
                    .noLeakCheck()
                    .returns("""
                            val
                            300
                            """);
            assertQuery("SELECT val FROM x WHERE val = 999")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningShortNegative() throws Exception {
        // SHORT is INT32-backed in parquet. The skip path reads inline stats
        // at INT32 physical width, so a negative min must round-trip through
        // the u64 slot as the correct i32. A SHORT column with min -74 must
        // not appear as an unsigned 65462 to the skip path; otherwise every
        // row group whose true min is negative gets dropped.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (-30_000, '2024-01-01T00:00:00.000000Z'),
                    (-200, '2024-01-01T01:00:00.000000Z'),
                    (-74, '2024-01-01T02:00:00.000000Z'),
                    (0, '2024-01-01T03:00:00.000000Z'),
                    (300, '2024-01-01T04:00:00.000000Z'),
                    (29_000, '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            // val = 0 must not skip the row group whose min is -30_000.
            // An unsigned-extended inline min of 35536 would be greater than 0
            // and would drop every match.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 0")
                    .noLeakCheck()
                    .returns("""
                            val
                            0
                            """);

            // val = -74 is the literal value from the fuzzer reproduction.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -74")
                    .noLeakCheck()
                    .returns("""
                            val
                            -74
                            """);

            // val = -32_000 sits below the row group min (-30_000); should skip.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -32_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('abc', '2024-01-01T00:00:00.000000Z'),
                    ('bbb', '2024-01-01T01:00:00.000000Z'),
                    ('ccc', '2024-01-01T02:00:00.000000Z'),
                    ('xxx', '2024-01-01T03:00:00.000000Z'),
                    ('zzz', '2024-01-01T04:00:00.000000Z'),
                    ('yyy', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 'ccc'")
                    .noLeakCheck()
                    .returns("""
                            val
                            ccc
                            """);
            assertQuery("SELECT val FROM x WHERE val = 'aaa'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('alpha', '2024-01-01T00:00:00.000000Z'),
                    ('beta', '2024-01-01T01:00:00.000000Z'),
                    ('gamma', '2024-01-01T02:00:00.000000Z'),
                    ('delta', '2024-01-01T03:00:00.000000Z'),
                    ('epsilon', '2024-01-01T04:00:00.000000Z'),
                    ('epsilon1', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 'gamma'")
                    .noLeakCheck()
                    .returns("""
                            val
                            gamma
                            """);
            assertQuery("SELECT val FROM x WHERE val = 'aa'")
                    .noLeakCheck()
                    .returns("val\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-01T00:00:00.000000Z', '2024-01-01T01:00:00.000000Z'),
                    ('2020-12-31T00:00:00.000000Z', '2024-01-01T02:00:00.000000Z'),
                    ('2021-12-31T00:00:00.000000Z', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '2020-06-01T00:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("""
                            val
                            2020-06-01T00:00:00.000000Z
                            """);
            assertQuery("SELECT val FROM x WHERE val = '2099-01-01T00:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:00.000000Z'),
                    ('22222222-2222-2222-2222-222222222222', '2024-01-01T01:00:00.000000Z'),
                    ('33333333-3333-3333-3333-333333333333', '2024-01-01T02:00:00.000000Z'),
                    ('33333333-3333-3333-3333-333333333334', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = '22222222-2222-2222-2222-222222222222'")
                    .noLeakCheck()
                    .returns("""
                            val
                            22222222-2222-2222-2222-222222222222
                            """);
            assertQuery("SELECT val FROM x WHERE val = 'ffffffff-ffff-ffff-ffff-ffffffffffff'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('hello', '2024-01-01T00:00:00.000000Z'),
                    ('world', '2024-01-01T01:00:00.000000Z'),
                    ('foo', '2024-01-01T02:00:00.000000Z'),
                    ('bar', '2024-01-01T03:00:00.000000Z'),
                    ('baz', '2024-01-01T04:00:00.000000Z'),
                    ('baz1', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 'foo'")
                    .noLeakCheck()
                    .returns("""
                            val
                            foo
                            """);
            assertQuery("SELECT val FROM x WHERE val = 'aaa'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMixedParquetAndNativePartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-02T00:00:00.000000Z'),
                    (4, '2024-01-02T01:00:00.000000Z'),
                    (5, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            assertQuery("SELECT val FROM x WHERE val = 2")
                    .noLeakCheck()
                    .returns("""
                            val
                            2
                            """);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 5")
                    .noLeakCheck()
                    .returns("""
                            val
                            5
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMultipleAndConditions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, 'aaa', '2024-01-01T00:00:00.000000Z'),
                    (2, 'bbb', '2024-01-01T01:00:00.000000Z'),
                    (3, 'ccc', '2024-01-01T02:00:00.000000Z'),
                    (1, 'bbb', '2024-01-01T03:00:00.000000Z'),
                    (2, 'ccc', '2024-01-01T04:00:00.000000Z'),
                    (4, 'ccc', '2024-01-02T04:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT a, b FROM x WHERE a = 1 AND b = 'bbb'")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            1\tbbb
                            """);
            assertQuery("SELECT a, b FROM x WHERE a = 99 AND b = 'zzz'")
                    .noLeakCheck()
                    .returns("a\tb\n");

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testNullColumnPruning() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (null, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (null, '2024-01-01T02:00:00.000000Z'),
                    (null, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQuery("SELECT val FROM x WHERE val = 42")
                    .noLeakCheck()
                    .returns("val\n");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            null
                            null
                            null
                            """);
        });
    }

    @Test
    public void testNullPruningByte() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (4, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 0")
                    .noLeakCheck()
                    .returns("""
                            val
                            0
                            """);
        });
    }

    @Test
    public void testNullPruningDouble() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.11, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3.33, '2024-01-01T02:00:00.000000Z'),
                    (3.34, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testNullPruningFloat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1.5, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3.5, '2024-01-01T02:00:00.000000Z'),
                    (3.6, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testNullPruningInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (4, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testNullPruningLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (3, '2024-01-01T02:00:00.000000Z'),
                    (4, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            """);
        });
    }

    @Test
    public void testNullPruningShort() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (100, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    (300, '2024-01-01T02:00:00.000000Z'),
                    (400, '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = 0")
                    .noLeakCheck()
                    .returns("""
                            val
                            0
                            """);
        });
    }

    @Test
    public void testNullPruningString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('aaa', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('ccc', '2024-01-01T02:00:00.000000Z'),
                    ('ddd', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
        });
    }

    @Test
    public void testNullPruningUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('11111111-1111-1111-1111-111111111111', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('33333333-3333-3333-3333-333333333333', '2024-01-01T02:00:00.000000Z'),
                    ('33333333-3333-3333-3333-333333333334', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
        });
    }

    @Test
    public void testNullPruningVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('hello', '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T01:00:00.000000Z'),
                    ('world', '2024-01-01T02:00:00.000000Z'),
                    ('world1', '2024-01-02T02:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT val FROM x WHERE val = null")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            """);
        });
    }

    @Test
    public void testOrConditionNoPruning() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1, 10, '2024-01-01T00:00:00.000000Z'),
                    (2, 20, '2024-01-01T01:00:00.000000Z'),
                    (3, 30, '2024-01-01T02:00:00.000000Z'),
                    (4, 40, '2024-01-02T00:00:00.000000Z'),
                    (5, 50, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'a,b')");

            assertQuery("SELECT a, b FROM x WHERE a = 1 OR b = 50")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            1\t10
                            5\t50
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT a, b FROM x WHERE a = 1 AND (b = 10 OR b = 99)")
                    .noLeakCheck()
                    .returns("""
                            a\tb
                            1\t10
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testOrEqualityFilter() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val = -2")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 42 OR val = 43")
                    .noLeakCheck()
                    .returns("""
                            val
                            42
                            43
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val = -2 OR val = -3")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val = 100 OR val = -3")
                    .noLeakCheck()
                    .returns("""
                            val
                            100
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR ts = '2099-01-01T00:00:00.000000Z'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val = 1 OR val > 0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n150\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE (val = -1 OR val = -2) AND val > 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // OR with IS NULL on non-nullable data → all absent, skip all
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val IS NULL")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // OR: matching value + IS NULL on non-nullable data → returns matching rows
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 42 OR val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            42
                            """);
        });
    }

    @Test
    public void testOrEqualityFilterString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('alice', '2024-01-01T00:00:00.000000Z'),
                    ('bob', '2024-01-01T01:00:00.000000Z'),
                    ('charlie', '2024-01-01T02:00:00.000000Z'),
                    ('diana', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name = 'xyz' OR name = 'unknown'")
                    .noLeakCheck()
                    .returns("name\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name = 'xyz' OR name = 'bob'")
                    .noLeakCheck()
                    .returns("""
                            name
                            bob
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name = 'xyz' OR name IS NULL")
                    .noLeakCheck()
                    .returns("name\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name = 'bob' OR name IS NULL")
                    .noLeakCheck()
                    .returns("""
                            name
                            bob
                            """);
        });
    }

    @Test
    public void testOrEqualityFilterWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (42, '2024-01-02T00:00:00.000000Z'),
                    (43, '2024-01-02T01:00:00.000000Z'),
                    (44, '2024-01-03T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -1 OR val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            null
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 42 OR val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            null
                            42
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = -999 OR val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            null
                            null
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testPruningAllTypesMultipleRowGroups() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE x (
                        v_byte BYTE,
                        v_short SHORT,
                        v_char CHAR,
                        v_int INT,
                        v_long LONG,
                        v_float FLOAT,
                        v_double DOUBLE,
                        v_string STRING,
                        v_varchar VARCHAR,
                        v_symbol SYMBOL,
                        v_date DATE,
                        v_timestamp TIMESTAMP,
                        v_uuid UUID,
                        v_ipv4 IPv4,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (1, 100, 'A', 1000, 100_000, 1.5, 1.55, 'aaa', 'alpha', 'sym1', '2024-01-01', '2024-01-01T00:00:00.000000Z', '11111111-1111-1111-1111-111111111111', '1.1.1.1', '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 'B', 2000, 200_000, 2.5, 2.55, 'bbb', 'beta',  'sym2', '2024-01-02', '2024-01-01T01:00:00.000000Z', '22222222-2222-2222-2222-222222222222', '2.2.2.2', '2024-01-01T01:00:00.000000Z'),
                    (3, 300, 'C', 3000, 300_000, 3.5, 3.55, 'ccc', 'gamma', 'sym3', '2024-01-03', '2024-01-01T02:00:00.000000Z', '33333333-3333-3333-3333-333333333333', '3.3.3.3', '2024-01-01T02:00:00.000000Z'),
                    (50, 500, 'X', 50_000, 500_000, 50.5, 50.55, 'xxx', 'xi', 'sym50', '2024-06-01', '2024-01-02T00:00:00.000000Z', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '50.50.50.50', '2024-01-02T00:00:00.000000Z'),
                    (60, 600, 'Y', 60_000, 600_000, 60.5, 60.55, 'yyy', 'upsilon', 'sym60', '2024-07-01', '2024-01-02T01:00:00.000000Z', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '60.60.60.60', '2024-01-02T01:00:00.000000Z'),
                    (70, 700, 'Z', 70_000, 700_000, 70.5, 70.55, 'zzz', 'zeta', 'sym70', '2024-08-01', '2024-01-02T02:00:00.000000Z', 'cccccccc-cccc-cccc-cccc-cccccccccccc', '70.70.70.70', '2024-01-02T02:00:00.000000Z'),
                    (80, 800, 'U', 80_000, 800_000, 80.5, 80.55, 'zzz', 'zeta', 'sym80', '2024-09-01', '2024-01-03T01:00:00.000000Z', 'cccccccc-cccc-cccc-cccc-cccccccccccc', '80.80.80.80', '2024-01-03T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT v_byte FROM x WHERE v_byte = 99::byte")
                    .noLeakCheck()
                    .returns("v_byte\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_short FROM x WHERE v_short = 999::short")
                    .noLeakCheck()
                    .returns("v_short\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_char FROM x WHERE v_char = 'M'")
                    .noLeakCheck()
                    .returns("v_char\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_int FROM x WHERE v_int = 99_999")
                    .noLeakCheck()
                    .returns("v_int\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_long FROM x WHERE v_long = 999_999")
                    .noLeakCheck()
                    .returns("v_long\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_float FROM x WHERE v_float = 99.9")
                    .noLeakCheck()
                    .returns("v_float\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_double FROM x WHERE v_double = 99.99")
                    .noLeakCheck()
                    .returns("v_double\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_string FROM x WHERE v_string = 'nnn'")
                    .noLeakCheck()
                    .returns("v_string\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_varchar FROM x WHERE v_varchar = 'omega'")
                    .noLeakCheck()
                    .returns("v_varchar\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_symbol FROM x WHERE v_symbol = 'sym99'")
                    .noLeakCheck()
                    .returns("v_symbol\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_date FROM x WHERE v_date = '2099-01-01'::DATE")
                    .noLeakCheck()
                    .returns("v_date\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_timestamp FROM x WHERE v_timestamp = '2099-01-01T00:00:00.000000Z'::TIMESTAMP")
                    .noLeakCheck()
                    .returns("v_timestamp\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_uuid FROM x WHERE v_uuid = '99999999-9999-9999-9999-999999999999'::UUID")
                    .noLeakCheck()
                    .returns("v_uuid\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_ipv4 FROM x WHERE v_ipv4 = '99.99.99.99'")
                    .noLeakCheck()
                    .returns("v_ipv4\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_int FROM x WHERE v_int = 3000")
                    .noLeakCheck()
                    .returns("v_int\n3000\n");
            assertQuery("SELECT v_string FROM x WHERE v_string = 'ccc'")
                    .noLeakCheck()
                    .returns("v_string\nccc\n");
            assertQuery("SELECT v_uuid FROM x WHERE v_uuid = '33333333-3333-3333-3333-333333333333'::UUID")
                    .noLeakCheck()
                    .returns("v_uuid\n33333333-3333-3333-3333-333333333333\n");
        });
    }

    @Test
    public void testPruningDisabled() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParquetRowGroupPruningEnabled(false);
            try {
                execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                execute("""
                        INSERT INTO x VALUES
                        (1, '2024-01-01T00:00:00.000000Z'),
                        (2, '2024-01-01T01:00:00.000000Z'),
                        (3, '2024-01-01T02:00:00.000000Z'),
                        (4, '2024-01-02T02:00:00.000000Z')
                        """);
                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

                assertQuery("SELECT val FROM x WHERE val = 99")
                        .noLeakCheck()
                        .returns("val\n");
                Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            } finally {
                sqlExecutionContext.setParquetRowGroupPruningEnabled(true);
            }
        });
    }

    @Test
    public void testPruningUnsupportedTypesFallback() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v_bool BOOLEAN, v_geo GEOHASH(4c), v_l256 LONG256, v_int INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (true, #u33d, CAST(1 AS LONG256), 10, '2024-01-01T00:00:00.000000Z'),
                    (false, #u33e, CAST(2 AS LONG256), 20, '2024-01-01T01:00:00.000000Z'),
                    (true, #u33f, CAST(3 AS LONG256), 30, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT v_bool FROM x WHERE v_bool = true")
                    .noLeakCheck()
                    .returns("""
                            v_bool
                            true
                            true
                            """);
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT v_int FROM x WHERE v_int = 99")
                    .noLeakCheck()
                    .returns("v_int\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterBetween() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val BETWEEN 10_000 AND 20_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val BETWEEN 50 AND 60")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n11\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val BETWEEN 110 AND 101")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n10\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterBoundaryConditions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 60_000_000)
                    FROM long_sequence(150)
                    """);
            execute("""
                    INSERT INTO x VALUES
                    (151, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val >= 152")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 150")
                    .noLeakCheck()
                    .returns("""
                            val
                            151
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 1")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n151\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 150")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n150\n");
        });
    }

    @Test
    public void testRangeFilterByte() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 50);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS BYTE), timestamp_sequence('2024-01-01', 1200_000_000)
                    FROM long_sequence(100)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 120")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
        });
    }

    @Test
    public void testRangeFilterByteNegative() throws Exception {
        // Range pruning over a BYTE column whose data spans negative and
        // positive values. Without correct sign-extension on the inline u64
        // stat slot, a row group with min = -50 reads back as 206 in the
        // skip path, and `val <= 0` skips every row group.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 50);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // x = 1..100, val = -50..49 (50 negative + 50 non-negative).
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 51 AS BYTE), timestamp_sequence('2024-01-01', 1200_000_000)
                    FROM long_sequence(100)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n51\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= -10")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n60\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= -10 AND val <= 10")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n21\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < -50")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 100")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterByteNegativeStats() throws Exception {
        // Same _pm sidecar inline-stat sign bug for BYTE: 1 narrow byte read back as 4
        // bytes for the i32 skip comparison.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 75 AS BYTE), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 0::BYTE")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n75\n");
        });
    }

    @Test
    public void testRangeFilterChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('A', '2024-01-01T00:00:00.000000Z'),
                    ('B', '2024-01-01T01:00:00.000000Z'),
                    ('C', '2024-01-01T02:00:00.000000Z'),
                    ('M', '2024-01-01T03:00:00.000000Z'),
                    ('X', '2024-01-01T04:00:00.000000Z'),
                    ('Y', '2024-01-01T05:00:00.000000Z'),
                    ('Z', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 'Y'")
                    .noLeakCheck()
                    .returns("""
                            val
                            Z
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 'A'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 'X'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n3\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 'C'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n3\n");
        });
    }

    @Test
    public void testRangeFilterDate() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(timestamp_sequence('2020-01-01', 86400_000_000) AS DATE), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '2025-01-01'::DATE")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '2019-01-01'::DATE")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= '2020-02-01'::DATE AND val <= '2020-03-02'::DATE")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n31\n");
        });
    }

    @Test
    public void testRangeFilterDouble() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS DOUBLE) * 0.1, timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 100.0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0.0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 5.0 AND val < 10.0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
        });
    }

    @Test
    public void testRangeFilterFloat() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS FLOAT) * 0.1, timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 100.0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0.0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 5.0 AND val < 10.0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
        });
    }

    @Test
    public void testRangeFilterIPv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('1.1.1.1', '2024-01-01T00:00:00.000000Z'),
                    ('10.0.0.1', '2024-01-01T01:00:00.000000Z'),
                    ('192.168.1.1', '2024-01-01T02:00:00.000000Z'),
                    ('255.255.255.254', '2024-01-01T03:00:00.000000Z'),
                    (NULL, '2024-01-01T04:00:00.000000Z'),
                    (NULL, '2024-01-02T01:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '10.0.0.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            192.168.1.1
                            255.255.255.254
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '192.168.1.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.1.1.1
                            10.0.0.1
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val >= '192.168.1.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            192.168.1.1
                            255.255.255.254
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= '10.0.0.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.1.1.1
                            10.0.0.1
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '255.255.255.254'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '1.1.1.1'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '0.0.0.1' ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.1.1.1
                            10.0.0.1
                            192.168.1.1
                            255.255.255.254
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IS NULL")
                    .noLeakCheck()
                    .returns("""
                            val
                            
                            
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val IS NOT NULL ORDER BY val")
                    .noLeakCheck()
                    .returns("""
                            val
                            1.1.1.1
                            10.0.0.1
                            192.168.1.1
                            255.255.255.254
                            """);
        });
    }

    @Test
    public void testRangeFilterIPv4HighValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val IPv4, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('200.0.0.1', '2024-01-01T00:00:00.000000Z'),
                    ('210.0.0.1', '2024-01-01T01:00:00.000000Z'),
                    ('220.0.0.1', '2024-01-01T02:00:00.000000Z'),
                    ('230.0.0.1', '2024-01-01T03:00:00.000000Z'),
                    ('240.0.0.1', '2024-01-01T04:00:00.000000Z'),
                    ('250.0.0.1', '2024-01-01T05:00:00.000000Z'),
                    ('250.0.0.1', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > '200.0.0.1'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n6\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '250.0.0.1'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '200.0.0.1'")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= '210.0.0.1' AND val <= '230.0.0.1'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n3\n");
        });
    }

    @Test
    public void testRangeFilterInt() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 10_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < -1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            cnt
                            100
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val >= 151")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val <= -1")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterLong() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT x, timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 10_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= 100")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n51\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 100")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n100\n");
        });
    }

    @Test
    public void testRangeFilterNegativeToPositive() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 101 AS INT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(300)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n149\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val < -50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 199")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < -100")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= -10 AND val <= 10")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n21\n");
        });
    }

    @Test
    public void testRangeFilterShort() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x AS SHORT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 10_000")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < 0")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 100")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");
        });
    }

    @Test
    public void testRangeFilterShortNegative() throws Exception {
        // Direct reproduction of the fuzzer-surfaced bug. SHORT data spans
        // negative and positive values across multiple row groups. Without
        // a parquet-physical-width round trip on the inline u64 stat slot,
        // a row group with min = -100 reads back as 65436 in the skip
        // path, and predicates like `val <= 0` skip every row group whose
        // true min was negative.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // x = 1..300, val = -100..199 (100 negative + 200 non-negative).
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 101 AS SHORT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(300)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 0")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n101\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val > 50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n149\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val < -50")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n50\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= -10 AND val <= 10")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n21\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < -100")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 199")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterShortNegativeStats() throws Exception {
        // Regression for the _pm sidecar inline-stat sign bug: SHORT min/max stored as
        // 2 narrow bytes, then read back as 4 bytes for the i32 skip comparison. Without
        // sign extension a negative min like -74 reads as 65462 and the row group is
        // wrongly skipped. See ParquetRowGroupFilter.prepareFilterList SHORT branch and
        // the convert_stat_to_qdb narrow-INT32 path.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT CAST(x - 75 AS SHORT), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            assertQuery("SELECT count() AS cnt FROM x WHERE val <= 0::SHORT")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n75\n");

            assertQuery("SELECT count() AS cnt FROM x WHERE 0::SHORT >= val")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n75\n");

            assertQuery("SELECT count() AS cnt FROM x WHERE 0.147451::FLOAT >= val")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n75\n");
        });
    }

    @Test
    public void testRangeFilterString() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (name STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    ('alice', '2024-01-01T00:00:00.000000Z'),
                    ('bob', '2024-01-01T01:00:00.000000Z'),
                    ('charlie', '2024-01-01T02:00:00.000000Z'),
                    ('diana', '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name > 'zzz'")
                    .noLeakCheck()
                    .returns("name\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT name FROM x WHERE name < 'A'")
                    .noLeakCheck()
                    .returns("name\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRangeFilterTimestamp() throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x
                    SELECT timestamp_sequence('2020-01-01', 600_000_000), timestamp_sequence('2024-01-01', 600_000_000)
                    FROM long_sequence(150)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '2025-01-01'::timestamp")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val < '2019-01-01'::timestamp")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val >= '2020-01-02'::timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n6\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE val <= '2020-01-02'::timestamp")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n145\n");
        });
    }

    @Test
    public void testBloomFilterSkippedAfterAlterColumnTypeIntToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            execute("ALTER TABLE x ALTER COLUMN val TYPE LONG");
            drainWalQueue();

            // After type change INT->LONG, pushdown must be disabled for val.
            // Otherwise the bloom filter bytes (stored as i32) would be probed
            // with an i64 hash, producing false-negative skips and missing rows.
            assertQuery("SELECT val FROM x WHERE val = 50_000").noLeakCheck().returns(
                    """
                            val
                            50000
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testRenamedColumnBetween() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the min/max-stats path: a BETWEEN
        // range on a renamed column must resolve to the right parquet column by stable id, not
        // by the frozen (now stale) parquet name.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // The match lives in the renamed column's low row group; the stale name maps to the
            // high-range column whose stats do not overlap [15000, 25000], wrongly pruning it.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b BETWEEN 15_000 AND 25_000 ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // A range genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b BETWEEN 1_000 AND 5_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterSkippedAfterAlterColumnTypeLongToInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (50_000, '2024-01-01T01:00:00.000000Z'),
                    (100_000, '2024-01-01T02:00:00.000000Z'),
                    (100_001, '2024-01-02T01:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            execute("ALTER TABLE x ALTER COLUMN val TYPE INT");
            drainWalQueue();

            // After type change LONG->INT, pushdown must be disabled for val.
            // The parquet file stores i64 bloom filters / min-max stats but the
            // filter serializes i32 values — different element sizes cause wrong
            // hashes and comparisons.
            assertQuery("SELECT val FROM x WHERE val = 50_000").noLeakCheck().returns(
                    """
                            val
                            50000
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 25_000").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testRenamedColumnInList() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the min/max-stats path: an IN list
        // on a renamed column must resolve by stable column id.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // 15_000 and 25_000 both live in the renamed column's first row group; the stale
            // name maps to the high-range column whose stats exclude both, so the buggy
            // resolution wrongly pruned that row group and dropped the matches.
            assertQuery("SELECT b FROM x WHERE b IN (15_000, 25_000) ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);

            // Values genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b IN (1_000, 5_000)")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testBloomFilterSkippedAfterAlterColumnTypeShortToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (500, '2024-01-01T01:00:00.000000Z'),
                    (1000, '2024-01-01T02:00:00.000000Z'),
                    (1001, '2024-01-02T01:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            execute("ALTER TABLE x ALTER COLUMN val TYPE LONG");
            drainWalQueue();

            // After type change SHORT->LONG, pushdown must be disabled for val.
            // The parquet file stores i32 bloom filters but the filter serializes
            // i64 values — wrong hash width causes false-negative skips.
            assertQuery("SELECT val FROM x WHERE val = 500").noLeakCheck().returns(
                    """
                            val
                            500
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 250").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testMinMaxPruningSkippedAfterAlterColumnTypeIntToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    (10_000, '2024-01-01T00:00:00.000000Z'),
                    (20_000, '2024-01-01T01:00:00.000000Z'),
                    (30_000, '2024-01-01T02:00:00.000000Z'),
                    (40_000, '2024-01-01T03:00:00.000000Z'),
                    (50_000, '2024-01-01T04:00:00.000000Z'),
                    (60_000, '2024-01-02T04:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            execute("ALTER TABLE x ALTER COLUMN val TYPE LONG");
            drainWalQueue();

            // After type change INT->LONG, min/max pushdown must be disabled for val.
            // The parquet file stores i32 stats but the filter serializes i64
            // values; cross-width comparisons would produce wrong skip decisions.
            assertQuery("SELECT val FROM x WHERE val = 30_000").noLeakCheck().returns(
                    """
                            val
                            30000
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > 100_000").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testRenamedColumnIsNull() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the null-count path: IS [NOT] NULL
        // pushdown resolved the filtered column by its frozen parquet name, so after a rename it
        // consulted the wrong column's null-count stats and wrongly pruned. The fix resolves by
        // stable column id.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // Column a (the future 'b') is non-null in the first partition and null in the second;
            // column b is the mirror image. The third partition keeps the first two non-active so
            // they convert to parquet.
            execute("""
                    INSERT INTO x VALUES
                    (100, NULL, '2024-01-01T00:00:00.000000Z'),
                    (NULL, 200, '2024-01-02T00:00:00.000000Z'),
                    (300, 400, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
            execute("ALTER TABLE x RENAME COLUMN b TO c");
            execute("ALTER TABLE x RENAME COLUMN a TO b");

            // IS NOT NULL must return the renamed column's non-null rows. The stale name maps to
            // the all-null frozen column in the first partition, which wrongly pruned row 100.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b IS NOT NULL ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            100
                            300
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // IS NULL must return the renamed column's null row. The stale name maps to the
            // no-nulls frozen column in the second partition, which wrongly pruned that null.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT count() AS cnt FROM x WHERE b IS NULL")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("cnt\n1\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testMinMaxPruningSkippedAfterAlterColumnTypeTimestampToTimestampNs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO x VALUES
                    ('2020-01-01T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    ('2020-06-01T00:00:00.000000Z', '2024-01-01T01:00:00.000000Z'),
                    ('2021-01-01T00:00:00.000000Z', '2024-01-01T02:00:00.000000Z'),
                    ('2021-06-01T00:00:00.000000Z', '2024-01-01T03:00:00.000000Z'),
                    ('2022-01-01T00:00:00.000000Z', '2024-01-01T04:00:00.000000Z'),
                    ('2022-06-01T00:00:00.000000Z', '2024-01-02T04:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");

            execute("ALTER TABLE x ALTER COLUMN val TYPE TIMESTAMP_NS");
            drainWalQueue();

            // After TIMESTAMP(us)->TIMESTAMP_NS, the parquet min/max stats are in
            // microseconds but the filter values are in nanoseconds (1000x larger).
            // The skip path must compare precision in addition to the TIMESTAMP
            // tag; otherwise cross-precision min/max comparisons drop matching
            // row groups.
            assertQuery("SELECT val FROM x WHERE val = '2021-01-01'::TIMESTAMP_NS").noLeakCheck().returns(
                    """
                            val
                            2021-01-01T00:00:00.000000000Z
                            """);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val > '2025-01-01'::TIMESTAMP_NS").noLeakCheck().returns(
                    "val\n");
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    @Test
    public void testRenamedColumnMinMaxEquality() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn: that test covers the bloom-filter
        // equality path; this one covers equality resolved through min/max statistics (no bloom
        // filter on the column). Both share the same prepareFilterList column resolution.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // 25_000 lives in the renamed column's first row group; the stale name maps to the
            // high-range column whose min/max excludes it, which wrongly pruned the match away.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 25_000")
                    .noLeakCheck()
                    .returns("""
                            b
                            25000
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // A value genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 5_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRenamedColumnOrEquality() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the min/max-stats path: an
        // OR-of-equalities on a renamed column must resolve by stable column id.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // 15_000 and 25_000 both live in the renamed column's first row group; the stale
            // name maps to the high-range column whose stats exclude both, so the buggy
            // resolution wrongly pruned that row group and dropped the matches.
            assertQuery("SELECT b FROM x WHERE b = 15_000 OR b = 25_000 ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);

            // Values genuinely absent from the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b = 1_000 OR b = 5_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testRenamedColumnRange() throws Exception {
        // Companion to testBloomFilterSymbolRenamedColumn for the min/max-stats path: range
        // predicates on a renamed column must resolve by stable column id.
        assertMemoryLeak(() -> {
            createRenamedNumericParquetTable();

            // '< 30_000' matches only the renamed column's low row group; the stale name maps to
            // the high-range column whose min is above 30_000, which wrongly pruned the match.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b < 30_000 ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b <= 25_000 ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            b
                            15000
                            25000
                            """);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // A range below every value in the renamed column must still prune.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT b FROM x WHERE b < 10_000")
                    .noLeakCheck()
                    .returns("b\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDoubleColumnExactEqStillPrunesOnBloomFilter() throws Exception {
        // The counterpart of testDoubleColumnToleranceEqPushdownNotFalsePruned: a bound whose
        // neighbouring doubles fall outside the tolerance band matches the exact value and nothing
        // else, so it keeps pushing as an equality and keeps consulting the bloom filter. The group's
        // [min, max] spans the bound, so the bloom filter is the only thing that can prune it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO x VALUES
                    (1000000.5, '2024-01-01T00:00:00.000000Z'),
                    (3000000.5, '2024-01-01T01:00:00.000000Z'),
                    (5000000.5, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQuery("SELECT val FROM x WHERE val = 2000000.5")
                    .noLeakCheck()
                    .returns("val\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQuery("SELECT val FROM x WHERE val = 3000000.5")
                    .noLeakCheck()
                    .returns("""
                            val
                            3000000.5
                            """);
        });
    }

    @Test
    public void testDoubleColumnToleranceEqPushdownNotFalsePruned() throws Exception {
        // Row-level DOUBLE equality is tolerance-based (Numbers.DOUBLE_TOLERANCE, 1e-10), so
        // "c6 = 1.0" keeps the row 1.00000000005. Pushing the exact bound into the row group filter
        // is not: the value falls outside the group's [min, max] (and its bits are absent from the
        // bloom filter), so pruning drops the group before the row filter ever sees it.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("DOUBLE", "1.00000000005", "0.0");

            assertNativeMatchesPartialParquet("c6 = 1.0", "c6\n1.00000000005\n");
            assertNativeMatchesPartialParquet("c6 IN (1.0, 5.0)", "c6\n1.00000000005\n");

            // The tolerance band is the only thing the bound gives up: a bound clear of the group
            // still prunes it.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 = 99.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDoubleColumnToleranceRangePushdownNotFalsePruned() throws Exception {
        // The same tolerance holds for the ops that include equality: "c6 <= 1.0" keeps the row
        // 1.00000000005 and "c6 >= 1.0" keeps 0.99999999995, while the native side prunes on
        // "min > bound" and "max < bound" - the exact bound drops both groups.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("DOUBLE", "1.00000000005", "0.0");
            assertNativeMatchesPartialParquet("c6 <= 1.0", "c6\n1.00000000005\n0.0\n");

            // The strict ops need no widening (the tolerance makes the row filter stricter than the
            // pruner), and they still prune the group they exclude.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.0", "c6\n0.0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            createBoundarySaturatedPartialParquetTyped("DOUBLE", "0.99999999995", "2.0");
            assertNativeMatchesPartialParquet("c6 >= 1.0", "c6\n0.99999999995\n2.0\n");

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 > 1.0", "c6\n2.0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDoubleConstantAlmostIntegralPushdownNotFalsePruned() throws Exception {
        // The integer stats slots widen the column to double and compare with the same tolerance, so
        // a bound within 1e-10 of an integer keeps that integer's rows: "c6 <= 0.99999999999" keeps
        // the row 1. Rounding the bare bound floors it to "c6 <= 0", which prunes the group holding
        // that row; rounding the tolerance-widened bound lands on "c6 <= 1" and keeps it.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(1, -1);
            assertNativeMatchesPartialParquet("c6 <= 0.99999999999", "c6\n1\n-1\n");
            assertNativeMatchesPartialParquet("c6 >= 1.00000000001", "c6\n1\n");

            // The same bound in the 64-bit slot.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createBoundarySaturatedPartialParquetTyped("LONG", "1", "-1");
            assertNativeMatchesPartialParquet("c6 <= 0.99999999999", "c6\n1\n-1\n");
        });
    }

    @Test
    public void testDoubleConstantFractionalPushdownNotFalsePruned() throws Exception {
        // A fractional DOUBLE bound truncates toward zero in the INT stats slot. Truncation
        // is not pruning-safe: "c6 < 1.5" becomes "c6 < 1", which prunes a group whose INT
        // stat is exactly 1 even though that row satisfies 1 < 1.5. Pruning runs before the
        // row filter, so a false-prune drops the parquet row and the partial-parquet table
        // returns fewer rows than its all-native sibling.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(1, 100);

            // Positive fractional bound: strict "<" false-prunes the group at 1 before the fix.
            assertNativeMatchesPartialParquet("c6 < 1.5", "c6\n1\n");
            assertNativeMatchesPartialParquet("c6 <= 1.5", "c6\n1\n");

            // Integral DOUBLE bound stays pushdown-safe: the group at 1 is pruned correctly
            // (row 1 fails 1 < 1.0) and the pushdown still fires -- the fix is surgical.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // Negative fractional bound truncates the other way: "c6 > -1.5" becomes "c6 > -1",
            // which false-prunes a group at -1 even though -1 > -1.5.
            createBoundarySaturatedPartialParquet(-1, -100);
            assertNativeMatchesPartialParquet("c6 > -1.5", "c6\n-1\n");
            assertNativeMatchesPartialParquet("c6 >= -1.5", "c6\n-1\n");
        });
    }

    @Test
    public void testDoubleConstantFractionalPushdownNotFalsePrunedLongColumn() throws Exception {
        // The LONG stats slot truncates a fractional DOUBLE bound via (long) getDouble(),
        // the 64-bit twin of the INT/narrow arms. "c6 < 1.5" -> "c6 < 1" false-prunes a
        // group at 1 even though 1 < 1.5.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("LONG", "1", "100");

            assertNativeMatchesPartialParquet("c6 < 1.5", "c6\n1\n");

            // Integral DOUBLE bound stays pushdown-safe and still prunes.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDoubleConstantAbove2Pow53PushdownNotFalsePrunedLongColumn() throws Exception {
        // There is no (LONG, DOUBLE) comparison: the row-level filter widens the column to DOUBLE and
        // compares at double width, while row group pruning compares the stats at long width. The two
        // agree only below 2^53, where a double still represents every integer exactly. Above it the
        // pruner is the finer of the two and skips a group whose rows the filter keeps:
        // (double) 10000000000000001 is exactly 1e16, so "c6 <= 1e16" and "c6 = 1e16" both keep that
        // row, while the pushed bound (long) 1e16 == 10000000000000000 excludes the group.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("LONG", "10_000_000_000_000_001", "0");

            assertNativeMatchesPartialParquet("c6 <= 1e16", "c6\n10000000000000001\n0\n");
            assertNativeMatchesPartialParquet("c6 = 1e16", "c6\n10000000000000001\n");
            assertNativeMatchesPartialParquet("c6 = 1e16::float", "c6\n10000000000000001\n");

            // A bound below 2^53 stays exact at double width, so it still pushes down and prunes.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1000.0", "c6\n0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testDoubleConstantOutsideIntRangePushdownNotFalsePruned() throws Exception {
        // An out-of-INT-range DOUBLE bound saturates to INT_MAX in the 32-bit stats slot,
        // the (int) getDouble() twin of the LONG-bound saturation. "c6 < 5e9" saturates to
        // "c6 < INT_MAX" and false-prunes an all-INT_MAX group whose rows satisfy the filter.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(2_147_483_647, 0);

            assertNativeMatchesPartialParquet("c6 < 5000000000.0", "c6\n2147483647\n0\n");

            // Control: no INT value exceeds the bound; empty result, group may prune.
            assertNativeMatchesPartialParquet("c6 > 5000000000.0", "c6\n");
        });
    }

    @Test
    public void testDoubleConstantPushdownTimestampAndDateColumns() throws Exception {
        // getLong() throws UnsupportedOperationException on a FLOAT/DOUBLE function, so a double bound
        // against a TIMESTAMP or DATE column threw inside the filter builder. The catch-all swallowed
        // it, logged an error and dropped row group pruning for every condition in the query. Both arms
        // now take the same double guard as the LONG arm: an exact in-range bound prunes, and a bound
        // the column cannot round-trip through DOUBLE declines instead of false-pruning.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("TIMESTAMP", "1_704_067_200_000_000", "0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.7e15", "c6\n1970-01-01T00:00:00.000000Z\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            // A FLOAT bound reaches the same arm through getDouble().
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.7e15::float", "c6\n1970-01-01T00:00:00.000000Z\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // Above 2^53 the row-level filter compares at double width and keeps the row, so the
            // pushdown must decline rather than prune the group at long width.
            createBoundarySaturatedPartialParquetTyped("TIMESTAMP", "10_000_000_000_000_001", "0");
            assertNativeMatchesPartialParquet("c6 <= 1e16", "c6\n2286-11-20T17:46:40.000001Z\n1970-01-01T00:00:00.000000Z\n");
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            createBoundarySaturatedPartialParquetTyped("DATE", "1_704_067_200_000", "0");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.7e12", "c6\n1970-01-01T00:00:00.000Z\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            createBoundarySaturatedPartialParquetTyped("DATE", "10_000_000_000_000_001", "0");
            assertNativeMatchesPartialParquet("c6 <= 1e16", "c6\n318857-05-20T17:46:40.001Z\n1970-01-01T00:00:00.000Z\n");
        });
    }

    @Test
    public void testFloatColumnPushdownNotFalsePruned() throws Exception {
        // A FLOAT column's stats slot is 32-bit, but there is no (FLOAT, DOUBLE) comparison: the
        // row-level filter widens the column to DOUBLE and compares at double width (the only
        // comparison factories are the double ones, e.g. LtDoubleVVFunctionFactory "<(DD)"). The
        // FLOAT arm narrowed the bound with (float) getDouble(), which rounds to NEAREST - and
        // nearest is not pruning-safe in either direction:
        //   "<"  needs the SMALLEST float >= the bound, but nearest can round DOWN, moving the
        //        bound onto a group's boundary float and pruning rows the filter keeps;
        //   ">"  needs the LARGEST float <= the bound, but nearest can round UP, likewise.
        // ("<=" and ">=" happen to survive nearest: rounding the wrong way only makes them prune
        // less.) Pruning runs before the row filter, so a false-prune drops the parquet rows
        // outright and the partial-parquet table returns fewer rows than its all-native sibling.
        assertMemoryLeak(() -> {
            // The parquet group holds the single float 1.0; the native sibling row holds 100.0.
            createBoundarySaturatedPartialParquetTyped("FLOAT", "1.0", "100.0");

            // (float) 1.00000003 rounds DOWN to 1.0f (the next float up is 1.00000011920928955),
            // so "< 1.00000003" pushed as "< 1.0f" prunes the group whose min stat is 1.0f - yet
            // (double) 1.0f = 1.0 < 1.00000003 keeps the row.
            assertNativeMatchesPartialParquet("c6 < 1.00000003", "c6\n1.0\n");
            // The mirror image: (float) 0.99999998 rounds UP to 1.0f (the next float down is
            // 0.99999994039535522), so "> 0.99999998" pushed as "> 1.0f" prunes a group whose max
            // stat is 1.0f - yet (double) 1.0f = 1.0 > 0.99999998 keeps the row.
            assertNativeMatchesPartialParquet("c6 > 0.99999998", "c6\n1.0\n100.0\n");

            // The two ops nearest already served: they must keep selecting the same rows.
            assertNativeMatchesPartialParquet("c6 <= 1.00000003", "c6\n1.0\n");
            assertNativeMatchesPartialParquet("c6 >= 0.99999998", "c6\n1.0\n100.0\n");

            // An exactly-representable bound loses nothing and must still prune: 1.0 is a float,
            // so "< 1.0" excludes the group at 1.0 outright.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 > 100.0", "c6\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // An integer bound has no exact float above 2^24 either: 16777217 sits halfway between
            // 16777216f and 16777218f and rounds to even, i.e. DOWN to 16777216f. "< 16777217"
            // then prunes the group whose min stat is 16777216f, though that row satisfies it.
            createBoundarySaturatedPartialParquetTyped("FLOAT", "16777216.0", "0.0");
            assertNativeMatchesPartialParquet("c6 < 16_777_217", "c6\n1.6777216E7\n0.0\n");
            assertNativeMatchesPartialParquet("c6 >= 16_777_217", "c6\n");

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // The bound must also carry the comparison TOLERANCE. QuestDB compares floating point
            // with Numbers.DOUBLE_TOLERANCE (1e-10), so "c6 >= d" keeps a row that is merely
            // tolerance-equal to d - a row strictly BELOW it. Rounding to the nearest float ignores
            // that, and so does rounding by an ulp: one float ulp near 1.0 is 1.2e-7, over a
            // thousand times the tolerance, so such a bound steps clean over the band and prunes the
            // group holding the row. Every bound below sits inside the tolerance band around 1.0.
            createBoundarySaturatedPartialParquetTyped("FLOAT", "1.0", "100.0");
            assertNativeMatchesPartialParquet("c6 >= 1.00000000005", "c6\n1.0\n100.0\n");
            assertNativeMatchesPartialParquet("c6 <= 0.99999999995", "c6\n1.0\n");
            assertNativeMatchesPartialParquet("c6 > 0.99999999995", "c6\n100.0\n");
            assertNativeMatchesPartialParquet("c6 < 1.00000000005", "c6\n");
            assertNativeMatchesPartialParquet("c6 = 1.00000000005", "c6\n1.0\n");

            // A bound ONE TOLERANCE away from the group's float is the hard case for the strict ops.
            // The row is 1.0000000827e-10 from the bound - just OUTSIDE the tolerance, so the filter
            // keeps it - but "d - tolerance" rounds back onto 1.0 exactly (the residual is far below
            // half a double ulp), so a bound narrowed from that pivot lands on 1.0f, and the native
            // predicate ("<" prunes on min >= bound) drops the group holding it.
            assertNativeMatchesPartialParquet("c6 < 1.0000000001", "c6\n1.0\n");
            assertNativeMatchesPartialParquet("c6 > 0.9999999999", "c6\n1.0\n100.0\n");
            assertNativeMatchesPartialParquet("c6 < 100.0000000001", "c6\n1.0\n100.0\n");
            assertNativeMatchesPartialParquet("c6 > 99.9999999999", "c6\n100.0\n");
            // The non-strict ops prune on min > bound / max < bound, so the bound itself is already
            // excluded from the pruned side, and they must not step the same way.
            assertNativeMatchesPartialParquet("c6 <= 0.9999999999", "c6\n");
            assertNativeMatchesPartialParquet("c6 >= 1.0000000001", "c6\n100.0\n");

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // Near zero the tolerance band spans many floats, so the bound has to be walked out of
            // it - or abandoned. A row of 0.0f is tolerance-equal to a bound of -5e-11, so the
            // filter keeps it for "<=" and the pruner must not skip the group.
            createBoundarySaturatedPartialParquetTyped("FLOAT", "0.0", "5.0");
            assertNativeMatchesPartialParquet("c6 <= -5e-11", "c6\n0.0\n");
            assertNativeMatchesPartialParquet("c6 >= 5e-11", "c6\n0.0\n5.0\n");

            // EQ has no direction to round in: it prunes when the pushed float falls outside
            // [min, max]. Below ~8e-4 the tolerance band holds several floats, so the group can hold
            // a matching row that is not the nearest one - 4.9999997E-4 is tolerance-equal to 0.0005
            // (8.2e-11 away) though 5.0E-4 is what a bound of 0.0005 narrows to. Pushing the nearest
            // float would prune this group; the bound declines instead.
            execute("DROP TABLE tn");
            execute("DROP TABLE tp");
            createBoundarySaturatedPartialParquetTyped("FLOAT", "4.9999997E-4", "5.0");
            assertNativeMatchesPartialParquet("c6 = 0.0005", "c6\n4.9999997E-4\n");
            // ... and a bound whose band holds exactly one float still pushes, and still prunes.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 = 5.0", "c6\n5.0\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testFloatConstantFractionalPushdownNotFalsePruned() throws Exception {
        // A fractional FLOAT bound truncates in the narrow (SHORT) stats slot via
        // (int) getDouble(), the FLOAT twin of the DOUBLE arm. "c6 < 1.5" -> "c6 < 1"
        // false-prunes a SHORT group at 1.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquetTyped("SHORT", "1", "100");

            assertNativeMatchesPartialParquet("c6 < 1.5::float", "c6\n1\n");
            assertNativeMatchesPartialParquet("c6 <= 1.5::float", "c6\n1\n");
        });
    }

    @Test
    public void testFractionalDoubleBoundStillPrunes() throws Exception {
        // The fix for the fractional false-prune must not surrender pruning: a fractional bound
        // rounds the way its op preserves ("c6 < 1.5" is "c6 < 2", "c6 > 4.5" is "c6 > 4"), which
        // selects the same rows as the double comparison and still skips a group that lies wholly
        // outside it.
        assertMemoryLeak(() -> {
            // Parquet group saturated at 5, native row at 1: "< 1.5" ("< 2") must skip the group.
            createBoundarySaturatedPartialParquet(5, 1);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.5", "c6\n1\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 <= 1.5", "c6\n1\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // Parquet group saturated at 1, native row at 5: "> 4.5" ("> 4") must skip the group.
            createBoundarySaturatedPartialParquet(1, 5);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 > 4.5", "c6\n5\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 >= 4.5", "c6\n5\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            execute("DROP TABLE tn");
            execute("DROP TABLE tp");

            // The 64-bit stats slot rounds the same way, below the 2^53 precision ceiling.
            createBoundarySaturatedPartialParquetTyped("LONG", "5", "1");
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertNativeMatchesPartialParquet("c6 < 1.5", "c6\n1\n");
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        });
    }

    @Test
    public void testLongConstantBelowIntRangePushdownNotFalsePruned() throws Exception {
        // Parquet partition saturated at INT_MIN+1 (-2147483647). A below-INT-range
        // LONG bound saturates in the INT stats slot; c > -5e9 matches every row, so
        // the group must NOT prune -- a false-prune would drop the parquet row.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(-2_147_483_647, 0);

            assertNativeMatchesPartialParquet("c6 > -5_000_000_000", "c6\n-2147483647\n0\n");
            assertNativeMatchesPartialParquet("c6 >= -5_000_000_000", "c6\n-2147483647\n0\n");

            // The other direction is unsatisfiable for every INT row, so every group prunes: "<"
            // pushes the saturated INT_MIN bound (min >= INT_MIN always holds) and "<=" rewrites
            // to it. The DOUBLE spelling of the bound takes the same route.
            for (String bound : new String[]{"-5_000_000_000", "-5e9"}) {
                for (String op : new String[]{"<", "<="}) {
                    ParquetRowGroupFilter.resetRowGroupsSkipped();
                    assertNativeMatchesPartialParquet("c6 " + op + ' ' + bound, "c6\n");
                    Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
                }
            }
        });
    }

    @Test
    public void testLongConstantOutsideIntRangePushdownNotFalsePruned() throws Exception {
        // Parquet partition saturated at INT_MAX (2147483647). An above-INT-range LONG
        // bound saturates in the INT stats slot; c < 5e9 matches every row, so the
        // group must NOT prune -- a false-prune would drop the parquet row.
        assertMemoryLeak(() -> {
            createBoundarySaturatedPartialParquet(2_147_483_647, 0);

            assertNativeMatchesPartialParquet("c6 < 5_000_000_000", "c6\n2147483647\n0\n");
            assertNativeMatchesPartialParquet("c6 <= 5_000_000_000", "c6\n2147483647\n0\n");

            // The other direction is unsatisfiable for every INT row, so every group prunes: ">"
            // pushes the saturated INT_MAX bound (max <= INT_MAX always holds) and ">=" rewrites
            // to it. The DOUBLE spelling of the bound takes the same route.
            for (String bound : new String[]{"5_000_000_000", "5e9"}) {
                for (String op : new String[]{">", ">="}) {
                    ParquetRowGroupFilter.resetRowGroupsSkipped();
                    assertNativeMatchesPartialParquet("c6 " + op + ' ' + bound, "c6\n");
                    Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
                }
            }
        });
    }

    @Test
    public void testOverflowingIntConstantPushdownWrapsToInt() throws Exception {
        // An overflowing INT constant compared against a narrow column (BYTE/SHORT/INT)
        // must wrap mod 2^32 in the parquet pushdown like the native INT-precision scan.
        // Before the fix the pushdown read the un-wrapped LONG and wrongly pruned a
        // parquet partition, so a partial-parquet table disagreed with its native sibling.
        //   (-2649 * 965_823) wraps to INT +1_736_502_169 (> any row): const>col matches all.
        //   ( 2649 * 965_823) wraps to INT -1_736_502_169 (< any row): col>const matches all.
        assertMemoryLeak(() -> {
            for (String type : new String[]{"BYTE", "SHORT", "INT"}) {
                createNativeAndPartialParquetNarrowColumn(type);

                // Positive wrap: every row passes; wrongly pruned on HEAD.
                assertNativeMatchesPartialParquet("(-2649::SHORT * (965_823)::INT) > c6", "c6\n1\n2\n");
                assertNativeMatchesPartialParquet("c6 < (-2649::SHORT * (965_823)::INT)", "c6\n1\n2\n");
                // Negative wrap: every row passes; wrongly pruned on HEAD from the other side.
                assertNativeMatchesPartialParquet("c6 > (2649::SHORT * (965_823)::INT)", "c6\n1\n2\n");

                // Controls: the wrapped constant excludes every row (pass on HEAD too).
                assertNativeMatchesPartialParquet("c6 > (-2649::SHORT * (965_823)::INT)", "c6\n");
                assertNativeMatchesPartialParquet("c6 < (2649::SHORT * (965_823)::INT)", "c6\n");

                execute("DROP TABLE tn");
                execute("DROP TABLE tp");
            }
        });
    }

    // All-native tn and a partial-parquet sibling tp with identical data: two single-row
    // daily partitions, the first parquet so the row-group pushdown decides to scan it.
    private void createNativeAndPartialParquetNarrowColumn(String columnType) throws Exception {
        execute("CREATE TABLE tn (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE tp (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO tn VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-02T00:00:00.000000Z')");
        execute("INSERT INTO tp VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    // All-native tn and a partial-parquet sibling tp with identical data. The first daily
    // partition (single row = parquetValue) converts to parquet, so its INT stats are
    // min == max == parquetValue -- a group saturated at that exact value. The second row
    // (nativeValue) stays native so the pushdown actually scans the parquet partition.
    private void createBoundarySaturatedPartialParquet(int parquetValue, int nativeValue) throws Exception {
        createBoundarySaturatedPartialParquetTyped("INT", Integer.toString(parquetValue), Integer.toString(nativeValue));
    }

    // Typed variant of createBoundarySaturatedPartialParquet: the column type and the two row
    // values are supplied as text so the same single-row-parquet-group setup can exercise the
    // INT, narrow (BYTE/SHORT) and LONG stats slots.
    private void createBoundarySaturatedPartialParquetTyped(String columnType, String parquetValue, String nativeValue) throws Exception {
        execute("CREATE TABLE tn (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE tp (c6 " + columnType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO tn VALUES (" + parquetValue + ", '2024-01-01T00:00:00.000000Z'), (" + nativeValue + ", '2024-01-02T00:00:00.000000Z')");
        execute("INSERT INTO tp VALUES (" + parquetValue + ", '2024-01-01T00:00:00.000000Z'), (" + nativeValue + ", '2024-01-02T00:00:00.000000Z')");
        execute("ALTER TABLE tp CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    private void assertNativeMatchesPartialParquet(String whereClause, String expected) throws Exception {
        assertQuery("SELECT c6 FROM tn WHERE " + whereClause + " ORDER BY ts")
                .noLeakCheck()
                .returns(expected);
        assertQuery("SELECT c6 FROM tp WHERE " + whereClause + " ORDER BY ts")
                .noLeakCheck()
                .returns(expected);
    }

    private void assertHasParquetPartitions(String tableName, boolean expected) {
        TableToken tableToken = engine.verifyTableName(tableName);
        try (MetadataCacheReader reader = engine.getMetadataCache().readLock()) {
            CairoTable table = reader.getTable(tableToken);
            Assert.assertNotNull(table);
            Assert.assertEquals(expected, table.hasParquetPartitions());
        }
    }

    // Builds a parquet table whose live column 'b' is the original low-range column 'a', while
    // the frozen parquet column still literally named 'b' is the original high-range column.
    // Resolving a filter by the stale parquet name targets the wrong column's stats and wrongly
    // prunes; resolving by stable column id keeps it correct. The third partition stays active so
    // the first two convert to parquet.
    private void createRenamedNumericParquetTable() throws Exception {
        execute("CREATE TABLE x (a INT, b INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO x VALUES
                (15_000, 75_000, '2024-01-01T00:00:00.000000Z'),
                (25_000, 85_000, '2024-01-01T01:00:00.000000Z'),
                (35_000, 95_000, '2024-01-02T00:00:00.000000Z'),
                (45_000, 99_000, '2024-01-02T01:00:00.000000Z'),
                (55_000, 65_000, '2024-01-03T00:00:00.000000Z')
                """);
        execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0");
        execute("ALTER TABLE x RENAME COLUMN b TO c");
        execute("ALTER TABLE x RENAME COLUMN a TO b");
    }
}
