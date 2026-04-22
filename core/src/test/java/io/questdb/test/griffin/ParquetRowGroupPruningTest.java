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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = -991 ORDER BY ts DESC",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            42
                            """,
                    "SELECT val FROM x WHERE val = 42 ORDER BY ts DESC",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val IN (-1, -2, -3) ORDER BY ts DESC",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            2001
                            1
                            """,
                    "SELECT val FROM x WHERE val IN (1, 2001) ORDER BY ts DESC",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", -991);
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = :v ORDER BY ts DESC", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 1);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", 42);
            assertQueryNoLeakCheck("val\n42\n", "SELECT val FROM x WHERE val = :v ORDER BY ts DESC", null, true, false);
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 50::byte",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            10
                            """,
                    "SELECT val FROM x WHERE val = 10::byte",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 50::SHORT", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck("val\n10\n", "SELECT val FROM x WHERE val = 10::SHORT", null, true, false);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 50", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck("val\n10\n", "SELECT val FROM x WHERE val = 10", null, true, false);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 50::LONG", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck("val\n10\n", "SELECT val FROM x WHERE val = 10::LONG", null, true, false);
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 'G'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            M
                            """,
                    "SELECT val FROM x WHERE val = 'M'",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '2020-03-15'::DATE",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            2020-06-15T00:00:00.000Z
                            """,
                    "SELECT val FROM x WHERE val = '2020-06-15'::DATE",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    """
                            val
                            2020-06-15T00:00:00.000Z
                            """,
                    "SELECT val FROM x WHERE val = '2020-06-15T00:00:00.000000001Z'",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '2500000000000.25'::DECIMAL(30,2)",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            5000000000000.50
                            """,
                    "SELECT val FROM x WHERE val = '5000000000000.50'::DECIMAL(30,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '30.30'::DECIMAL(4,2)",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            50.50
                            """,
                    "SELECT val FROM x WHERE val = '50.50'::DECIMAL(4,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '250000000000000000000.25'::DECIMAL(50,2)",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            500000000000000000000.50
                            """,
                    "SELECT val FROM x WHERE val = '500000000000000000000.50'::DECIMAL(50,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '25000.25'::DECIMAL(8,2)",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            50000.50
                            """,
                    "SELECT val FROM x WHERE val = '50000.50'::DECIMAL(8,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '2500000.25'::DECIMAL(15,2)",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            5000000.50
                            """,
                    "SELECT val FROM x WHERE val = '5000000.50'::DECIMAL(15,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '3.3'::DECIMAL(2,1)",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            5.5
                            """,
                    "SELECT val FROM x WHERE val = '5.5'::DECIMAL(2,1)",
                    null, true, false
            );
        });
    }

    @Test
    public void testBloomFilterDouble() throws Exception {
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 3.33",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            5.55
                            """,
                    "SELECT val FROM x WHERE val = 5.55",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 3.0",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            5.0
                            """,
                    "SELECT val FROM x WHERE val = 5.0::FLOAT",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '5.5.5.5'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            10.0.0.1
                            """,
                    "SELECT val FROM x WHERE val = '10.0.0.1'",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            
                            """,
                    "SELECT val FROM x WHERE val in (NULL)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 25_000",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            50000
                            """,
                    "SELECT val FROM x WHERE val = 50_000",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 25_000::LONG", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck("val\n50000\n", "SELECT val FROM x WHERE val = 50_000::LONG", null, true, false);
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
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = :v", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", 50_000);
            assertQueryNoLeakCheck("val\n50000\n", "SELECT val FROM x WHERE val = :v", null, true, false);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt(0, 25_000);
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = $1", null, true, false);
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val IN (2, 3, 4)",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            1
                            """,
                    "SELECT val FROM x WHERE val IN (1, 25_000)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 250_000",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            500000
                            """,
                    "SELECT val FROM x WHERE val = 500_000",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = to_long128(0, 25)",
                    null, true, false
            );


            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            00000000-0000-0032-0000-000000000000
                            """,
                    "SELECT val FROM x WHERE val = to_long128(0, 50)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "a\tb\n",
                    "SELECT a, b FROM x WHERE a = 25 AND b = 'ggg'",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "a\tb\n",
                    "SELECT a, b FROM x WHERE a = 1 AND b = 'ggg'",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            a\tb
                            50\tmmm
                            """,
                    "SELECT a, b FROM x WHERE a = 50 AND b = 'mmm'",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 501",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            200
                            """,
                    "SELECT val FROM x WHERE val = 200",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 501", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck("val\n200\n", "SELECT val FROM x WHERE val = 200", null, true, false);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 501::LONG", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck("val\n200\n", "SELECT val FROM x WHERE val = 200::LONG", null, true, false);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = 501::SHORT", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck("val\n200\n", "SELECT val FROM x WHERE val = 200::SHORT", null, true, false);
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 'ggg'",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            mmm
                            """,
                    "SELECT val FROM x WHERE val = 'mmm'",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 'delta'",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            gamma
                            """,
                    "SELECT val FROM x WHERE val = 'gamma'",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '2020-03-15T00:00:00.000000Z'::TIMESTAMP",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            2020-06-15T12:00:00.000000Z
                            """,
                    "SELECT val FROM x WHERE val = '2020-06-15T12:00:00.000000Z'::TIMESTAMP",
                    null, true, false
            );

            assertQueryNoLeakCheck(
                    """
                            val
                            2020-06-15T12:00:00.000000Z
                            """,
                    "SELECT val FROM x WHERE val = '2020-06-15T12:00:00.000000Z'::TIMESTAMP_NS",
                    null, true, false
            );

            assertQueryNoLeakCheck(
                    """
                            val
                            2020-01-01T00:00:00.000000Z
                            """,
                    "SELECT val FROM x WHERE val = '2020-01-01'::Date",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '33333333-3333-3333-3333-333333333334'",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            55555555-5555-5555-5555-555555555555
                            """,
                    "SELECT val FROM x WHERE val = '55555555-5555-5555-5555-555555555555'",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '33333333-3333-3333-3333-333333333334'::UUID",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQueryNoLeakCheck(
                    """
                            val
                            55555555-5555-5555-5555-555555555555
                            """,
                    "SELECT val FROM x WHERE val = '55555555-5555-5555-5555-555555555555'::UUID",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = NULL::UUID",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val IN ('33333333-3333-3333-3333-333333333334', '44444444-4444-4444-4444-444444444444')",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQueryNoLeakCheck(
                    """
                            val
                            11111111-1111-1111-1111-111111111111
                            99999999-9999-9999-9999-999999999999
                            """,
                    "SELECT val FROM x WHERE val IN ('11111111-1111-1111-1111-111111111111', '99999999-9999-9999-9999-999999999999')",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 'ghi'",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            val
                            ❤️
                            """,
                    "SELECT val FROM x WHERE val = '❤️'",
                    null, true, false
            );
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
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = :v", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setStr("v", "❤️");
            assertQueryNoLeakCheck("val\n❤️\n", "SELECT val FROM x WHERE val = :v", null, true, false);
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
                    (6, '2024-01-02T01:00:00.000000Z', 10000)
                    """);
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts >= 0 WITH (bloom_filter_columns = 'val')");

            assertQueryNoLeakCheck(
                    "id\tval\n",
                    "SELECT id, val FROM x WHERE val = 50",
                    null, true, false
            );

            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            4\t100
                            """,
                    "SELECT id, val FROM x WHERE val = 100",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\tnull
                            2\tnull
                            """,
                    "SELECT id, val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            id\tval
                            """,
                    "SELECT id, val FROM x WHERE val = 1.0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQueryNoLeakCheck(
                    """
                            id\tval
                            3\t1.11
                            """,
                    "SELECT id, val FROM x WHERE val = 1.11",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\tnull
                            2\tnull
                            """,
                    "SELECT id, val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            id\tval
                            4\t100
                            """,
                    "SELECT id, val FROM x WHERE val = 100",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\tnull
                            2\tnull
                            3\tnull
                            """,
                    "SELECT id, val FROM x WHERE val = null",
                    null, true, false
            );

            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
            assertQueryNoLeakCheck(
                    "id\tval\n",
                    "SELECT id, val FROM x WHERE val = 999",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            id\tval
                            3\t100000
                            """,
                    "SELECT id, val FROM x WHERE val = 100_000",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\tnull
                            2\tnull
                            """,
                    "SELECT id, val FROM x WHERE val = null",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            """,
                    "SELECT id, val FROM x WHERE val = 'aaa'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            4\thello
                            """,
                    "SELECT id, val FROM x WHERE val = 'hello'",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t
                            2\t
                            3\t
                            """,
                    "SELECT id, val FROM x WHERE val = null",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            """,
                    "SELECT id, val FROM x WHERE val = 'aaa'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            assertQueryNoLeakCheck(
                    """
                            id\tval
                            3\tabc
                            """,
                    "SELECT id, val FROM x WHERE val = 'abc'",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    """
                            id\tval
                            1\t
                            2\t
                            """,
                    "SELECT id, val FROM x WHERE val = null",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "cnt\n19\n",
                    "SELECT count() AS cnt FROM x WHERE val > 100 AND val < 120",
                    null, false, true
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > 10_000 AND val IS NOT NULL",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            """,
                    "SELECT val FROM x WHERE val IN (null, 3.34)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            3
                            """,
                    "SELECT val FROM x WHERE val IN (null, 3)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            """,
                    "SELECT val FROM x WHERE val IN (null)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            """,
                    "SELECT val FROM x WHERE val IN (null, 99)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            300000
                            """,
                    "SELECT val FROM x WHERE val IN (null, 300_000)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            
                            """,
                    "SELECT val FROM x WHERE val IN (null, 'cccd')",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            
                            world
                            """,
                    "SELECT val FROM x WHERE val IN (null, 'world')",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "cnt\n50\n",
                    "SELECT count() AS cnt FROM x WHERE val IS NOT NULL",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    """
                            val
                            42
                            """,
                    "SELECT val FROM x WHERE val IS NOT NULL",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val IS NULL",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "cnt\n50\n",
                    "SELECT count() AS cnt FROM x WHERE val IS NULL",
                    null, false, true
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            2
                            """,
                    "SELECT val FROM x WHERE val = 2",
                    null, true, false
            );
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = -1",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            C
                            """,
                    "SELECT val FROM x WHERE val = 'C'",
                    null, true, false
            );

            assertQueryNoLeakCheck(
                    """
                            val
                            """,
                    "SELECT val FROM x WHERE val = 'c'",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            2020-06-01T00:00:00.000Z
                            """,
                    "SELECT val FROM x WHERE val = '2020-06-01'::DATE",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '2099-01-01'::DATE",
                    null, true, false
            );

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

            assertQueryNoLeakCheck(
                    """
                            val
                            5000000000000.50
                            """,
                    "SELECT val FROM x WHERE val = '5000000000000.50'::DECIMAL(30,2)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '100.10'::DECIMAL(30,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            50.50
                            """,
                    "SELECT val FROM x WHERE val = '50.50'::DECIMAL(4,2)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '1.01'::DECIMAL(4,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            500000000000000000000.50
                            """,
                    "SELECT val FROM x WHERE val = '500000000000000000000.50'::DECIMAL(50,2)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '10.10'::DECIMAL(50,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            50000.50
                            """,
                    "SELECT val FROM x WHERE val = '50000.50'::DECIMAL(8,2)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '100.10'::DECIMAL(8,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            5000000.50
                            """,
                    "SELECT val FROM x WHERE val = '5000000.50'::DECIMAL(15,2)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '100.10'::DECIMAL(15,2)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            5.5
                            """,
                    "SELECT val FROM x WHERE val = '5.5'::DECIMAL(2,1)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '0.1'::DECIMAL(2,1)",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
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

            assertQueryNoLeakCheck(
                    """
                            val
                            3.33
                            """,
                    "SELECT val FROM x WHERE val = 3.33",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 99.99",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            3.5
                            """,
                    "SELECT val FROM x WHERE val = 3.5::FLOAT",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 99.9::FLOAT",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
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
            assertQueryNoLeakCheck(
                    """
                            val
                            10.0.0.1
                            """,
                    "SELECT val FROM x WHERE val = '10.0.0.1'",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '1.1.1.0'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            
                            """,
                    "SELECT val FROM x WHERE val in (NULL)",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            192.168.1.1
                            192.168.1.2
                            """,
                    "SELECT val FROM x WHERE val IN ('192.168.1.1', '192.168.1.2') ORDER BY val",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            30000
                            """,
                    "SELECT val FROM x WHERE val = 30_000",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 99_999",
                    null, true, false
            );
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
            assertQueryNoLeakCheck("val\n30000\n", "SELECT val FROM x WHERE val = :v", null, true, false);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt("v", 99_999);
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = :v", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            bindVariableService.clear();
            bindVariableService.setInt(0, 99_999);
            assertQueryNoLeakCheck("val\n", "SELECT val FROM x WHERE val = $1", null, true, false);
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

            assertQueryNoLeakCheck(
                    """
                            val
                            10000
                            50000
                            """,
                    "SELECT val FROM x WHERE val IN (10_000, 50_000)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val IN (99_998, 99_999)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            300000
                            """,
                    "SELECT val FROM x WHERE val = 300_000",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 999_999",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            00000000-0000-0032-0000-000000000000
                            """,
                    "SELECT val FROM x WHERE val = to_long128(0, 50)",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = to_long128(0, 999)",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            300
                            """,
                    "SELECT val FROM x WHERE val = 300",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 999",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            ccc
                            """,
                    "SELECT val FROM x WHERE val = 'ccc'",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 'aaa'",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            gamma
                            """,
                    "SELECT val FROM x WHERE val = 'gamma'",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 'aa'",
                    null, true, false
            );

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

            assertQueryNoLeakCheck(
                    """
                            val
                            2020-06-01T00:00:00.000000Z
                            """,
                    "SELECT val FROM x WHERE val = '2020-06-01T00:00:00.000000Z'::TIMESTAMP",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = '2099-01-01T00:00:00.000000Z'::TIMESTAMP",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            22222222-2222-2222-2222-222222222222
                            """,
                    "SELECT val FROM x WHERE val = '22222222-2222-2222-2222-222222222222'",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 'ffffffff-ffff-ffff-ffff-ffffffffffff'",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            foo
                            """,
                    "SELECT val FROM x WHERE val = 'foo'",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 'aaa'",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            2
                            """,
                    "SELECT val FROM x WHERE val = 2",
                    null, true, false
            );
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            5
                            """,
                    "SELECT val FROM x WHERE val = 5",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            a\tb
                            1\tbbb
                            """,
                    "SELECT a, b FROM x WHERE a = 1 AND b = 'bbb'",
                    null, true, false
            );
            assertQueryNoLeakCheck(
                    "a\tb\n",
                    "SELECT a, b FROM x WHERE a = 99 AND b = 'zzz'",
                    null, true, false
            );

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

            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 42",
                    null, true, false
            );

            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            null
                            null
                            null
                            """,
                    "SELECT val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            0
                            """,
                    "SELECT val FROM x WHERE val = 0",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            """,
                    "SELECT val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            """,
                    "SELECT val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            """,
                    "SELECT val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            """,
                    "SELECT val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            0
                            """,
                    "SELECT val FROM x WHERE val = 0",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            
                            """,
                    "SELECT val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            
                            """,
                    "SELECT val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            val
                            
                            """,
                    "SELECT val FROM x WHERE val = null",
                    null, true, false
            );
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

            assertQueryNoLeakCheck(
                    """
                            a\tb
                            1\t10
                            5\t50
                            """,
                    "SELECT a, b FROM x WHERE a = 1 OR b = 50",
                    null, true, false
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            a\tb
                            1\t10
                            """,
                    "SELECT a, b FROM x WHERE a = 1 AND (b = 10 OR b = 99)",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = -1 OR val = -2",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            42
                            43
                            """,
                    "SELECT val FROM x WHERE val = 42 OR val = 43",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = -1 OR val = -2 OR val = -3",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            100
                            """,
                    "SELECT val FROM x WHERE val = -1 OR val = 100 OR val = -3",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = -1 OR ts = '2099-01-01T00:00:00.000000Z'",
                    null, true, false
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n150\n",
                    "SELECT count() AS cnt FROM x WHERE val = 1 OR val > 0",
                    null, false, true
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE (val = -1 OR val = -2) AND val > 0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // OR with IS NULL on non-nullable data → all absent, skip all
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = -1 OR val IS NULL",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            // OR: matching value + IS NULL on non-nullable data → returns matching rows
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            42
                            """,
                    "SELECT val FROM x WHERE val = 42 OR val IS NULL",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "name\n",
                    "SELECT name FROM x WHERE name = 'xyz' OR name = 'unknown'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            name
                            bob
                            """,
                    "SELECT name FROM x WHERE name = 'xyz' OR name = 'bob'",
                    null, true, false
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "name\n",
                    "SELECT name FROM x WHERE name = 'xyz' OR name IS NULL",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            name
                            bob
                            """,
                    "SELECT name FROM x WHERE name = 'bob' OR name IS NULL",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            null
                            """,
                    "SELECT val FROM x WHERE val = -1 OR val IS NULL",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            null
                            42
                            """,
                    "SELECT val FROM x WHERE val = 42 OR val IS NULL",
                    null, true, false
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            null
                            null
                            """,
                    "SELECT val FROM x WHERE val = -999 OR val IS NULL",
                    null, true, false
            );
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

            assertQueryNoLeakCheck("v_byte\n", "SELECT v_byte FROM x WHERE v_byte = 99::byte", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_short\n", "SELECT v_short FROM x WHERE v_short = 999::short", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_char\n", "SELECT v_char FROM x WHERE v_char = 'M'", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_int\n", "SELECT v_int FROM x WHERE v_int = 99_999", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_long\n", "SELECT v_long FROM x WHERE v_long = 999_999", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_float\n", "SELECT v_float FROM x WHERE v_float = 99.9", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_double\n", "SELECT v_double FROM x WHERE v_double = 99.99", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_string\n", "SELECT v_string FROM x WHERE v_string = 'nnn'", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_varchar\n", "SELECT v_varchar FROM x WHERE v_varchar = 'omega'", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_symbol\n", "SELECT v_symbol FROM x WHERE v_symbol = 'sym99'", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_date\n", "SELECT v_date FROM x WHERE v_date = '2099-01-01'::DATE", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_timestamp\n", "SELECT v_timestamp FROM x WHERE v_timestamp = '2099-01-01T00:00:00.000000Z'::TIMESTAMP", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_uuid\n", "SELECT v_uuid FROM x WHERE v_uuid = '99999999-9999-9999-9999-999999999999'::UUID", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_ipv4\n", "SELECT v_ipv4 FROM x WHERE v_ipv4 = '99.99.99.99'", null, true, false);
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck("v_int\n3000\n", "SELECT v_int FROM x WHERE v_int = 3000", null, true, false);
            assertQueryNoLeakCheck("v_string\nccc\n", "SELECT v_string FROM x WHERE v_string = 'ccc'", null, true, false);
            assertQueryNoLeakCheck("v_uuid\n33333333-3333-3333-3333-333333333333\n", "SELECT v_uuid FROM x WHERE v_uuid = '33333333-3333-3333-3333-333333333333'::UUID", null, true, false);
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

                assertQueryNoLeakCheck(
                        "val\n",
                        "SELECT val FROM x WHERE val = 99",
                        null, true, false
                );
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

            assertQueryNoLeakCheck(
                    """
                            v_bool
                            true
                            true
                            """,
                    "SELECT v_bool FROM x WHERE v_bool = true",
                    null, true, false
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "v_int\n",
                    "SELECT v_int FROM x WHERE v_int = 99",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val BETWEEN 10_000 AND 20_000",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n11\n",
                    "SELECT count() AS cnt FROM x WHERE val BETWEEN 50 AND 60",
                    null, false, true
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n10\n",
                    "SELECT count() AS cnt FROM x WHERE val BETWEEN 110 AND 101",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val >= 152",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            151
                            """,
                    "SELECT val FROM x WHERE val > 150",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val <= 0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < 1",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n151\n",
                    "SELECT count() AS cnt FROM x WHERE val >= 1",
                    null, false, true
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n150\n",
                    "SELECT count() AS cnt FROM x WHERE val <= 150",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > 120",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < 0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n50\n",
                    "SELECT count() AS cnt FROM x WHERE val > 50",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    """
                            val
                            Z
                            """,
                    "SELECT val FROM x WHERE val > 'Y'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < 'A'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n3\n",
                    "SELECT count() AS cnt FROM x WHERE val >= 'X'",
                    null, false, true
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n3\n",
                    "SELECT count() AS cnt FROM x WHERE val <= 'C'",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > '2025-01-01'::DATE",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < '2019-01-01'::DATE",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n31\n",
                    "SELECT count() AS cnt FROM x WHERE val >= '2020-02-01'::DATE AND val <= '2020-03-02'::DATE",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > 100.0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < 0.0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n50\n",
                    "SELECT count() AS cnt FROM x WHERE val >= 5.0 AND val < 10.0",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > 100.0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < 0.0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n50\n",
                    "SELECT count() AS cnt FROM x WHERE val >= 5.0 AND val < 10.0",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    """
                            val
                            192.168.1.1
                            255.255.255.254
                            """,
                    "SELECT val FROM x WHERE val > '10.0.0.1' ORDER BY val",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            1.1.1.1
                            10.0.0.1
                            """,
                    "SELECT val FROM x WHERE val < '192.168.1.1' ORDER BY val",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            192.168.1.1
                            255.255.255.254
                            """,
                    "SELECT val FROM x WHERE val >= '192.168.1.1' ORDER BY val",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            1.1.1.1
                            10.0.0.1
                            """,
                    "SELECT val FROM x WHERE val <= '10.0.0.1' ORDER BY val",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > '255.255.255.254'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < '1.1.1.1'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            1.1.1.1
                            10.0.0.1
                            192.168.1.1
                            255.255.255.254
                            """,
                    "SELECT val FROM x WHERE val > '0.0.0.1' ORDER BY val",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            
                            
                            """,
                    "SELECT val FROM x WHERE val IS NULL",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            val
                            1.1.1.1
                            10.0.0.1
                            192.168.1.1
                            255.255.255.254
                            """,
                    "SELECT val FROM x WHERE val IS NOT NULL ORDER BY val",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "cnt\n6\n",
                    "SELECT count() AS cnt FROM x WHERE val > '200.0.0.1'",
                    null, false, true
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > '250.0.0.1'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < '200.0.0.1'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n3\n",
                    "SELECT count() AS cnt FROM x WHERE val >= '210.0.0.1' AND val <= '230.0.0.1'",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > 10_000",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < -1",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    """
                            cnt
                            100
                            """,
                    "SELECT count() AS cnt FROM x WHERE val > 50",
                    null, false, true
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val >= 151",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val <= -1",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > 10000",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < 0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n51\n",
                    "SELECT count() AS cnt FROM x WHERE val >= 100",
                    null, false, true
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n100\n",
                    "SELECT count() AS cnt FROM x WHERE val <= 100",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    "cnt\n149\n",
                    "SELECT count() AS cnt FROM x WHERE val > 50",
                    null, false, true
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n50\n",
                    "SELECT count() AS cnt FROM x WHERE val < -50",
                    null, false, true
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > 199",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < -100",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n21\n",
                    "SELECT count() AS cnt FROM x WHERE val >= -10 AND val <= 10",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > 10000",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < 0",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n50\n",
                    "SELECT count() AS cnt FROM x WHERE val > 100",
                    null, false, true
            );
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
            assertQueryNoLeakCheck(
                    "name\n",
                    "SELECT name FROM x WHERE name > 'zzz'",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "name\n",
                    "SELECT name FROM x WHERE name < 'A'",
                    null, true, false
            );
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
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > '2025-01-01'::timestamp",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val < '2019-01-01'::timestamp",
                    null, true, false
            );
            Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n6\n",
                    "SELECT count() AS cnt FROM x WHERE val >= '2020-01-02'::timestamp",
                    null, false, true
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "cnt\n145\n",
                    "SELECT count() AS cnt FROM x WHERE val <= '2020-01-02'::timestamp",
                    null, false, true
            );
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
            // Without the fix, bloom filter bytes are interpreted under the wrong
            // physical type, which can cause false-negative skips (missing rows).
            assertQueryNoLeakCheck(
                    """
                            val
                            50000
                            """,
                    "SELECT val FROM x WHERE val = 50_000",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 25_000",
                    null, true, false
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
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
            assertQueryNoLeakCheck(
                    """
                            val
                            50000
                            """,
                    "SELECT val FROM x WHERE val = 50_000",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 25_000",
                    null, true, false
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
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
            assertQueryNoLeakCheck(
                    """
                            val
                            500
                            """,
                    "SELECT val FROM x WHERE val = 500",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val = 250",
                    null, true, false
            );
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
            // Without the fix, comparing i64 filter bytes against i32 stats would
            // produce wrong comparisons.
            assertQueryNoLeakCheck(
                    """
                            val
                            30000
                            """,
                    "SELECT val FROM x WHERE val = 30_000",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > 100_000",
                    null, true, false
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
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
            // Without the fix, the tag-only check passes (both are TIMESTAMP tag)
            // and min/max comparisons produce wrong results.
            assertQueryNoLeakCheck(
                    """
                            val
                            2021-01-01T00:00:00.000000000Z
                            """,
                    "SELECT val FROM x WHERE val = '2021-01-01'::TIMESTAMP_NS",
                    null, true, false
            );

            ParquetRowGroupFilter.resetRowGroupsSkipped();
            assertQueryNoLeakCheck(
                    "val\n",
                    "SELECT val FROM x WHERE val > '2025-01-01'::TIMESTAMP_NS",
                    null, true, false
            );
            Assert.assertEquals(0, ParquetRowGroupFilter.getRowGroupsSkipped());
        });
    }

    private void assertHasParquetPartitions(String tableName, boolean expected) {
        TableToken tableToken = engine.verifyTableName(tableName);
        try (MetadataCacheReader reader = engine.getMetadataCache().readLock()) {
            CairoTable table = reader.getTable(tableToken);
            Assert.assertNotNull(table);
            Assert.assertEquals(expected, table.hasParquetPartitions());
        }
    }
}
