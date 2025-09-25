/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CreateTableDedupTest extends AbstractCairoTest {

    @Test
    public void testAddIndexOnDeduplicatedColumn() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol, i int) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUPLICATE UPSERT KEYS (ts, i, s ) "
            );
            execute("alter table " + tableName + " alter column s add index");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
                Assert.assertTrue(writer.getMetadata().isDedupKey(3));
            }

            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\ttrue\t256\ttrue\t128\t0\tfalse\ttrue\n" +
                            "i\tINT\tfalse\t0\tfalse\t0\t0\tfalse\ttrue\n",
                    "SHOW COLUMNS FROM " + tableName
            );

            execute("alter table " + tableName + " alter column s drop index");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
                Assert.assertTrue(writer.getMetadata().isDedupKey(3));
            }

            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\ttrue\n" +
                            "i\tINT\tfalse\t0\tfalse\t0\t0\tfalse\ttrue\n",
                    "SHOW COLUMNS FROM " + tableName
            );
        });
    }

    @Test
    public void testAlterReadonlyFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table dups as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY WAL");

            SqlExecutionContext roExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                    ReadOnlySecurityContext.INSTANCE,
                    bindVariableService,
                    null,
                    -1
                    , null
            );

            try {
                assertExceptionNoLeakCheck("Alter table dups dedup upsert keys(ts)", roExecutionContext);
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "permission denied");
            }
        });
    }

    @Test
    public void testAlterTableSetTypeSqlSyntaxErrors() throws Exception {
        assertMemoryLeak(ff, () -> {
            execute("create table a (ts timestamp, i int, s symbol, l long, str string) timestamp(ts) partition by day wal");
            String alterPrefix = "alter table a ";

            assertException(
                    alterPrefix + "deduplicate UPSERT KEYS",
                    37,
                    "deduplicate key column list expected"
            );
            assertException(
                    alterPrefix + "deduplicate UPSERT KEYS (;",
                    39,
                    "literal expected"
            );
            assertException(
                    alterPrefix + "deduplicate UPSERT KEYS (a)",
                    39,
                    "deduplicate key column not found "
            );
            assertException(
                    alterPrefix + "deduplicate UPSERT KEYS (s)",
                    39,
                    "deduplicate key list must include dedicated timestamp column"
            );
            assertException(
                    alterPrefix + "deduplicate KEYS (s);",
                    26,
                    "expected 'upsert'"
            );
            assertException(
                    alterPrefix + "deduplicate UPSERT (s);",
                    33,
                    "expected 'keys'"
            );
            assertException(
                    alterPrefix + "deduplicate UPSERT KEYS",
                    37,
                    "column list expected"
            );
        });
    }

    @Test
    public void testCreateTableSetTypeSqlSyntaxErrors() throws Exception {
        assertMemoryLeak(ff, () -> {
            String createPrefix = "create table a (ts timestamp, i int, s symbol, l long, str string)";
            assertException(
                    createPrefix + " timestamp(ts) partition by day bypass wal deduplicate UPSERT KEYS(l);",
                    121,
                    "deduplication is possible only on WAL tables"
            );
            assertException(
                    createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (;",
                    127,
                    "literal expected"
            );
            assertException(
                    createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (a);",
                    127,
                    "deduplicate key column not found [column=a]"
            );
            assertException(
                    createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (s);",
                    127,
                    "deduplicate key list must include dedicated timestamp column"
            );
            assertException(
                    createPrefix + " timestamp(ts) partition by day wal deduplicate KEYS (s);",
                    114,
                    "expected 'upsert'"
            );
            assertException(
                    createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT (s);",
                    121,
                    "expected 'keys'"
            );
            assertException(
                    createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS",
                    125,
                    "column list expected"
            );
        });
    }

    @Test
    public void testCreateTableWithArrayDedupKey() throws Exception {
        assertMemoryLeak(() -> assertException("CREATE TABLE x (ts TIMESTAMP, arr DOUBLE[])" +
                        " TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, arr)",
                101, "dedup key columns cannot include ARRAY [column=arr, type=DOUBLE[]]"));
    }

    @Test
    public void testCreateTableWithDoubleQuotes() throws Exception {
        String tableName = testName.getMethodName() + " a 欢迎回来 to you";
        assertMemoryLeak(ff, () -> {
            execute(
                    "CREATE TABLE '" + tableName + "' (\n" +
                            "  Status SYMBOL capacity 16 CACHE,\n" +
                            "  \"Reported time\" TIMESTAMP\n" +
                            "  ) timestamp (\"Reported time\") PARTITION BY DAY WAL  DEDUP UPSERT KEYS(\"Reported time\");"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(1));
            }
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "Status\tSYMBOL\tfalse\t256\ttrue\t16\t0\tfalse\tfalse\n" +
                            "Reported time\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n",
                    "SHOW COLUMNS FROM '" + tableName + '\''
            );
            execute("alter table '" + tableName + "' DEDUP DISABLE;");
            execute("alter table '" + tableName + "' DEDUP ENABLE UPSERT KEYS(\"Reported time\");");
            drainWalQueue();
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "Status\tSYMBOL\tfalse\t256\ttrue\t16\t0\tfalse\tfalse\n" +
                            "Reported time\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n",
                    "SHOW COLUMNS FROM '" + tableName + '\''
            );
        });
    }

    @Test
    public void testDecimalDeduplicationDisableEnable() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            // Create table with decimal dedup key
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, amount DECIMAL(10,2), description STRING) " +
                            "timestamp(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS (ts, amount)"
            );

            // Insert initial data
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 'first')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 'duplicate1')");
            drainWalQueue();

            assertSql(
                    "count\n1\n",
                    "SELECT count(*) FROM " + tableName
            );

            // Disable deduplication
            execute("ALTER table " + tableName + " dedup disable");
            drainWalQueue();

            // Insert duplicate - should be allowed now
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 'duplicate2')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 'duplicate3')");
            drainWalQueue();

            assertSql(
                    "count\n3\n",
                    "SELECT count(*) FROM " + tableName
            );

            // Re-enable deduplication with decimal key
            execute("ALTER table " + tableName + " dedup enable UPSERT KEYS(ts, amount)");
            drainWalQueue();

            // New duplicates should be prevented again
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 200.00m, 'new1')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 200.00m, 'new2')");
            drainWalQueue();

            assertSql(
                    "ts\tamount\tdescription\n" +
                            "2024-01-01T00:00:00.000000Z\t100.00\tduplicate1\n" +
                            "2024-01-01T00:00:00.000000Z\t100.00\tduplicate2\n" +
                            "2024-01-01T00:00:00.000000Z\t100.00\tduplicate3\n" +
                            "2024-01-01T00:00:00.000000Z\t200.00\tnew2\n",
                    "SELECT * FROM " + tableName + " ORDER BY amount, description"
            );
        });
    }

    @Test
    public void testDecimalDeduplicationHighPrecision() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            // Create table with high precision decimals
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, measurement DECIMAL(38,15), sensor_id INT) " +
                            "timestamp(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS (ts, measurement)"
            );

            // Insert data with very precise decimal values
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 123456789012345.123456789012345m, 1)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 123456789012345.123456789012345m, 2)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 123456789012345.123456789012346m, 3)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 123456789012345.123456789012344m, 4)");
            drainWalQueue();

            // Should have 3 unique measurement values
            assertSql(
                    "ts\tmeasurement\tsensor_id\n" +
                            "2024-01-01T00:00:00.000000Z\t123456789012345.123456789012344\t4\n" +
                            "2024-01-01T00:00:00.000000Z\t123456789012345.123456789012345\t2\n" +
                            "2024-01-01T00:00:00.000000Z\t123456789012345.123456789012346\t3\n",
                    "SELECT * FROM " + tableName + " ORDER BY measurement"
            );
        });
    }

    @Test
    public void testDecimalDeduplicationMixedTypes() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            // Create table with mixed types including decimal as dedup keys
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, symbol SYMBOL, price DECIMAL(10,2), volume LONG, active BOOLEAN) " +
                            "timestamp(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS (ts, symbol, price)"
            );

            // Insert data with duplicates
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 'AAPL', 150.50m, 1000, true)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 'AAPL', 150.50m, 2000, false)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 'AAPL', 150.51m, 3000, true)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 'GOOGL', 150.50m, 4000, true)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 'GOOGL', 150.50m, 5000, false)");
            drainWalQueue();

            // Should have 3 unique combinations
            assertSql(
                    "ts\tsymbol\tprice\tvolume\tactive\n" +
                            "2024-01-01T00:00:00.000000Z\tAAPL\t150.50\t2000\tfalse\n" +
                            "2024-01-01T00:00:00.000000Z\tAAPL\t150.51\t3000\ttrue\n" +
                            "2024-01-01T00:00:00.000000Z\tGOOGL\t150.50\t5000\tfalse\n",
                    "SELECT * FROM " + tableName + " ORDER BY symbol, price"
            );
        });
    }

    @Test
    public void testDecimalDeduplicationMultipleKeys() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            // Create table with multiple decimal columns as dedup keys
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, price DECIMAL(10,2), tax DECIMAL(5,3), discount DECIMAL(4,2), notes STRING) " +
                            "timestamp(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS (ts, price, tax, discount)"
            );

            // Verify dedup keys
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0)); // ts
                Assert.assertTrue(writer.getMetadata().isDedupKey(1)); // price
                Assert.assertTrue(writer.getMetadata().isDedupKey(2)); // tax
                Assert.assertTrue(writer.getMetadata().isDedupKey(3)); // discount
                Assert.assertFalse(writer.getMetadata().isDedupKey(4)); // notes
            }

            // Insert data with duplicates
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 8.250m, 5.00m, 'first')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 8.250m, 5.00m, 'second')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 8.250m, 5.00m, 'third')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 8.250m, 5.01m, 'different discount')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 8.251m, 5.00m, 'different tax')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.01m, 8.250m, 5.00m, 'different price')");
            drainWalQueue();

            // Should have 4 rows (unique combinations of dedup keys)
            assertSql(
                    "ts\tprice\ttax\tdiscount\tnotes\n" +
                            "2024-01-01T00:00:00.000000Z\t100.00\t8.250\t5.00\tthird\n" +
                            "2024-01-01T00:00:00.000000Z\t100.00\t8.250\t5.01\tdifferent discount\n" +
                            "2024-01-01T00:00:00.000000Z\t100.00\t8.251\t5.00\tdifferent tax\n" +
                            "2024-01-01T00:00:00.000000Z\t100.01\t8.250\t5.00\tdifferent price\n",
                    "SELECT * FROM " + tableName + " ORDER BY price, tax, discount"
            );
        });
    }

    @Test
    public void testDecimalDeduplicationSingleKey() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            // Create table with decimal as dedup key
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, price DECIMAL(10,2), quantity INT) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUP UPSERT KEYS (ts, price)"
            );

            // Verify dedup keys are set correctly
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0)); // ts
                Assert.assertTrue(writer.getMetadata().isDedupKey(1)); // price
                Assert.assertFalse(writer.getMetadata().isDedupKey(2)); // quantity
            }

            // Insert duplicate rows
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 99.99m, 10)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 99.99m, 20)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 99.99m, 30)");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 100.00m, 40)");
            drainWalQueue();

            // Should have only 2 rows (one for each unique ts+price combination)
            assertSql(
                    "ts\tprice\tquantity\n" +
                            "2024-01-01T00:00:00.000000Z\t99.99\t30\n" +
                            "2024-01-01T00:00:00.000000Z\t100.00\t40\n",
                    "SELECT * FROM " + tableName + " ORDER BY price"
            );
        });
    }

    @Test
    public void testDecimalDeduplicationWithNulls() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            // Create table with decimal that can have nulls
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, price DECIMAL(10,2), notes STRING) " +
                            "timestamp(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS (ts, price)"
            );

            // Insert data with null decimal values
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 99.99m, 'first')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 99.99m, 'duplicate')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', null, 'null price 1')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', null, 'null price 2')");
            execute("insert into " + tableName + " values ('2024-01-01T00:00:00', 0.00m, 'zero price')");
            drainWalQueue();

            // Nulls should be treated as distinct values for deduplication
            assertSql(
                    "ts\tprice\tnotes\n" +
                            "2024-01-01T00:00:00.000000Z\t\tnull price 2\n" +
                            "2024-01-01T00:00:00.000000Z\t0.00\tzero price\n" +
                            "2024-01-01T00:00:00.000000Z\t99.99\tduplicate\n",
                    "SELECT * FROM " + tableName + " ORDER BY price"
            );
        });
    }

    @Test
    public void testDedupEnabledTimestampOnly() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUP UPSERT KEYS (ts)"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
            }
        });
    }

    @Test
    public void testDedupSyntaxError() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL"
            );
            assertException("ALTER table " + tableName + " dedup UPSERT KEYS(ts,", 54, "')' expected");
            assertException("ALTER table " + tableName + " dedup UPSERT KEYS(ts,s", 55, "')' expected");
        });
    }

    @Test
    public void testDeduplicationEnabledIntAndSymbol() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol, i int) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUPLICATE UPSERT KEYS (ts, i, s ) "
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
                Assert.assertTrue(writer.getMetadata().isDedupKey(3));
            }

            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\ttrue\n" +
                            "i\tINT\tfalse\t0\tfalse\t0\t0\tfalse\ttrue\n",
                    "SHOW COLUMNS FROM " + tableName
            );
        });
    }

    @Test
    public void testDeduplicationEnabledTimestampAndSymbol() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUPLICATE UPSERT KEYS(ts, s)"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
            }
        });
    }

    @Test
    public void testDeduplicationEnabledTimestampOnly() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUPLICATE UPSERT KEYS (ts)"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
            }
        });
    }

    @Test
    public void testDisableDedupOnTable() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            execute(
                    "create table " + tableName +
                            " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                            " PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts,s)"
            );
            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
            }

            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\ttrue\n",
                    "SHOW COLUMNS FROM " + tableName
            );

            execute("ALTER table " + tableName + " dedup disable");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertFalse(writer.isDeduplicationEnabled());
                Assert.assertFalse(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertFalse(writer.getMetadata().isDedupKey(2));
            }

            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\tfalse\n",
                    "SHOW COLUMNS FROM " + tableName
            );

            execute("ALTER table " + tableName + " dedup UPSERT KEYS(ts)");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertFalse(writer.getMetadata().isDedupKey(2));
            }

            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\tfalse\n",
                    "SHOW COLUMNS FROM " + tableName
            );

            execute("ALTER table " + tableName + " dedup disable");
            execute("ALTER table " + tableName + " drop column x");
            execute("ALTER table " + tableName + " dedup UPSERT KEYS(ts,s)");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
            }

            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\ttrue\n",
                    "SHOW COLUMNS FROM " + tableName
            );

            execute("ALTER table " + tableName + " dedup disable");
            execute("ALTER table " + tableName + " drop column s");
            assertException(
                    "ALTER table " + tableName + " dedup UPSERT KEYS(ts,s)",
                    57,
                    "deduplicate key column not found [column=s]"
            );
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertFalse(writer.isDeduplicationEnabled());
                Assert.assertFalse(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertFalse(writer.getMetadata().isDedupKey(2));
            }
        });
    }

    @Test
    public void testEnableDedup() throws Exception {
        String tableName = testName.getMethodName();
        execute(
                "create table " + tableName +
                        " (ts TIMESTAMP, x long, s symbol) timestamp(ts)" +
                        " PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts,s)"
        );
        assertSql(
                "table_name\tdedup\n" +
                        "testEnableDedup\ttrue\n",
                "select table_name, dedup from tables() where table_name ='" + tableName + "'"
        );
        assertSql(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n" +
                        "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                        "s\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\ttrue\n",
                "show columns from '" + tableName + "'"
        );
        execute("alter table " + tableName + " dedup disable");
        drainWalQueue();
        assertSql(
                "table_name\tdedup\n" +
                        "testEnableDedup\tfalse\n",
                "select table_name, dedup from tables() where table_name ='" + tableName + "'"
        );

        execute("alter table " + tableName + " dedup enable upsert keys(ts)");
        drainWalQueue();
        assertSql(
                "table_name\tdedup\n" +
                        "testEnableDedup\ttrue\n",
                "select table_name, dedup from tables() where table_name ='" + tableName + "'"
        );
        assertSql(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\ttrue\n" +
                        "x\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                        "s\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\tfalse\n",
                "show columns from '" + tableName + "'"
        );
    }

    @Test
    public void testEnableDedupDroppedColumnColumnConcurrently() throws Exception {
        assertMemoryLeak(() -> {
            String tableNameStr = testName.getMethodName();
            execute(
                    "create table " + tableNameStr + " as (" +
                            "select x, " +
                            " rnd_symbol('AB', 'BC', 'CD') sym, " +
                            " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                            " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                            " from long_sequence(1)" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            TableToken tableToken = engine.verifyTableName(tableNameStr);

            try (TableWriterAPI tw1 = engine.getTableWriterAPI(tableToken, "test")) {
                try (TableWriterAPI tw2 = engine.getTableWriterAPI(tableToken, "test")) {

                    AlterOperationBuilder setDedupAlterBuilder = new AlterOperationBuilder()
                            .ofDedupEnable(1, tableToken);
                    setDedupAlterBuilder.setDedupKeyFlag(1);
                    AlterOperation ao = setDedupAlterBuilder.build();
                    ao.withContext(sqlExecutionContext);
                    ao.withSqlStatement("ALTER TABLE " + tableToken.getTableName() + " DEDUP UPSERT KEYS (ts, sym)");

                    AlterOperationBuilder dropColumnAlterBuilder = new AlterOperationBuilder()
                            .ofDropColumn(1, tableToken, 0)
                            .ofDropColumn("sym");

                    tw1.apply(dropColumnAlterBuilder.build(), true);
                    try {
                        tw2.apply(ao, true);
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "cannot use dropped column for deduplication [column=sym]");
                    }
                }
            }

            drainWalQueue();

            try (TableWriter tw = getWriter(tableToken)) {
                Assert.assertFalse(tw.isDeduplicationEnabled());
            }
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
        });
    }

    @Test
    public void testEnableDedupDroppedColumnColumnFails() throws Exception {
        assertMemoryLeak(() -> {
            String tableNameStr = testName.getMethodName();
            execute("create table " + tableNameStr + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL");

            TableToken tableToken = engine.verifyTableName(tableNameStr);
            drainWalQueue();

            try (TableWriter tw = getWriter(tableToken)) {
                LongList columnIndexes = new LongList();
                columnIndexes.add(-1);
                try {
                    tw.enableDeduplicationWithUpsertKeys(columnIndexes);
                    Assert.assertFalse(tw.isDeduplicationEnabled());
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column index to make a dedup key");
                }

                columnIndexes.clear();
                columnIndexes.add(0);
                try {
                    tw.enableDeduplicationWithUpsertKeys(columnIndexes);
                    Assert.assertFalse(tw.isDeduplicationEnabled());
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Unsupported column type used as deduplicate key");
                }
            }

            execute("ALTER TABLE " + tableNameStr + " drop column sym");
            try (TableWriter tw = getWriter(tableToken)) {
                LongList columnIndexes = new LongList();
                columnIndexes.add(1);
                try {
                    tw.enableDeduplicationWithUpsertKeys(columnIndexes);
                    Assert.assertFalse(tw.isDeduplicationEnabled());
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column index to make a dedup key");
                }
            }
        });
    }
}
