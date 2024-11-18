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
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\ttrue\t256\ttrue\t128\tfalse\ttrue\n" +
                            "i\tINT\tfalse\t0\tfalse\t0\tfalse\ttrue\n",
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
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\ttrue\n" +
                            "i\tINT\tfalse\t0\tfalse\t0\tfalse\ttrue\n",
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
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "Status\tSYMBOL\tfalse\t256\ttrue\t16\tfalse\tfalse\n" +
                            "Reported time\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n",
                    "SHOW COLUMNS FROM '" + tableName + '\''
            );
            execute("alter table '" + tableName + "' DEDUP DISABLE;");
            execute("alter table '" + tableName + "' DEDUP ENABLE UPSERT KEYS(\"Reported time\");");
            drainWalQueue();
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "Status\tSYMBOL\tfalse\t256\ttrue\t16\tfalse\tfalse\n" +
                            "Reported time\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n",
                    "SHOW COLUMNS FROM '" + tableName + '\''
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
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\ttrue\n" +
                            "i\tINT\tfalse\t0\tfalse\t0\tfalse\ttrue\n",
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
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\ttrue\n",
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
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n",
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
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                            "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n",
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
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                            "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\ttrue\n",
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
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                        "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                        "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\ttrue\n",
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
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                        "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                        "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n",
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
