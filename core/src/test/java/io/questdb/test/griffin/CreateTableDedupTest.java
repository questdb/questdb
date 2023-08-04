/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.LongList;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CreateTableDedupTest extends AbstractGriffinTest {

    @Test
    public void testAlterReadonlyFails() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table dups as" +
                    " (select timestamp_sequence(0, 1000000) ts," +
                    " cast(x as int) x" +
                    " from long_sequence(5))" +
                    " timestamp(ts) partition by DAY WAL", sqlExecutionContext);

            SqlExecutionContext roExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                    ReadOnlySecurityContext.INSTANCE,
                    bindVariableService,
                    null,
                    -1
                    , null
            );

            try {
                compiler.compile("Alter table dups dedup upsert keys(ts)", roExecutionContext);
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "permission denied");
            }
        });
    }

    @Test
    public void testAlterTableSetTypeSqlSyntaxErrors() throws Exception {
        assertMemoryLeak(ff, () -> {
            compile("create table a (ts timestamp, i int, s symbol, l long, str string) timestamp(ts) partition by day wal");
            String alterPrefix = "alter table a ";

            assertCreate(alterPrefix + "deduplicate UPSERT KEYS(ts, str);", "UPSERT KEYS(ts, str)", "deduplicate key column can only be fixed size column [column=str, type=STRING]");
            assertCreate(alterPrefix + "deduplicate UPSERT KEYS", "UPSERT KEYS", "deduplication column list expected");
            assertCreate(alterPrefix + "deduplicate UPSERT KEYS (;", "UPSERT KEYS (;", "literal expected");
            assertCreate(alterPrefix + "deduplicate UPSERT KEYS (a)", "UPSERT KEYS (a)", "deduplicate column not found ");
            assertCreate(alterPrefix + "deduplicate UPSERT KEYS (s)", "UPSERT KEYS (s)", "deduplicate key list must include dedicated timestamp column");
            assertCreate(alterPrefix + "deduplicate KEYS (s);", "deduplicate KEYS ", "expected 'upsert'");
            assertCreate(alterPrefix + "deduplicate UPSERT (s);", "deduplicate UPSERT (", "expected 'keys'");
            assertCreate(alterPrefix + "deduplicate UPSERT KEYS", "UPSERT KEYS", "column list expected");
        });
    }

    @Test
    public void testCreateTableSetTypeSqlSyntaxErrors() throws Exception {
        assertMemoryLeak(ff, () -> {
            String createPrefix = "create table a (ts timestamp, i int, s symbol, l long, str string)";
            assertCreate(createPrefix + " timestamp(ts) partition by day bypass wal deduplicate UPSERT KEYS(l);", "deduplicate ", "deduplication is possible only on WAL tables");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (l, str);", "KEYS (l, s", "deduplicate key column can only be fixed size column [column=str, type=STRING]");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (;", "KEYS (;", "literal expected");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (a);", "KEYS (a", "deduplicate column not found [column=a]");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS (s);", "KEYS (s)", "deduplicate key list must include dedicated timestamp column");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate KEYS (s);", "deduplicate ", "expected 'upsert'");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT (s);", "UPSERT (", "expected 'keys'");
            assertCreate(createPrefix + " timestamp(ts) partition by day wal deduplicate UPSERT KEYS", "UPSERT KEYS", "column list expected");
        });
    }

    @Test
    public void testDedupEnabledTimestampOnly() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
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
    public void testDeduplicationEnabledIntAndSymbol() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
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

            assertSql("SHOW COLUMNS FROM " + tableName, "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                    "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                    "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\ttrue\n" +
                    "i\tINT\tfalse\t0\tfalse\t0\tfalse\ttrue\n");
        });
    }

    @Test
    public void testDeduplicationEnabledTimestampAndSymbol() throws Exception {
        String tableName = testName.getMethodName();
        assertMemoryLeak(() -> {
            compile(
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
            compile(
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
            compile(
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

            assertSql("SHOW COLUMNS FROM " + tableName, "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                    "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                    "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\ttrue\n");

            compile("ALTER table " + tableName + " dedup disable");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertFalse(writer.isDeduplicationEnabled());
                Assert.assertFalse(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertFalse(writer.getMetadata().isDedupKey(2));
            }

            assertSql("SHOW COLUMNS FROM " + tableName, "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                    "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                    "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n");

            compile("ALTER table " + tableName + " dedup UPSERT KEYS(ts)");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertFalse(writer.getMetadata().isDedupKey(2));
            }

            assertSql("SHOW COLUMNS FROM " + tableName, "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                    "x\tLONG\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                    "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\tfalse\n");

            compile("ALTER table " + tableName + " dedup disable");
            compile("ALTER table " + tableName + " drop column x");
            compile("ALTER table " + tableName + " dedup UPSERT KEYS(ts,s)");
            drainWalQueue();

            try (TableWriter writer = getWriter(tableName)) {
                Assert.assertTrue(writer.isDeduplicationEnabled());
                Assert.assertTrue(writer.getMetadata().isDedupKey(0));
                Assert.assertFalse(writer.getMetadata().isDedupKey(1));
                Assert.assertTrue(writer.getMetadata().isDedupKey(2));
            }

            assertSql("SHOW COLUMNS FROM " + tableName, "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\ttrue\n" +
                    "s\tSYMBOL\tfalse\t256\ttrue\t128\tfalse\ttrue\n");

            compile("ALTER table " + tableName + " dedup disable");
            compile("ALTER table " + tableName + " drop column s");
            try {
                compile("ALTER table " + tableName + " dedup UPSERT KEYS(ts,s)");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "deduplicate column not found [column=s]");
            }
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
    public void testEnableDedupDroppedColumnColumnConcurrently() throws Exception {
        assertMemoryLeak(() -> {
            String tableNameStr = testName.getMethodName();
            compile("create table " + tableNameStr + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName(tableNameStr);

            try (TableWriterAPI tw1 = engine.getTableWriterAPI(tableToken, "test")) {
                try (TableWriterAPI tw2 = engine.getTableWriterAPI(tableToken, "test")) {

                    AlterOperationBuilder setDedupAlterBuilder = new AlterOperationBuilder()
                            .ofDedupEnable(1, tableToken, true);
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
            compile("create table " + tableNameStr + " as (" +
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

            compiler.compile("ALTER TABLE " + tableNameStr + " drop column sym", sqlExecutionContext);
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

    private static void assertCreate(String alterStmt, String errorAfter, String expected) {
        int pos = alterStmt.indexOf(errorAfter);
        assert pos > -1;
        int errorPos = pos + errorAfter.length();
        try {
            compile(alterStmt);
            Assert.fail("expected SQLException is not thrown");
        } catch (SqlException ex) {
            TestUtils.assertContains(ex.getFlyweightMessage(), expected);
            Assert.assertEquals(errorPos, ex.getPosition());
        }
    }
}
