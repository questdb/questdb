/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.wal;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import org.junit.Assert;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import org.junit.Test;

public class WalTableSqlTest extends AbstractGriffinTest {
    @Test
    public void createWalAndInsertFromSql() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");

            drainWalQueue();

            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n");
        });
    }

    @Test
    public void test2InsertsAtSameTime() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);

            try (InsertOperation insertOperation = compiledQuery.getInsertOperation();
                 InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)) {
                insertMethod.execute();

                CompiledQuery compiledQuery2 = compiler.compile("insert into " + tableName +
                        " values (102, 'bbb', '2022-02-24T02', 'ccc')", sqlExecutionContext);
                try (InsertOperation insertOperation2 = compiledQuery2.getInsertOperation();
                     InsertMethod insertMethod2 = insertOperation2.createMethod(sqlExecutionContext)) {
                    insertMethod2.execute();
                    insertMethod2.commit();
                }
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                    "102\tbbb\t2022-02-24T02:00:00.000000Z\tccc\n");
        });
    }

    @Test
    public void testAddColumnWalRollsWalSegment() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {
                insertMethod.execute();
                insertMethod.execute();
                insertMethod.commit();

                insertMethod.execute();
                compile("alter table " + tableName + " add column new_column int");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', 'str-2', '2022-02-24T02', 'asdd', 1234)");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tstr\tts\tsym2\tnew_column\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "101\ta1a1\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "103\tdfd\tstr-2\t2022-02-24T02:00:00.000000Z\tasdd\t1234\n");
        });
    }

    @Test
    public void testAddFixedSizeColumnBeforeInsertCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {

                insertMethod.execute();
                compile("alter table " + tableName + " add column jjj int");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', 1234)");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tjjj\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\n");

        });
    }

    @Test
    public void testAddMultipleWalColumnsBeforeCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {

                insertMethod.execute();
                compile("alter table " + tableName + " add column jjj int");
                compile("alter table " + tableName + " add column col_str string");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', 1234, 'sss-value')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tjjj\tcol_str\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\t\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\tsss-value\n");

        });
    }

    @Test
    public void testAddWalColumnAfterCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {

                insertMethod.execute();
                insertMethod.commit();
//                drainWalQueue();
                compile("alter table " + tableName + " add column jjj int");
            }

//            drainWalQueue();
            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', 1234)");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tjjj\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\tNaN\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\n");

        });
    }

    @Test
    public void testCreateWalDropColumnInsert() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(1)" +
                    ") timestamp(ts) partition by DAY WAL"
            );

            compile("alter table " + tableName + " drop column sym");
            compile("insert into " + tableName + "(x, ts) values (2, '2022-02-24T23:00:01')");
            drainWalQueue();

            assertSql(tableName, "x\tsym2\tts\n" +
                    "1\tEF\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t\t2022-02-24T23:00:01.000000Z\n");
        });
    }

    @Test
    public void testCreateWalTableAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            drainWalQueue();

            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n");
        });
    }

    @Test
    public void testCreateWalTableAsSelectAndInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            compile("insert into " + tableName +
                    " select x + 100, rnd_symbol('AB2', 'BC2', 'CD2') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE2', null, 'EF2', 'FG2') sym2 " +
                    " from long_sequence(3)");

            drainWalQueue();

            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "101\tBC2\t2022-02-24T00:00:00.000000Z\tDE2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "102\tBC2\t2022-02-24T00:00:01.000000Z\tFG2\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "103\tBC2\t2022-02-24T00:00:02.000000Z\tDE2\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n");
        });
    }

    @Test
    public void testRemoveColumnWalRollsWalSegment() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "str string," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', 'str-1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {
                insertMethod.execute();
                insertMethod.execute();
                insertMethod.commit();

                insertMethod.execute();
                compile("alter table " + tableName + " drop column sym");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'str-2', '2022-02-24T02', 'asdd')");

            drainWalQueue();
            assertSql(tableName, "x\tstr\tts\tsym2\n" +
                    "101\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                    "101\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                    "101\tstr-1\t2022-02-24T01:00:00.000000Z\ta2a2\n" +
                    "103\tstr-2\t2022-02-24T02:00:00.000000Z\tasdd\n");

        });
    }

    @Test
    public void testRogueTableWriterBlocksApplyJob() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");


            try (TableWriter ignore = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "Rogue")) {
                drainWalQueue();
                assertSql(tableName, "x\tsym\tts\tsym2\n");
            }

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n");

            // Next insert should fix it
            executeInsert("insert into " + tableName +
                    " values (101, 'dfd', '2022-02-24T01', 'asd')");
            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T00:00:01.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T00:00:02.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T00:00:03.000000Z\tFG\n" +
                    "5\tAB\t2022-02-24T00:00:04.000000Z\tDE\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n" +
                    "101\tdfd\t2022-02-24T01:00:00.000000Z\tasd\n");
        });
    }

    @Test
    public void testWalPurgeOneSegment() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            assertWalExistance(true, tableName, 1);
            assertSegmentExistance(true, tableName, 1, 0);

            drainWalQueue();

            assertWalExistance(true, tableName, 1);

            assertSql(tableName, "x\tts\n" +
                    "1\t2022-02-24T00:00:00.000000Z\n" +
                    "2\t2022-02-24T00:00:01.000000Z\n" +
                    "3\t2022-02-24T00:00:02.000000Z\n" +
                    "4\t2022-02-24T00:00:03.000000Z\n" +
                    "5\t2022-02-24T00:00:04.000000Z\n");

            purgeWalSegments();

            assertSegmentExistance(true, tableName, 1, 0);
            assertWalExistance(true, tableName, 1);

            engine.releaseInactive();

            purgeWalSegments();

            assertSegmentExistance(false, tableName, 1, 0);
            assertWalExistance(false, tableName, 1);
        });
    }

    @Test
    public void testWalPurgeTwoSegments() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            assertWalExistance(true, tableName, 1);
            assertSegmentExistance(true, tableName, 1, 0);

            drainWalQueue();

            assertWalExistance(true, tableName, 1);
            assertSegmentExistance(true, tableName, 1, 0);

            executeInsert("insert into " + tableName + " values (6, '2022-02-24T00:00:05.000000Z')");
            compile("alter table " + tableName + " add column sss string");

            drainWalQueue();

            assertSql(tableName, "x\tts\tsss\n" +
                    "1\t2022-02-24T00:00:00.000000Z\t\n" +
                    "2\t2022-02-24T00:00:01.000000Z\t\n" +
                    "3\t2022-02-24T00:00:02.000000Z\t\n" +
                    "4\t2022-02-24T00:00:03.000000Z\t\n" +
                    "5\t2022-02-24T00:00:04.000000Z\t\n" +
                    "6\t2022-02-24T00:00:05.000000Z\t\n");

            assertWalExistance(true, tableName, 1);
            assertSegmentExistance(true, tableName, 1, 0);
            assertSegmentExistance(true, tableName, 1, 1);
        });
    }

    private void assertWalExistance(boolean expectExists, String tableName, int walId) {
        CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableName).concat("wal").put(walId).$();
            Assert.assertEquals(Chars.toString(path), expectExists, FilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    private void assertSegmentExistance(boolean expectExists, String tableName, int walId, int segmentId) {
        CharSequence root = engine.getConfiguration().getRoot();
        try (Path path = new Path()) {
            path.of(root).concat(tableName).concat("wal").put(walId).slash().put(segmentId).$();
            Assert.assertEquals(Chars.toString(path), expectExists, FilesFacadeImpl.INSTANCE.exists(path));
        }
    }

    @Test
    public void testVarSizeColumnBeforeInsertCommit() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " (" +
                    "x long," +
                    "sym symbol," +
                    "ts timestamp," +
                    "sym2 symbol" +
                    ") timestamp(ts) partition by DAY WAL");

            CompiledQuery compiledQuery = compiler.compile("insert into " + tableName +
                    " values (101, 'a1a1', '2022-02-24T01', 'a2a2')", sqlExecutionContext);
            try (
                    InsertOperation insertOperation = compiledQuery.getInsertOperation();
                    InsertMethod insertMethod = insertOperation.createMethod(sqlExecutionContext)
            ) {

                insertMethod.execute();
                compile("alter table " + tableName + " add column sss string");
                insertMethod.commit();
            }

            executeInsert("insert into " + tableName + " values (103, 'dfd', '2022-02-24T01', 'asdd', '1234')");

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\tsss\n" +
                    "101\ta1a1\t2022-02-24T01:00:00.000000Z\ta2a2\t\n" +
                    "103\tdfd\t2022-02-24T01:00:00.000000Z\tasdd\t1234\n");

        });
    }
}
