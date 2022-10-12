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

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;

public class WalAlterTableSqlTest extends AbstractGriffinTest {

    @Test
    public void createWalDropTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            executeOperation("drop table " + tableName, CompiledQuery.DROP);

            drainWalQueue();
            assertSql(
                    "select commitLag from tables() where name = '" + tableName + "'",
                    "commitLag\n117000000\n");
        });
    }

    @Test
    public void createWalSetParam() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            executeOperation(
                    "alter table " + tableName + " set param commitLag = 117s",
                    CompiledQuery.ALTER
            );

            drainWalQueue();
            assertSql(
                    "select commitLag from tables() where name = '" + tableName + "'",
                    "commitLag\n117000000\n");
        });
    }

    @Test
    public void createWalAndDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            executeOperation(
                    "alter table " + tableName + " drop partition list '2022-02-24'",
                    CompiledQuery.ALTER
            );

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "5\tAB\t2022-02-25T00:00:00.000000Z\tDE\n");
        });
    }

    @Test
    public void createWalAndDetachAttachPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            String partition = "2022-02-24";

            executeOperation(
                    "alter table " + tableName + " detach partition list '" + partition + "'",
                    CompiledQuery.ALTER
            );

            drainWalQueue();

            try (Path path = new Path(); Path other = new Path()) {
                CharSequence systemTableName = engine.getSystemTableName(tableName);
                path.of(configuration.getRoot()).concat(systemTableName).concat(partition).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getRoot()).concat(systemTableName).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
                Assert.assertTrue(Files.rename(path, other) > -1);
            }

            executeOperation(
                    "alter table " + tableName + " attach partition list '" + partition + "'",
                    CompiledQuery.ALTER
            );

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T06:00:00.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T12:00:00.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T18:00:00.000000Z\tFG\n" +
                    "5\tAB\t2022-02-25T00:00:00.000000Z\tDE\n");
        });
    }

    @Test
    public void createWalAndDropAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    "), index(sym capacity 8) timestamp(ts) partition by DAY WAL");

            executeOperation(
                    "alter table " + tableName + " alter column sym drop index",
                    CompiledQuery.ALTER
            );

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T06:00:00.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T12:00:00.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T18:00:00.000000Z\tFG\n" +
                    "5\tAB\t2022-02-25T00:00:00.000000Z\tDE\n");

            executeOperation(
                    "alter table " + tableName + " alter column sym add index capacity 8",
                    CompiledQuery.ALTER
            );

            drainWalQueue();
            assertSql(tableName, "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T06:00:00.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T12:00:00.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T18:00:00.000000Z\tFG\n" +
                    "5\tAB\t2022-02-25T00:00:00.000000Z\tDE\n");
        });
    }

    @Test
    public void testInsertAfterAddColumn() throws Exception {
        String tableName = testName.getMethodName();
        compile("create table " + tableName + "("
                + "x long,"
                + "ts timestamp"
                + ") timestamp(ts) partition by DAY WAL");
        compile("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
        compile("alter table " + tableName + " add column s1 string");
        compile("insert into " + tableName + " values (2, '2022-02-24T00:00:01.000000Z', 'str')");

        // Release WAL and sequencer
        engine.releaseInactive();

        drainWalQueue();
        assertSql(tableName, "x\tts\ts1\n" +
                "1\t2022-02-24T00:00:00.000000Z\t\n" +
                "2\t2022-02-24T00:00:01.000000Z\tstr\n");
    }
}
