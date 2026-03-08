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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.DETACHED_DIR_MARKER;

public class WalAlterTableSqlTest extends AbstractCairoTest {

    @Test
    public void createWalAndDetachAttachPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            String partition = "2022-02-24";

            execute("alter table " + tableName + " detach partition list '" + partition + "'");

            drainWalQueue();

            try (Path path = new Path(); Path other = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(DETACHED_DIR_MARKER).$();
                other.of(configuration.getDbRoot()).concat(tableToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
                Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
            }

            execute("alter table " + tableName + " attach partition list '" + partition + "'");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T06:00:00.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T12:00:00.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T18:00:00.000000Z\tFG\n" +
                    "5\tAB\t2022-02-25T00:00:00.000000Z\tDE\n", tableName);
        });
    }

    @Test
    public void createWalAndDropAddIndex() throws Exception {
        assertMemoryLeak(() -> {
            String expected = "x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T06:00:00.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T12:00:00.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T18:00:00.000000Z\tFG\n" +
                    "5\tAB\t2022-02-25T00:00:00.000000Z\tDE\n";

            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    "), index(sym capacity 8) timestamp(ts) partition by DAY WAL");

            execute("alter table " + tableName + " alter column sym drop index");

            drainWalQueue();
            assertSql(expected, tableName);

            execute("alter table " + tableName + " alter column sym drop index");

            drainWalQueue();
            assertSql(expected, tableName);

            execute("alter table " + tableName + " alter column sym2 drop index");

            drainWalQueue();
            assertSql(expected, tableName);

            execute("alter table " + tableName + " alter column sym add index capacity 8");

            drainWalQueue();
            assertSql(expected, tableName);
        });
    }

    @Test
    public void createWalAndDropNonExistentPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("alter table " + tableName + " drop partition list '2022-02-26'");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "1\tAB\t2022-02-24T00:00:00.000000Z\tEF\n" +
                    "2\tBC\t2022-02-24T06:00:00.000000Z\tFG\n" +
                    "3\tCD\t2022-02-24T12:00:00.000000Z\tFG\n" +
                    "4\tCD\t2022-02-24T18:00:00.000000Z\tFG\n" +
                    "5\tAB\t2022-02-25T00:00:00.000000Z\tDE\n", tableName);
        });
    }

    @Test
    public void createWalAndDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("alter table " + tableName + " drop partition list '2022-02-24'");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "5\tAB\t2022-02-25T00:00:00.000000Z\tDE\n", tableName);
        });
    }

    @Test
    public void createWalAndDropPartitionList() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-23', 21600000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(10)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("alter table " + tableName + " drop partition list '2022-02-23', '2022-02-24'");

            drainWalQueue();
            assertSql("x\tsym\tts\tsym2\n" +
                    "9\tAB\t2022-02-25T00:00:00.000000Z\t\n" +
                    "10\tAB\t2022-02-25T06:00:00.000000Z\tEF\n", tableName);
        });
    }

    @Test
    public void createWalAndDropPartitionsWithWhere22() throws Exception {
        final String tableName = testName.getMethodName();
        createWalAndDropPartitionsWithWhere(tableName, 22, 2, "x\tsym\tts\tsym2\n" +
                "1\tAB\t2022-02-23T00:00:00.000000Z\tEF\n" +
                "2\tBC\t2022-02-23T06:00:00.000000Z\tFG\n" +
                "3\tCD\t2022-02-23T12:00:00.000000Z\tFG\n" +
                "4\tCD\t2022-02-23T18:00:00.000000Z\tFG\n" +
                "5\tAB\t2022-02-24T00:00:00.000000Z\tDE\n" +
                "6\tBC\t2022-02-24T06:00:00.000000Z\tDE\n" +
                "7\tBC\t2022-02-24T12:00:00.000000Z\tFG\n" +
                "8\tBC\t2022-02-24T18:00:00.000000Z\tDE\n" +
                "9\tAB\t2022-02-25T00:00:00.000000Z\t\n" +
                "10\tAB\t2022-02-25T06:00:00.000000Z\tEF\n" +
                "11\tBC\t2022-02-25T12:00:00.000000Z\tFG\n" +
                "12\tAB\t2022-02-25T18:00:00.000000Z\t\n" +
                "13\tAB\t2022-02-26T00:00:00.000000Z\tDE\n");
    }

    @Test
    public void createWalAndDropPartitionsWithWhere23() throws Exception {
        final String tableName = testName.getMethodName();
        createWalAndDropPartitionsWithWhere(tableName, 23, 2, "x\tsym\tts\tsym2\n" +
                "1\tAB\t2022-02-23T00:00:00.000000Z\tEF\n" +
                "2\tBC\t2022-02-23T06:00:00.000000Z\tFG\n" +
                "3\tCD\t2022-02-23T12:00:00.000000Z\tFG\n" +
                "4\tCD\t2022-02-23T18:00:00.000000Z\tFG\n" +
                "5\tAB\t2022-02-24T00:00:00.000000Z\tDE\n" +
                "6\tBC\t2022-02-24T06:00:00.000000Z\tDE\n" +
                "7\tBC\t2022-02-24T12:00:00.000000Z\tFG\n" +
                "8\tBC\t2022-02-24T18:00:00.000000Z\tDE\n" +
                "9\tAB\t2022-02-25T00:00:00.000000Z\t\n" +
                "10\tAB\t2022-02-25T06:00:00.000000Z\tEF\n" +
                "11\tBC\t2022-02-25T12:00:00.000000Z\tFG\n" +
                "12\tAB\t2022-02-25T18:00:00.000000Z\t\n" +
                "13\tAB\t2022-02-26T00:00:00.000000Z\tDE\n");
    }

    @Test
    public void createWalAndDropPartitionsWithWhere24() throws Exception {
        final String tableName = testName.getMethodName();
        createWalAndDropPartitionsWithWhere(tableName, 24, 2, "x\tsym\tts\tsym2\n" +
                "5\tAB\t2022-02-24T00:00:00.000000Z\tDE\n" +
                "6\tBC\t2022-02-24T06:00:00.000000Z\tDE\n" +
                "7\tBC\t2022-02-24T12:00:00.000000Z\tFG\n" +
                "8\tBC\t2022-02-24T18:00:00.000000Z\tDE\n" +
                "9\tAB\t2022-02-25T00:00:00.000000Z\t\n" +
                "10\tAB\t2022-02-25T06:00:00.000000Z\tEF\n" +
                "11\tBC\t2022-02-25T12:00:00.000000Z\tFG\n" +
                "12\tAB\t2022-02-25T18:00:00.000000Z\t\n" +
                "13\tAB\t2022-02-26T00:00:00.000000Z\tDE\n");
    }

    @Test
    public void createWalAndDropPartitionsWithWhere25() throws Exception {
        final String tableName = testName.getMethodName();
        createWalAndDropPartitionsWithWhere(tableName, 25, 3, "x\tsym\tts\tsym2\n" +
                "9\tAB\t2022-02-25T00:00:00.000000Z\t\n" +
                "10\tAB\t2022-02-25T06:00:00.000000Z\tEF\n" +
                "11\tBC\t2022-02-25T12:00:00.000000Z\tFG\n" +
                "12\tAB\t2022-02-25T18:00:00.000000Z\t\n" +
                "13\tAB\t2022-02-26T00:00:00.000000Z\tDE\n");
    }

    @Test
    public void createWalAndDropPartitionsWithWhere26() throws Exception {
        final String tableName = testName.getMethodName();
        createWalAndDropPartitionsWithWhere(tableName, 26, 4, "x\tsym\tts\tsym2\n" +
                "13\tAB\t2022-02-26T00:00:00.000000Z\tDE\n");
    }

    @Test
    public void createWalAndDropPartitionsWithWhere27() throws Exception {
        final String tableName = testName.getMethodName();
        createWalAndDropPartitionsWithWhere(tableName, 27, 5, "x\tsym\tts\tsym2\n");
    }

    @Test
    public void createWalAndDropPartitionsWithWhere28() throws Exception {
        final String tableName = testName.getMethodName();
        createWalAndDropPartitionsWithWhere(tableName, 28, 5, "x\tsym\tts\tsym2\n");
    }

    @Test
    public void createWalDropTable() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute(
                    "create table " + tableName + " as (" +
                            "select x, " +
                            " rnd_symbol('AB', 'BC', 'CD') sym, " +
                            " timestamp_sequence('2022-02-24', 21600000000L) ts " +
                            " from long_sequence(5)" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            assertSql(
                    "table_name\n" + tableName + "\n", "select table_name from tables()"
            );

            execute("drop table " + tableName);
            drainWalQueue();

            assertSql(
                    "table_name\n", "select table_name from tables() where table_name = '" + tableName + "'"
            );
        });
    }

    @Test
    public void createWalSetParam() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("alter table " + tableName + " set param o3MaxLag = 117s");

            drainWalQueue();
            assertSql(
                    "o3MaxLag\n117000000\n", "select o3MaxLag from tables() where table_name = '" + tableName + "'"
            );
        });
    }

    @Test
    public void testDropNonExistingPartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            execute("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('AB', 'BC', 'CD') sym, " +
                    " timestamp_sequence('2022-02-24', 21600000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                    " from long_sequence(5)" +
                    ") timestamp(ts) partition by DAY WAL");

            execute("alter table " + tableName + " drop partition list '2022-02-23'");
            execute("alter table " + tableName + " drop partition where ts < '2022-02-23'");
            execute("alter table " + tableName + " drop partition where ts > '2023-02-23'");
            execute("alter table " + tableName + " drop partition list '2023-02-23'");

            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(
                    engine.verifyTableName(tableName)
            ));
        });
    }

    @Test
    public void testReleaseAndReopenWriters() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            execute("create table " + tableName + "("
                    + "x long,"
                    + "ts timestamp"
                    + ") timestamp(ts) partition by DAY WAL");
            execute("insert into " + tableName + " values (1, '2022-02-24T00:00:00.000000Z')");
            execute("alter table " + tableName + " add column s1 string");
            execute("insert into " + tableName + " values (2, '2022-02-24T00:00:01.000000Z', 'str2')");

            // Release TableWriter, WAL and Sequencer
            engine.releaseInactive();
            drainWalQueue();

            assertSql("x\tts\ts1\n" +
                    "1\t2022-02-24T00:00:00.000000Z\t\n" +
                    "2\t2022-02-24T00:00:01.000000Z\tstr2\n", tableName);

            // Release TableWriter, WAL and Sequencer
            engine.releaseInactive();

            execute("insert into " + tableName + " values (3, '2022-02-24T00:00:02.000000Z', 'str3')");
            execute("insert into " + tableName + " values (4, '2022-02-22T00:00:00.000000Z', 'str4')");
            execute("insert into " + tableName + " values (5, '2022-02-21T00:00:00.000000Z', 'str5')");
            execute("insert into " + tableName + " values (6, '2022-02-20T00:00:00.000000Z', 'str6')");
            execute("insert into " + tableName + " values (7, '2022-02-25T00:00:00.000000Z', 'str7')");
            drainWalQueue();

            execute("alter table " + tableName + " drop partition where ts < to_timestamp('2022-02-24:00:00:00', 'yyyy-MM-dd:HH:mm:ss')");
            execute("insert into " + tableName + " values (8, '2022-02-26T00:00:00.000000Z', 'str8')");
            drainWalQueue();

            // Release TableWriter, WAL and Sequencer
            engine.releaseInactive();

            execute("insert into " + tableName + " values (9, '2022-02-26T01:00:00.000000Z', 'str9')");
            drainWalQueue();

            assertSql("x\tts\ts1\n" +
                    "1\t2022-02-24T00:00:00.000000Z\t\n" +
                    "2\t2022-02-24T00:00:01.000000Z\tstr2\n" +
                    "3\t2022-02-24T00:00:02.000000Z\tstr3\n" +
                    "7\t2022-02-25T00:00:00.000000Z\tstr7\n" +
                    "8\t2022-02-26T00:00:00.000000Z\tstr8\n" +
                    "9\t2022-02-26T01:00:00.000000Z\tstr9\n", tableName);
        });
    }

    private void createWalAndDropPartitionsWithWhere(String tableName, int day, long expectedTxn, String expected) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table " + tableName + " as (" +
                            "select x, " +
                            " rnd_symbol('AB', 'BC', 'CD') sym, " +
                            " timestamp_sequence('2022-02-23', 21600000000L) ts, " +
                            " rnd_symbol('DE', null, 'EF', 'FG') sym2 " +
                            " from long_sequence(13)" +
                            ") timestamp(ts) partition by DAY WAL"
            );

            execute("alter table " + tableName + " drop partition where ts < to_timestamp('2022-02-" + day + ":00:00:00', 'yyyy-MM-dd:HH:mm:ss')");

            drainWalQueue();

            try (TableReader reader = getReader(tableName)) {
                Assert.assertEquals(expectedTxn, reader.getTxn());
                Assert.assertEquals(2, reader.getTxFile().getSeqTxn());
            }
            assertSql(expected, tableName);
        });
    }
}
