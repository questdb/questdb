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

import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class RenameTableTest extends AbstractCairoTest {

    @Test
    public void testApplyRename() throws SqlException {
        compile(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i, " +
                        " timestamp_sequence(1,1) timestamp " +
                        " from long_sequence(10)" +
                        ") timestamp (timestamp) partition by DAY WAL;"
        );
        TableToken from = engine.verifyTableName("x");
        TableToken to = from.renamed("y");
        engine.applyTableRename(from, to);
        engine.getTableSequencerAPI().reload(to);

        Assert.assertEquals(from.getDirName(), engine.verifyTableName("y").getDirName());
        Assert.assertNull(engine.getTableTokenIfExists("x"));
    }

    @Test
    public void testFunctionDestTableName() throws Exception {
        assertException("rename table x to y()", 19, "function call is not allowed here");
    }

    @Test
    public void testRenameTrailingDebris() throws Exception {
        assertException("rename table x to y xyz", 20, "debris?");
    }

    @Test
    public void testFunctionSrcTableName() throws Exception {
        assertException("rename table x() to y", 14, "function call is not allowed here");
    }

    @Test
    public void testRenameTableCaseInsensitive() throws Exception {
        String tableName = testName.getMethodName();
        String upperCaseName = testName.getMethodName().toUpperCase();
        String newTableName = testName.getMethodName() + "_new";

        assertMemoryLeak(ff, () -> {
            compile("create table " + tableName + " as (" +
                    "select x, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2, " +
                    " timestamp_sequence('2022-02-24', 24 * 60 * 60 * 1000000L) ts " +
                    " from long_sequence(2)" +
                    ")"
            );

            TableToken table2directoryName = engine.verifyTableName(tableName);
            compile("rename table " + tableName + " to " + upperCaseName);
            insert("insert into " + upperCaseName + " values (1, 'abc', '2022-02-25')");
            insert("insert into " + tableName + " values (1, 'abc', '2022-02-25')");

            TableToken newTableDirectoryName = engine.verifyTableName(upperCaseName);
            Assert.assertEquals(table2directoryName.getDirName(), newTableDirectoryName.getDirName());


            assertSql("x\tsym2\tts\n" +
                    "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                    "2\tEF\t2022-02-25T00:00:00.000000Z\n" +
                    "1\tabc\t2022-02-25T00:00:00.000000Z\n" +
                    "1\tabc\t2022-02-25T00:00:00.000000Z\n", "select * from " + upperCaseName);

            compile("rename table " + upperCaseName + " to " + newTableName);

            assertSql("x\tsym2\tts\n" +
                    "1\tDE\t2022-02-24T00:00:00.000000Z\n" +
                    "2\tEF\t2022-02-25T00:00:00.000000Z\n" +
                    "1\tabc\t2022-02-25T00:00:00.000000Z\n" +
                    "1\tabc\t2022-02-25T00:00:00.000000Z\n", "select * from " + newTableName);
        });
    }

    @Test
    public void testSimpleRename() throws Exception {
        assertMemoryLeak(
                () -> {
                    createX();
                    ddl("rename table 'x' to 'y'");
                    assertQuery("i\tsym\tamt\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                                    "1\tmsft\t50.938\t2018-01-01T00:12:00.000000Z\tfalse\tXYZ\t0.4621835429127854\t0.5599\t31\t2015-06-22T18:58:53.562Z\tPEHN\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                                    "2\tgoogl\t42.281\t2018-01-01T00:24:00.000000Z\tfalse\tABC\t0.4138164748227684\t0.5522\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                                    "3\tgoogl\t17.371\t2018-01-01T00:36:00.000000Z\tfalse\tABC\t0.05384400312338511\t0.0975\t327\t2015-09-26T18:05:10.217Z\tHYRX\t-3214230645884399728\t1970-01-01T00:33:20.000000Z\t28\t00000000 8e e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28\tSUWDSWUGS\n" +
                                    "4\tibm\t44.805\t2018-01-01T00:48:00.000000Z\ttrue\tXYZ\t0.14830552335848957\t0.9442\t95\t2015-01-04T19:58:55.654Z\tPEHN\t-5024542231726589509\t1970-01-01T00:50:00.000000Z\t39\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\tOJIPHZEPIHVL\n" +
                                    "5\tgoogl\t42.956\t2018-01-01T01:00:00.000000Z\ttrue\t\t0.22895725920713628\t0.9821\t696\t2015-03-18T09:57:14.898Z\tCPSW\t-8757007522346766135\t1970-01-01T01:06:40.000000Z\t23\t\t\n" +
                                    "6\tibm\t82.59700000000001\t2018-01-01T01:12:00.000000Z\ttrue\tCDE\t0.021189232728939578\tNaN\t369\t2015-07-21T10:33:47.953Z\tPEHN\t-9147563299122452591\t1970-01-01T01:23:20.000000Z\t48\t00000000 18 93 bd 0b 61 f5 5d d0 eb 67 44 a7 6a\t\n" +
                                    "7\tgoogl\t98.59100000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:40:00.000000Z\t31\t\tHZSQLDGLOGIFO\n" +
                                    "8\tgoogl\t57.086\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.6707018622395736\t0.0759\t199\t2015-09-12T07:21:40.050Z\t\t-4058426794463997577\t1970-01-01T01:56:40.000000Z\t37\t00000000 ea 4e ea 8b f5 0f 2d b3 14 33\tFFLRBROMNXKUIZ\n" +
                                    "9\tgoogl\t81.44200000000001\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.2677326840703891\t0.5425\t1001\t2015-11-14T07:05:22.934Z\tHYRX\t-8793423647053878901\t1970-01-01T02:13:20.000000Z\t33\t00000000 25 c2 20 ff 70 3a c7 8a b3 14 cd 47 0b 0c\tFMQNTOG\n" +
                                    "10\tmsft\t3.973\t2018-01-01T02:00:00.000000Z\tfalse\tXYZ\tNaN\tNaN\t828\t2015-06-18T18:07:42.406Z\tPEHN\t-7398902448022205322\t1970-01-01T02:30:00.000000Z\t50\t00000000 fb 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff\n" +
                                    "00000010 9a ef 88 cb\tCNGTNLEGPUHH\n",
                            "y",
                            null,

                            "timestamp",
                            true,
                            true
                    );
                }
        );
    }

    private void createX() throws SqlException {
        ddl(
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
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(10)" +
                        ") timestamp (timestamp);"
        );
    }
}
