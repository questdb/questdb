/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.CairoTestUtils;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableModel;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertStatement;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OutOfOrderTest extends AbstractGriffinTest {

    private final long MICROS_IN_A_DAY = 86_400_000_000L;

    @Test
    public void testOutOfOrderWithOnePartition() throws Exception {
        assertMemoryLeak(() -> {
            String ddl = "create table sd (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY";
            String query = "select * from sd";
            String expected = "sym\tprice\tts\n";
            compiler.compile(ddl, sqlExecutionContext);
            printSqlResult(expected, query, "ts", null, null, true, true);
            // 1970-Jan-01
            executeInsertStatement("AA", 0);
            expected = "sym\tprice\tts\n" +
                    "AA\t1.3\t1970-01-01T00:00:00.000000Z\n";
            printSqlResult(expected, query, "ts", null, null, true, true);
            // 1970-Jan-02
            executeInsertStatement("BB",MICROS_IN_A_DAY);
            expected = "sym\tprice\tts\n" +
                    "AA\t1.3\t1970-01-01T00:00:00.000000Z\n" +
                    "BB\t1.3\t1970-01-02T00:00:00.000000Z\n";
            printSqlResult(expected, query, "ts", null, null, true, true);
            // 1970-Jan-04
            long time = MICROS_IN_A_DAY * 3;
            executeInsertStatement("ZZ",time);
            expected = "sym\tprice\tts\n" +
                    "AA\t1.3\t1970-01-01T00:00:00.000000Z\n" +
                    "BB\t1.3\t1970-01-02T00:00:00.000000Z\n" +
                    "ZZ\t1.3\t1970-01-04T00:00:00.000000Z\n";
            printSqlResult(expected, query, "ts", null, null, true, true);
            // 1970-Jan-03
            time = MICROS_IN_A_DAY * 2;
            executeInsertStatement("CC",time);
//            expected = "sym\tprice\tts\n" +
//                    "A\t1.3\t1970-01-01T00:00:00.000000Z\n" +
//                    "B\t1.3\t1970-01-02T00:00:00.000000Z\n" +
//                    "C\t1.3\t1970-01-03T00:00:00.000000Z\n" +
//                    "Z\t1.3\t1970-01-04T00:00:00.000000Z\n";
            expected = "sym\tprice\tts\n" +
                    "AA\t1.3\t1970-01-01T00:00:00.000000Z\n" +
                    "BB\t1.3\t1970-01-02T00:00:00.000000Z\n" +
                    "ZZ\t1.3\t1970-01-04T00:00:00.000000Z\n";
            printSqlResult(expected, query, "ts", null, null, true, true);
        });
    }


    @NotNull
    private void executeInsertStatement(String sym, long time) throws SqlException {
        String ddl = "insert into sd (sym, price, ts) values ('"+sym+"', 1.3, " + time + ")";
        executeInsert(ddl);
    }

    private void executeInsert(String ddl) throws SqlException {
        CompiledQuery compiledQuery = compiler.compile(ddl, sqlExecutionContext);
        final InsertStatement insertStatement = compiledQuery.getInsertStatement();
        try (InsertMethod insertMethod = insertStatement.createMethod(sqlExecutionContext)) {
            insertMethod.execute();
            insertMethod.commit();
        }
    }

    @Test
    public void testOutOfOrderWithOnePartition2() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "test";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("x", ColumnType.SYMBOL).timestamp()
            ) {
                CairoTestUtils.create(model);
                compiler.compile(
                        "insert into test select * from (" +
                                "select" +
                                " rnd_symbol('xx', 'yy', 'zz')," +
                                " timestamp_sequence(0, (x/100) * 1000000*60*60*24*2 + (x%100)) k" +
                                " from" +
                                " long_sequence(120)) timestamp(k)"
                        ,
                        sqlExecutionContext
                );

                assertTrue(engine.migrateNullFlag(sqlExecutionContext.getCairoSecurityContext(), tableName));
                // second time must not update
                assertFalse(engine.migrateNullFlag(sqlExecutionContext.getCairoSecurityContext(), tableName));
            }
        });
    }
}
