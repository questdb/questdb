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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.wal.TableWriterFrontend;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.griffin.wal.fuzz.FuzzTransaction;
import io.questdb.griffin.wal.fuzz.FuzzTransactionGenerator;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

public class WalWriterFuzzTest extends AbstractGriffinTest {
    @Test
    @Ignore
    public void testWalAddRemoveCommitFuzz() throws Exception {
        assertMemoryLeak(() -> {
            String tableName1 = testName.getMethodName() + "_wal";
            String tableName2 = testName.getMethodName() + "_nonwal";

            compile("create table " + tableName1 + " as (" +
                    "select x as c1, " +
                    " rnd_symbol('AB', 'BC', 'CD') c2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                    " cast(x as int) c3," +
                    " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk') " +
                    " from long_sequence(5000)" +
                    ") timestamp(ts) partition by DAY WAL");

            SharedRandom.RANDOM.set(new Rnd());
            compile("create table " + tableName2 + " as (" +
                    "select x as c1, " +
                    " rnd_symbol('AB', 'BC', 'CD') c2, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                    " cast(x as int) c3," +
                    " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk') " +
                    " from long_sequence(5000)" +
                    ") timestamp(ts) partition by DAY BYPASS WAL");

            ObjList<FuzzTransaction> transactions;
            int tableId1, tableId2;
            try (TableReader reader = new TableReader(configuration, tableName1)) {
                var metadata = reader.getMetadata();
                tableId1 = metadata.getTableId();
                transactions = FuzzTransactionGenerator.generateSet(
                        metadata,
                        new Rnd(),
                        IntervalUtils.parseFloorPartialDate("2022-02-24T17"),
                        IntervalUtils.parseFloorPartialDate("2022-02-27T17"),
                        100_000,
                        true,
                        0.05,
                        0.2,
                        0.1,
                        0.005,
                        0.05,
                        0.01,
                        100,
                        20,
                        10000
                );
            }
            try (TableReader reader = new TableReader(configuration, tableName1)) {
                var metadata = reader.getMetadata();
                tableId2 = metadata.getTableId();
            }

            applyNonWal(transactions, tableName2, tableId2);
            applyWal(transactions, tableName1, tableId1, 3);

            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableName2, tableName1, LOG);
        });
    }

    private void applyWal(ObjList<FuzzTransaction> transactions, String tableName, int tableId, int walWriterCount) {
        ObjList<WalWriter> writers = new ObjList<>();
        for(int i = 0; i < walWriterCount; i++) {
            writers.add((WalWriter) engine.getTableWriterFrontEnd(sqlExecutionContext.getCairoSecurityContext(), tableName, "apply trans test"));
        }

        IntList tempList = new IntList();
        Rnd writerRnd = new Rnd();
        for (int i = 0, n = transactions.size(); i < n; i++) {
            WalWriter writer = writers.getQuick(writerRnd.nextPositiveInt() % walWriterCount);
            writer.goActive();
            FuzzTransaction transaction = transactions.getQuick(i);
            for (int tranIndex = 0; tranIndex < transaction.operationList.size(); tranIndex++) {
                var operation = transaction.operationList.getQuick(tranIndex);
                operation.apply(writer, tableName, tableId, tempList);
            }

            if (transaction.rollback) {
                writer.rollback();
            } else {
                writer.commit();
            }
        }

        Misc.freeObjList(writers);
        drainWalQueue();
    }

    private static void applyNonWal(ObjList<FuzzTransaction> transactions, String tableName, int tableId) {
        try(TableWriterFrontend writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "apply trans test")) {
            IntList tempList = new IntList();
            int transactionSize = transactions.size();
            for (int i = 0; i < transactionSize; i++) {
                FuzzTransaction transaction = transactions.getQuick(i);
                int size = transaction.operationList.size();
                for (int tranIndex = 0; tranIndex < size; tranIndex++) {
                    var operation = transaction.operationList.getQuick(tranIndex);
                    operation.apply(writer, tableName, tableId, tempList);
                }

                if (transaction.rollback) {
                    writer.rollback();
                } else {
                    writer.commit();
                }
            }
        }
    }
}
