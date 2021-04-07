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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderTxnScoreboardInteractionTest extends AbstractCairoTest {

    @Test
    public void testVanilla() throws Exception {
        assertMemoryLeak(() -> {

            createTable();

            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                long txnScoreboard = reader.getTxnScoreboard();
                // when table is empty the "min" is set to max long
                Assert.assertEquals(0, TxnScoreboard.getMin(txnScoreboard));
                Assert.assertEquals(0, reader.getTxn());
            }

            try (TableWriter w = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                addRow(w);
                long txnScoreboard;
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                    txnScoreboard = reader.getTxnScoreboard();
                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, TxnScoreboard.getMin(txnScoreboard));
                    Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 1));

                    addRow(w);

                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, TxnScoreboard.getMin(txnScoreboard));
                    Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 1));
                }

                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                    Assert.assertEquals(2, reader.getTxn());
                    Assert.assertEquals(2, TxnScoreboard.getMin(txnScoreboard));
                    Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 2));

                    try (TableReader reader2 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                        Assert.assertEquals(2, reader2.getTxn());
                        Assert.assertEquals(2, TxnScoreboard.getMin(txnScoreboard));
                        Assert.assertEquals(2, TxnScoreboard.getCount(txnScoreboard, 2));

                        addRow(w);
                        try (TableReader reader3 = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                            Assert.assertEquals(3, reader3.getTxn());
                            Assert.assertEquals(2, TxnScoreboard.getMin(txnScoreboard));
                            Assert.assertEquals(2, TxnScoreboard.getCount(txnScoreboard, 2));
                            Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 3));
                        }
                    }

                    Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 2));
                }

                w.addColumn("z", ColumnType.LONG);

                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                    Assert.assertEquals(4, reader.getTxn());
                    Assert.assertEquals(4, TxnScoreboard.getMin(txnScoreboard));
                    Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 4));
                    Assert.assertEquals(0, TxnScoreboard.getCount(txnScoreboard, 5));
                }

                Assert.assertEquals(4, TxnScoreboard.getMin(txnScoreboard));
                Assert.assertEquals(0, TxnScoreboard.getCount(txnScoreboard, 4));

                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                    Assert.assertEquals(4, reader.getTxn());
                    Assert.assertEquals(4, TxnScoreboard.getMin(txnScoreboard));
                    Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 4));
                }
            }
        });
    }

    @Test
    public void testVanillaNoEngine() throws Exception {
        assertMemoryLeak(() -> {
            createTable();

            try (TableReader reader = new TableReader(configuration, "x")) {
                long txnScoreboard = reader.getTxnScoreboard();
                // when table is empty the "min" is set to max long
                Assert.assertEquals(0, TxnScoreboard.getMin(txnScoreboard));
                Assert.assertEquals(0, reader.getTxn());
            }

            try (TableWriter w = new TableWriter(configuration, "x")) {
                addRow(w);

                try (TableReader reader = new TableReader(configuration, "x")) {
                    long txnScoreboard = reader.getTxnScoreboard();
                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, TxnScoreboard.getMin(txnScoreboard));
                    Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 1));

                    addRow(w);

                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, TxnScoreboard.getMin(txnScoreboard));
                    Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 1));

                    Assert.assertTrue(reader.reload());

                    Assert.assertEquals(2, reader.getTxn());
                    Assert.assertEquals(2, TxnScoreboard.getMin(txnScoreboard));
                    Assert.assertEquals(1, TxnScoreboard.getCount(txnScoreboard, 2));

                    reader.goPassive();
                    Assert.assertEquals(2, TxnScoreboard.getMin(txnScoreboard));
                    Assert.assertEquals(0, TxnScoreboard.getCount(txnScoreboard, 2));
                }
            }
        });
    }

    private static void createTable() {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
            model
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SHORT);
            CairoTestUtils.create(model);
        }
    }

    private static void addRow(TableWriter w) {
        TableWriter.Row r = w.newRow();
        r.putByte(0, (byte) 9);
        r.putShort(1, (short) 89);
        r.append();
        w.commit();
    }
}
