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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.TxnScoreboardV2;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderTxnScoreboardInteractionTest extends AbstractCairoTest {

    @Test
    public void testVanilla() throws Exception {
        assertMemoryLeak(() -> {

            TableToken tt = createTable();

            try (TableReader reader = getReader(tt)) {
                TxnScoreboard txnScoreboard = reader.getTxnScoreboard();
                // when table is empty the "min" is set to max long
                Assert.assertEquals(0, getMin(txnScoreboard));
                Assert.assertEquals(0, reader.getTxn());
            }

            try (TableWriter w = getWriter(tt)) {
                addRow(w);
                final TxnScoreboard txnScoreboard = w.getTxnScoreboard();
                try (TableReader reader = getReader(tt)) {
                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, getMin(txnScoreboard));
                    Assert.assertFalse(txnScoreboard.isTxnAvailable(1));

                    addRow(w);

                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, getMin(txnScoreboard));
                    Assert.assertFalse(txnScoreboard.isTxnAvailable(1));
                }

                try (TableReader reader = getReader(tt)) {
                    Assert.assertEquals(2, reader.getTxn());
                    Assert.assertEquals(2, getMin(txnScoreboard));
                    Assert.assertFalse(txnScoreboard.isTxnAvailable(2));

                    try (TableReader reader2 = getReader(tt)) {
                        Assert.assertEquals(2, reader2.getTxn());
                        Assert.assertEquals(2, getMin(txnScoreboard));
                        Assert.assertFalse(txnScoreboard.isTxnAvailable(2));

                        addRow(w);
                        try (TableReader reader3 = getReader(tt)) {
                            Assert.assertEquals(3, reader3.getTxn());
                            Assert.assertEquals(2, getMin(txnScoreboard));
                            Assert.assertFalse(txnScoreboard.isTxnAvailable(2));
                            Assert.assertFalse(txnScoreboard.isTxnAvailable(3));
                        }
                        // bump max txn to new value, > 3
                        txnScoreboard.acquireTxn(5, 4);
                        txnScoreboard.releaseTxn(5, 4);

                        // expect 0, the writer is released
                        Assert.assertTrue(txnScoreboard.isTxnAvailable(3));
                    }

                    Assert.assertTrue(txnScoreboard.isTxnAvailable(3));
                    Assert.assertFalse(txnScoreboard.isTxnAvailable(2));
                }
                Assert.assertTrue(txnScoreboard.isTxnAvailable(2));
                Assert.assertTrue(txnScoreboard.isTxnAvailable(3));

                w.addColumn("z", ColumnType.LONG);

                try (TableReader reader = getReader(tt)) {
                    Assert.assertEquals(4, reader.getTxn());
                    Assert.assertEquals(4, getMin(txnScoreboard));
                    Assert.assertFalse(txnScoreboard.isTxnAvailable(4));
                }

                assertMin(4, txnScoreboard);

                try (TableReader reader = getReader(tt)) {
                    Assert.assertEquals(4, reader.getTxn());
                    assertMin(4, txnScoreboard);
                    Assert.assertFalse(txnScoreboard.isTxnAvailable(4));
                }
                // bump max txn to new value, > 4
                txnScoreboard.acquireTxn(5, 5);
                txnScoreboard.releaseTxn(5, 5);
                Assert.assertTrue(txnScoreboard.isTxnAvailable(4));
            }
        });
    }

    @Test
    public void testVanillaNoEngine() throws Exception {
        assertMemoryLeak(() -> {
            createTable();

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                TxnScoreboard txnScoreboard = reader.getTxnScoreboard();
                // when table is empty the "min" is set to max long
                Assert.assertEquals(0, getMin(txnScoreboard));
                Assert.assertEquals(0, reader.getTxn());
            }

            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                addRow(writer);

                final TxnScoreboard txnScoreboard = writer.getTxnScoreboard();

                try (TableReader reader = newOffPoolReader(configuration, "x")) {
                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, getMin(txnScoreboard));
                    Assert.assertFalse(txnScoreboard.isTxnAvailable(1));

                    addRow(writer);

                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, getMin(txnScoreboard));
                    Assert.assertFalse(txnScoreboard.isTxnAvailable(1));

                    Assert.assertTrue(reader.reload());

                    Assert.assertEquals(2, reader.getTxn());
                    Assert.assertEquals(2, getMin(txnScoreboard));
                    Assert.assertFalse(txnScoreboard.isTxnAvailable(2));
                }

                assertMin(2, txnScoreboard);
                txnScoreboard.acquireTxn(0, 3);
                Assert.assertTrue(txnScoreboard.isTxnAvailable(2));
            }
        });
    }

    private void assertMin(int min, TxnScoreboard txnScoreboard) {
        Assert.assertTrue(min == ((TxnScoreboardV2) txnScoreboard).getMin() || -1 == ((TxnScoreboardV2) txnScoreboard).getMin());
    }

    private static long getMin(TxnScoreboard scoreboard) {
        return ((TxnScoreboardV2) scoreboard).getMin();
    }

    private static void addRow(TableWriter w) {
        TableWriter.Row r = w.newRow();
        r.putByte(0, (byte) 9);
        r.putShort(1, (short) 89);
        r.append();
        w.commit();
    }

    private static TableToken createTable() {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE);
        model
                .col("a", ColumnType.BYTE)
                .col("b", ColumnType.SHORT);
        return AbstractCairoTest.create(model);
    }
}
