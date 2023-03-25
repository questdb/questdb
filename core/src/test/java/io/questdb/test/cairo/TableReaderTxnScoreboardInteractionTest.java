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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
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
                Assert.assertEquals(0, txnScoreboard.getMin());
                Assert.assertEquals(0, reader.getTxn());
            }

            try (TableWriter w = getWriter(tt)) {
                addRow(w);
                final TxnScoreboard txnScoreboard = w.getTxnScoreboard();
                try (TableReader reader = getReader(tt)) {
                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, txnScoreboard.getMin());
                    Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(1));

                    addRow(w);

                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, txnScoreboard.getMin());
                    Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(1));
                }

                try (TableReader reader = getReader(tt)) {
                    Assert.assertEquals(2, reader.getTxn());
                    Assert.assertEquals(2, txnScoreboard.getMin());
                    Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(2));

                    try (TableReader reader2 = getReader(tt)) {
                        Assert.assertEquals(2, reader2.getTxn());
                        Assert.assertEquals(2, txnScoreboard.getMin());
                        Assert.assertEquals(2, txnScoreboard.getActiveReaderCount(2));

                        addRow(w);
                        try (TableReader reader3 = getReader(tt)) {
                            Assert.assertEquals(3, reader3.getTxn());
                            Assert.assertEquals(2, txnScoreboard.getMin());
                            Assert.assertEquals(2, txnScoreboard.getActiveReaderCount(2));
                            Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(3));
                        }
                        // expect 0, writer is released
                        Assert.assertEquals(0, txnScoreboard.getActiveReaderCount(3));
                    }

                    Assert.assertEquals(0, txnScoreboard.getActiveReaderCount(3));
                    Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(2));
                }
                Assert.assertEquals(0, txnScoreboard.getActiveReaderCount(2));
                Assert.assertEquals(0, txnScoreboard.getActiveReaderCount(3));

                w.addColumn("z", ColumnType.LONG);

                try (TableReader reader = getReader(tt)) {
                    Assert.assertEquals(4, reader.getTxn());
                    Assert.assertEquals(4, txnScoreboard.getMin());
                    Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(4));
                    Assert.assertEquals(0, txnScoreboard.getActiveReaderCount(5));
                }

                Assert.assertEquals(4, txnScoreboard.getMin());
                Assert.assertEquals(0, txnScoreboard.getActiveReaderCount(4));

                try (TableReader reader = getReader(tt)) {
                    Assert.assertEquals(4, reader.getTxn());
                    Assert.assertEquals(4, txnScoreboard.getMin());
                    Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(4));
                }
                Assert.assertEquals(0, txnScoreboard.getActiveReaderCount(4));
            }
        });
    }

    @Test
    public void testVanillaNoEngine() throws Exception {
        assertMemoryLeak(() -> {
            createTable();

            try (TableReader reader = newTableReader(configuration, "x")) {
                TxnScoreboard txnScoreboard = reader.getTxnScoreboard();
                // when table is empty the "min" is set to max long
                Assert.assertEquals(0, txnScoreboard.getMin());
                Assert.assertEquals(0, reader.getTxn());
            }

            try (TableWriter w = newTableWriter(configuration, "x", metrics)) {
                addRow(w);

                final TxnScoreboard txnScoreboard = w.getTxnScoreboard();

                try (TableReader reader = newTableReader(configuration, "x")) {
                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, txnScoreboard.getMin());
                    Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(1));

                    addRow(w);

                    Assert.assertEquals(1, reader.getTxn());
                    Assert.assertEquals(1, txnScoreboard.getMin());
                    Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(1));

                    Assert.assertTrue(reader.reload());

                    Assert.assertEquals(2, reader.getTxn());
                    Assert.assertEquals(2, txnScoreboard.getMin());
                    Assert.assertEquals(1, txnScoreboard.getActiveReaderCount(2));
                }

                Assert.assertEquals(2, txnScoreboard.getMin());
                Assert.assertEquals(0, txnScoreboard.getActiveReaderCount(2));
            }
        });
    }

    private static void addRow(TableWriter w) {
        TableWriter.Row r = w.newRow();
        r.putByte(0, (byte) 9);
        r.putShort(1, (short) 89);
        r.append();
        w.commit();
    }

    private static TableToken createTable() {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
            model
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SHORT);
            return CreateTableTestUtils.create(model);
        }
    }
}
