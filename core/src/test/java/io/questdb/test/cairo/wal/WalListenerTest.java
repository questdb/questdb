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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.DefaultWalListener;
import io.questdb.cairo.wal.WalListener;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class WalListenerTest extends AbstractCairoTest {

    private static final TestWalListener listener = new TestWalListener();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        listener.events.clear();
        engine.setWalListener(listener);
    }

    @AfterClass
    public static void tearDownStatic() {
        engine.setWalListener(DefaultWalListener.INSTANCE);
        if (!listener.events.isEmpty()) {
            System.err.println("Unexpected or unasserted WalListener events:");
            for (WalListenerEvent event : listener.events) {
                System.err.println("    " + event);
            }
            Assert.fail();
        }
        AbstractCairoTest.tearDownStatic();
    }

    @Test
    public void testWalListener() throws Exception {
        final AtomicReference<TableToken> tableToken1 = new AtomicReference<>();
        final AtomicReference<TableToken> tableToken2 = new AtomicReference<>();
        assertMemoryLeak(() -> {
            tableToken1.set(createTable(testName.getMethodName()));
            assertTableExistence(true, tableToken1.get());

            Assert.assertEquals(
                    new WalListenerEvent(
                            WalListenerEventType.TABLE_CREATED,
                            tableToken1.get(),
                            0,
                            0,
                            -1,
                            -1,
                            -1,
                            null
                    ),
                    listener.events.remove()
            );

            try (WalWriter walWriter1 = engine.getWalWriter(tableToken1.get())) {
                final TableWriter.Row row = walWriter1.newRow(0);
                row.putByte(0, (byte) 1);
                row.append();
                walWriter1.commit();
                Assert.assertEquals(
                        new WalListenerEvent(
                                WalListenerEventType.DATA_TXN_COMMITTED,
                                tableToken1.get(),
                                1,
                                0,
                                1,
                                0,
                                0,
                                null
                        ),
                        listener.events.remove()
                );
                Assert.assertEquals(0, listener.events.size());

                final String newTableName = tableToken1.get().getTableName() + "_new";
                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    tableToken2.set(engine.rename(
                            securityContext,
                            Path.getThreadLocal(""),
                            mem,
                            tableToken1.get().getTableName(),
                            Path.getThreadLocal2(""),
                            newTableName
                    ));

                    Assert.assertEquals(newTableName, tableToken2.get().getTableName());
                    Assert.assertEquals(tableToken1.get().getTableId(), tableToken2.get().getTableId());
                    Assert.assertEquals(tableToken1.get().getDirName(), tableToken2.get().getDirName());

                    Assert.assertEquals(
                            new WalListenerEvent(
                                    WalListenerEventType.TABLE_RENAMED,
                                    tableToken2.get(),
                                    2,
                                    0,
                                    -1,
                                    -1,
                                    -1,
                                    tableToken1.get()
                            ),
                            listener.events.remove()
                    );

                    drainWalQueue();
                    releaseInactive(engine);

                    // The rename WalWriter (walId=2) is released from pool and closed,
                    // generating WAL_CLOSED. Uses tableToken2 because the rename updated the token.
                    // Empty segment does not generate SEGMENT_CLOSED.
                    // txn=2 because the rename operation was assigned sequencer txn 2.
                    Assert.assertEquals(
                            new WalListenerEvent(
                                    WalListenerEventType.WAL_CLOSED,
                                    tableToken2.get(),
                                    2,
                                    0,
                                    2,
                                    -1,
                                    -1,
                                    null
                            ),
                            listener.events.remove()
                    );
                }
            }

            releaseInactive(engine);

            Assert.assertEquals(
                    new WalListenerEvent(
                            WalListenerEventType.SEGMENT_CLOSED,
                            tableToken1.get(),
                            1,
                            0,
                            1,
                            0,
                            -1,
                            null
                    ),
                    listener.events.remove()
            );

            // txn=1 because walWriter1 committed data at txn 1.
            Assert.assertEquals(
                    new WalListenerEvent(
                            WalListenerEventType.WAL_CLOSED,
                            tableToken1.get(),
                            1,
                            0,
                            1,
                            -1,
                            -1,
                            null
                    ),
                    listener.events.remove()
            );

            try (WalWriter walWriter2 = engine.getWalWriter(tableToken2.get())) {
                walWriter2.addColumn("c", ColumnType.INT);

                Assert.assertEquals(
                        new WalListenerEvent(
                                WalListenerEventType.NON_DATA_TXN_COMMITTED,
                                tableToken2.get(),
                                3,
                                0,
                                -1,
                                -1,
                                -1,
                                null
                        ),
                        listener.events.remove()
                );
            }

            releaseInactive(engine);

            // No data event, segment closed ignored, but WAL closed is still emitted.
            // walId=3 because: walWriter1=1, rename operation=2, walWriter2=3
            // txn=3 because walWriter2 committed addColumn at txn 3.
            Assert.assertEquals(
                    new WalListenerEvent(
                            WalListenerEventType.WAL_CLOSED,
                            tableToken2.get(),
                            3,
                            0,
                            3,
                            -1,
                            -1,
                            null
                    ),
                    listener.events.remove()
            );

            engine.dropTableOrViewOrMatView(Path.getThreadLocal(""), tableToken2.get());

            Assert.assertEquals(
                    new WalListenerEvent(
                            WalListenerEventType.TABLE_DROPPED,
                            tableToken2.get(),
                            4,
                            0,
                            -1,
                            -1,
                            -1,
                            null
                    ),
                    listener.events.remove()
            );
        });
    }

    static TableToken createTable(String tableName) {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("a", ColumnType.BYTE)
                .col("b", ColumnType.STRING)
                .timestamp("ts")
                .wal();
        return createTable(model);
    }

    enum WalListenerEventType {
        DATA_TXN_COMMITTED,
        NON_DATA_TXN_COMMITTED,
        SEGMENT_CLOSED,
        TABLE_CREATED,
        TABLE_DROPPED,
        TABLE_RENAMED,
        WAL_CLOSED
    }

    static class TestWalListener implements WalListener {
        public Deque<WalListenerEvent> events = new ArrayDeque<>();

        @Override
        public void dataTxnCommitted(TableToken tableToken, long txn, long timestamp, int walId, int segmentId, int segmentTxn) {
            events.add(new WalListenerEvent(
                    WalListenerEventType.DATA_TXN_COMMITTED,
                    tableToken,
                    txn,
                    timestamp,
                    walId,
                    segmentId,
                    segmentTxn,
                    null
            ));
        }

        @Override
        public void nonDataTxnCommitted(TableToken tableToken, long txn, long timestamp) {
            events.add(new WalListenerEvent(
                    WalListenerEventType.NON_DATA_TXN_COMMITTED,
                    tableToken,
                    txn,
                    timestamp,
                    -1,
                    -1,
                    -1,
                    null
            ));
        }

        @Override
        public void segmentClosed(TableToken tableToken, long txn, int walId, int segmentId) {
            events.add(new WalListenerEvent(
                    WalListenerEventType.SEGMENT_CLOSED,
                    tableToken,
                    txn,
                    0,
                    walId,
                    segmentId,
                    -1,
                    null
            ));
        }

        @Override
        public void tableCreated(TableToken tableToken, long timestamp) {
            events.add(new WalListenerEvent(
                    WalListenerEventType.TABLE_CREATED,
                    tableToken,
                    0,
                    timestamp,
                    -1,
                    -1,
                    -1,
                    null
            ));
        }

        @Override
        public void tableDropped(TableToken tableToken, long txn, long timestamp) {
            events.add(new WalListenerEvent(
                    WalListenerEventType.TABLE_DROPPED,
                    tableToken,
                    txn,
                    timestamp,
                    -1,
                    -1,
                    -1,
                    null
            ));
        }

        @Override
        public void tableRenamed(TableToken tableToken, long txn, long timestamp, TableToken oldTableToken) {
            events.add(new WalListenerEvent(
                    WalListenerEventType.TABLE_RENAMED,
                    tableToken,
                    txn,
                    timestamp,
                    -1,
                    -1,
                    -1,
                    oldTableToken
            ));
        }

        @Override
        public void walClosed(TableToken tableToken, long txn, int walId) {
            events.add(new WalListenerEvent(
                    WalListenerEventType.WAL_CLOSED,
                    tableToken,
                    txn,
                    0,
                    walId,
                    -1,
                    -1,
                    null
            ));
        }
    }

    record WalListenerEvent(WalListenerEventType type, TableToken tableToken, long txn, long timestamp, int walId,
                            int segmentId, int segmentTxn, TableToken oldTableToken) {

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof WalListenerEvent that) {
                return this.type == that.type &&
                        Objects.equals(this.tableToken, that.tableToken) &&
                        this.txn == that.txn &&
                        // this.timestamp == that.timestamp && // do not compare timestamp in tests
                        this.walId == that.walId &&
                        this.segmentId == that.segmentId &&
                        this.segmentTxn == that.segmentTxn &&
                        Objects.equals(this.oldTableToken, that.oldTableToken);
            }
            return false;
        }

        @Override
        public @NotNull String toString() {
            return "WalListenerEvent{" +
                    "type=" + type +
                    ", tableToken=" + tableToken +
                    ", txn=" + txn +
                    ", timestamp=" + timestamp +
                    ", walId=" + walId +
                    ", segmentId=" + segmentId +
                    ", segmentTxn=" + segmentTxn +
                    ", oldTableToken=" + oldTableToken +
                    '}';
        }
    }
}
