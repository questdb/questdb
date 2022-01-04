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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.tcp.fuzzer.LineData;
import io.questdb.cutlass.line.tcp.fuzzer.TableData;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.*;
import org.junit.*;

import java.util.concurrent.atomic.AtomicLong;

public class LineTcpReceiverLoadTest extends AbstractLineTcpReceiverTest {
    private static final Log LOG = LogFactory.getLog(LineTcpReceiverLoadTest.class);
    private static final Rnd random = new Rnd();

    private final AtomicLong timestampMillis = new AtomicLong(1465839830102300L);

    private final int numOfLines = 10000;
    private final int numOfIterations = 10;
    private final int numOfThreads = 10;
    private final int numOfTables = 10;

    private final SOCountDownLatch threadPushFinished = new SOCountDownLatch(numOfThreads - 1);

    private final String[] colNameBases = new String[] {"location", "temperature"};
    private final String[] colValueBases = new String[] {"us-midwest", "8"};
    private final CharSequenceObjHashMap<TableData> tables = new CharSequenceObjHashMap<>();

    @Test
    public void testLoad() throws Exception {
        runInContext(receiver -> {
            for (int i = 0; i < numOfTables; i++) {
                final CharSequence tableName = getTableName(i);
                tables.put(tableName, new TableData(tableName));
            }

            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                    final TableData table  = tables.get(name);
                    if (!table.isChecked()) {
                        checkTableReady(table);
                    } else {
                        table.setChecked(false);
                    }
                }
            });

            try {
                for (int i = 0; i < numOfThreads; i++) {
                    new Thread(() -> {
                        final StringBuilder sb = new StringBuilder();
                        try {
                            for (int n = 0; n < numOfIterations; n++) {
                                sb.setLength(0);
                                for (int j = 0; j < numOfLines; j++) {
                                    final TableData table = pickTable();
                                    final CharSequence tableName = table.getName();
                                    final LineData line = generateLine();
                                    table.addLine(line);
                                    sb.append(line.toLine(tableName));
                                }
                                send(receiver, sb.toString());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            threadPushFinished.countDown();
                        }
                    }).start();
                }
                threadPushFinished.await();

                for (int i = 0; i < numOfTables; i++) {
                    final CharSequence tableName = getTableName(i);
                    final TableData table = tables.get(tableName);
                    table.await();
                    assertTable(table);
                }
            } finally {
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {});
            }
        });
    }

    protected void checkTableReady(TableData table) {
        if (threadPushFinished.getCount() > 0) {
            // we are still sending, no point to check the table yet
            return;
        }
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, table.getName(), "checkTableReady")) {
            LOG.info().$("table.getName(): ").$(table.getName()).$(", table.size(): ").$(table.size()).$(", writer.size(): ").$(writer.size()).$();
            table.setReady(writer);
            table.setChecked(true);
        }
    }

    protected void assertTable(TableData table) {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, table.getName())) {
            final TableReaderMetadata metadata = reader.getMetadata();
            assertCursorTwoPass(table.generateRows(metadata), reader.getCursor(), metadata);
        }
    }

    private TableData pickTable() {
        return tables.get(getTableName(random.nextInt(numOfTables)));
    }

    private CharSequence getTableName(int tableIndex) {
        return "weather" + tableIndex;
    }

    private LineData generateLine() {
        final LineData line = new LineData(timestampMillis.incrementAndGet());
        for (int j = 0; j < colNameBases.length; j++) {
            final CharSequence colName = colNameBases[j];
            final CharSequence colValue = colValueBases[j] + (j == 1 ? random.nextInt(9) + ".0" : "");
            line.add(colName, colValue);
        }
        return line;
    }

    private void send(LineTcpReceiver receiver, String lineData) {
        LOG.info().$("ilp:\n").$(lineData).$();
        send(receiver, null, WAIT_NO_WAIT, () -> sendToSocket(lineData, false));
    }

    @Override
    protected WorkerPoolConfiguration getWorkerPoolConfiguration() {
        return new WorkerPoolConfiguration() {
            private final int[] affinity = {-1, -1, -1, -1};

            @Override
            public int[] getWorkerAffinity() {
                return affinity;
            }

            @Override
            public int getWorkerCount() {
                return 4;
            }

            @Override
            public boolean haltOnError() {
                return true;
            }
        };
    }
}
