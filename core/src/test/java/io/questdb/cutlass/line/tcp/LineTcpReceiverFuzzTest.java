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
import io.questdb.cutlass.line.tcp.load.LineData;
import io.questdb.cutlass.line.tcp.load.TableData;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.ColumnType.DOUBLE;
import static io.questdb.cairo.ColumnType.STRING;

public class LineTcpReceiverFuzzTest extends AbstractLineTcpReceiverTest {
    private static final Log LOG = LogFactory.getLog(LineTcpReceiverFuzzTest.class);

    private static final int MAX_NUM_OF_SKIPPED_COLS = 2;
    private static final int NEW_COLUMN_RANDOMIZE_FACTOR = 2;

    private final Rnd random = new Rnd();
    private final AtomicLong timestampMillis = new AtomicLong(1465839830102300L);
    private final short[] colTypes = new short[]{STRING, DOUBLE, DOUBLE, DOUBLE, STRING, DOUBLE};
    private final String[][] colNameBases = new String[][]{
            {"location", "Location", "LOCATION", "loCATion", "LocATioN"},
            {"temperature", "TEMPERATURE", "Temperature", "TempeRaTuRe"},
            {"humidity", "HUMIdity", "HumiditY", "HUmiDIty", "HUMIDITY", "Humidity"},
            {"hőmérséklet", "HŐMÉRSÉKLET", "HŐmérséKLEt", "hőMÉRséKlET"},
            {"terület", "TERÜLet", "tERülET", "TERÜLET"},
            {"ветер", "Ветер", "ВЕТЕР", "вЕТЕр", "ВетЕР"}
    };
    private final String[] colValueBases = new String[]{"us-midwest", "8", "2", "1", "europe", "6"};
    private final char[] nonAsciiChars = {'ó', 'í', 'Á', 'ч', 'Ъ', 'Ж', 'ю', 0x3000, 0x3080, 0x3a55};

    private int numOfLines;
    private int numOfIterations;
    private int numOfThreads;
    private int numOfTables;
    private long waitBetweenIterationsMillis;

    private SOCountDownLatch threadPushFinished;
    private CharSequenceObjHashMap<TableData> tables;

    private int duplicatesFactor;
    private int columnReorderingFactor;
    private int columnSkipFactor;
    private int nonAsciiValueFactor;
    private int newColumnFactor;
    private boolean diffCasesInColNames;

    // there seem to be an issue with the transactionality od adding new columns
    // when the issue is fixed 'newColumnFactor' can be used and this test should be enabled
    @Ignore
    @Test
    public void testAddColumns() throws Exception {
        initLoadParameters(10, 10, 10, 10, 100);
        initFuzzParameters(-1, -1, -1, 4, -1, false);
        runTest();
    }

    @Test
    public void testDuplicatesReorderingColumns() throws Exception {
        initLoadParameters(500, 15, 15, 15, 50);
        initFuzzParameters(4, 4, -1, -1, -1, true);
        runTest();
    }

    @Test
    public void testLoad() throws Exception {
        initLoadParameters(1000, 10, 10, 10, 50);
        initFuzzParameters(-1, -1, -1, -1, -1, false);
        runTest();
    }

    @Test
    public void testReorderingAddSkipDuplicateColumnsWithNonAscii() throws Exception {
        initLoadParameters(500, 10, 10, 10, 50);
        initFuzzParameters(4, 4, 4, -1, 4, true);
        runTest();
    }

    @Test
    public void testReorderingColumns() throws Exception {
        initLoadParameters(500, 15, 15, 15, 50);
        initFuzzParameters(-1, 4, -1, -1, -1, false);
        runTest();
    }

    private CharSequence addColumn(LineData line, int colIndex) {
        final CharSequence colName = generateName(colIndex, false);
        final CharSequence colValue = generateValue(colIndex);
        line.add(colName, colValue);
        return colName;
    }

    private void addDuplicateColumn(LineData line, int colIndex, CharSequence colName) {
        if (shouldFuzz(duplicatesFactor)) {
            final CharSequence colValueDupe = generateValue(colIndex);
            line.add(colName, colValueDupe);
        }
    }

    private void addNewColumn(LineData line) {
        if (shouldFuzz(newColumnFactor)) {
            final int extraColIndex = random.nextInt(colNameBases.length);
            final CharSequence colNameNew = generateName(extraColIndex, true);
            final CharSequence colValueNew = generateValue(extraColIndex);
            line.add(colNameNew, colValueNew);
        }
    }

    private void assertTable(TableData table) {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, table.getName())) {
            final TableReaderMetadata metadata = reader.getMetadata();
            final CharSequence expected = table.generateRows(metadata);
            LOG.info().$(table.getName()).$(" expected:\n").utf8(expected).$();
            assertCursorTwoPass(expected, reader.getCursor(), metadata);
        }
    }

    private void checkTableReady(TableData table) {
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

    private int[] generateColumnOrdering() {
        final int[] columnOrdering = new int[colNameBases.length];
        if (shouldFuzz(columnReorderingFactor)) {
            final List<Integer> indexes = new ArrayList<>();
            for (int i = 0; i < columnOrdering.length; i++) {
                indexes.add(i);
            }
            Collections.shuffle(indexes);
            for (int i = 0; i < columnOrdering.length; i++) {
                columnOrdering[i] = indexes.get(i);
            }
        } else {
            for (int i = 0; i < columnOrdering.length; i++) {
                columnOrdering[i] = i;
            }
        }
        return columnOrdering;
    }

    private LineData generateLine() {
        final LineData line = new LineData(timestampMillis.incrementAndGet());
        final int[] columnIndexes = getColumnIndexes();
        for (int i = 0; i < columnIndexes.length; i++) {
            final int colIndex = columnIndexes[i];
            final CharSequence colName = addColumn(line, colIndex);
            addDuplicateColumn(line, colIndex, colName);
            addNewColumn(line);
        }
        LOG.info().utf8(line.toString()).$();
        return line;
    }

    private String generateName(int index, boolean randomize) {
        final int caseIndex = diffCasesInColNames ? random.nextInt(colNameBases[index].length) : 0;
        final String postfix = randomize ? Integer.toString(random.nextInt(NEW_COLUMN_RANDOMIZE_FACTOR)) : "";
        return colNameBases[index][caseIndex] + postfix;
    }

    private String generateValue(int index) {
        final String postfix;
        switch (colTypes[index]) {
            case DOUBLE:
                postfix = random.nextInt(9) + ".0";
                break;
            case STRING:
                postfix = Character.toString(shouldFuzz(nonAsciiValueFactor) ? nonAsciiChars[random.nextInt(nonAsciiChars.length)] : random.nextChar());
                break;
            default:
                postfix = "";
        }
        return colValueBases[index] + postfix;
    }

    private int[] getColumnIndexes() {
        return skipColumns(generateColumnOrdering());
    }

    private CharSequence getTableName(int tableIndex) {
        return "weather" + tableIndex;
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

    private void initFuzzParameters(int duplicatesFactor, int columnReorderingFactor, int columnSkipFactor, int newColumnFactor, int nonAsciiValueFactor, boolean diffCasesInColNames) {
        this.duplicatesFactor = duplicatesFactor;
        this.columnReorderingFactor = columnReorderingFactor;
        this.columnSkipFactor = columnSkipFactor;
        this.nonAsciiValueFactor = nonAsciiValueFactor;
        this.newColumnFactor = newColumnFactor;
        this.diffCasesInColNames = diffCasesInColNames;
    }

    private void initLoadParameters(int numOfLines, int numOfIterations, int numOfThreads, int numOfTables, long waitBetweenIterationsMillis) {
        this.numOfLines = numOfLines;
        this.numOfIterations = numOfIterations;
        this.numOfThreads = numOfThreads;
        this.numOfTables = numOfTables;
        this.waitBetweenIterationsMillis = waitBetweenIterationsMillis;

        threadPushFinished = new SOCountDownLatch(numOfThreads - 1);
        tables = new CharSequenceObjHashMap<>();
    }

    private TableData pickTable() {
        return tables.get(getTableName(random.nextInt(numOfTables)));
    }

    private void runTest() throws Exception {
        runInContext(receiver -> {
            for (int i = 0; i < numOfTables; i++) {
                final CharSequence tableName = getTableName(i);
                tables.put(tableName, new TableData(tableName));
            }

            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                    final TableData table = tables.get(name);
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
                        try (Socket socket = getSocket(false)) {
                            for (int n = 0; n < numOfIterations; n++) {
                                for (int j = 0; j < numOfLines; j++) {
                                    final TableData table = pickTable();
                                    final CharSequence tableName = table.getName();
                                    final LineData line = generateLine();
                                    table.addLine(line);
                                    sendToSocket(socket, line.toLine(tableName));
                                }
                                Os.sleep(waitBetweenIterationsMillis);
                            }
                        } catch (Exception e) {
                            Assert.fail("Data sending failed [e=" + e + "]");
                            throw new RuntimeException(e);
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
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                });
            }
        });
    }

    private boolean shouldFuzz(int fuzzFactor) {
        return fuzzFactor > 0 && random.nextInt(fuzzFactor) == 0;
    }

    private int[] skipColumns(int[] originalColumnIndexes) {
        if (shouldFuzz(columnSkipFactor)) {
            // avoid list here and just copy slices of the original array into the new one
            final List<Integer> indexes = new ArrayList<>();
            for (int i = 0; i < originalColumnIndexes.length; i++) {
                indexes.add(originalColumnIndexes[i]);
            }
            final int numOfSkippedCols = random.nextInt(MAX_NUM_OF_SKIPPED_COLS) + 1;
            for (int i = 0; i < numOfSkippedCols; i++) {
                final int skipIndex = random.nextInt(indexes.size());
                indexes.remove(skipIndex);
            }
            final int[] columnIndexes = new int[indexes.size()];
            for (int i = 0; i < columnIndexes.length; i++) {
                columnIndexes[i] = indexes.get(i);
            }
            return columnIndexes;
        }
        return originalColumnIndexes;
    }
}
