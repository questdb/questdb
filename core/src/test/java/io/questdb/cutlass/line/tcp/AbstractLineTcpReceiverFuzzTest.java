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
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.tcp.load.LineData;
import io.questdb.cutlass.line.tcp.load.TableData;
import io.questdb.log.Log;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.ColumnType.*;

abstract class AbstractLineTcpReceiverFuzzTest extends AbstractLineTcpReceiverTest {

    private static final int MAX_NUM_OF_SKIPPED_COLS = 2;
    private static final int NEW_COLUMN_RANDOMIZE_FACTOR = 2;
    private static final int UPPERCASE_TABLE_RANDOMIZE_FACTOR = 2;
    private static final int SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR = 2;

    private Rnd random;
    private final AtomicLong timestampMillis = new AtomicLong(1465839830102300L);
    private final short[] colTypes = new short[]{STRING, DOUBLE, DOUBLE, DOUBLE, STRING, DOUBLE};
    private final String[][] colNameBases = new String[][]{
            {"terület", "TERÜLet", "tERülET", "TERÜLET"},
            {"temperature", "TEMPERATURE", "Temperature", "TempeRaTuRe"},
            {"humidity", "HUMIdity", "HumiditY", "HUmiDIty", "HUMIDITY", "Humidity"},
            {"hőmérséklet", "HŐMÉRSÉKLET", "HŐmérséKLEt", "hőMÉRséKlET"},
            {"notes", "NOTES", "NotEs", "noTeS"},
            {"ветер", "Ветер", "ВЕТЕР", "вЕТЕр", "ВетЕР"}
    };
    private final String[] colValueBases = new String[]{"europe", "8", "2", "1", "note", "6"};
    private final String[][] tagNameBases = new String[][]{
            {"location", "Location", "LOCATION", "loCATion", "LocATioN"},
            {"city", "ciTY", "CITY"}
    };
    private final String[] tagValueBases = new String[]{"us-midwest", "London"};
    private final char[] nonAsciiChars = {'ó', 'í', 'Á', 'ч', 'Ъ', 'Ж', 'ю', 0x3000, 0x3080, 0x3a55};

    private int numOfLines;
    private int numOfIterations;
    private int numOfThreads;
    private int numOfTables;
    private long waitBetweenIterationsMillis;
    private boolean pinTablesToThreads;

    private SOCountDownLatch threadPushFinished;
    private LowerCaseCharSequenceObjHashMap<TableData> tables;
    private ConcurrentHashMap<CharSequence> tableNames;

    private int duplicatesFactor = -1;
    private int columnReorderingFactor = -1;
    private int columnSkipFactor = -1;
    private int nonAsciiValueFactor = -1;
    private int newColumnFactor = -1;
    private boolean diffCasesInColNames = false;
    private boolean exerciseTags = true;
    private boolean sendSymbolsWithSpace = false;

    private volatile String errorMsg = null;

    @Before
    public void setUp2() {
        long s0 = System.currentTimeMillis();
        long s1 = System.nanoTime();
        random = new Rnd(s0, s1);
        getLog().info().$("random seed : ").$(s0).$(", ").$(s1).$();
    }

    protected abstract Log getLog();

    private CharSequence addTag(LineData line, int tagIndex) {
        final CharSequence tagName = generateTagName(tagIndex, false);
        final CharSequence tagValue = generateTagValue(tagIndex);
        line.addTag(tagName, tagValue);
        return tagName;
    }

    private CharSequence addColumn(LineData line, int colIndex) {
        final CharSequence colName = generateColumnName(colIndex, false);
        final CharSequence colValue = generateColumnValue(colIndex);
        line.addColumn(colName, colValue);
        return colName;
    }

    private void addDuplicateTag(LineData line, int tagIndex, CharSequence tagName) {
        if (shouldFuzz(duplicatesFactor)) {
            final CharSequence tagValueDupe = generateTagValue(tagIndex);
            line.addTag(tagName, tagValueDupe);
        }
    }

    private void addDuplicateColumn(LineData line, int colIndex, CharSequence colName) {
        if (shouldFuzz(duplicatesFactor)) {
            final CharSequence colValueDupe = generateColumnValue(colIndex);
            line.addColumn(colName, colValueDupe);
        }
    }

    private void addNewTag(LineData line) {
        if (shouldFuzz(newColumnFactor)) {
            final int extraTagIndex = random.nextInt(tagNameBases.length);
            final CharSequence tagNameNew = generateTagName(extraTagIndex, true);
            final CharSequence tagValueNew = generateTagValue(extraTagIndex);
            line.addTag(tagNameNew, tagValueNew);
        }
    }

    private void addNewColumn(LineData line) {
        if (shouldFuzz(newColumnFactor)) {
            final int extraColIndex = random.nextInt(colNameBases.length);
            final CharSequence colNameNew = generateColumnName(extraColIndex, true);
            final CharSequence colValueNew = generateColumnValue(extraColIndex);
            line.addColumn(colNameNew, colValueNew);
        }
    }

    void assertTable(TableData table) {
        // timeout is 180 seconds
        long timeoutMicros = 180_000_000;
        long prev = testMicrosClock.getTicks();
        boolean checked = false;
        while (!checked) {
            if (table.await(timeoutMicros)) {
                checked = checkTable(table);
                if (!checked) {
                    long current = testMicrosClock.getTicks();
                    timeoutMicros -= current - prev;
                    prev = current;
                }
            } else {
                Assert.fail("Timed out on waiting for the data to be ingested");
                break;
            }
        }
    }

    // return false means could not assert and should be called again
    boolean checkTable(TableData table) {
        final CharSequence tableName = tableNames.get(table.getName());
        if (tableName == null) {
            getLog().info().$(table.getName()).$(" has not been created yet").$();
            table.notReady();
            return false;
        }
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            getLog().info().$("table.getName(): ").$(table.getName()).$(", tableName: ").$(tableName)
                    .$(", table.size(): ").$(table.size()).$(", reader.size(): ").$(reader.size()).$();
            if (table.size() <= reader.size()) {
                final TableReaderMetadata metadata = reader.getMetadata();
                final CharSequence expected = table.generateRows(metadata);
                getLog().info().$(table.getName()).$(" expected:\n").utf8(expected).$();
                assertCursorTwoPass(expected, reader.getCursor(), metadata);
                return true;
            } else {
                table.notReady();
                return false;
            }
        }
    }

    private int[] generateOrdering(int numOfCols) {
        final int[] columnOrdering = new int[numOfCols];
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
        if (exerciseTags) {
            final int[] tagIndexes = getTagIndexes();
            for (int i = 0; i < tagIndexes.length; i++) {
                final int tagIndex = tagIndexes[i];
                final CharSequence tagName = addTag(line, tagIndex);
                addDuplicateTag(line, tagIndex, tagName);
                addNewTag(line);
            }
        }
        final int[] columnIndexes = getColumnIndexes();
        for (int i = 0; i < columnIndexes.length; i++) {
            final int colIndex = columnIndexes[i];
            final CharSequence colName = addColumn(line, colIndex);
            addDuplicateColumn(line, colIndex, colName);
            addNewColumn(line);
        }
        return line;
    }

    private String generateTagName(int index, boolean randomize) {
        return generateName(tagNameBases[index], randomize);
    }

    private String generateColumnName(int index, boolean randomize) {
        return generateName(colNameBases[index], randomize);
    }

    private String generateName(String[] names, boolean randomize) {
        final int caseIndex = diffCasesInColNames ? random.nextInt(names.length) : 0;
        final String postfix = randomize ? Integer.toString(random.nextInt(NEW_COLUMN_RANDOMIZE_FACTOR)) : "";
        return names[caseIndex] + postfix;
    }

    private String generateTagValue(int index) {
        return generateValue(SYMBOL, tagValueBases[index]);
    }

    private String generateColumnValue(int index) {
        return generateValue(colTypes[index], colValueBases[index]);
    }

    private String generateValue(short type, String valueBase) {
        final String postfix;
        switch (type) {
            case DOUBLE:
                postfix = random.nextInt(9) + ".0";
                return valueBase + postfix;
            case SYMBOL:
                postfix = Character.toString(shouldFuzz(nonAsciiValueFactor) ? nonAsciiChars[random.nextInt(nonAsciiChars.length)] : random.nextChar());
                if (sendSymbolsWithSpace && random.nextInt(SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR) == 0) {
                    final int spaceIndex = random.nextInt(valueBase.length()-1);
                    valueBase = valueBase.substring(0, spaceIndex) + "  " + valueBase.substring(spaceIndex);
                }
                return valueBase + postfix;
            case STRING:
                postfix = Character.toString(shouldFuzz(nonAsciiValueFactor) ? nonAsciiChars[random.nextInt(nonAsciiChars.length)] : random.nextChar());
                return "\"" + valueBase + postfix + "\"";
            default:
                return valueBase;
        }
    }

    private int[] getTagIndexes() {
        return skipColumns(generateOrdering(tagNameBases.length));
    }

    private int[] getColumnIndexes() {
        return skipColumns(generateOrdering(colNameBases.length));
    }

    private CharSequence getTableName(int tableIndex) {
        return getTableName(tableIndex, false);
    }

    private CharSequence getTableName(int tableIndex, boolean randomCase) {
        final String tableName;
        if (randomCase) {
            tableName = random.nextInt(UPPERCASE_TABLE_RANDOMIZE_FACTOR) == 0 ? "WEATHER" : "weather";
        } else {
            tableName = "weather";
        }
        return tableName + tableIndex;
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

    void initFuzzParameters(int duplicatesFactor, int columnReorderingFactor, int columnSkipFactor, int newColumnFactor, int nonAsciiValueFactor,
                            boolean diffCasesInColNames, boolean exerciseTags, boolean sendSymbolsWithSpace) {
        this.duplicatesFactor = duplicatesFactor;
        this.columnReorderingFactor = columnReorderingFactor;
        this.columnSkipFactor = columnSkipFactor;
        this.nonAsciiValueFactor = nonAsciiValueFactor;
        this.newColumnFactor = newColumnFactor;
        this.diffCasesInColNames = diffCasesInColNames;
        this.exerciseTags = exerciseTags;
        this.sendSymbolsWithSpace = sendSymbolsWithSpace;
    }

    void initLoadParameters(int numOfLines, int numOfIterations, int numOfThreads, int numOfTables, long waitBetweenIterationsMillis) {
        initLoadParameters(numOfLines, numOfIterations, numOfThreads, numOfTables, waitBetweenIterationsMillis, false);
    }

    void initLoadParameters(int numOfLines, int numOfIterations, int numOfThreads, int numOfTables, long waitBetweenIterationsMillis, boolean pinTablesToThreads) {
        assert !pinTablesToThreads || (numOfThreads == numOfTables);

        this.numOfLines = numOfLines;
        this.numOfIterations = numOfIterations;
        this.numOfThreads = numOfThreads;
        this.numOfTables = numOfTables;
        this.waitBetweenIterationsMillis = waitBetweenIterationsMillis;
        this.pinTablesToThreads = pinTablesToThreads;

        threadPushFinished = new SOCountDownLatch(numOfThreads);
        tables = new LowerCaseCharSequenceObjHashMap<>();
        tableNames = new ConcurrentHashMap<>();
    }

    private CharSequence pickTableName(int threadId) {
        return getTableName(pinTablesToThreads ? threadId : random.nextInt(numOfTables), true);
    }

    void runTest() throws Exception {
        runTest((factoryType, thread, name, event, segment, position) -> {
            if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_LOCK_SUCCESS) {
                handleWriterLockSuccessEvent(name);
            }
            if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                handleWriterReturnEvent(name);
            }
        }, 250);
    }

    void handleWriterLockSuccessEvent(CharSequence name) {
        final String tableName = name.toString();
        tableNames.putIfAbsent(tableName.toLowerCase(), tableName);
    }

    void handleWriterReturnEvent(CharSequence name) {
        if (threadPushFinished.getCount() > 0) {
            // we are still sending, no point to check the table yet
            return;
        }
        final TableData table = tables.get(name);
        table.ready();
    }

    void runTest(PoolListener listener, long minIdleMsBeforeWriterRelease) throws Exception {
        runInContext(receiver -> {
            for (int i = 0; i < numOfTables; i++) {
                final CharSequence tableName = getTableName(i);
                tables.put(tableName, new TableData(tableName));
            }

            engine.setPoolListener(listener);

            try {
                for (int i = 0; i < numOfThreads; i++) {
                    final int threadId = i;
                    new Thread(() -> {
                        try (Socket socket = getSocket()) {
                            for (int n = 0; n < numOfIterations; n++) {
                                for (int j = 0; j < numOfLines; j++) {
                                    final LineData line = generateLine();
                                    final CharSequence tableName = pickTableName(threadId);
                                    final TableData table = tables.get(tableName);
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
                    assertTable(table);
                }
            } finally {
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                });
            }
        }, false, minIdleMsBeforeWriterRelease);

        if (errorMsg != null) {
            Assert.fail(errorMsg);
        }
    }

    void setError(String errorMsg) {
        this.errorMsg = errorMsg;
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
