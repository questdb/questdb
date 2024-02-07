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

package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableReaderRecordCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.http.LineHttpSender;
import io.questdb.log.Log;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.line.tcp.load.LineData;
import io.questdb.test.cutlass.line.tcp.load.TableData;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.ColumnType.*;

abstract class AbstractLineHttpFuzzTest extends AbstractBootstrapTest {

    static final int UPPERCASE_TABLE_RANDOMIZE_FACTOR = 2;
    private static final int MAX_NUM_OF_SKIPPED_COLS = 2;
    private static final int NEW_COLUMN_RANDOMIZE_FACTOR = 2;
    private static final int SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR = 2;
    private static final StringSink sink = new StringSink();
    protected final short[] colTypes = new short[]{STRING, DOUBLE, DOUBLE, DOUBLE, STRING, DOUBLE};
    private final int batchSize = 10;
    private final String[][] colNameBases = new String[][]{
            {"terület", "TERÜLet", "tERülET", "TERÜLET"},
            {"temperature", "TEMPERATURE", "Temperature", "TempeRaTuRe"},
            {"humidity", "HUMIdity", "HumiditY", "HUmiDIty", "HUMIDITY", "Humidity"},
            {"hőmérséklet", "HŐMÉRSÉKLET", "HŐmérséKLEt", "hőMÉRséKlET"},
            {"notes", "NOTES", "NotEs", "noTeS"},
            {"ветер", "Ветер", "ВЕТЕР", "вЕТЕр", "ВетЕР"}
    };
    private final String[] colValueBases = new String[]{"europe", "8", "2", "1", "note", "6"};
    private final char[] nonAsciiChars = {'ó', 'í', 'Á', 'ч', 'Ъ', 'Ж', 'ю', 0x3000, 0x3080, 0x3a55};
    private final String[][] tagNameBases = new String[][]{
            {"location", "Location", "LOCATION", "loCATion", "LocATioN"},
            {"city", "ciTY", "CITY"}
    };
    private final String[] tagValueBases = new String[]{"us-midwest", "London"};
    private final AtomicLong timestampMicros = new AtomicLong(1465839830102300L);
    protected int numOfIterations;
    protected int numOfLines;
    protected int numOfTables;
    protected int numOfThreads;
    protected boolean pinTablesToThreads;
    protected Rnd random;
    protected LowerCaseCharSequenceObjHashMap<TableData> tables;
    protected long waitBetweenIterationsMillis;
    private int columnReorderingFactor = -1;
    private int columnSkipFactor = -1;
    private boolean diffCasesInColNames = false;
    private int duplicatesFactor = -1;
    private volatile String errorMsg = null;
    private boolean exerciseTags = true;
    private int newColumnFactor = -1;
    private int nonAsciiValueFactor = -1;
    private boolean sendStringsAsSymbols = false;
    private boolean sendSymbolsWithSpace = false;
    private boolean symbolAsFieldSupported;
    private SOCountDownLatch threadPushFinished;

    public void ingest(int port) {
        threadPushFinished.setCount(numOfThreads);
        AtomicInteger failureCounter = new AtomicInteger();
        for (int i = 0; i < numOfThreads; i++) {
            startThread(i, port, threadPushFinished, failureCounter);
        }
        threadPushFinished.await();
        Assert.assertEquals(0, failureCounter.get());
    }

    public void initFuzzParameters(
            int duplicatesFactor, int columnReorderingFactor, int columnSkipFactor, int newColumnFactor, int nonAsciiValueFactor,
            boolean diffCasesInColNames, boolean exerciseTags, boolean sendStringsAsSymbols, boolean sendSymbolsWithSpace
    ) {
        this.duplicatesFactor = duplicatesFactor;
        this.columnReorderingFactor = columnReorderingFactor;
        this.columnSkipFactor = columnSkipFactor;
        this.nonAsciiValueFactor = nonAsciiValueFactor;
        this.newColumnFactor = newColumnFactor;
        this.diffCasesInColNames = diffCasesInColNames;
        this.exerciseTags = exerciseTags;
        this.sendStringsAsSymbols = sendStringsAsSymbols;
        this.sendSymbolsWithSpace = sendSymbolsWithSpace;
        symbolAsFieldSupported = sendStringsAsSymbols;
    }

    public void initLoadParameters(int numOfLines, int numOfIterations, int numOfThreads, int numOfTables, long waitBetweenIterationsMillis) {
        initLoadParameters(numOfLines, numOfIterations, numOfThreads, numOfTables, waitBetweenIterationsMillis, false);
    }

    public void initLoadParameters(int numOfLines, int numOfIterations, int numOfThreads, int numOfTables, long waitBetweenIterationsMillis, boolean pinTablesToThreads) {
        assert !pinTablesToThreads || (numOfThreads == numOfTables);

        this.numOfLines = numOfLines;
        this.numOfIterations = numOfIterations;
        this.numOfThreads = numOfThreads;
        this.numOfTables = numOfTables;
        this.waitBetweenIterationsMillis = waitBetweenIterationsMillis;
        this.pinTablesToThreads = pinTablesToThreads;

        threadPushFinished = new SOCountDownLatch();
        tables = new LowerCaseCharSequenceObjHashMap<>();
    }

    public void runTest() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int httpPortRandom = 7000 + random.nextInt(1000);
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.HTTP_BIND_TO.getEnvVarName(), "127.0.0.1:" + httpPortRandom,
                    PropertyKey.LINE_TCP_UNDOCUMENTED_SYMBOL_AS_FIELD_SUPPORTED.getEnvVarName(), Boolean.toString(symbolAsFieldSupported)
            )) {
                Assert.assertEquals(0, tables.size());
                for (int i = 0; i < numOfTables; i++) {
                    final CharSequence tableName = getTableName(i);
                    tables.put(tableName, new TableData(tableName));
                }

                try {
                    ingest(LineHttpUtils.getHttpPort(serverMain));

                    for (int i = 0; i < numOfTables; i++) {
                        final String tableName = getTableName(i);
                        final TableData table = tables.get(tableName);
                        if (table.size() > 0) {
                            serverMain.waitWalTxnApplied(tableName);
                            assertTable(serverMain, table, tableName);
                        }
                    }
                } catch (Exception e) {
                    getLog().error().$(e).$();
                    setError(e.getMessage());
                }
            }

            if (errorMsg != null) {
                Assert.fail(errorMsg);
            }
        });
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Before
    public void setUp2() {
        long s0 = System.currentTimeMillis();
        long s1 = System.nanoTime();
        random = new Rnd(s0, s1);
        getLog().info().$("random seed : ").$(random.getSeed0()).$("L, ").$(random.getSeed1()).$('L').$();
    }

    private CharSequence addColumn(LineData line, int colIndex) {
        final CharSequence colName = generateColumnName(colIndex, false);
        final CharSequence colValue = generateColumnValue(colIndex);
        line.addColumn(colName, colValue);
        return colName;
    }

    private void addDuplicateColumn(LineData line, int colIndex, CharSequence colName) {
        if (shouldFuzz(duplicatesFactor)) {
            final CharSequence colValueDupe = generateColumnValue(colIndex);
            line.addColumn(colName, colValueDupe);
        }
    }

    private void addDuplicateTag(LineData line, int tagIndex, CharSequence tagName) {
        if (shouldFuzz(duplicatesFactor)) {
            final CharSequence tagValueDupe = generateTagValue(tagIndex);
            line.addTag(tagName, tagValueDupe);
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

    private void addNewTag(LineData line) {
        if (shouldFuzz(newColumnFactor)) {
            final int extraTagIndex = random.nextInt(tagNameBases.length);
            final CharSequence tagNameNew = generateTagName(extraTagIndex, true);
            final CharSequence tagValueNew = generateTagValue(extraTagIndex);
            line.addTag(tagNameNew, tagValueNew);
        }
    }

    private CharSequence addTag(LineData line, int tagIndex) {
        final CharSequence tagName = generateTagName(tagIndex, false);
        final CharSequence tagValue = generateTagValue(tagIndex);
        line.addTag(tagName, tagValue);
        return tagName;
    }

    private void assertTable(TestServerMain serverMain, TableData table, @NotNull String tableName) {
        if (table.size() < 1) {
            return;
        }
        try (TableReader reader = serverMain.getEngine().getReader(tableName)) {
            getLog().info().$("table.getName(): ").$(table.getName()).$(", tableName: ").$(tableName)
                    .$(", table.size(): ").$(table.size()).$(", reader.size(): ").$(reader.size()).$();
            final TableReaderMetadata metadata = reader.getMetadata();
            final CharSequence expected = table.generateRows(metadata);
            getLog().info().$(table.getName()).$(" expected:\n").utf8(expected).$();

            final TableReaderRecordCursor cursor = reader.getCursor();
            // Assert reader min timestamp
            long txnMinTs = reader.getMinTimestamp();
            int timestampIndex = reader.getMetadata().getTimestampIndex();
            if (cursor.hasNext()) {
                long dataMinTs = cursor.getRecord().getLong(timestampIndex);
                Assert.assertEquals(dataMinTs, txnMinTs);
                cursor.toTop();
            }

            try {
                assertCursorTwoPass(expected, cursor, metadata);
            } catch (AssertionError e) {
                throw new AssertionError("Table: " + table.getName(), e);
            }
        }
    }

    private String generateColumnName(int index, boolean randomize) {
        return generateName(colNameBases[index], randomize);
    }

    private String generateColumnValue(int index) {
        return generateValue(colTypes[index], colValueBases[index]);
    }

    private String generateName(String[] names, boolean randomize) {
        final int caseIndex = diffCasesInColNames ? random.nextInt(names.length) : 0;
        final String postfix = randomize ? Integer.toString(random.nextInt(NEW_COLUMN_RANDOMIZE_FACTOR)) : "";
        return names[caseIndex] + postfix;
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

    private String generateTagName(int index, boolean randomize) {
        return generateName(tagNameBases[index], randomize);
    }

    private String generateTagValue(int index) {
        return generateValue(SYMBOL, tagValueBases[index]);
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
                    final int spaceIndex = random.nextInt(valueBase.length() - 1);
                    valueBase = valueBase.substring(0, spaceIndex) + "  " + valueBase.substring(spaceIndex);
                }
                return valueBase + postfix;
            case STRING:
                postfix = Character.toString(shouldFuzz(nonAsciiValueFactor) ? nonAsciiChars[random.nextInt(nonAsciiChars.length)] : random.nextChar());
                return sendStringsAsSymbols ? valueBase + postfix : "\"" + valueBase + postfix + "\"";
            default:
                return valueBase;
        }
    }

    private int[] getColumnIndexes() {
        return skipColumns(generateOrdering(colNameBases.length));
    }

    private String getTableName(int tableIndex) {
        return getTableName(tableIndex, false);
    }

    private int[] getTagIndexes() {
        return skipColumns(generateOrdering(tagNameBases.length));
    }

    private boolean shouldFuzz(int fuzzFactor) {
        return fuzzFactor > 0 && random.nextInt(fuzzFactor) == 0;
    }

    private int[] skipColumns(int[] originalColumnIndexes) {
        if (shouldFuzz(columnSkipFactor)) {
            // avoid list here and just copy slices of the original array into the new one
            final List<Integer> indexes = new ArrayList<>();
            for (int originalColumnIndex : originalColumnIndexes) {
                indexes.add(originalColumnIndex);
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

    protected static void assertCursorTwoPass(CharSequence expected, RecordCursor cursor, RecordMetadata metadata) {
        TestUtils.assertCursor(expected, cursor, metadata, true, sink);
        cursor.toTop();
        TestUtils.assertCursor(expected, cursor, metadata, true, sink);
    }

    protected LineData generateLine() {
        final LineData line = new LineData(timestampMicros.incrementAndGet());
        if (exerciseTags) {
            final int[] tagIndexes = getTagIndexes();
            for (final int tagIndex : tagIndexes) {
                final CharSequence tagName = addTag(line, tagIndex);
                addDuplicateTag(line, tagIndex, tagName);
                addNewTag(line);
            }
        }
        final int[] columnIndexes = getColumnIndexes();
        for (final int colIndex : columnIndexes) {
            final CharSequence colName = addColumn(line, colIndex);
            addDuplicateColumn(line, colIndex, colName);
            addNewColumn(line);
        }
        return line;
    }

    protected abstract Log getLog();

    protected String getTableName(int tableIndex, boolean randomCase) {
        final String tableName;
        if (randomCase) {
            tableName = random.nextInt(UPPERCASE_TABLE_RANDOMIZE_FACTOR) == 0 ? "WEATHER" : "weather";
        } else {
            tableName = "weather";
        }
        return tableName + tableIndex;
    }


    protected CharSequence pickTableName(int threadId) {
        return getTableName(pinTablesToThreads ? threadId : random.nextInt(numOfTables), true);
    }

    void setError(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    protected void startThread(int threadId, int port, SOCountDownLatch threadPushFinished, AtomicInteger failureCounter) {
        new Thread(() -> {
            final String username = "root";
            final String password = "root";
            try (
                    Sender sender = Sender.builder()
                            .address("localhost:" + port)
                            .http()
                            .httpUsernamePassword(username, password)
                            .build()
            ) {
                LineHttpSender httpSender = (LineHttpSender) sender;
                List<String> points = new ArrayList<>();
                for (int n = 0; n < numOfIterations; n++) {
                    for (int j = 0; j < numOfLines; j++) {
                        final LineData line = generateLine();
                        final CharSequence tableName = pickTableName(threadId);
                        final TableData table = tables.get(tableName);
                        table.addLine(line);
                        points.add(line.toLine(tableName));
                        if (points.size() % batchSize == 0) {
                            for (String point : points) {
                                httpSender.putRawMessage(point);
                            }
                            httpSender.flush();
                            points.clear();
                        }
                    }
                    if (!points.isEmpty()) {
                        for (String point : points) {
                            httpSender.putRawMessage(point);
                        }
                        httpSender.flush();
                        points.clear();
                    }

                    Os.sleep(waitBetweenIterationsMillis);
                }
            } catch (Exception e) {
                failureCounter.incrementAndGet();
                Assert.fail("Data sending failed [e=" + e + "]");
            } finally {
                threadPushFinished.countDown();
            }
        }).start();
    }
}
