/*+*****************************************************************************
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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.cutlass.line.tcp.load.LineData;
import io.questdb.test.cutlass.line.tcp.load.TableData;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.ColumnType.*;

public class QwpSenderFuzzTest extends AbstractQwpWebSocketTest {

    private static final Log LOG = LogFactory.getLog(QwpSenderFuzzTest.class);
    private static final int MAX_NUM_OF_SKIPPED_COLS = 2;
    private static final int NEW_COLUMN_RANDOMIZE_FACTOR = 2;
    private static final int SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR = 2;
    private static final int UPPERCASE_TABLE_RANDOMIZE_FACTOR = 2;
    private final int batchSize = 10;
    private final String[][] colNameBases = new String[][]{
            {"terület", "TERÜLet", "tERülET", "TERÜLET"},
            {"temperature", "TEMPERATURE", "Temperature", "TempeRaTuRe"},
            {"humidity", "HUMIdity", "HumiditY", "HUmiDIty", "HUMIDITY", "Humidity"},
            {"hőmérséklet", "HŐMÉRSÉKLET", "HŐmérséKLEt", "hőMÉRséKlET"},
            {"notes", "NOTES", "NotEs", "noTeS"},
            {"ветер", "Ветер", "ВЕТЕР", "вЕТЕр", "ВетЕР"}
    };
    private final short[] colTypes = new short[]{STRING, DOUBLE, DOUBLE, DOUBLE, STRING, DOUBLE};
    private final String[] colValueBases = new String[]{"europe", "8", "2", "1", "note", "6"};
    private final char[] nonAsciiChars = {'ó', 'í', 'Á', 'ч', 'Ъ', 'Ж', 'ю', 0x3000, 0x3080, 0x3a55};
    private final String[][] symbolNameBases = new String[][]{
            {"location", "Location", "LOCATION", "loCATion", "LocATioN"},
            {"city", "ciTY", "CITY"}
    };
    private final String[] symbolValueBases = new String[]{"us-midwest", "London"};
    private final AtomicLong timestampMicros = new AtomicLong(1_465_839_830_102_300L);
    private int columnReorderingFactor = -1;
    private int columnSkipFactor = -1;
    private boolean diffCasesInColNames = false;
    private volatile String errorMsg = null;
    private boolean exerciseSymbols = true;
    private int newColumnFactor = -1;
    private int nonAsciiValueFactor = -1;
    private int numOfIterations;
    private int numOfLines;
    private int numOfTables;
    private int numOfThreads;
    private Rnd random;
    private boolean sendSymbolsWithSpace = false;
    private LowerCaseCharSequenceObjHashMap<TableData> tables;
    private SOCountDownLatch threadPushFinished;
    private long waitBetweenIterationsMillis;

    @Before
    public void setUp() {
        super.setUp();
        random = TestUtils.generateRandom(LOG);
    }

    @Test
    public void testAddColumns() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, 4, -1, false, true, false);
        runTest();
    }

    @Test
    public void testAddColumnsNoSymbols() throws Exception {
        initLoadParameters(15, 2, 2, 5, 75);
        initFuzzParameters(-1, -1, 4, -1, false, false, false);
        runTest();
    }

    @Test
    public void testAllMixed() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 5, 10, 5, true, true, true);
        runTest();
    }

    @Test
    public void testAllMixedNoSymbols() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 5, 10, 5, true, false, true);
        runTest();
    }

    @Test
    public void testAllMixedSingleTable() throws Exception {
        initLoadParameters(50, Os.isWindows() ? 3 : 5, 5, 1, 50);
        initFuzzParameters(4, 5, 10, 5, true, true, true);
        runTest();
    }

    @Test
    public void testCaseVariationReorderingColumns() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, -1, -1, true, true, false);
        runTest();
    }

    @Test
    public void testCaseVariationReorderingColumnsNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, -1, -1, true, false, false);
        runTest();
    }

    @Test
    public void testCaseVariationReorderingColumnsSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, -1, -1, true, true, true);
        runTest();
    }

    @Test
    public void testLoad() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 7, 12, 20);
        runTest();
    }

    @Test
    public void testLoadLargePayload() throws Exception {
        initLoadParameters(500, Os.isWindows() ? 3 : 5, 5, 5, 10);
        runTest();
    }

    @Test
    public void testLoadNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 7, 12, 20);
        initFuzzParameters(-1, -1, -1, -1, false, false, false);
        runTest();
    }

    @Test
    public void testLoadSendSymbolsWithSpace() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 4, 8, 20);
        initFuzzParameters(-1, -1, -1, -1, false, true, true);
        runTest();
    }

    @Test
    public void testNonAsciiValues() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, -1, -1, 4, false, true, false);
        runTest();
    }

    @Test
    public void testNonAsciiValuesNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(-1, -1, -1, 4, false, false, false);
        runTest();
    }

    @Test
    public void testReorderingColumns() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, -1, -1, false, true, false);
        runTest();
    }

    @Test
    public void testReorderingColumnsNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, -1, -1, false, false, false);
        runTest();
    }

    @Test
    public void testReorderingManyThreads() throws Exception {
        initLoadParameters(15 + random.nextInt(100), 5 + random.nextInt(5),
                2 + random.nextInt(Os.isWindows() ? 5 : 20), 1 + random.nextInt(4),
                random.nextInt(75));
        initFuzzParameters(3, -1, -1, -1, false, true, false);
        runTest();
    }

    @Test
    public void testReorderingNonAscii() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, -1, -1, 4, false, true, false);
        runTest();
    }

    @Test
    public void testReorderingSkipColumnsWithNonAscii() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, 4, true, true, false);
        runTest();
    }

    @Test
    public void testReorderingSkipColumnsWithNonAsciiNoSymbols() throws Exception {
        initLoadParameters(100, Os.isWindows() ? 3 : 5, 5, 5, 50);
        initFuzzParameters(4, 4, -1, 4, true, false, false);
        runTest();
    }

    private CharSequence addColumn(LineData line, int colIndex, QwpWebSocketSender sender) {
        CharSequence colName = generateColumnName(colIndex, false);
        CharSequence colValue = addColumnValue(colTypes[colIndex], colValueBases[colIndex], colName, sender);
        line.addColumn(colName, colValue);
        return colName;
    }

    private String addColumnValue(short type, String valueBase, CharSequence colName, QwpWebSocketSender sender) {
        return switch (type) {
            case DOUBLE -> {
                int d = random.nextInt(9);
                double value = Numbers.parseInt(valueBase) * Math.pow(10, valueBase.length()) + d;
                sender.doubleColumn(colName, value);
                yield valueBase + d + ".0";
            }
            case SYMBOL -> {
                String postfix = Character.toString(shouldFuzz(nonAsciiValueFactor) ? nonAsciiChars[random.nextInt(nonAsciiChars.length)] : random.nextChar());
                String base = valueBase;
                if (sendSymbolsWithSpace && random.nextInt(SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR) == 0) {
                    int spaceIndex = random.nextInt(base.length() - 1);
                    base = base.substring(0, spaceIndex) + "  " + base.substring(spaceIndex);
                }
                String symVal = base + postfix;
                sender.symbol(colName, symVal);
                yield symVal;
            }
            case STRING -> {
                String postfix = Character.toString(shouldFuzz(nonAsciiValueFactor) ? nonAsciiChars[random.nextInt(nonAsciiChars.length)] : random.nextChar());
                sender.stringColumn(colName, valueBase + postfix);
                yield "\"" + valueBase + postfix + "\"";
            }
            default -> {
                sender.stringColumn(colName, valueBase);
                yield valueBase;
            }
        };
    }

    private void addNewColumn(LineData line, QwpWebSocketSender sender) {
        if (shouldFuzz(newColumnFactor)) {
            int extraColIndex = random.nextInt(colNameBases.length);
            CharSequence colName = generateColumnName(extraColIndex, true);
            CharSequence colValue = addColumnValue(colTypes[extraColIndex], colValueBases[extraColIndex], colName, sender);
            line.addColumn(colName, colValue);
        }
    }

    private void addNewSymbol(LineData line, QwpWebSocketSender sender) {
        if (shouldFuzz(newColumnFactor)) {
            int extraSymIndex = random.nextInt(symbolNameBases.length);
            CharSequence symName = generateSymbolName(extraSymIndex, true);
            CharSequence symValue = addSymbolValue(extraSymIndex, symName, sender);
            line.addColumn(symName, symValue);
        }
    }

    private CharSequence addSymbol(LineData line, int symIndex, QwpWebSocketSender sender) {
        CharSequence symName = generateSymbolName(symIndex, false);
        CharSequence symValue = addSymbolValue(symIndex, symName, sender);
        line.addColumn(symName, symValue);
        return symName;
    }

    private String addSymbolValue(int index, CharSequence colName, QwpWebSocketSender sender) {
        return addColumnValue(SYMBOL, symbolValueBases[index], colName, sender);
    }

    private void assertTable(TableData table, String tableName) {
        if (table.size() < 1) {
            return;
        }
        try (
                TableReader reader = engine.getReader(tableName);
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            LOG.info().$("table.getName(): ").$safe(table.getName()).$(", tableName: ").$safe(tableName)
                    .$(", table.size(): ").$(table.size()).$(", reader.size(): ").$(reader.size()).$();
            TableReaderMetadata metadata = reader.getMetadata();
            CharSequence expected = table.generateRows(metadata);
            LOG.info().$safe(table.getName()).$(" expected:\n").$safe(expected).$();

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

    private LineData generateLine(CharSequence tableName, QwpWebSocketSender sender) {
        LineData line = new LineData(timestampMicros.incrementAndGet());
        sender.table(tableName);

        if (exerciseSymbols) {
            int[] symIndexes = getSymbolIndexes();
            for (int symIndex : symIndexes) {
                addSymbol(line, symIndex, sender);
                addNewSymbol(line, sender);
            }
        }
        int[] columnIndexes = getColumnIndexes();
        for (int colIndex : columnIndexes) {
            addColumn(line, colIndex, sender);
            addNewColumn(line, sender);
        }
        sender.at(line.getTimestamp(), ChronoUnit.MICROS);
        return line;
    }

    private String generateName(String[] names, boolean randomize) {
        int caseIndex = diffCasesInColNames ? random.nextInt(names.length) : 0;
        String postfix = randomize ? Integer.toString(random.nextInt(NEW_COLUMN_RANDOMIZE_FACTOR)) : "";
        return names[caseIndex] + postfix;
    }

    private int[] generateOrdering(int numOfCols) {
        int[] columnOrdering = new int[numOfCols];
        if (shouldFuzz(columnReorderingFactor)) {
            List<Integer> indexes = new ArrayList<>();
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

    private String generateSymbolName(int index, boolean randomize) {
        return generateName(symbolNameBases[index], randomize);
    }

    private int[] getColumnIndexes() {
        return skipColumns(generateOrdering(colNameBases.length));
    }

    private int[] getSymbolIndexes() {
        return skipColumns(generateOrdering(symbolNameBases.length));
    }

    private String getTableName(int tableIndex) {
        return getTableName(tableIndex, false);
    }

    private String getTableName(int tableIndex, boolean randomCase) {
        String tableName;
        if (randomCase) {
            tableName = random.nextInt(UPPERCASE_TABLE_RANDOMIZE_FACTOR) == 0 ? "WEATHER" : "weather";
        } else {
            tableName = "weather";
        }
        return tableName + tableIndex;
    }

    private void initFuzzParameters(
            int columnReorderingFactor, int columnSkipFactor,
            int newColumnFactor, int nonAsciiValueFactor,
            boolean diffCasesInColNames, boolean exerciseSymbols, boolean sendSymbolsWithSpace
    ) {
        this.columnReorderingFactor = columnReorderingFactor;
        this.columnSkipFactor = columnSkipFactor;
        this.newColumnFactor = newColumnFactor;
        this.nonAsciiValueFactor = nonAsciiValueFactor;
        this.diffCasesInColNames = diffCasesInColNames;
        this.exerciseSymbols = exerciseSymbols;
        this.sendSymbolsWithSpace = sendSymbolsWithSpace;
    }

    private void initLoadParameters(int numOfLines, int numOfIterations, int numOfThreads, int numOfTables, long waitBetweenIterationsMillis) {
        this.numOfLines = numOfLines;
        this.numOfIterations = numOfIterations;
        this.numOfThreads = numOfThreads;
        this.numOfTables = numOfTables;
        this.waitBetweenIterationsMillis = waitBetweenIterationsMillis;

        threadPushFinished = new SOCountDownLatch();
        tables = new LowerCaseCharSequenceObjHashMap<>();
    }

    private CharSequence pickTableName() {
        return getTableName(random.nextInt(numOfTables), true);
    }

    private void runTest() throws Exception {

        Assert.assertEquals(0, tables.size());
        for (int i = 0; i < numOfTables; i++) {
            CharSequence tableName = getTableName(i);
            tables.put(tableName, new TableData(tableName));
        }

        runInContext(port -> {
            int waitCount = numOfThreads;
            threadPushFinished.setCount(waitCount);
            AtomicInteger failureCounter = new AtomicInteger();
            for (int i = 0; i < numOfThreads; i++) {
                startThread(port, threadPushFinished, failureCounter);
            }
            threadPushFinished.await();
            Assert.assertEquals(0, failureCounter.get());

            drainWalQueue();

            for (int i = 0; i < numOfTables; i++) {
                String tableName = getTableName(i);
                TableData table = tables.get(tableName);
                if (table.size() > 0) {
                    assertTable(table, tableName);
                }
            }
        });

        if (errorMsg != null) {
            Assert.fail(errorMsg);
        }
    }

    private boolean shouldFuzz(int fuzzFactor) {
        return fuzzFactor > 0 && random.nextInt(fuzzFactor) == 0;
    }

    private int[] skipColumns(int[] originalColumnIndexes) {
        if (shouldFuzz(columnSkipFactor)) {
            List<Integer> indexes = new ArrayList<>();
            for (int originalColumnIndex : originalColumnIndexes) {
                indexes.add(originalColumnIndex);
            }
            int numOfSkippedCols = random.nextInt(MAX_NUM_OF_SKIPPED_COLS) + 1;
            for (int i = 0; i < numOfSkippedCols; i++) {
                int skipIndex = random.nextInt(indexes.size());
                indexes.remove(skipIndex);
            }
            int[] columnIndexes = new int[indexes.size()];
            for (int i = 0; i < columnIndexes.length; i++) {
                columnIndexes[i] = indexes.get(i);
            }
            return columnIndexes;
        }
        return originalColumnIndexes;
    }

    private void startThread(int port, SOCountDownLatch threadPushFinished, AtomicInteger failureCounter) {
        new Thread(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", port, false)) {
                long points = 0;
                for (int n = 0; n < numOfIterations; n++) {
                    for (int j = 0; j < numOfLines; j++) {
                        CharSequence tableName = pickTableName();
                        LineData line = generateLine(tableName, sender);
                        TableData table = tables.get(tableName);
                        table.addLine(line);

                        if (++points % batchSize == 0) {
                            sender.flush();
                        }
                    }

                    sender.flush();
                    Os.sleep(waitBetweenIterationsMillis);
                }
            } catch (Exception e) {
                LOG.error().$("Data sending failed [e=").$((Throwable) e).I$();
                failureCounter.incrementAndGet();
                errorMsg = "Data sending failed [e=" + e + "]";
            } finally {
                threadPushFinished.countDown();
                Path.clearThreadLocals();
            }
        }).start();
    }
}
