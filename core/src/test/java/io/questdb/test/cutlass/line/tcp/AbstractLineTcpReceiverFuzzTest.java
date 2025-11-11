/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.cutlass.line.tcp.load.LineData;
import io.questdb.test.cutlass.line.tcp.load.TableData;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.ColumnType.*;

abstract class AbstractLineTcpReceiverFuzzTest extends AbstractLineTcpReceiverTest {

    protected static final Log LOG = LogFactory.getLog(AbstractLineTcpReceiverTest.class);
    static final int UPPERCASE_TABLE_RANDOMIZE_FACTOR = 2;
    private static final int MAX_NUM_OF_SKIPPED_COLS = 2;
    private static final int NEW_COLUMN_RANDOMIZE_FACTOR = 2;
    private static final int SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR = 2;
    protected final short[] colTypes = new short[]{STRING, DOUBLE, DOUBLE, DOUBLE, STRING, DOUBLE};
    protected final boolean walEnabled;
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
    protected ConcurrentHashMap<CharSequence> tableNames;
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
    private boolean sendSymbolsWithSpace = false;
    private SOCountDownLatch threadPushFinished;
    private long timestampMark = -1;

    public AbstractLineTcpReceiverFuzzTest() {
        this.walEnabled = TestUtils.isWal();
    }

    public void runTest() throws Exception {
        runTest((factoryType, thread, token, event, segment, position) -> {
            String tableName = token.getTableName();
            if (walEnabled) {
                // There is no locking as such in WAL, so we treat writer return as an unlock event.
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_GET) {
                    handleWriterUnlockEvent(tableName);
                    handleWriterGetEvent(tableName);
                }
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                    handleWriterUnlockEvent(tableName);
                    handleWriterReturnEvent(tableName);
                }
            } else {
                if (factoryType == PoolListener.SRC_SEQUENCER_METADATA && event == PoolListener.EV_UNLOCKED) {
                    handleWriterUnlockEvent(tableName);
                }
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_GET) {
                    handleWriterGetEvent(tableName);
                }
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                    handleWriterReturnEvent(tableName);
                }
            }
        }, 250);
    }

    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
        random = TestUtils.generateRandom(getLog());
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

    private void assertTable(TableData table) throws SqlException {
        if (table.size() < 1) {
            return;
        }
        final CharSequence tableName = tableNames.get(table.getName());
        if (tableName == null) {
            throw new RuntimeException("Table name is missing");
        }
        try (TableReader reader = getReader(tableName)) {
            getLog().info().$("table.getName(): ").$safe(table.getName()).$(", tableName: ").$safe(tableName)
                    .$(", table.size(): ").$(table.size()).$(", reader.size(): ").$(reader.size()).$();
            final TableReaderMetadata metadata = reader.getMetadata();
            final CharSequence expected = table.generateRows(metadata);
            getLog().info().$safe(table.getName()).$(" expected:\n").$safe(expected).$();

            if (timestampMark < 0L) {
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
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
            } else {
                final String sql = tableName + " where timestamp > " + timestampMark;
                try (RecordCursorFactory factory = select(sql)) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        // Extract for safe logging
                        CharSequence name = table.getName();
                        int size = table.size();
                        long cursorSize = cursor.size();

                        getLog().info().$("table.getName(): ").$(name).$(", tableName: ").$safe(tableName)
                                .$(", table.size(): ").$(size).$(", cursor.size(): ").$(cursorSize).$();
                        assertCursorTwoPass(expected, cursor, metadata);
                    }
                }
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
                return "\"" + valueBase + postfix + "\"";
            default:
                return valueBase;
        }
    }

    private int[] getColumnIndexes() {
        return skipColumns(generateOrdering(colNameBases.length));
    }

    private CharSequence getTableName(int tableIndex) {
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

    // return false means data is not in the table yet and should be called again
    boolean checkTable(TableData table) throws SqlException {
        final CharSequence tableName = tableNames.get(table.getName());
        if (tableName == null) {
            getLog().info().$safe(table.getName()).$(" has not been created yet").$();
            return false;
        }

        if (timestampMark < 0L) {
            try (TableReader reader = getReader(tableName)) {
                getLog().info().$("table.getName(): ").$safe(table.getName()).$(", tableName: ").$safe(tableName)
                        .$(", table.size(): ").$(table.size()).$(", reader.size(): ").$(reader.size()).$();
                return table.size() <= reader.size();
            } catch (CairoException ex) {
                if (ex.getFlyweightMessage().toString().contains("table does not exist")) {
                    getLog().info().$("table.getName(): ").$safe(table.getName()).$(", tableName: ").$safe(tableName)
                            .$(", table.size(): ").$(table.size()).$(", reader.size(): table does not exist").$();
                    return table.size() <= 0;
                } else {
                    throw ex;
                }
            }
        }

        final String sql = tableName + " where timestamp > " + timestampMark;
        try (RecordCursorFactory factory = select(sql)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                // Extract vars for safe logging
                long size = 0;
                while (cursor.hasNext()) {
                    size++;
                }
                int tableSize = table.size();
                CharSequence name = table.getName();

                getLog().info().$("table.getName(): ")
                        .$safe(name).$(", tableName: ").$safe(tableName)
                        .$(", table.size(): ").$(tableSize)
                        .$(", cursor.size(): ").$(size).$();
                return tableSize <= size;
            }
        } catch (SqlException ex) {
            if (ex.getFlyweightMessage().toString().contains("table does not exist")
                    || ex.getFlyweightMessage().toString().contains("table directory is of unknown format")) {

                // Extract vars for safe logging
                int size = table.size();
                CharSequence name = table.getName();

                getLog().info().$("table.getName(): ")
                        .$safe(name).$(", tableName: ").$safe(tableName)
                        .$(", table.size(): ").$(size)
                        .$(", cursor.size(): table does not exist")
                        .$();

                return size <= 0;
            } else {
                throw ex;
            }
        }
    }

    protected void clearTables() {
        final ObjList<CharSequence> names = tables.keys();
        for (int i = 0, n = names.size(); i < n; i++) {
            final CharSequence tableName = names.get(i);
            final TableData table = tables.get(tableName);
            table.clear();
        }
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

    protected CharSequence getTableName(int tableIndex, boolean randomCase) {
        final String tableName;
        if (randomCase) {
            tableName = random.nextInt(UPPERCASE_TABLE_RANDOMIZE_FACTOR) == 0 ? "WEATHER" : "weather";
        } else {
            tableName = "weather";
        }
        return tableName + tableIndex;
    }

    @Override
    protected int getWorkerCount() {
        return 4;
    }

    void handleWriterGetEvent(CharSequence name) {
        final TableData table = tables.get(name);
        if (table != null) {
            table.obtainPermit();
        }
    }

    void handleWriterReturnEvent(CharSequence name) {
        final TableData table = tables.get(name);
        if (table != null) {
            table.returnPermit();
        }
    }

    void handleWriterUnlockEvent(CharSequence name) {
        if (tables.get(name) != null) {
            final String tableName = name.toString();
            tableNames.putIfAbsent(tableName.toLowerCase(), tableName);
        }
    }

    void ingest(ObjList<Socket> sockets) {
        threadPushFinished.setCount(numOfThreads);
        AtomicInteger failureCounter = new AtomicInteger();
        for (int i = 0; i < numOfThreads; i++) {
            startThread(i, sockets.get(i), threadPushFinished, failureCounter);
        }
        threadPushFinished.await();
        Assert.assertEquals(0, failureCounter.get());
    }

    void initFuzzParameters(
            int duplicatesFactor, int columnReorderingFactor, int columnSkipFactor, int newColumnFactor, int nonAsciiValueFactor,
            boolean diffCasesInColNames, boolean exerciseTags, boolean sendSymbolsWithSpace
    ) {
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

        threadPushFinished = new SOCountDownLatch();
        tables = new LowerCaseCharSequenceObjHashMap<>();
        tableNames = new ConcurrentHashMap<>();
    }

    protected void markTimestamp() {
        timestampMark = timestampMicros.get();
    }

    protected void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    protected CharSequence pickTableName(int threadId) {
        return getTableName(pinTablesToThreads ? threadId : random.nextInt(numOfTables), true);
    }

    void runTest(PoolListener listener, long minIdleMsBeforeWriterRelease) throws Exception {
        runInContext(receiver -> {
            Assert.assertEquals(0, tables.size());
            for (int i = 0; i < numOfTables; i++) {
                final CharSequence tableName = getTableName(i);
                tables.put(tableName, new TableData(tableName));
            }

            engine.setPoolListener(listener);

            final ObjList<Socket> sockets = new ObjList<>(numOfThreads);
            try {
                for (int i = 0; i < numOfThreads; i++) {
                    final Socket socket = newSocket();
                    sockets.add(socket);
                }
                ingest(sockets);
                waitDone(sockets);

                for (int i = 0; i < numOfTables; i++) {
                    final CharSequence tableName = getTableName(i);
                    final TableData table = tables.get(tableName);
                    assertTable(table);
                }
            } catch (Exception e) {
                getLog().error().$(e).$();
                setError(e.getMessage());
            } finally {
                for (int i = 0; i < sockets.size(); i++) {
                    final Socket socket = sockets.get(i);
                    socket.close();
                }
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

    protected void startThread(int threadId, Socket socket, SOCountDownLatch threadPushFinished, AtomicInteger failureCounter) {
        new Thread(() -> {
            try {
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
                failureCounter.incrementAndGet();
                Assert.fail("Data sending failed [e=" + e + "]");
            } finally {
                threadPushFinished.countDown();
            }
        }).start();
    }

    protected void waitDone(ObjList<Socket> sockets) throws SqlException {
        for (int i = 0; i < numOfTables; i++) {
            final CharSequence tableName = getTableName(i);
            final TableData table = tables.get(tableName);
            if (table.size() > 0) {
                waitForTable(table);
            }
        }
    }

    void waitForTable(TableData table) throws SqlException {
        // if CI is very slow the table could be released before ingestion stops
        // then acquired again for further data ingestion
        // because of the above we will wait in a loop with a timeout for the data to appear in the table
        // in most cases we should not hit the sleep() below
        table.await();
        for (int i = 0; i < 180; i++) {
            mayDrainWalQueue();
            if (checkTable(table)) {
                return;
            }
            Os.sleep(1000);
        }
        throw new RuntimeException("Timed out on waiting for the data, table=" + table.getName());
    }
}
