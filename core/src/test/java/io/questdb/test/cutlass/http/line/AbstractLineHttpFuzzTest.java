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

package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.http.AbstractLineHttpSender;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.NetworkError;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.cutlass.line.tcp.load.LineData;
import io.questdb.test.cutlass.line.tcp.load.TableData;
import io.questdb.test.fuzz.FuzzChangeColumnTypeOperation;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.ColumnType.*;
import static io.questdb.client.Sender.PROTOCOL_VERSION_V2;

abstract class AbstractLineHttpFuzzTest extends AbstractBootstrapTest {

    static final int UPPERCASE_TABLE_RANDOMIZE_FACTOR = 2;
    // the REFRESH LIMIT materialized view and its base table, see initBackfillViewParameters()
    private static final String BACKFILL_BASE = "backfill_base";
    private static final String BACKFILL_VIEW = "backfill_view";
    private static final int MAX_NUM_OF_SKIPPED_COLS = 2;
    private static final int NEW_COLUMN_RANDOMIZE_FACTOR = 2;
    private static final int SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR = 2;
    private static final short[] integerColumnTypes = new short[]{ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG};
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
    // The frozen-zone view rows of the batches the server committed, the batches that committed some, and the
    // refused batches with lines of other tables.
    private final AtomicLong acceptedViewRows = new AtomicLong();
    private final AtomicInteger acceptedViewBatches = new AtomicInteger();
    private final AtomicInteger refusedMixedBatches = new AtomicInteger();
    // A frozen-zone view row's timestamp: from 2024-09-08, before the view's 09:00 bucket of 2024-09-10.
    private final AtomicLong viewRowTimestampMicros = new AtomicLong(1_725_753_600_000_000L);
    protected int numOfIterations;
    protected int numOfLines;
    protected int numOfTables;
    protected int numOfThreads;
    protected boolean pinTablesToThreads;
    protected Rnd random;
    protected LowerCaseCharSequenceObjHashMap<TableData> tables;
    protected long waitBetweenIterationsMillis;
    // When above 0, one line in about this many is a view row, see initBackfillViewParameters().
    private int backfillViewFactor = -1;
    private double columnConvertProb;
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

    public static int changeColumnTypeTo(Rnd rnd, int columnType) {
        int nextColType = columnType;
        return switch (columnType) {
            case ColumnType.STRING -> rnd.nextBoolean() ? ColumnType.SYMBOL : ColumnType.VARCHAR;
            case ColumnType.SYMBOL -> rnd.nextBoolean() ? ColumnType.STRING : ColumnType.VARCHAR;
            case ColumnType.VARCHAR -> rnd.nextBoolean() ? ColumnType.STRING : ColumnType.SYMBOL;
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG -> {
                while (nextColType == columnType) { // disallow noop conversion
                    nextColType = integerColumnTypes[rnd.nextInt(integerColumnTypes.length)];
                }
                yield nextColType;
            }
            case ColumnType.FLOAT -> ColumnType.DOUBLE;
            case ColumnType.DOUBLE -> ColumnType.FLOAT;
            case TIMESTAMP -> ColumnType.LONG;
            default -> columnType;
        };
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractBootstrapTest.tearDownStatic();
    }

    protected static void assertCursorTwoPass(CharSequence expected, RecordCursor cursor, RecordMetadata metadata) {
        TestUtils.assertCursor(expected, cursor, metadata, true, sink);
        cursor.toTop();
        TestUtils.assertCursor(expected, cursor, metadata, true, sink);
    }

    // Waits until the table's WAL has applied.
    private static void awaitWalApplied(CairoEngine engine, TableToken tableToken) {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
        final long deadline = System.currentTimeMillis() + 60_000;
        while (tracker.getWriterTxn() < tracker.getSeqTxn() && System.currentTimeMillis() < deadline) {
            Os.sleep(10);
        }
        Assert.assertEquals("the WAL did not apply in time [table=" + tableToken.getTableName() + ']', tracker.getSeqTxn(), tracker.getWriterTxn());
    }

    public void ingest(CairoEngine engine, int port, Rnd rnd) {
        int waitCount = numOfThreads;
        threadPushFinished.setCount(waitCount);
        AtomicInteger failureCounter = new AtomicInteger();
        for (int i = 0; i < numOfThreads; i++) {
            startThread(i, port, threadPushFinished, failureCounter, rnd.nextInt(PROTOCOL_VERSION_V2 + 1));
        }
        Thread alterTableThread = null;
        if (columnConvertProb > 0) {
            alterTableThread = startAlterTableThread(engine, threadPushFinished, failureCounter);
        }
        threadPushFinished.await();
        try {
            if (alterTableThread != null) {
                alterTableThread.join();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(0, failureCounter.get());
    }

    /**
     * Mixes rows of a materialized view with REFRESH LIMIT 1 HOUR into the threads' batches: one line in about
     * {@code backfillViewFactor} is a view row, a quarter of them in the view's managed zone. The server refuses a
     * batch with a managed-zone row as a client error and commits none of its lines, those of other tables included;
     * any other batch commits whole. The tables' expected rows follow that model, and the run asserts both kinds of
     * batches occurred.
     */
    public void initBackfillViewParameters(int backfillViewFactor) {
        this.backfillViewFactor = backfillViewFactor;
    }

    public void initFuzzParameters(
            int duplicatesFactor, int columnReorderingFactor, int columnSkipFactor, int newColumnFactor, int nonAsciiValueFactor,
            boolean diffCasesInColNames, boolean exerciseTags, boolean sendSymbolsWithSpace,
            double columnConvertProb) {
        this.duplicatesFactor = duplicatesFactor;
        this.columnReorderingFactor = columnReorderingFactor;
        this.columnSkipFactor = columnSkipFactor;
        this.nonAsciiValueFactor = nonAsciiValueFactor;
        this.newColumnFactor = newColumnFactor;
        this.diffCasesInColNames = diffCasesInColNames;
        this.exerciseTags = exerciseTags;
        this.sendSymbolsWithSpace = sendSymbolsWithSpace;
        this.columnConvertProb = columnConvertProb;
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
            while (true) {
                int httpPortRandom = 7000 + random.nextInt(1000);
                try (final TestServerMain serverMain = startWithEnvVariables(
                        PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                        PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                        PropertyKey.PG_ENABLED.getEnvVarName(), "false",
                        PropertyKey.PG_LEGACY_MODE_ENABLED.getEnvVarName(), "false",
                        PropertyKey.LINE_TCP_ENABLED.getEnvVarName(), "false",
                        PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                        PropertyKey.PG_ENABLED.getEnvVarName(), "false",
                        PropertyKey.PG_LEGACY_MODE_ENABLED.getEnvVarName(), "false",
                        PropertyKey.LINE_TCP_ENABLED.getEnvVarName(), "false",
                        PropertyKey.HTTP_NET_CONNECTION_LIMIT.getEnvVarName(), String.valueOf(2 * numOfThreads),
                        PropertyKey.HTTP_BIND_TO.getEnvVarName(), "127.0.0.1:" + httpPortRandom
                )) {
                    Assert.assertEquals(0, tables.size());
                    for (int i = 0; i < numOfTables; i++) {
                        final CharSequence tableName = getTableName(i);
                        tables.put(tableName, new TableData(tableName));
                    }

                    try {
                        if (backfillViewFactor > 0) {
                            createBackfillView(serverMain);
                        }
                        ingest(serverMain.getEngine(), serverMain.getHttpServerPort(), random);

                        for (int i = 0; i < numOfTables; i++) {
                            final String tableName = getTableName(i);
                            final TableData table = tables.get(tableName);
                            if (table.size(sendSymbolsWithSpace) > 0) {
                                serverMain.awaitTable(tableName);
                                assertTable(serverMain, table, tableName);
                            }
                        }
                        if (backfillViewFactor > 0) {
                            assertBackfillView(serverMain);
                        }
                    } catch (Exception e) {
                        getLog().errorW().$(e).$();
                        setError(e.getMessage());
                    }
                } catch (NetworkError e) {
                    LOG.error().$("Failed to start server [e=").$((Throwable) e).I$();
                    Os.sleep(100);
                    continue;
                }

                if (errorMsg != null) {
                    Assert.fail(errorMsg);
                }
                return;
            }
        });
    }

    @Override
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

    private CharSequence addColumn(LineData line, int colIndex, AbstractLineHttpSender httpSender) {
        final CharSequence colName = generateColumnName(colIndex, false);
        final CharSequence colValue = addColumnValue(colIndex, colName, httpSender);
        line.addColumn(colName, colValue);
        return colName;
    }

    private String addColumnValue(int index, CharSequence colName, AbstractLineHttpSender httpSender) {
        return addColumnValue(colTypes[index], colValueBases[index], colName, httpSender);
    }

    private String addColumnValue(short type, String valueBase, CharSequence colName, AbstractLineHttpSender httpSender) {
        final String postfix;
        switch (type) {
            case DOUBLE:
                int d = random.nextInt(9);
                double value = Numbers.parseInt(valueBase) * Math.pow(10, valueBase.length()) + d;
                httpSender.doubleColumn(colName, value);
                postfix = d + ".0";
                return valueBase + postfix;
            case SYMBOL:
                postfix = Character.toString(shouldFuzz(nonAsciiValueFactor) ? nonAsciiChars[random.nextInt(nonAsciiChars.length)] : random.nextChar());
                if (sendSymbolsWithSpace && random.nextInt(SEND_SYMBOLS_WITH_SPACE_RANDOMIZE_FACTOR) == 0) {
                    final int spaceIndex = random.nextInt(valueBase.length() - 1);
                    valueBase = valueBase.substring(0, spaceIndex) + "  " + valueBase.substring(spaceIndex);
                }
                String val = valueBase + postfix;
                httpSender.symbol(colName, val);
                return val;
            case STRING:
                postfix = Character.toString(shouldFuzz(nonAsciiValueFactor) ? nonAsciiChars[random.nextInt(nonAsciiChars.length)] : random.nextChar());
                httpSender.stringColumn(colName, valueBase + postfix);
                return "\"" + valueBase + postfix + "\"";
            default:
                httpSender.stringColumn(colName, valueBase);
                return valueBase;
        }
    }

    private void addDuplicateColumn(LineData line, int colIndex, CharSequence colName, AbstractLineHttpSender httpSender) {
        if (shouldFuzz(duplicatesFactor)) {
            final CharSequence colValueDupe = addColumnValue(colIndex, colName, httpSender);
            line.addColumn(colName, colValueDupe);
        }
    }

    private void addDuplicateTag(LineData line, int tagIndex, CharSequence tagName, AbstractLineHttpSender httpSender) {
        if (shouldFuzz(duplicatesFactor)) {
            final CharSequence tagValueDupe = addTagValue(tagIndex, tagName, httpSender);
            line.addTag(tagName, tagValueDupe);
        }
    }

    private void addNewColumn(LineData line, AbstractLineHttpSender httpSender) {
        if (shouldFuzz(newColumnFactor)) {
            final int extraColIndex = random.nextInt(colNameBases.length);
            final CharSequence colNameNew = generateColumnName(extraColIndex, true);
            final CharSequence colValueNew = addColumnValue(extraColIndex, colNameNew, httpSender);
            line.addColumn(colNameNew, colValueNew);
        }
    }

    private void addNewTag(LineData line, AbstractLineHttpSender httpSender) {
        if (shouldFuzz(newColumnFactor)) {
            final int extraTagIndex = random.nextInt(tagNameBases.length);
            final CharSequence tagNameNew = generateTagName(extraTagIndex, true);
            final CharSequence tagValueNew = addTagValue(extraTagIndex, tagNameNew, httpSender);
            line.addTag(tagNameNew, tagValueNew);
        }
    }

    private CharSequence addTag(LineData line, int tagIndex, AbstractLineHttpSender httpSender) {
        final CharSequence tagName = generateTagName(tagIndex, false);
        final CharSequence tagValue = addTagValue(tagIndex, tagName, httpSender);
        line.addTag(tagName, tagValue);
        return tagName;
    }

    private String addTagValue(int index, CharSequence colName, AbstractLineHttpSender httpSender) {
        return addColumnValue(SYMBOL, tagValueBases[index], colName, httpSender);
    }

    // Writes a view row, a quarter of them in the managed zone: the 12:00 bucket, past the 11:00 frozen-zone floor.
    private void addViewRow(AbstractLineHttpSender httpSender, BackfillBatch batch) {
        final boolean isManaged = random.nextInt(4) == 0;
        final long ts = isManaged
                ? 1_725_969_600_000_000L + random.nextInt(1_000_000)
                : viewRowTimestampMicros.incrementAndGet();
        httpSender.table(BACKFILL_VIEW).symbol("sym", "a").doubleColumn("price", 1.0).at(ts, ChronoUnit.MICROS);
        if (isManaged) {
            batch.managedViewRows++;
        } else {
            batch.frozenViewRows++;
        }
    }

    // The view's frozen-zone rows are those of the committed batches, and the run had a refused batch with lines of
    // other tables and a committed batch with view rows.
    private void assertBackfillView(TestServerMain serverMain) {
        final CairoEngine engine = serverMain.getEngine();
        awaitWalApplied(engine, engine.verifyTableName(BACKFILL_VIEW));
        serverMain.assertSql(
                "select count() from " + BACKFILL_VIEW + " where ts < '2024-09-10T09:00:00.000000Z'",
                "count\n" + acceptedViewRows.get() + "\n"
        );
        getLog().info().$("backfill view batches [accepted=").$(acceptedViewBatches.get())
                .$(", refusedMixed=").$(refusedMixedBatches.get()).I$();
        Assert.assertTrue("a batch with a managed-zone view row and lines of other tables is refused", refusedMixedBatches.get() > 0);
        Assert.assertTrue("a batch with frozen-zone view rows commits", acceptedViewBatches.get() > 0);
    }

    private void assertTable(TestServerMain serverMain, TableData table, @NotNull String tableName) {
        if (table.size(sendSymbolsWithSpace) < 1) {
            return;
        }
        try (
                TableReader reader = serverMain.getEngine().getReader(tableName);
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            getLog().info().$("table.getName(): ").$safe(table.getName()).$(", tableName: ").$safe(tableName)
                    .$(", table.size(): ").$(table.size(sendSymbolsWithSpace)).$(", reader.size(): ").$(reader.size()).$();
            final TableReaderMetadata metadata = reader.getMetadata();
            final CharSequence expected = table.generateRows(metadata, sendSymbolsWithSpace);
            getLog().info().$safe(table.getName()).$(" expected:\n").$safe(expected).$();

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

    // A view of 1h buckets over a base table with rows at 09:10 and 12:00 on 2024-09-10, with REFRESH LIMIT 1 HOUR:
    // base max 12:00 puts its frozen zone below the 11:00 bucket.
    private void createBackfillView(TestServerMain serverMain) {
        final CairoEngine engine = serverMain.getEngine();
        serverMain.execute("CREATE TABLE " + BACKFILL_BASE + " (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        serverMain.execute("INSERT INTO " + BACKFILL_BASE + " VALUES ('a', 1.0, '2024-09-10T09:10'), ('a', 2.0, '2024-09-10T12:00')");
        serverMain.awaitTxn(BACKFILL_BASE, 1);
        serverMain.execute("CREATE MATERIALIZED VIEW " + BACKFILL_VIEW + " AS (SELECT sym, last(price) price, ts FROM " + BACKFILL_BASE + " SAMPLE BY 1h)");
        final TableToken viewToken = engine.verifyTableName(BACKFILL_VIEW);
        final long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            final MatViewState viewState = engine.getMatViewStateStore().getViewState(viewToken);
            if (viewState != null && viewState.getLastRefreshBaseTxn() >= 1) {
                break;
            }
            Os.sleep(10);
        }
        serverMain.execute("ALTER MATERIALIZED VIEW " + BACKFILL_VIEW + " SET REFRESH LIMIT 1 HOUR");
        while (!engine.isBackfillableMatView(viewToken) && System.currentTimeMillis() < deadline) {
            Os.sleep(10);
        }
        Assert.assertTrue("the view did not take its REFRESH LIMIT in time", engine.isBackfillableMatView(viewToken));
        awaitWalApplied(engine, viewToken);
    }

    // Flushes the batch. With the view on, the server refuses a batch with a managed-zone view row as a client error
    // and commits none of its lines: they leave their tables' expected rows. Any other batch commits whole.
    private void flushBatch(AbstractLineHttpSender httpSender, BackfillBatch batch) {
        if (batch.managedViewRows > 0) {
            try {
                httpSender.flush();
                throw new IllegalStateException("a batch with a managed-zone view row must be refused");
            } catch (LineSenderException e) {
                if (e.isRetryable() || !e.getMessage().contains("[http-status=400, ")
                        || !e.getMessage().contains("backfill row falls in or past the managed zone of materialized view [view=" + BACKFILL_VIEW + ", ")) {
                    throw new IllegalStateException("expected the batch's refusal as a client error [isRetryable=" + e.isRetryable() + ", message=" + e.getMessage() + ']');
                }
            }
            httpSender.reset();
            for (int i = 0, n = batch.lines.size(); i < n; i++) {
                batch.tables.getQuick(i).removeLine(batch.lines.getQuick(i));
            }
            if (batch.lines.size() > 0) {
                refusedMixedBatches.incrementAndGet();
            }
        } else {
            httpSender.flush();
            if (batch.frozenViewRows > 0) {
                acceptedViewRows.addAndGet(batch.frozenViewRows);
                acceptedViewBatches.incrementAndGet();
            }
        }
        batch.clear();
    }

    private String generateColumnName(int index, boolean randomize) {
        return generateName(colNameBases[index], randomize);
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

    private Thread startAlterTableThread(CairoEngine engine, SOCountDownLatch threadPushFinished, AtomicInteger failureCounter) {
        Thread thread = new Thread(() -> {
            int totalTestConversions = random.nextInt((int) (numOfLines * numOfTables * columnConvertProb));
            try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, 1)) {
                while (totalTestConversions > 0 && threadPushFinished.getCount() > 0 && failureCounter.get() == 0) {
                    final CharSequence tableName = getTableName(random.nextInt(numOfTables));
                    TableToken tableToken = engine.getTableTokenIfExists(tableName);
                    if (tableToken != null) {
                        try (TableMetadata meta = engine.getTableMetadata(tableToken)) {
                            int startColIndex = random.nextInt(meta.getColumnCount());
                            for (int i = 0; i < meta.getColumnCount(); i++) {
                                int colIndex = (startColIndex + i) % meta.getColumnCount();
                                int type = meta.getColumnType(colIndex);
                                if (type > 0 && FuzzChangeColumnTypeOperation.canGenerateColumnTypeChange(meta, colIndex)) {
                                    int newType = changeColumnTypeTo(random, type);
                                    try {
                                        engine.execute("ALTER TABLE " + tableName + " ALTER COLUMN "
                                                + meta.getColumnName(colIndex) + " TYPE " + nameOf(newType), executionContext);
                                        totalTestConversions--;
                                    } catch (SqlException ex) {
                                        // Can be a race and the type is already converted.
                                        if (!Chars.contains(ex.getFlyweightMessage(), "type is already")) {
                                            throw ex;
                                        }
                                    }
                                    break;
                                }
                            }
                        } catch (SqlException e) {
                            LOG.error().$("Failed to alter table [e=").$((Throwable) e).I$();
                            failureCounter.incrementAndGet();
                        }
                    }
                    Os.sleep(10 + random.nextInt(100));
                }
            } finally {
                Path.clearThreadLocals();
            }
        });
        thread.start();
        return thread;
    }

    protected LineData generateLine(CharSequence tableName, AbstractLineHttpSender httpSender) {
        final LineData line = new LineData(timestampMicros.incrementAndGet());
        httpSender.table(tableName);

        if (exerciseTags) {
            final int[] tagIndexes = getTagIndexes();
            for (final int tagIndex : tagIndexes) {
                final CharSequence tagName = addTag(line, tagIndex, httpSender);
                addDuplicateTag(line, tagIndex, tagName, httpSender);
                addNewTag(line, httpSender);
            }
        }
        final int[] columnIndexes = getColumnIndexes();
        for (final int colIndex : columnIndexes) {
            final CharSequence colName = addColumn(line, colIndex, httpSender);
            addDuplicateColumn(line, colIndex, colName, httpSender);
            addNewColumn(line, httpSender);
        }
        httpSender.at(line.getTimestamp(), ChronoUnit.MICROS);
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

    protected void startThread(int threadId, int port, SOCountDownLatch threadPushFinished, AtomicInteger failureCounter, final int protocolVersion) {
        new Thread(() -> {
            final String username = "root";
            final String password = "root";
            Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.HTTP)
                    .address("localhost:" + port)
                    .httpUsernamePassword(username, password)
                    .retryTimeoutMillis(0)
                    .httpTimeoutMillis(60000);

            if (protocolVersion > 0) {
                builder.protocolVersion(protocolVersion);
            }

            try (Sender sender = builder.build()) {
                AbstractLineHttpSender httpSender = (AbstractLineHttpSender) sender;
                final BackfillBatch batch = new BackfillBatch();
                long points = 0;
                for (int n = 0; n < numOfIterations; n++) {
                    for (int j = 0; j < numOfLines; j++) {
                        if (shouldFuzz(backfillViewFactor)) {
                            addViewRow(httpSender, batch);
                        } else {
                            final CharSequence tableName = pickTableName(threadId);
                            final LineData line = generateLine(tableName, httpSender);
                            final TableData table = tables.get(tableName);
                            table.addLine(line);
                            batch.tables.add(table);
                            batch.lines.add(line);
                        }

                        if (++points % batchSize == 0) {
                            flushBatch(httpSender, batch);
                        }
                    }

                    flushBatch(httpSender, batch);
                    Os.sleep(waitBetweenIterationsMillis);
                }
            } catch (Exception e) {
                failureCounter.incrementAndGet();
                Assert.fail("Data sending failed [e=" + e + "]");
            } finally {
                threadPushFinished.countDown();
                Path.clearThreadLocals();
            }
        }).start();
    }

    // A thread's batch since its last flush: the lines of other tables, with their tables, and its view rows.
    private static class BackfillBatch {
        private final ObjList<LineData> lines = new ObjList<>();
        private final ObjList<TableData> tables = new ObjList<>();
        private int frozenViewRows;
        private int managedViewRows;

        private void clear() {
            lines.clear();
            tables.clear();
            frozenViewRows = 0;
            managedViewRows = 0;
        }
    }
}
