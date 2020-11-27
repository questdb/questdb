/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.MessageBus;
import io.questdb.Telemetry;
import io.questdb.cairo.*;
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.line.*;
import io.questdb.cutlass.line.CairoLineProtoParserSupport.BadCastException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.time.MillisecondClock;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.Arrays;

class LineTcpMeasurementScheduler implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementScheduler.class);
    private static final int REBALANCE_EVENT_ID = -1; // A rebalance event is used to rebalance load across different threads
    private static final int INCOMPLETE_EVENT_ID = -2; // An incomplete event is used when the queue producer has grabbed an event but is
    // not able to populate it for some reason, the event needs to be committed to the
    // queue incomplete
    private static final IntHashSet ALLOWED_LONG_CONVERSIONS = new IntHashSet();
    private final CairoEngine engine;
    private final CairoSecurityContext securityContext;
    private final CairoConfiguration cairoConfiguration;
    private final MillisecondClock milliClock;
    private final RingQueue<LineTcpMeasurementEvent> queue;
    private final CharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsByTableName;
    private final int[] loadByThread;
    private final int nUpdatesPerLoadRebalance;
    private final double maxLoadRatio;
    private final int maxUncommittedRows;
    private final long maintenanceJobHysteresisInMs;
    private final SqlExecutionContext sqlExecutionContext;
    private Sequence pubSeq;
    private long nextEventCursor = -1;
    private int nLoadCheckCycles = 0;
    private int nRebalances = 0;

    LineTcpMeasurementScheduler(
            LineTcpReceiverConfiguration lineConfiguration,
            CairoEngine engine,
            WorkerPool writerWorkerPool,
            @Nullable MessageBus messageBus
    ) {
        this.engine = engine;
        this.securityContext = lineConfiguration.getCairoSecurityContext();
        this.cairoConfiguration = engine.getConfiguration();
        this.milliClock = cairoConfiguration.getMillisecondClock();
        // Worker count is set to 1 because we do not use this execution context
        // in worker threads.
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1, messageBus);
        tableUpdateDetailsByTableName = new CharSequenceObjHashMap<>();
        loadByThread = new int[writerWorkerPool.getWorkerCount()];
        int maxMeasurementSize = lineConfiguration.getMaxMeasurementSize();
        int queueSize = lineConfiguration.getWriterQueueSize();
        queue = new RingQueue<>(
                () -> new LineTcpMeasurementEvent(
                        maxMeasurementSize,
                        lineConfiguration.getMicrosecondClock(),
                        lineConfiguration.getTimestampAdapter()
                ),
                queueSize
        );
        pubSeq = new SPSequence(queueSize);

        int nWriterThreads = writerWorkerPool.getWorkerCount();
        if (nWriterThreads > 1) {
            FanOut fanOut = new FanOut();
            for (int n = 0; n < nWriterThreads; n++) {
                SCSequence subSeq = new SCSequence();
                fanOut.and(subSeq);
                WriterJob writerJob = new WriterJob(n, subSeq);
                writerWorkerPool.assign(n, writerJob);
                writerWorkerPool.assign(n, writerJob::close);
            }
            pubSeq.then(fanOut).then(pubSeq);
        } else {
            SCSequence subSeq = new SCSequence();
            pubSeq.then(subSeq).then(pubSeq);
            WriterJob writerJob = new WriterJob(0, subSeq);
            writerWorkerPool.assign(0, writerJob);
            writerWorkerPool.assign(0, writerJob::close);
        }

        nUpdatesPerLoadRebalance = lineConfiguration.getNUpdatesPerLoadRebalance();
        maxLoadRatio = lineConfiguration.getMaxLoadRatio();
        maxUncommittedRows = lineConfiguration.getMaxUncommittedRows();
        maintenanceJobHysteresisInMs = lineConfiguration.getMaintenanceJobHysteresisInMs();
    }

    @Override
    public void close() {
        // Both the writer and the net worker pools must have been closed so that their respective cleaners have run
        if (null != pubSeq) {
            pubSeq = null;
            tableUpdateDetailsByTableName.clear();
            for (int n = 0; n < queue.getCapacity(); n++) {
                queue.get(n).close();
            }
        }
    }

    private void calcThreadLoad() {
        Arrays.fill(loadByThread, 0);
        ObjList<CharSequence> tableNames = tableUpdateDetailsByTableName.keys();
        for (int n = 0, sz = tableNames.size(); n < sz; n++) {
            TableUpdateDetails stats = tableUpdateDetailsByTableName.get(tableNames.get(n));
            loadByThread[stats.threadId] += stats.nUpdates;
        }
    }

    void commitNewEvent(LineTcpMeasurementEvent event, boolean complete) {
        assert isOpen() && nextEventCursor != -1 && queue.get(nextEventCursor) == event;

        TableUpdateDetails tableUpdateDetails;
        if (complete) {
            int keyIndex = tableUpdateDetailsByTableName.keyIndex(event.getTableName());
            if (keyIndex > -1) {
                String tableName = Chars.toString(event.getTableName());
                calcThreadLoad();
                int leastLoad = Integer.MAX_VALUE;
                int threadId = 0;
                for (int n = 0; n < loadByThread.length; n++) {
                    if (loadByThread[n] < leastLoad) {
                        leastLoad = loadByThread[n];
                        threadId = n;
                    }
                }
                tableUpdateDetails = new TableUpdateDetails(tableName, threadId);
                tableUpdateDetailsByTableName.putAt(keyIndex, tableName, tableUpdateDetails);
                LOG.info().$("assigned ").$(tableName).$(" to thread ").$(threadId).$();
            } else {
                tableUpdateDetails = tableUpdateDetailsByTableName.valueAt(keyIndex);
            }

            event.threadId = tableUpdateDetails.threadId;
        } else {
            tableUpdateDetails = null;
            event.threadId = INCOMPLETE_EVENT_ID;
        }
        pubSeq.done(nextEventCursor);
        nextEventCursor = -1;

        if (null != tableUpdateDetails && tableUpdateDetails.nUpdates++ > nUpdatesPerLoadRebalance) {
            loadRebalance();
        }
    }

    void commitRebalanceEvent(LineTcpMeasurementEvent event, int fromThreadId, int toThreadId, String tableName) {
        assert isOpen() && nextEventCursor != -1 && queue.get(nextEventCursor) == event;
        event.createRebalanceEvent(fromThreadId, toThreadId, tableName);
        pubSeq.done(nextEventCursor);
        nextEventCursor = -1;
    }

    int[] getLoadByThread() {
        return loadByThread;
    }

    int getNLoadCheckCycles() {
        return nLoadCheckCycles;
    }

    int getNRebalances() {
        return nRebalances;
    }

    LineTcpMeasurementEvent getNewEvent() {
        assert isOpen();
        if (nextEventCursor != -1 || (nextEventCursor = pubSeq.next()) > -1) {
            return queue.get(nextEventCursor);
        }

        while (nextEventCursor == -2) {
            nextEventCursor = pubSeq.next();
        }

        if (nextEventCursor < 0) {
            nextEventCursor = -1;
            return null;
        }

        return queue.get(nextEventCursor);
    }

    private boolean isOpen() {
        return null != pubSeq;
    }

    private void loadRebalance() {
        LOG.info().$("load check [cycle=").$(++nLoadCheckCycles).$(']').$();
        calcThreadLoad();
        ObjList<CharSequence> tableNames = tableUpdateDetailsByTableName.keys();
        int fromThreadId = -1;
        int toThreadId = -1;
        String tableNameToMove = null;
        int maxLoad = Integer.MAX_VALUE;
        while (true) {
            int highestLoad = Integer.MIN_VALUE;
            int highestLoadedThreadId = -1;
            int lowestLoad = Integer.MAX_VALUE;
            int lowestLoadedThreadId = -1;
            for (int n = 0; n < loadByThread.length; n++) {
                if (loadByThread[n] >= maxLoad) {
                    continue;
                }

                if (highestLoad < loadByThread[n]) {
                    highestLoad = loadByThread[n];
                    highestLoadedThreadId = n;
                }

                if (lowestLoad > loadByThread[n]) {
                    lowestLoad = loadByThread[n];
                    lowestLoadedThreadId = n;
                }
            }

            if (highestLoadedThreadId == -1 || lowestLoadedThreadId == -1 || highestLoadedThreadId == lowestLoadedThreadId) {
                break;
            }

            double loadRatio = (double) highestLoad / (double) lowestLoad;
            if (loadRatio < maxLoadRatio) {
                // Load is not sufficiently unbalanced
                break;
            }

            int nTables = 0;
            lowestLoad = Integer.MAX_VALUE;
            String leastLoadedTableName = null;
            for (int n = 0, sz = tableNames.size(); n < sz; n++) {
                TableUpdateDetails stats = tableUpdateDetailsByTableName.get(tableNames.get(n));
                if (stats.threadId == highestLoadedThreadId && stats.nUpdates > 0) {
                    nTables++;
                    if (stats.nUpdates < lowestLoad) {
                        lowestLoad = stats.nUpdates;
                        leastLoadedTableName = stats.tableName;
                    }
                }
            }

            if (nTables < 2) {
                // The most loaded thread only has 1 table with load assigned to it
                maxLoad = highestLoad;
                continue;
            }

            fromThreadId = highestLoadedThreadId;
            toThreadId = lowestLoadedThreadId;
            tableNameToMove = leastLoadedTableName;
            break;
        }

        for (int n = 0, sz = tableNames.size(); n < sz; n++) {
            TableUpdateDetails stats = tableUpdateDetailsByTableName.get(tableNames.get(n));
            stats.nUpdates = 0;
        }

        if (null != tableNameToMove) {
            LineTcpMeasurementEvent event = getNewEvent();
            if (null == event) {
                return;
            }
            LOG.info().$("rebalance cycle, requesting table move [nRebalances=").$(++nRebalances).$(", table=").$(tableNameToMove).$(", fromThreadId=").$(fromThreadId).$(", toThreadId=")
                    .$(toThreadId).$(']').$();
            commitRebalanceEvent(event, fromThreadId, toThreadId, tableNameToMove);
            TableUpdateDetails stats = tableUpdateDetailsByTableName.get(tableNameToMove);
            stats.threadId = toThreadId;
        }
    }

    static class LineTcpMeasurementEvent implements Closeable {
        private final CharSequenceCache cache;
        private final MicrosecondClock clock;
        private final LineProtoTimestampAdapter timestampAdapter;
        private final LongList addresses = new LongList();
        private TruncatedLineProtoLexer lexer;
        private long measurementNameAddress;
        private int firstFieldIndex;
        private long timestampAddress;
        private int errorPosition;
        private int errorCode;
        private int threadId;
        private long timestamp;

        private int rebalanceFromThreadId;
        private int rebalanceToThreadId;
        private String rebalanceTableName;
        private volatile boolean rebalanceReleasedByFromThread;

        private LineTcpMeasurementEvent(int maxMeasurementSize, MicrosecondClock clock, LineProtoTimestampAdapter timestampAdapter) {
            lexer = new TruncatedLineProtoLexer(maxMeasurementSize);
            cache = lexer.getCharSequenceCache();
            this.clock = clock;
            this.timestampAdapter = timestampAdapter;
            lexer.withParser(new LineProtoParser() {
                @Override
                public void onError(int position, int state, int code) {
                    assert errorPosition == -1;
                    errorPosition = position;
                    errorCode = code;
                }

                @Override
                public void onEvent(CachedCharSequence token, int type, CharSequenceCache cache) {
                    assert cache == LineTcpMeasurementEvent.this.cache;
                    switch (type) {
                        case EVT_MEASUREMENT:
                            assert measurementNameAddress == 0;
                            measurementNameAddress = token.getCacheAddress();
                            break;
                        case EVT_TAG_NAME:
                        case EVT_TAG_VALUE:
                            assert firstFieldIndex == -1;
                            addresses.add(token.getCacheAddress());
                            break;
                        case EVT_FIELD_NAME:
                            if (firstFieldIndex == -1) {
                                firstFieldIndex = addresses.size() / 2;
                            }
                        case EVT_FIELD_VALUE:
                            assert firstFieldIndex != -1;
                            addresses.add(token.getCacheAddress());
                            break;
                        case EVT_TIMESTAMP:
                            assert timestampAddress == 0;
                            timestampAddress = token.getCacheAddress();
                            break;
                        default:
                            throw new RuntimeException("Unrecognised type " + type);
                    }
                }

                @Override
                public void onLineEnd(CharSequenceCache cache) {
                }
            });
        }

        @Override
        public void close() {
            lexer.close();
            lexer = null;
        }

        private void clear() {
            measurementNameAddress = 0;
            addresses.clear();
            firstFieldIndex = -1;
            timestampAddress = 0;
            errorPosition = -1;
        }

        void createRebalanceEvent(int fromThreadId, int toThreadId, String tableName) {
            clear();
            threadId = REBALANCE_EVENT_ID;
            rebalanceFromThreadId = fromThreadId;
            rebalanceToThreadId = toThreadId;
            rebalanceTableName = tableName;
            rebalanceReleasedByFromThread = false;
        }

        int getErrorCode() {
            return errorCode;
        }

        int getErrorPosition() {
            return errorPosition;
        }

        int getFirstFieldIndex() {
            return firstFieldIndex;
        }

        int getNValues() {
            return addresses.size() / 2;
        }

        CharSequence getName(int i) {
            return cache.get(addresses.getQuick(2 * i));
        }

        CharSequence getTableName() {
            return cache.get(measurementNameAddress);
        }

        long getTimestamp() throws NumericException {
            if (timestampAddress != 0) {
                try {
                    timestamp = timestampAdapter.getMicros(cache.get(timestampAddress));
                    timestampAddress = 0;
                } catch (NumericException e) {
                    LOG.error().$("invalid timestamp: ").$(cache.get(timestampAddress)).$();
                    timestamp = Long.MIN_VALUE;
                    throw e;
                }
            }
            return timestamp;
        }

        CharSequence getValue(int i) {
            return cache.get(addresses.getQuick(2 * i + 1));
        }

        boolean isComplete() {
            return errorPosition == -1;
        }

        boolean isRebalanceEvent() {
            return threadId == REBALANCE_EVENT_ID;
        }

        long parseLine(long bytesPtr, long hi) {
            clear();
            long recvBufLineNext = lexer.parseLine(bytesPtr, hi);
            if (recvBufLineNext != -1) {
                if (isComplete() && firstFieldIndex == -1) {
                    errorPosition = (int) (recvBufLineNext - bytesPtr);
                    errorCode = LineProtoParser.ERROR_EMPTY;
                }

                if (isComplete()) {
                    if (timestampAddress == 0) {
                        timestamp = clock.getTicks();
                    }
                }
            }
            return recvBufLineNext;
        }
    }

    private static class TableUpdateDetails {
        private final String tableName;
        private int threadId;
        private int nUpdates; // Number of updates since the last load rebalance

        private TableUpdateDetails(String tableName, int threadId) {
            super();
            this.tableName = tableName;
            this.threadId = threadId;
        }
    }

    private class WriterJob implements Job {
        private final int id;
        private final Sequence sequence;
        private final CharSequenceObjHashMap<Parser> parserCache = new CharSequenceObjHashMap<>();
        private final AppendMemory appendMemory = new AppendMemory();
        private final Path path = new Path();
        private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
        private final String jobName;
        private long lastMaintenanceJobMillis = 0;

        private WriterJob(int id, Sequence sequence) {
            super();
            this.id = id;
            this.sequence = sequence;
            this.jobName = "tcp-line-writer-" + id;
        }

        @Override
        public boolean run(int workerId) {
            assert workerId == id;
            boolean busy = drainQueue();
            doMaintenance(busy);
            return busy;
        }

        private void close() {
            // Finish all jobs in the queue before stopping
            for (int n = 0; n < queue.getCapacity(); n++) {
                if (!run(id)) {
                    break;
                }
            }

            ObjList<CharSequence> tableNames = parserCache.keys();
            for (int n = 0; n < tableNames.size(); n++) {
                parserCache.get(tableNames.get(n)).close();
            }
            parserCache.clear();
            appendMemory.close();
            path.close();
        }

        private void doMaintenance(boolean busy) {
            long millis = milliClock.getTicks();
            if (busy && (millis - lastMaintenanceJobMillis) < maintenanceJobHysteresisInMs) {
                return;
            }

            lastMaintenanceJobMillis = millis;
            ObjList<CharSequence> tableNames = parserCache.keys();
            for (int n = 0, sz = tableNames.size(); n < sz; n++) {
                parserCache.get(tableNames.get(n)).doMaintenance();
            }
        }

        private boolean drainQueue() {
            boolean busy = false;
            while (true) {
                long cursor;
                while ((cursor = sequence.next()) < 0) {
                    if (cursor == -1) {
                        return busy;
                    }
                }
                busy = true;
                LineTcpMeasurementEvent event = queue.get(cursor);
                boolean eventProcessed;
                if (event.threadId == id) {
                    processNextEvent(event);
                    eventProcessed = true;
                } else {
                    if (event.isRebalanceEvent()) {
                        eventProcessed = processRebalance(event);
                    } else {
                        eventProcessed = true;
                    }
                }
                if (eventProcessed) {
                    sequence.done(cursor);
                }
            }
        }

        private void handleEventException(LineTcpMeasurementEvent event, Parser parser, CairoException ex) {
            LOG.error()
                    .$("could not create parser, measurement will be skipped [jobName=").$(jobName)
                    .$(", table=").$(event.getTableName())
                    .$(", ex=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .$(']').$();
            Misc.free(parser);
            parserCache.remove(event.getTableName());
        }

        private void processNextEvent(LineTcpMeasurementEvent event) {
            Parser parser = parserCache.get(event.getTableName());
            try {
                if (null != parser) {
                    parser.processEvent(event);
                } else {
                    parser = new Parser();
                    parser.processFirstEvent(engine, securityContext, event);
                    LOG.info().$("created parser [jobName=").$(jobName).$(" table=").$(event.getTableName()).$(']').$();
                    parserCache.put(Chars.toString(event.getTableName()), parser);
                }
            } catch (CairoException ex) {
                handleEventException(event, parser, ex);
            }
        }

        private boolean processRebalance(LineTcpMeasurementEvent event) {
            if (event.rebalanceToThreadId == id) {
                if (event.rebalanceReleasedByFromThread) {
                    LOG.info().$("rebalance cycle, new thread ready [threadId=").$(id).$(", table=").$(event.rebalanceTableName).$(']').$();
                    return true;
                }

                return false;
            }

            if (event.rebalanceFromThreadId == id) {
                LOG.info().$("rebalance cycle, old thread finished [threadId=").$(id).$(", table=").$(event.rebalanceTableName).$(']').$();
                Parser parser = parserCache.get(event.rebalanceTableName);
                parserCache.remove(event.rebalanceTableName);
                parser.close();
                event.rebalanceReleasedByFromThread = true;
            }

            return true;
        }

        private class Parser implements Closeable {
            private final IntList colTypes = new IntList();
            private final IntList colIndexMappings = new IntList();
            private TableWriter writer;
            private int nUncommitted = 0;

            private transient int nMeasurementValues;
            private transient boolean error;

            @Override
            public void close() {
                if (null != writer) {
                    doMaintenance();
                    LOG.info().$("closed parser [jobName=").$(jobName).$(" name=").$(writer.getName()).$(']').$();
                    writer.close();
                    writer = null;
                }
            }

            private int addColumn(LineTcpMeasurementEvent event, RecordMetadata metadata, int n, int colType) {
                final int colIndex = metadata.getColumnCount();
                CharSequence columnName = event.getName(n);
                if (TableUtils.isValidColumnName(columnName)) {
                    writer.addColumn(columnName, colType);
                } else {
                    LOG.error().$("invalid column name [table=").$(writer.getName())
                            .$(", columnName=").$(columnName)
                            .$(']').$();
                    error = true;
                }
                return colIndex;
            }

            private void addRow(LineTcpMeasurementEvent event) {
                if (error) {
                    return;
                }
                Row row = null;
                try {
                    long timestamp = event.getTimestamp();
                    row = writer.newRow(timestamp);
                    for (int i = 0; i < nMeasurementValues; i++) {
                        final int columnType = colTypes.getQuick(i);
                        final int columnIndex = colIndexMappings.getQuick(i);
                        final CharSequence value = event.getValue(i);
                        try {
                            switch (columnType) {
                                case ColumnType.LONG:
                                    row.putLong(columnIndex, Numbers.parseLong(value, 0, value.length()-1));
                                    break;
                                case ColumnType.BOOLEAN:
                                    row.putBool(columnIndex, CairoLineProtoParserSupport.isTrue(value));
                                    break;
                                case ColumnType.STRING:
                                    row.putStr(columnIndex, value, 1, value.length() - 2);
                                    break;
                                case ColumnType.SYMBOL:
                                    row.putSym(columnIndex, value);
                                    break;
                                case ColumnType.DOUBLE:
                                    row.putDouble(columnIndex, Numbers.parseDouble(value));
                                    break;
                                case ColumnType.SHORT:
                                    row.putShort(columnIndex, Numbers.parseShort(value, 0, value.length() - 1));
                                    break;
                                case ColumnType.LONG256:
                                    if (value.charAt(0) == '0' && value.charAt(1) == 'x') {
                                        row.putLong256(columnIndex, value, 2, value.length() - 1);
                                    } else {
                                        throw BadCastException.INSTANCE;
                                    }
                                    break;
                                case ColumnType.TIMESTAMP:
                                    row.putTimestamp(columnIndex, Numbers.parseLong(value, 0, value.length() - 1));
                                    break;
                                default:
                                    break;
                            }
                        } catch (NumericException e) {
                            LOG.info().$("cast error [value=").$(value).$(", toType=").$(ColumnType.nameOf(columnType)).$(']').$();
                            throw BadCastException.INSTANCE;
                        }
                    }
                    row.append();
                } catch (NumericException | BadCastException ex) {
                    // These exceptions are logged elsewhere
                    if (null != row) {
                        row.cancel();
                    }
                    return;
                } catch (CairoException ex) {
                    LOG.error()
                            .$("could not insert measurement [jobName=").$(jobName)
                            .$(", table=").$(event.getTableName())
                            .$(", ex=").$(ex.getFlyweightMessage())
                            .$(", errno=").$(ex.getErrno())
                            .$(']').$();
                    if (null != row) {
                        row.cancel();
                    }
                    return;
                }
                nUncommitted++;
                if (nUncommitted > maxUncommittedRows) {
                    commit();
                }
            }

            private void commit() {
                writer.commit();
                nUncommitted = 0;
            }

            void doMaintenance() {
                if (nUncommitted == 0) {
                    return;
                }
                commit();
            }

            private int getColumnType(int i) {
                return colTypes.getQuick(i);
            }

            private void parseNames(LineTcpMeasurementEvent event) {
                RecordMetadata metadata = writer.getMetadata();
                for (int n = 0; n < nMeasurementValues; n++) {
                    int colIndex = metadata.getColumnIndexQuiet(event.getName(n));
                    final int colType = colTypes.getQuick(n);
                    if (colIndex == -1) {
                        colIndex = addColumn(event, metadata, n, colType);
                    } else {
                        final int tableColType = metadata.getColumnType(colIndex);
                        if (tableColType != colType) {
                            if (colType == ColumnType.LONG && ALLOWED_LONG_CONVERSIONS.contains(tableColType)) {
                                colTypes.setQuick(n, tableColType);
                            } else {
                                LOG.error().$("mismatched column and value types [table=").$(writer.getName())
                                        .$(", column=").$(metadata.getColumnName(colIndex))
                                        .$(", columnType=").$(ColumnType.nameOf(metadata.getColumnType(colIndex)))
                                        .$(", valueType=").$(ColumnType.nameOf(colTypes.getQuick(n)))
                                        .$(']').$();
                                error = true;
                                return;
                            }
                        }
                    }
                    colIndexMappings.set(n, colIndex);
                }
            }

            private void parseTypes(LineTcpMeasurementEvent event) {
                for (int n = 0; n < nMeasurementValues; n++) {
                    int colType;
                    if (n < event.getFirstFieldIndex()) {
                        colType = ColumnType.SYMBOL;
                    } else {
                        colType = CairoLineProtoParserSupport.getValueType(event.getValue(n));
                    }
                    colTypes.setQuick(n, colType);
                }
            }

            private void preprocessEvent(LineTcpMeasurementEvent event) {
                error = false;
                nMeasurementValues = event.getNValues();
                colTypes.ensureCapacity(nMeasurementValues);
                colIndexMappings.ensureCapacity(nMeasurementValues);
                parseTypes(event);
            }

            private void processEvent(LineTcpMeasurementEvent event) {
                assert event.getTableName().equals(writer.getName());
                preprocessEvent(event);
                parseNames(event);
                addRow(event);
            }

            private void processFirstEvent(CairoEngine engine, CairoSecurityContext securityContext, LineTcpMeasurementEvent event) {
                sqlExecutionContext.storeTelemetry(Telemetry.SYSTEM_ILP_RESERVE_WRITER, Telemetry.ORIGIN_ILP_TCP);
                assert null == writer;
                int status = engine.getStatus(securityContext, path, event.getTableName(), 0, event.getTableName().length());
                if (status == TableUtils.TABLE_EXISTS) {
                    writer = engine.getWriter(securityContext, event.getTableName());
                    processEvent(event);
                    return;
                }

                preprocessEvent(event);
                engine.createTable(
                        securityContext,
                        appendMemory,
                        path,
                        tableStructureAdapter.of(event, this));
                int nValues = event.getNValues();
                for (int n = 0; n < nValues; n++) {
                    colIndexMappings.set(n, n);
                }
                writer = engine.getWriter(securityContext, event.getTableName());
                addRow(event);
            }
        }

        private class TableStructureAdapter implements TableStructure {
            private LineTcpMeasurementEvent event;
            private Parser parser;
            private int columnCount;
            private int timestampIndex;

            @Override
            public int getColumnCount() {
                return columnCount;
            }

            @Override
            public CharSequence getColumnName(int columnIndex) {
                if (columnIndex == getTimestampIndex()) {
                    return "timestamp";
                }
                CharSequence colName = event.getName(columnIndex);
                if (TableUtils.isValidColumnName(colName)) {
                    return colName;
                }
                throw CairoException.instance(0).put("column name contains invalid characters [colName=").put(colName).put(']');
            }

            @Override
            public int getColumnType(int columnIndex) {
                if (columnIndex == getTimestampIndex()) {
                    return ColumnType.TIMESTAMP;
                }
                return parser.getColumnType(columnIndex);
            }

            @Override
            public int getIndexBlockCapacity(int columnIndex) {
                return 0;
            }

            @Override
            public boolean isIndexed(int columnIndex) {
                return false;
            }

            @Override
            public boolean isSequential(int columnIndex) {
                return false;
            }

            @Override
            public int getPartitionBy() {
                return PartitionBy.NONE;
            }

            @Override
            public boolean getSymbolCacheFlag(int columnIndex) {
                return cairoConfiguration.getDefaultSymbolCacheFlag();
            }

            @Override
            public int getSymbolCapacity(int columnIndex) {
                return cairoConfiguration.getDefaultSymbolCapacity();
            }

            @Override
            public CharSequence getTableName() {
                return event.getTableName();
            }

            @Override
            public int getTimestampIndex() {
                return timestampIndex;
            }

            TableStructureAdapter of(LineTcpMeasurementEvent event, Parser parser) {
                this.event = event;
                this.parser = parser;
                this.timestampIndex = event.getNValues();
                this.columnCount = timestampIndex + 1;
                return this;
            }
        }
    }

    static {
        ALLOWED_LONG_CONVERSIONS.add(ColumnType.SHORT);
        ALLOWED_LONG_CONVERSIONS.add(ColumnType.LONG256);
        ALLOWED_LONG_CONVERSIONS.add(ColumnType.TIMESTAMP);
    }
}
