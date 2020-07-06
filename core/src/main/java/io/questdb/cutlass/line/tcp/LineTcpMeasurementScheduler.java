package io.questdb.cutlass.line.tcp;

import java.io.Closeable;
import java.util.Arrays;

import io.questdb.cairo.AppendMemory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriter.Row;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.line.CachedCharSequence;
import io.questdb.cutlass.line.CairoLineProtoParserSupport;
import io.questdb.cutlass.line.CairoLineProtoParserSupport.BadCastException;
import io.questdb.cutlass.line.CharSequenceCache;
import io.questdb.cutlass.line.LineProtoParser;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.cutlass.line.TruncatedLineProtoLexer;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
import io.questdb.mp.Job;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.WorkerPool;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.time.MillisecondClock;

class LineTcpMeasurementScheduler implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementScheduler.class);
    private static int REBALANCE_EVENT_ID = -1; // A rebalance event is used to rebalance load across different threads
    private static int INCOMPLETE_EVENT_ID = -2; // An incomplete event is used when the queue producer has grabbed an event but is not able
                                                 // to populate it for some reason, the event needs to be committed to the queue incomplete
    private final CairoEngine engine;
    private final CairoSecurityContext securityContext;
    private final CairoConfiguration cairoConfiguration;
    private final MillisecondClock milliClock;
    private RingQueue<LineTcpMeasurementEvent> queue;
    private Sequence pubSeq;
    private long nextEventCursor = -1;
    private final CharSequenceObjHashMap<TableUpdateDetails> tableUpdateDetailsByTableName;
    private final int[] loadByThread;

    private final int nUpdatesPerLoadRebalance;
    private final double maxLoadRatio;
    private final int maxUncommittedRows;
    private final long maintenanceJobHysteresisInMs;

    private int nLoadCheckCycles = 0;
    private int nRebalances = 0;

    LineTcpMeasurementScheduler(CairoConfiguration cairoConfiguration, LineTcpReceiverConfiguration lineConfiguration, CairoEngine engine, WorkerPool writerWorkerPool) {
        this.engine = engine;
        this.securityContext = lineConfiguration.getCairoSecurityContext();
        this.cairoConfiguration = cairoConfiguration;
        this.milliClock = cairoConfiguration.getMillisecondClock();
        tableUpdateDetailsByTableName = new CharSequenceObjHashMap<>();
        loadByThread = new int[writerWorkerPool.getWorkerCount()];
        int maxMeasurementSize = lineConfiguration.getMaxMeasurementSize();
        int queueSize = lineConfiguration.getWriterQueueSize();
        queue = new RingQueue<>(() -> {
            return new LineTcpMeasurementEvent(maxMeasurementSize, lineConfiguration.getMicrosecondClock(), lineConfiguration.getTimestampAdapter());
        }, queueSize);
        pubSeq = new SPSequence(queueSize);

        int nWriterThreads = writerWorkerPool.getWorkerCount();
        Sequence[] subSeqById = new Sequence[nWriterThreads];
        if (nWriterThreads > 1) {
            FanOut fanOut = new FanOut();
            for (int n = 0, sz = nWriterThreads; n < sz; n++) {
                SCSequence subSeq = new SCSequence();
                fanOut.and(subSeq);
                subSeqById[n] = subSeq;
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

        nUpdatesPerLoadRebalance = lineConfiguration.getnUpdatesPerLoadRebalance();
        maxLoadRatio = lineConfiguration.getMaxLoadRatio();
        maxUncommittedRows = lineConfiguration.getMaxUncommittedRows();
        maintenanceJobHysteresisInMs = lineConfiguration.getMaintenanceJobHysteresisInMs();
    }

    LineTcpMeasurementEvent getNewEvent() {
        assert !closed();
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

    void commitNewEvent(LineTcpMeasurementEvent event, boolean complete) {
        assert !closed();
        if (nextEventCursor == -1) {
            throw new IllegalStateException("Cannot commit without prior call to getNewEvent()");
        }

        assert queue.get(nextEventCursor) == event;

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
        assert !closed();
        if (nextEventCursor == -1) {
            throw new IllegalStateException("Cannot commit without prior call to getNewEvent()");
        }

        assert queue.get(nextEventCursor) == event;
        event.createRebalanceEvent(fromThreadId, toThreadId, tableName);
        pubSeq.done(nextEventCursor);
        nextEventCursor = -1;
    }

    private boolean closed() {
        return null == pubSeq;
    }

    private void loadRebalance() {
        LOG.info().$("load check cycle ").$(++nLoadCheckCycles).$();
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
            LOG.info().$("rebalance cycle ").$(++nRebalances).$(" moving ").$(tableNameToMove).$(" from ").$(fromThreadId).$(" to ").$(toThreadId).$();
            commitRebalanceEvent(event, fromThreadId, toThreadId, tableNameToMove);
            TableUpdateDetails stats = tableUpdateDetailsByTableName.get(tableNameToMove);
            stats.threadId = toThreadId;
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

    int[] getLoadByThread() {
        return loadByThread;
    }

    int getnRebalances() {
        return nRebalances;
    }

    int getnLoadCheckCycles() {
        return nLoadCheckCycles;
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

    static class LineTcpMeasurementEvent implements Closeable {
        private TruncatedLineProtoLexer lexer;
        private final CharSequenceCache cache;
        private final MicrosecondClock clock;
        private final LineProtoTimestampAdapter timestampAdapter;
        private long measurementNameAddress;
        private final LongList addresses = new LongList();
        private int firstFieldIndex;
        private long timestampAddress;
        private int errorPosition;
        private int errorCode;
        private int threadId;
        private long timestamp;

        private int rebalanceFromThreadId;
        private int rebalanceToThreadId;
        private String rebalanceTableName;
        private boolean rebalanceReleasedByFromThread;

        private LineTcpMeasurementEvent(int maxMeasurementSize, MicrosecondClock clock, LineProtoTimestampAdapter timestampAdapter) {
            lexer = new TruncatedLineProtoLexer(maxMeasurementSize);
            cache = lexer.getCharSequenceCache();
            this.clock = clock;
            this.timestampAdapter = timestampAdapter;
            lexer.withParser(new LineProtoParser() {
                @Override
                public void onLineEnd(CharSequenceCache cache) {
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
                public void onError(int position, int state, int code) {
                    assert errorPosition == -1;
                    errorPosition = position;
                    errorCode = code;
                }
            });
        }

        long parseLine(long bytesPtr, long hi) {
            clear();
            long recvBufLineNext = lexer.parseLine(bytesPtr, hi);
            if (recvBufLineNext != -1) {
                if (!isError() && firstFieldIndex == -1) {
                    errorPosition = (int) (recvBufLineNext - bytesPtr);
                    errorCode = LineProtoParser.ERROR_EMPTY;
                }

                if (!isError()) {
                    if (timestampAddress == 0) {
                        timestamp = clock.getTicks();
                    }
                }
            }
            return recvBufLineNext;
        }

        private void clear() {
            measurementNameAddress = 0;
            addresses.clear();
            firstFieldIndex = -1;
            timestampAddress = 0;
            errorPosition = -1;
        }

        boolean isError() {
            return errorPosition != -1;
        }

        int getErrorPosition() {
            return errorPosition;
        }

        int getErrorCode() {
            return errorCode;
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
                    LOG.info().$("invalid timestamp: ").$(cache.get(timestampAddress)).$();
                    timestamp = Long.MIN_VALUE;
                    throw e;
                }
            }
            return timestamp;
        }

        CharSequence getName(int i) {
            return cache.get(addresses.getQuick(2 * i));
        }

        int getNValues() {
            return addresses.size() / 2;
        }

        CharSequence getValue(int i) {
            return cache.get(addresses.getQuick(2 * i + 1));
        }

        int getFirstFieldIndex() {
            return firstFieldIndex;
        }

        void createRebalanceEvent(int fromThreadId, int toThreadId, String tableName) {
            clear();
            threadId = REBALANCE_EVENT_ID;
            rebalanceFromThreadId = fromThreadId;
            rebalanceToThreadId = toThreadId;
            rebalanceTableName = tableName;
        }

        boolean isRebalanceEvent() {
            return threadId == REBALANCE_EVENT_ID;
        }

        @Override
        public void close() {
            lexer.close();
            lexer = null;
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

        @Override
        public boolean run(int workerId) {
            assert workerId == id;
            boolean busy = drainQueue();
            doMaintenance(busy);
            return busy;
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
                try {
                    if (event.threadId == id) {
                        eventProcessed = processNextEvent(event);
                    } else {
                        if (event.isRebalanceEvent()) {
                            eventProcessed = processRebalance(event);
                        } else {
                            eventProcessed = true;
                        }
                    }
                } catch (RuntimeException ex) {
                    LOG.error().$(ex).$();
                    eventProcessed = true;
                }
                if (eventProcessed) {
                    sequence.done(cursor);
                }
            }
        }

        private void doMaintenance(boolean busy) {
            long millis = milliClock.getTicks();
            if (busy && (millis - lastMaintenanceJobMillis) < maintenanceJobHysteresisInMs) {
                return;
            }

            lastMaintenanceJobMillis = millis;
            ObjList<CharSequence> tableNames = parserCache.keys();
            for (int n = 0, sz = tableNames.size(); n < sz; n++) {
                Parser parser = parserCache.get(tableNames.get(n));
                parser.doMaintenance();
            }
        }

        private boolean processNextEvent(LineTcpMeasurementEvent event) {
            Parser parser = parserCache.get(event.getTableName());
            if (null == parser) {
                parser = new Parser();
                try {
                    parser.processFirstEvent(engine, securityContext, event);
                } catch (CairoException ex) {
                    LOG.info().$("could not create parser [jobName=").$(jobName).$(" name=").$(event.getTableName()).$(", ex=").$(ex.getFlyweightMessage()).$(']').$();
                    parser.close();
                    return false;
                }
                LOG.info().$("created parser [jobName=").$(jobName).$(" name=").$(event.getTableName()).$(']').$();
                parserCache.put(Chars.toString(event.getTableName()), parser);
            } else {
                parser.processEvent(event);
            }
            return true;
        }

        private boolean processRebalance(LineTcpMeasurementEvent event) {
            if (event.rebalanceToThreadId == id) {
                if (!event.rebalanceReleasedByFromThread) {
                    return false;
                }

                return true;
            }

            if (event.rebalanceFromThreadId == id) {
                Parser parser = parserCache.get(event.rebalanceTableName);
                parserCache.remove(event.rebalanceTableName);
                parser.close();
                event.rebalanceReleasedByFromThread = true;
            }

            return true;
        }

        private class Parser implements Closeable {
            private TableWriter writer;
            private final IntList colTypes = new IntList();
            private final IntList colIndexMappings = new IntList();
            private int nUncommitted = 0;

            private transient int nMeasurementValues;
            private transient boolean error;

            private void processFirstEvent(CairoEngine engine, CairoSecurityContext securityContext, LineTcpMeasurementEvent event) {
                assert null == writer;
                int status = engine.getStatus(securityContext, path, event.getTableName(), 0, event.getTableName().length());
                if (status == TableUtils.TABLE_EXISTS) {
                    writer = engine.getWriter(securityContext, event.getTableName());
                    processEvent(event);
                    return;
                }

                preprocessEvent(event);
                engine.creatTable(
                        securityContext,
                        appendMemory,
                        path,
                        tableStructureAdapter.of(event, this));
                int nValues = event.getNValues();
                for (int n = 0; n < nValues; n++) {
                    colIndexMappings.add(n, n);
                }
                writer = engine.getWriter(securityContext, event.getTableName());
                addRow(event);
            }

            private void processEvent(LineTcpMeasurementEvent event) {
                assert event.getTableName().equals(writer.getName());
                preprocessEvent(event);
                parseNames(event);
                addRow(event);
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
                        int columnType = colTypes.getQuick(i);
                        int columnIndex = colIndexMappings.getQuick(i);
                        CairoLineProtoParserSupport.writers.getQuick(columnType).write(row, columnIndex, event.getValue(i));
                    }
                    row.append();
                } catch (NumericException | CairoException | BadCastException ignore) {
                    // These exceptions are logged elsewhere
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

            private void preprocessEvent(LineTcpMeasurementEvent event) {
                error = false;
                nMeasurementValues = event.getNValues();
                colTypes.ensureCapacity(nMeasurementValues);
                colIndexMappings.ensureCapacity(nMeasurementValues);
                parseTypes(event);
            }

            private void parseTypes(LineTcpMeasurementEvent event) {
                for (int n = 0; n < nMeasurementValues; n++) {
                    int colType;
                    if (n < event.getFirstFieldIndex()) {
                        colType = ColumnType.SYMBOL;
                    } else {
                        colType = CairoLineProtoParserSupport.getValueType(event.getValue(n));
                    }
                    colTypes.add(n, colType);
                }
            }

            private void parseNames(LineTcpMeasurementEvent event) {
                RecordMetadata metadata = writer.getMetadata();
                for (int n = 0; n < nMeasurementValues; n++) {
                    int colIndex = metadata.getColumnIndexQuiet(event.getName(n));
                    if (colIndex == -1) {
                        colIndex = metadata.getColumnCount();
                        writer.addColumn(event.getName(n), colTypes.getQuick(n));
                    } else {
                        if (metadata.getColumnType(colIndex) != colTypes.getQuick(n)) {
                            LOG.error().$("mismatched column and value types [table=").$(writer.getName())
                                    .$(", column=").$(metadata.getColumnName(colIndex))
                                    .$(", columnType=").$(ColumnType.nameOf(metadata.getColumnType(colIndex)))
                                    .$(", valueType=").$(ColumnType.nameOf(colTypes.getQuick(n)))
                                    .$(']').$();
                            error = true;
                            return;
                        }
                    }
                    colIndexMappings.add(n, colIndex);
                }
            }

            private int getColumnType(int i) {
                return colTypes.getQuick(i);
            }

            void doMaintenance() {
                if (nUncommitted == 0) {
                    return;
                }
                commit();
            }

            @Override
            public void close() {
                if (null != writer) {
                    doMaintenance();
                    LOG.info().$("closed parser [jobName=").$(jobName).$(" name=").$(writer.getName()).$(']').$();
                    writer.close();
                    writer = null;
                }
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
                return event.getName(columnIndex);
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
}
