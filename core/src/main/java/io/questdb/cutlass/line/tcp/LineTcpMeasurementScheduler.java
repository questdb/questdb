package io.questdb.cutlass.line.tcp;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

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
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.microtime.MicrosecondClock;
import io.questdb.std.str.Path;

class LineTcpMeasurementScheduler implements Closeable {
    private static final Log LOG = LogFactory.getLog(LineTcpMeasurementScheduler.class);
    private static final int NO_THREAD_ID = -1;
    private final CairoEngine engine;
    private final CairoSecurityContext securityContext;
    private final CairoConfiguration cairoConfiguration;
    private RingQueue<LineTcpMeasurementEvent> queue;
    private Sequence pubSeq;
    private WriterThread[] writerThreads;
    private long nextEventCursor = -1;
    private final CharSequenceIntHashMap threadIdByTableName;

    LineTcpMeasurementScheduler(CairoConfiguration cairoConfiguration, LineTcpReceiverConfiguration lineConfiguration, CairoEngine engine) {
        this.engine = engine;
        this.securityContext = lineConfiguration.getCairoSecurityContext();
        this.cairoConfiguration = cairoConfiguration;
        threadIdByTableName = new CharSequenceIntHashMap(8, 0.5, NO_THREAD_ID);
        int maxMeasurementSize = lineConfiguration.getMaxMeasurementSize();
        int queueSize = lineConfiguration.getWriterQueueSize();
        queue = new RingQueue<>(() -> {
            return new LineTcpMeasurementEvent(maxMeasurementSize, lineConfiguration.getMicrosecondClock(), lineConfiguration.getTimestampAdapter());
        }, queueSize);
        pubSeq = new SPSequence(queueSize);
        if (lineConfiguration.getNWriterThreads() > 1) {
            writerThreads = new WriterThread[lineConfiguration.getNWriterThreads()];
            FanOut fanOut = new FanOut();
            for (int n = 0, sz = lineConfiguration.getNWriterThreads(); n < sz; n++) {
                SCSequence subSeq = new SCSequence();
                fanOut.and(subSeq);
                writerThreads[n] = new WriterThread(n, subSeq);
            }
            pubSeq.then(fanOut).then(pubSeq);
        } else {
            writerThreads = new WriterThread[1];
            SCSequence subSeq = new SCSequence();
            pubSeq.then(subSeq).then(pubSeq);
            writerThreads[0] = new WriterThread(0, subSeq);
        }

        for (int n = 0; n < writerThreads.length; n++) {
            writerThreads[n].start();
        }
    }

    LineTcpMeasurementEvent getNewEvent() {
        assert !closed();
        if (nextEventCursor == -1) {
            do {
                nextEventCursor = pubSeq.next();
            } while (nextEventCursor == -2);
            if (nextEventCursor < 0) {
                nextEventCursor = -1;
                return null;
            }
        }
        return queue.get(nextEventCursor);
    }

    void commitNewEvent(LineTcpMeasurementEvent event) {
        assert !closed();
        if (nextEventCursor == -1) {
            throw new IllegalStateException("Cannot commit without prior call to getNewEvent()");
        }

        assert queue.get(nextEventCursor) == event;

        int threadId = threadIdByTableName.get(event.getTableName());
        if (threadId == NO_THREAD_ID) {
            String tableName = event.getTableName().toString();
            threadId = threadIdByTableName.size() % writerThreads.length;
            threadIdByTableName.put(tableName, threadId);
            LOG.info().$("assigned ").$(tableName).$(" to thread ").$(threadId).$();
        }
        event.threadId = threadId;

        pubSeq.done(nextEventCursor);
        nextEventCursor = -1;
    }

    private boolean closed() {
        return null == pubSeq;
    }

    @Override
    public void close() {
        if (null != pubSeq) {
            for (int n = 0; n < writerThreads.length; n++) {
                writerThreads[n].interrupt();
            }
            pubSeq = null;
            threadIdByTableName.clear();
        }
    }

    public void waitUntilClosed() {
        close();
        boolean finished;
        do {
            finished = true;

            synchronized (writerThreads) {
                for (int n = 0; n < writerThreads.length; n++) {
                    if (writerThreads[n] != null) {
                        finished = false;
                        try {
                            writerThreads.wait(10);
                        } catch (InterruptedException e) {
                            // Ignore
                        }
                        continue;
                    }
                }
            }
        } while (!finished);
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
            measurementNameAddress = 0;
            addresses.clear();
            firstFieldIndex = -1;
            timestampAddress = 0;
            errorPosition = -1;
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

        long getTimestamp() {
            if (timestampAddress != 0) {
                try {
                    timestamp = timestampAdapter.getMicros(cache.get(timestampAddress));
                    timestampAddress = 0;
                } catch (NumericException e) {
                    LOG.info().$("invalid timestamp: ").$(cache.get(timestampAddress)).$();
                    timestamp = Long.MIN_VALUE;
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

        @Override
        public void close() {
            lexer.close();
            lexer = null;
        }
    }

    private class WriterThread extends Thread {
        private final int id;
        private final Sequence sequence;
        private final CharSequenceObjHashMap<Parser> parserCache = new CharSequenceObjHashMap<>();
        private final AppendMemory appendMemory = new AppendMemory();
        private final Path path = new Path();
        private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();

        private WriterThread(int id, Sequence sequence) {
            super();
            this.id = id;
            this.sequence = sequence;
            setName(LineTcpMeasurementScheduler.class.getSimpleName() + "-writer-" + id);
            setDaemon(true);
        }

        @Override
        public void run() {
            LOG.info().$(getName()).$(" starting").$();
            uninterruptedLoop:
            while (true) {
                try {
                    long cursor;
                    int nWaitIter = 0;
                    while ((cursor = sequence.next()) < 0) {
                        if (cursor == -1) {
                            if (isInterrupted()) {
                                LOG.info().$(getName()).$(" interrupted, exiting").$();
                                break uninterruptedLoop;
                            }
                            if (nWaitIter < 1_000_000) {
                                nWaitIter++;
                                yield();
                            } else {
                                LockSupport.parkNanos(100_000);
                            }
                        }
                    }
                    LineTcpMeasurementEvent event = queue.get(cursor);
                    boolean eventProcessed;
                    try {
                        if (event.threadId == id) {
                            eventProcessed = processNextEvent(event);
                        } else {
                            eventProcessed = true;
                        }
                    } catch (RuntimeException ex) {
                        LOG.error().$(ex).$();
                        eventProcessed = true;
                    }
                    if (eventProcessed) {
                        sequence.done(cursor);
                    }
                } catch (RuntimeException ex) {
                    LOG.error().$(ex).$();
                }
            }

            ObjList<CharSequence> tableNames = parserCache.keys();
            for (int n = 0; n < tableNames.size(); n++) {
                parserCache.get(tableNames.get(n)).close();
            }
            parserCache.clear();
            appendMemory.close();
            path.close();

            synchronized (writerThreads) {
                writerThreads[id] = null;
                for (int n = 0; n < writerThreads.length; n++) {
                    if (writerThreads[n] != null) {
                        LOG.info().$(getName()).$(" finished, thread ").$(n).$(" is still running, exiting without cleanup").$();
                        return;
                    }
                }
                writerThreads.notifyAll();
            }
            LOG.info().$(getName()).$(" all other threads finished, cleaning up").$();
            for (int n = 0; n < queue.getCapacity(); n++) {
                queue.get(n).close();
            }
            LOG.info().$(getName()).$(" finished").$();
        }

        private boolean processNextEvent(LineTcpMeasurementEvent event) {
            Parser parser = parserCache.get(event.getTableName());
            if (null == parser) {
                parser = new Parser();
                try {
                    parser.processFirstEvent(engine, securityContext, event);
                } catch (CairoException ex) {
                    LOG.info().$(getName()).$(" could not create parser [name=").$(event.getTableName()).$(", ex=").$(ex.getFlyweightMessage()).$(']').$();
                    parser.close();
                    return false;
                }
                LOG.info().$(getName()).$(" created parser [name=").$(event.getTableName()).$(']').$();
                parserCache.put(event.getTableName().toString(), parser);
                return true;
            } else {
                parser.processEvent(event);
                return true;
            }
        }

        private class Parser implements Closeable {
            private TableWriter writer;
            private final IntList colTypes = new IntList();
            private final IntList colIndexMappings = new IntList();

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
                long timestamp = event.getTimestamp();
                Row row = writer.newRow(timestamp);
                try {
                    for (int i = 0; i < nMeasurementValues; i++) {
                        int columnType = colTypes.getQuick(i);
                        int columnIndex = colIndexMappings.getQuick(i);
                        CairoLineProtoParserSupport.writers.getQuick(columnType).write(row, columnIndex, event.getValue(i));
                    }
                    row.append();
                } catch (BadCastException ignore) {
                    row.cancel();
                }
                writer.commit();
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

            @Override
            public void close() {
                if (null != writer) {
                    LOG.info().$(getName()).$(" closed parser [name=").$(writer.getName()).$(']').$();
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
}
