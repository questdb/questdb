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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.cutlass.http.HttpChunkedResponseSocket;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.text.TextUtil;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.SCSequence;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class JsonQueryProcessorState implements Mutable, Closeable {
    public static final String HIDDEN = "hidden";
    static final int QUERY_METADATA = 2;
    static final int QUERY_METADATA_SUFFIX = 3;
    static final int QUERY_PREFIX = 1;
    static final int QUERY_RECORD = 5;
    static final int QUERY_RECORD_PREFIX = 9;
    static final int QUERY_RECORD_START = 4;
    static final int QUERY_RECORD_SUFFIX = 6;
    static final int QUERY_SETUP_FIRST_RECORD = 8;
    static final int QUERY_SUFFIX = 7;
    private static final Log LOG = LogFactory.getLog(JsonQueryProcessorState.class);
    private final ObjList<String> columnNames = new ObjList<>();
    private final IntList columnSkewList = new IntList();
    private final IntList columnTypesAndFlags = new IntList();
    private final StringSink columnsQueryParameter = new StringSink();
    private final int doubleScale;
    private final SCSequence eventSubSequence = new SCSequence();
    private final int floatScale;
    private final HttpConnectionContext httpConnectionContext;
    private final NanosecondClock nanosecondClock;
    private final StringSink query = new StringSink();
    private final ObjList<StateResumeAction> resumeActions = new ObjList<>();
    private final long statementTimeout;
    private int columnCount;
    private int columnIndex;
    private long compilerNanos;
    private boolean containsSecret;
    private long count;
    private boolean countRows = false;
    private RecordCursor cursor;
    private long executeStartNanos;
    private boolean explain = false;
    private boolean noMeta = false;
    private OperationFuture operationFuture;
    private boolean pausedQuery = false;
    private boolean queryCacheable = false;
    private boolean queryJitCompiled = false;
    private int queryState = QUERY_PREFIX;
    private int queryTimestampIndex;
    private short queryType;
    private boolean quoteLargeNum = false;
    private Record record;
    private long recordCountNanos;
    private RecordCursorFactory recordCursorFactory;
    private Rnd rnd;
    private long skip;
    private long stop;
    private boolean timings = false;

    public JsonQueryProcessorState(
            HttpConnectionContext httpConnectionContext,
            NanosecondClock nanosecondClock,
            int floatScale,
            int doubleScale
    ) {
        this.httpConnectionContext = httpConnectionContext;
        resumeActions.extendAndSet(QUERY_PREFIX, this::onQueryPrefix);
        resumeActions.extendAndSet(QUERY_METADATA, this::onQueryMetadata);
        resumeActions.extendAndSet(QUERY_METADATA_SUFFIX, this::onQueryMetadataSuffix);
        resumeActions.extendAndSet(QUERY_SETUP_FIRST_RECORD, this::doFirstRecordLoop);
        resumeActions.extendAndSet(QUERY_RECORD_PREFIX, this::onQueryRecordPrefix);
        resumeActions.extendAndSet(QUERY_RECORD, this::onQueryRecord);
        resumeActions.extendAndSet(QUERY_RECORD_SUFFIX, this::onQueryRecordSuffix);
        resumeActions.extendAndSet(QUERY_SUFFIX, this::doQuerySuffix);

        this.nanosecondClock = nanosecondClock;
        this.floatScale = floatScale;
        this.doubleScale = doubleScale;
        this.statementTimeout = httpConnectionContext.getRequestHeader().getStatementTimeout();
    }

    @Override
    public void clear() {
        columnCount = 0;
        columnSkewList.clear();
        columnTypesAndFlags.clear();
        columnNames.clear();
        queryTimestampIndex = -1;
        cursor = Misc.free(cursor);
        record = null;
        if (recordCursorFactory != null) {
            if (queryCacheable) {
                QueryCache.getThreadLocalInstance().push(query, recordCursorFactory);
            } else {
                recordCursorFactory.close();
            }
            recordCursorFactory = null;
        }
        query.clear();
        columnsQueryParameter.clear();
        queryState = QUERY_PREFIX;
        columnIndex = 0;
        countRows = false;
        explain = false;
        noMeta = false;
        timings = false;
        pausedQuery = false;
        quoteLargeNum = false;
        queryJitCompiled = false;
        operationFuture = Misc.free(operationFuture);
        skip = 0;
        count = 0;
        stop = 0;
        containsSecret = false;
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
        freeAsyncOperation();
    }

    public void configure(
            HttpRequestHeader request,
            DirectByteCharSequence query,
            long skip,
            long stop
    ) throws Utf8Exception {
        this.query.clear();
        TextUtil.utf8ToUtf16(query.getLo(), query.getHi(), this.query);
        this.skip = skip;
        this.stop = stop;
        count = 0L;
        noMeta = Chars.equalsNc("true", request.getUrlParam("nm"));
        countRows = Chars.equalsNc("true", request.getUrlParam("count"));
        timings = Chars.equalsNc("true", request.getUrlParam("timings"));
        explain = Chars.equalsNc("true", request.getUrlParam("explain"));
        quoteLargeNum = Chars.equalsNc("true", request.getUrlParam("quoteLargeNum"))
                || Chars.equalsNc("con", request.getUrlParam("src"));
    }

    public LogRecord critical() {
        return LOG.critical().$('[').$(getFd()).$("] ");
    }

    public LogRecord error() {
        return LOG.error().$('[').$(getFd()).$("] ");
    }

    public void freeAsyncOperation() {
        operationFuture = Misc.free(operationFuture);
    }

    public SCSequence getEventSubSequence() {
        return eventSubSequence;
    }

    public long getExecutionTimeNanos() {
        return nanosecondClock.getTicks() - this.executeStartNanos;
    }

    public HttpConnectionContext getHttpConnectionContext() {
        return httpConnectionContext;
    }

    public OperationFuture getOperationFuture() {
        return operationFuture;
    }

    public CharSequence getQuery() {
        return query;
    }

    public CharSequence getQueryOrHidden() {
        return containsSecret ? HIDDEN : query;
    }

    public short getQueryType() {
        return queryType;
    }

    public Rnd getRnd() {
        return rnd;
    }

    public long getStatementTimeout() {
        return statementTimeout;
    }

    public LogRecord info() {
        return LOG.info().$('[').$(getFd()).$("] ");
    }

    public boolean isPausedQuery() {
        return pausedQuery;
    }

    public void logBufferTooSmall() {
        info().$("Response buffer is too small, state=").$(queryState).$();
    }

    public void logExecuteCached() {
        info().$("execute-cached [skip: ").$(skip)
                .$(", stop: ").$(stop).I$();
    }

    public void logExecuteNew() {
        info().$("execute-new [skip: ").$(skip)
                .$(", stop: ").$(stop).I$();
    }

    public void logSqlError(FlyweightMessageContainer container) {
        info().$("sql error [q=`").utf8(getQueryOrHidden())
                .$("`, at=").$(container.getPosition())
                .$(", message=`").utf8(container.getFlyweightMessage()).$('`').$(']').$();
    }

    public void logTimings() {
        info().$("timings ")
                .$("[compiler: ").$(compilerNanos)
                .$(", count: ").$(recordCountNanos)
                .$(", execute: ").$(nanosecondClock.getTicks() - executeStartNanos)
                .$(", q=`").utf8(getQueryOrHidden())
                .$("`]").$();
    }

    public void setCompilerNanos(long compilerNanos) {
        this.compilerNanos = compilerNanos;
    }

    public void setContainsSecret(boolean containsSecret) {
        this.containsSecret = containsSecret;
    }

    public void setOperationFuture(OperationFuture fut) {
        operationFuture = fut;
    }

    public void setPausedQuery(boolean pausedQuery) {
        this.pausedQuery = pausedQuery;
    }

    public void setQueryType(short type) {
        queryType = type;
    }

    public void setRnd(Rnd rnd) {
        this.rnd = rnd;
    }

    public void startExecutionTimer() {
        this.executeStartNanos = nanosecondClock.getTicks();
    }

    private static void putBooleanValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getBool(col));
    }

    private static void putByteValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getByte(col));
    }

    private static void putCharValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        char c = rec.getChar(col);
        if (c == 0) {
            socket.put("\"\"");
        } else {
            socket.put('"').putUtf8(c).put('"');
        }
    }

    private static void putDateValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final long d = rec.getDate(col);
        if (d == Long.MIN_VALUE) {
            socket.put("null");
            return;
        }
        socket.put('"').putISODateMillis(d).put('"');
    }

    private static void putGeoHashStringByteValue(HttpChunkedResponseSocket socket, Record rec, int col, int bitFlags) {
        byte l = rec.getGeoByte(col);
        putGeoHashStringValue(socket, l, bitFlags);
    }

    private static void putGeoHashStringIntValue(HttpChunkedResponseSocket socket, Record rec, int col, int bitFlags) {
        int l = rec.getGeoInt(col);
        putGeoHashStringValue(socket, l, bitFlags);
    }

    private static void putGeoHashStringLongValue(HttpChunkedResponseSocket socket, Record rec, int col, int bitFlags) {
        long l = rec.getGeoLong(col);
        putGeoHashStringValue(socket, l, bitFlags);
    }

    private static void putGeoHashStringShortValue(HttpChunkedResponseSocket socket, Record rec, int col, int bitFlags) {
        short l = rec.getGeoShort(col);
        putGeoHashStringValue(socket, l, bitFlags);
    }

    private static void putGeoHashStringValue(HttpChunkedResponseSocket socket, long value, int bitFlags) {
        if (value == GeoHashes.NULL) {
            socket.put("null");
        } else {
            socket.put('\"');
            if (bitFlags < 0) {
                GeoHashes.appendCharsUnsafe(value, -bitFlags, socket);
            } else {
                GeoHashes.appendBinaryStringUnsafe(value, bitFlags, socket);
            }
            socket.put('\"');
        }
    }

    private static void putIPv4Value(HttpChunkedResponseSocket socket, Record rec, int col) {
        final int i = rec.getIPv4(col);
        if (i == Numbers.IPv4_NULL) {
            socket.put("null");
        } else {
            socket.put('"');
            Numbers.intToIPv4Sink(socket, i);
            socket.put('"');
        }
    }

    private static void putIntValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final int i = rec.getInt(col);
        if (i == Integer.MIN_VALUE) {
            socket.put("null");
        } else {
            Numbers.append(socket, i);
        }
    }

    private static void putLong256Value(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put('"');
        rec.getLong256(col, socket);
        socket.put('"');
    }

    private static void putLongValue(HttpChunkedResponseSocket socket, Record rec, int col, boolean quoteLargeNum) {
        final long l = rec.getLong(col);
        if (l == Long.MIN_VALUE) {
            socket.put("null");
        } else if (quoteLargeNum) {
            socket.put('"').put(l).put('"');
        } else {
            socket.put(l);
        }
    }

    private static void putRecValue(HttpChunkedResponseSocket socket) {
        putStringOrNull(socket, null);
    }

    private static void putShortValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getShort(col));
    }

    private static void putStrValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putStringOrNull(socket, rec.getStr(col));
    }

    private static void putStringOrNull(CharSink r, CharSequence str) {
        if (str == null) {
            r.put("null");
        } else {
            r.encodeUtf8AndQuote(str);
        }
    }

    private static void putSymValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putStringOrNull(socket, rec.getSym(col));
    }

    private static void putTimestampValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final long t = rec.getTimestamp(col);
        if (t == Long.MIN_VALUE) {
            socket.put("null");
            return;
        }
        socket.put('"').putISODate(t).put('"');
    }

    private static void putUuidValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        long lo = rec.getLong128Lo(col);
        long hi = rec.getLong128Hi(col);
        if (Uuid.isNull(lo, hi)) {
            socket.put("null");
            return;
        }
        socket.put('"');
        Numbers.appendUuid(lo, hi, socket);
        socket.put('"');
    }

    private boolean addColumnToOutput(
            RecordMetadata metadata,
            CharSequence columnNames,
            int start,
            int hi
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (start == hi) {
            info().$("empty column in list '").$(columnNames).$('\'').$();
            HttpChunkedResponseSocket socket = getHttpConnectionContext().getChunkedResponseSocket();
            JsonQueryProcessor.header(socket, "", 400);
            socket.put('{')
                    .putQuoted("query").put(':').encodeUtf8AndQuote(query).put(',')
                    .putQuoted("error").put(':').putQuoted("empty column in list")
                    .put('}');
            socket.sendChunk(true);
            return true;
        }

        int columnIndex = metadata.getColumnIndexQuiet(columnNames, start, hi);
        if (columnIndex == RecordMetadata.COLUMN_NOT_FOUND) {
            info().$("invalid column in list: '").$(columnNames, start, hi).$('\'').$();
            HttpChunkedResponseSocket socket = getHttpConnectionContext().getChunkedResponseSocket();
            JsonQueryProcessor.header(socket, "", 400);
            socket.put('{')
                    .putQuoted("query").put(':').encodeUtf8AndQuote(query).put(',')
                    .putQuoted("error").put(':').put('\'').put("invalid column in list: ").put(columnNames, start, hi).put('\'')
                    .put('}');
            socket.sendChunk(true);
            return true;
        }

        addColumnTypeAndName(metadata, columnIndex);
        this.columnSkewList.add(columnIndex);
        return false;
    }

    private void addColumnTypeAndName(RecordMetadata metadata, int i) {
        int columnType = metadata.getColumnType(i);
        int flags = GeoHashes.getBitFlags(columnType);
        this.columnTypesAndFlags.add(columnType);
        this.columnTypesAndFlags.add(flags);
        this.columnNames.add(metadata.getColumnName(i));
    }

    private void doFirstRecordLoop(
            HttpChunkedResponseSocket socket,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (onQuerySetupFirstRecord()) {
            doRecordFetchLoop(socket, columnCount);
        } else {
            doQuerySuffix(socket, columnCount);
        }
    }

    private void doNextRecordLoop(
            HttpChunkedResponseSocket socket,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (doQueryNextRecord()) {
            doRecordFetchLoop(socket, columnCount);
        } else {
            doQuerySuffix(socket, columnCount);
        }
    }

    private void doQueryMetadata(HttpChunkedResponseSocket socket, int columnCount) {
        queryState = QUERY_METADATA;
        for (; columnIndex < columnCount; columnIndex++) {
            socket.bookmark();
            if (columnIndex > 0) {
                socket.put(',');
            }
            int columnType = columnTypesAndFlags.getQuick(2 * columnIndex);
            socket.put('{')
                    .putQuoted("name").put(':').encodeUtf8AndQuote(columnNames.getQuick(columnIndex))
                    .put(',')
                    .putQuoted("type").put(':').putQuoted(ColumnType.nameOf(columnType == ColumnType.NULL ? ColumnType.STRING : columnType))
                    .put('}');
        }
    }

    private void doQueryMetadataSuffix(HttpChunkedResponseSocket socket) {
        queryState = QUERY_METADATA_SUFFIX;
        socket.bookmark();
        socket.put("],\"dataset\":[");
    }

    private boolean doQueryNextRecord() {
        if (cursor.hasNext()) {
            if (count < stop) {
                return true;
            } else {
                onNoMoreData();
            }
        }
        return false;
    }

    private boolean doQueryPrefix(HttpChunkedResponseSocket socket) {
        if (noMeta) {
            socket.bookmark();
            socket.put('{').putQuoted("dataset").put(":[");
            return false;
        }
        socket.bookmark();
        socket.put('{')
                .putQuoted("query").put(':').encodeUtf8AndQuote(query)
                .put(',').putQuoted("columns").put(':').put('[');
        columnIndex = 0;
        return true;
    }

    private void doQueryRecord(HttpChunkedResponseSocket socket, int columnCount) {
        queryState = QUERY_RECORD;
        for (; columnIndex < columnCount; columnIndex++) {
            socket.bookmark();
            if (columnIndex > 0) {
                socket.put(',');
            }

            int columnIdx = columnSkewList.size() > 0 ? columnSkewList.getQuick(columnIndex) : columnIndex;
            int columnType = columnTypesAndFlags.getQuick(2 * columnIndex);
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BOOLEAN:
                    putBooleanValue(socket, record, columnIdx);
                    break;
                case ColumnType.BYTE:
                    putByteValue(socket, record, columnIdx);
                    break;
                case ColumnType.DOUBLE:
                    putDoubleValue(socket, record, columnIdx);
                    break;
                case ColumnType.FLOAT:
                    putFloatValue(socket, record, columnIdx);
                    break;
                case ColumnType.INT:
                    putIntValue(socket, record, columnIdx);
                    break;
                case ColumnType.LONG:
                    putLongValue(socket, record, columnIdx, quoteLargeNum);
                    break;
                case ColumnType.DATE:
                    putDateValue(socket, record, columnIdx);
                    break;
                case ColumnType.TIMESTAMP:
                    putTimestampValue(socket, record, columnIdx);
                    break;
                case ColumnType.SHORT:
                    putShortValue(socket, record, columnIdx);
                    break;
                case ColumnType.CHAR:
                    putCharValue(socket, record, columnIdx);
                    break;
                case ColumnType.STRING:
                    putStrValue(socket, record, columnIdx);
                    break;
                case ColumnType.SYMBOL:
                    putSymValue(socket, record, columnIdx);
                    break;
                case ColumnType.BINARY:
                    putBinValue(socket);
                    break;
                case ColumnType.LONG256:
                    putLong256Value(socket, record, columnIdx);
                    break;
                case ColumnType.GEOBYTE:
                    putGeoHashStringByteValue(socket, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex + 1));
                    break;
                case ColumnType.GEOSHORT:
                    putGeoHashStringShortValue(socket, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex + 1));
                    break;
                case ColumnType.GEOINT:
                    putGeoHashStringIntValue(socket, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex + 1));
                    break;
                case ColumnType.GEOLONG:
                    putGeoHashStringLongValue(socket, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex + 1));
                    break;
                case ColumnType.RECORD:
                    putRecValue(socket);
                    break;
                case ColumnType.NULL:
                    socket.put("null");
                    break;
                case ColumnType.LONG128:
                    throw new UnsupportedOperationException();
                case ColumnType.UUID:
                    putUuidValue(socket, record, columnIdx);
                    break;
                case ColumnType.IPv4:
                    putIPv4Value(socket, record, columnIdx);
                    break;
                default:
                    assert false : "Not supported type in output " + ColumnType.nameOf(columnType);
                    socket.put("null"); // To make JSON valid
                    break;
            }
        }
    }

    private void doQueryRecordPrefix(HttpChunkedResponseSocket socket) {
        queryState = QUERY_RECORD_PREFIX;
        socket.bookmark();
        if (count > skip) {
            socket.put(',');
        }
        socket.put('[');
        columnIndex = 0;
        count++;
    }

    private void doQueryRecordSuffix(HttpChunkedResponseSocket socket) {
        queryState = QUERY_RECORD_SUFFIX;
        socket.bookmark();
        socket.put(']');
    }

    private void doQuerySuffix(
            HttpChunkedResponseSocket socket,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        queryState = QUERY_SUFFIX;
        if (count > -1) {
            logTimings();
            socket.bookmark();
            socket.put(']');
            socket.put(',').putQuoted("timestamp").put(':').put(queryTimestampIndex);
            socket.put(',').putQuoted("count").put(':').put(count);
            if (timings) {
                socket.put(',').putQuoted("timings").put(':')
                        .put('{')
                        .putQuoted("compiler").put(':').put(compilerNanos).put(',')
                        .putQuoted("execute").put(':').put(nanosecondClock.getTicks() - executeStartNanos).put(',')
                        .putQuoted("count").put(':').put(recordCountNanos)
                        .put('}');
            }
            if (explain) {
                socket.put(',').putQuoted("explain").put(':')
                        .put('{')
                        .putQuoted("jitCompiled").put(':').put(queryJitCompiled ? "true" : "false")
                        .put('}');
            }
            socket.put('}');
            count = -1;
            socket.sendChunk(true);
            return;
        }
        socket.done();
    }

    private void doRecordFetchLoop(
            HttpChunkedResponseSocket socket,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        do {
            doQueryRecordPrefix(socket);
            doQueryRecord(socket, columnCount);
            doQueryRecordSuffix(socket);
        } while (doQueryNextRecord());
        doQuerySuffix(socket, columnCount);
    }

    private int getFd() {
        return httpConnectionContext.getFd();
    }

    private void onNoMoreData() {
        long nanos = nanosecondClock.getTicks();
        if (countRows) {
            // this is the tail end of the cursor
            // we don't need to read records, just round up record count
            final RecordCursor cursor = this.cursor;
            final long size = cursor.size();
            if (size < 0) {
                LOG.info().$("counting").$();
                long count = 1;
                while (cursor.hasNext()) {
                    count++;
                }
                this.count += count;
            } else {
                this.count = size;
            }
        }
        recordCountNanos = nanosecondClock.getTicks() - nanos;
    }

    private void onQueryMetadata(
            HttpChunkedResponseSocket socket,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryMetadata(socket, columnCount);
        onQueryMetadataSuffix(socket, columnCount);
    }

    private void onQueryMetadataSuffix(
            HttpChunkedResponseSocket socket,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryMetadataSuffix(socket);
        doFirstRecordLoop(socket, columnCount);
    }

    private void onQueryPrefix(HttpChunkedResponseSocket socket, int columnCount) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (doQueryPrefix(socket)) {
            doQueryMetadata(socket, columnCount);
            doQueryMetadataSuffix(socket);
        }
        doFirstRecordLoop(socket, columnCount);
    }

    private void onQueryRecord(HttpChunkedResponseSocket socket, int columnCount) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryRecord(socket, columnCount);
        onQueryRecordSuffix(socket, columnCount);
    }

    private void onQueryRecordPrefix(
            HttpChunkedResponseSocket socket,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryRecordPrefix(socket);
        onQueryRecord(socket, columnCount);
    }

    private void onQueryRecordSuffix(
            HttpChunkedResponseSocket socket,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryRecordSuffix(socket);
        doNextRecordLoop(socket, columnCount);
    }

    private boolean onQuerySetupFirstRecord() {
        if (skip > 0) {
            final RecordCursor cursor = this.cursor;
            long target = skip + 1;
            while (target > 0 && cursor.hasNext()) {
                target--;
            }
            if (target > 0) {
                return false;
            }
            count = skip;
        } else {
            if (!cursor.hasNext()) {
                return false;
            }
        }

        columnIndex = 0;
        record = cursor.getRecord();
        return true;
    }

    private void putBinValue(HttpChunkedResponseSocket socket) {
        socket.put('[');
        socket.put(']');
    }

    private void putDoubleValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getDouble(col), doubleScale);
    }

    private void putFloatValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getFloat(col), floatScale);
    }

    static void prepareExceptionJson(
            HttpChunkedResponseSocket socket,
            int position,
            CharSequence message,
            CharSequence query
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.put('{')
                .putQuoted("query").put(':').encodeUtf8AndQuote(query == null ? "" : query).put(',')
                .putQuoted("error").put(':').encodeUtf8AndQuote(message).put(',')
                .putQuoted("position").put(':').put(position)
                .put('}');
        socket.sendChunk(true);
    }

    boolean noCursor() {
        return cursor == null;
    }

    boolean of(
            RecordCursorFactory factory,
            SqlExecutionContextImpl sqlExecutionContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        return of(factory, true, sqlExecutionContext);
    }

    boolean of(
            RecordCursorFactory factory,
            boolean queryCacheable,
            SqlExecutionContextImpl sqlExecutionContext
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        this.recordCursorFactory = factory;
        this.queryCacheable = queryCacheable;
        this.queryJitCompiled = factory.usesCompiledFilter();
        // Enable column pre-touch in REST API only when LIMIT K,N is not specified since when limit is defined
        // we do a no-op loop over the cursor to calculate the total row count and pre-touch only slows things down.
        sqlExecutionContext.setColumnPreTouchEnabled(stop == Long.MAX_VALUE);
        this.cursor = factory.getCursor(sqlExecutionContext);
        final RecordMetadata metadata = factory.getMetadata();
        this.queryTimestampIndex = metadata.getTimestampIndex();
        HttpRequestHeader header = httpConnectionContext.getRequestHeader();
        DirectByteCharSequence columnNames = header.getUrlParam("cols");
        int columnCount;
        columnSkewList.clear();
        if (columnNames != null) {
            columnsQueryParameter.clear();
            try {
                TextUtil.utf8ToUtf16(columnNames.getLo(), columnNames.getHi(), columnsQueryParameter);
            } catch (Utf8Exception e) {
                info().$("utf8 error when decoding column list '").$(columnNames).$('\'').$();
                HttpChunkedResponseSocket socket = getHttpConnectionContext().getChunkedResponseSocket();
                JsonQueryProcessor.header(socket, "", 400);
                socket.put('{')
                        .putQuoted("error").put(':').putQuoted("utf8 error in column list")
                        .put('}');
                socket.sendChunk(true);
                return false;
            }

            columnCount = 1;
            int start = 0;
            int comma = 0;
            while (comma > -1) {
                comma = Chars.indexOf(columnsQueryParameter, start, ',');
                if (comma > -1) {
                    if (addColumnToOutput(metadata, columnsQueryParameter, start, comma)) {
                        return false;
                    }
                    start = comma + 1;
                    columnCount++;
                } else {
                    int hi = columnsQueryParameter.length();
                    if (addColumnToOutput(metadata, columnsQueryParameter, start, hi)) {
                        return false;
                    }
                }
            }
        } else {
            columnCount = metadata.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                addColumnTypeAndName(metadata, i);
            }
        }
        this.columnCount = columnCount;
        return true;
    }

    void resume(HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        resumeActions.getQuick(queryState).onResume(socket, columnCount);
    }

    void setQueryCacheable(boolean queryCacheable) {
        this.queryCacheable = queryCacheable;
    }

    @FunctionalInterface
    interface StateResumeAction {
        void onResume(
                HttpChunkedResponseSocket socket,
                int columnCount
        ) throws PeerDisconnectedException, PeerIsSlowToReadException;
    }
}
