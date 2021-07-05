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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
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
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class JsonQueryProcessorState implements Mutable, Closeable {
    static final int QUERY_RECORD_PREFIX = 9;
    static final int QUERY_SETUP_FIRST_RECORD = 8;
    static final int QUERY_SUFFIX = 7;
    static final int QUERY_RECORD_SUFFIX = 6;
    static final int QUERY_RECORD = 5;
    static final int QUERY_RECORD_START = 4;
    static final int QUERY_METADATA_SUFFIX = 3;
    static final int QUERY_METADATA = 2;
    static final int QUERY_PREFIX = 1;
    private static final Log LOG = LogFactory.getLog(JsonQueryProcessorState.class);
    private final ObjList<ValueWriter> allValueWriters = new ObjList<>();
    private final StringSink query = new StringSink();
    private final StringSink columnsQueryParameter = new StringSink();
    private final ObjList<StateResumeAction> resumeActions = new ObjList<>();
    private final ObjList<ValueWriter> valueWriters = new ObjList<>();
    private final ArrayColumnTypes columnTypes = new ArrayColumnTypes();
    private final ObjList<String> columnNames = new ObjList<>();
    private final HttpConnectionContext httpConnectionContext;
    private final IntList columnSkewList = new IntList();
    private final ObjList<ValueWriter> skewedValueWriters = new ObjList<>();
    private final NanosecondClock nanosecondClock;
    private final int floatScale;
    private final int doubleScale;
    private Rnd rnd;
    private RecordCursorFactory recordCursorFactory;
    private RecordCursor cursor;
    private boolean noMeta = false;
    private Record record;
    private int queryState = QUERY_PREFIX;
    private int columnIndex;
    private boolean countRows = false;
    private long count;
    private long skip;
    private long stop;
    private int columnCount;
    private long executeStartNanos;
    private long recordCountNanos;
    private long compilerNanos;
    private boolean timings;
    private boolean queryCacheable = false;

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

        skewedValueWriters.extendAndSet(ColumnType.BOOLEAN, this::putSkewedBooleanValue);
        skewedValueWriters.extendAndSet(ColumnType.BYTE, this::putSkewedByteValue);
        skewedValueWriters.extendAndSet(ColumnType.DOUBLE, this::putSkewedDoubleValue);
        skewedValueWriters.extendAndSet(ColumnType.FLOAT, this::putSkewedFloatValue);
        skewedValueWriters.extendAndSet(ColumnType.INT, this::putSkewedIntValue);
        skewedValueWriters.extendAndSet(ColumnType.LONG, this::putSkewedLongValue);
        skewedValueWriters.extendAndSet(ColumnType.DATE, this::putSkewedDateValue);
        skewedValueWriters.extendAndSet(ColumnType.TIMESTAMP, this::putSkewedTimestampValue);
        skewedValueWriters.extendAndSet(ColumnType.SHORT, this::putSkewedShortValue);
        skewedValueWriters.extendAndSet(ColumnType.CHAR, this::putSkewedCharValue);
        skewedValueWriters.extendAndSet(ColumnType.STRING, this::putSkewedStrValue);
        skewedValueWriters.extendAndSet(ColumnType.SYMBOL, this::putSkewedSymValue);
        skewedValueWriters.extendAndSet(ColumnType.BINARY, this::putSkewedBinValue);
        skewedValueWriters.extendAndSet(ColumnType.LONG256, this::putSkewedLong256Value);
        skewedValueWriters.extendAndSet(ColumnType.CURSOR, JsonQueryProcessorState::putCursorValue);

        allValueWriters.extendAndSet(ColumnType.BOOLEAN, JsonQueryProcessorState::putBooleanValue);
        allValueWriters.extendAndSet(ColumnType.BYTE, JsonQueryProcessorState::putByteValue);
        allValueWriters.extendAndSet(ColumnType.DOUBLE, this::putDoubleValue);
        allValueWriters.extendAndSet(ColumnType.FLOAT, this::putFloatValue);
        allValueWriters.extendAndSet(ColumnType.INT, JsonQueryProcessorState::putIntValue);
        allValueWriters.extendAndSet(ColumnType.LONG, JsonQueryProcessorState::putLongValue);
        allValueWriters.extendAndSet(ColumnType.DATE, JsonQueryProcessorState::putDateValue);
        allValueWriters.extendAndSet(ColumnType.TIMESTAMP, JsonQueryProcessorState::putTimestampValue);
        allValueWriters.extendAndSet(ColumnType.SHORT, JsonQueryProcessorState::putShortValue);
        allValueWriters.extendAndSet(ColumnType.CHAR, JsonQueryProcessorState::putCharValue);
        allValueWriters.extendAndSet(ColumnType.STRING, JsonQueryProcessorState::putStrValue);
        allValueWriters.extendAndSet(ColumnType.SYMBOL, JsonQueryProcessorState::putSymValue);
        allValueWriters.extendAndSet(ColumnType.BINARY, JsonQueryProcessorState::putBinValue);
        allValueWriters.extendAndSet(ColumnType.LONG256, JsonQueryProcessorState::putLong256Value);
        allValueWriters.extendAndSet(ColumnType.RECORD, JsonQueryProcessorState::putCursorValue);

        this.nanosecondClock = nanosecondClock;
        this.floatScale = floatScale;
        this.doubleScale = doubleScale;
    }

    @Override
    public void clear() {
        columnCount = 0;
        columnTypes.clear();
        columnNames.clear();
        cursor = Misc.free(cursor);
        record = null;
        if (null != recordCursorFactory) {
            if (queryCacheable) {
                QueryCache.getInstance().push(query, recordCursorFactory);
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
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        recordCursorFactory = Misc.free(recordCursorFactory);
    }

    public void configure(
            HttpRequestHeader request,
            DirectByteCharSequence query,
            long skip,
            long stop
    ) throws Utf8Exception {
        this.query.clear();
        TextUtil.utf8Decode(query.getLo(), query.getHi(), this.query);
        this.skip = skip;
        this.count = 0L;
        this.stop = stop;
        this.noMeta = Chars.equalsNc("true", request.getUrlParam("nm"));
        this.countRows = Chars.equalsNc("true", request.getUrlParam("count"));
        this.timings = Chars.equalsNc("true", request.getUrlParam("timings"));
    }

    public LogRecord error() {
        return LOG.error().$('[').$(getFd()).$("] ");
    }

    public HttpConnectionContext getHttpConnectionContext() {
        return httpConnectionContext;
    }

    public CharSequence getQuery() {
        return query;
    }

    public Rnd getRnd() {
        return rnd;
    }

    public void setRnd(Rnd rnd) {
        this.rnd = rnd;
    }

    public LogRecord info() {
        return LOG.info().$('[').$(getFd()).$("] ");
    }

    public void logBufferTooSmall() {
        info().$("Response buffer is too small, state=").$(queryState).$();
    }

    public void logExecuteCached() {
        info().$("execute-cached ").$("[skip: ").$(skip).$(", stop: ").$(stop).$(']').$();
    }

    public void logExecuteNew() {
        info().$("execute-new ").
                $("[skip: ").$(skip).
                $(", stop: ").$(stop).
                $(']').$();
    }

    public void logSyntaxError(SqlException e) {
        info().$("syntax-error [q=`").utf8(query).$("`, at=").$(e.getPosition()).$(", message=`").utf8(e.getFlyweightMessage()).$('`').$(']').$();
    }

    public void logTimings() {
        info().$("timings ").
                $("[compiler: ").$(compilerNanos).
                $(", count: ").$(recordCountNanos).
                $(", execute: ").$(nanosecondClock.getTicks() - executeStartNanos).
                $(", q=`").$(query).
                $("`]").$();
    }

    public void setCompilerNanos(long compilerNanos) {
        this.compilerNanos = compilerNanos;
    }

    public void startExecutionTimer() {
        this.executeStartNanos = nanosecondClock.getTicks();
    }

    static void prepareExceptionJson(HttpChunkedResponseSocket socket, int position, CharSequence message, CharSequence query) throws PeerDisconnectedException, PeerIsSlowToReadException {
        socket.put('{').
                putQuoted("query").put(':').encodeUtf8AndQuote(query == null ? "" : query).put(',').
                putQuoted("error").put(':').encodeUtf8AndQuote(message).put(',').
                putQuoted("position").put(':').put(position);
        socket.put('}');
        socket.sendChunk(true);
    }

    private static void putStringOrNull(CharSink r, CharSequence str) {
        if (str == null) {
            r.put("null");
        } else {
            r.encodeUtf8AndQuote(str);
        }
    }

    private static void putBinValue(HttpChunkedResponseSocket socket, Record record, int col) {
        socket.put('[');
        socket.put(']');
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

    private static void putLongValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        final long l = rec.getLong(col);
        if (l == Long.MIN_VALUE) {
            socket.put("null");
        } else {
            socket.put(l);
        }
    }

    private static void putShortValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getShort(col));
    }

    private static void putStrValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putStringOrNull(socket, rec.getStr(col));
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

    private static void putCursorValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putStringOrNull(socket, null);
    }

    private boolean addColumnToOutput(RecordMetadata metadata, CharSequence columnNames, int start, int hi) throws PeerDisconnectedException, PeerIsSlowToReadException {

        if (start == hi) {
            info().$("empty column in list '").$(columnNames).$('\'').$();
            HttpChunkedResponseSocket socket = getHttpConnectionContext().getChunkedResponseSocket();
            JsonQueryProcessor.header(socket, "");
            socket.put('{').
                    putQuoted("query").put(':').encodeUtf8AndQuote(query).put(',').
                    putQuoted("error").put(':').putQuoted("empty column in list");
            socket.put('}');
            socket.sendChunk(true);
            return true;
        }

        int columnIndex = metadata.getColumnIndexQuiet(columnNames, start, hi);
        if (columnIndex == RecordMetadata.COLUMN_NOT_FOUND) {
            info().$("invalid column in list: '").$(columnNames, start, hi).$('\'').$();
            HttpChunkedResponseSocket socket = getHttpConnectionContext().getChunkedResponseSocket();
            JsonQueryProcessor.header(socket, "");
            socket.put('{').
                    putQuoted("query").put(':').encodeUtf8AndQuote(query).put(',').
                    putQuoted("error").put(':').put('\'').put("invalid column in list: ").put(columnNames, start, hi).put('\'');
            socket.put('}');
            socket.sendChunk(true);
            return true;
        }

        int columnType = metadata.getColumnType(columnIndex);
        if (columnType == ColumnType.NULL) {
            columnType = ColumnType.STRING;
        }
        this.columnTypes.add(columnType);
        this.columnSkewList.add(columnIndex);
        this.valueWriters.add(skewedValueWriters.getQuick(columnType));
        this.columnNames.add(metadata.getColumnName(columnIndex));
        return false;
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
            socket.put('{').
                    putQuoted("name").put(':').encodeUtf8AndQuote(columnNames.getQuick(columnIndex)).
                    put(',').
                    putQuoted("type").put(':').putQuoted(ColumnType.nameOf(columnTypes.getColumnType(columnIndex)));
            socket.put('}');
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
        socket.put('{').putQuoted("query").put(':').encodeUtf8AndQuote(query);
        socket.put(',').putQuoted("columns").put(':').put('[');
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
            valueWriters.getQuick(columnIndex).write(socket, record, columnIndex);
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
    }

    private void doQueryRecordSuffix(HttpChunkedResponseSocket socket) {
        queryState = QUERY_RECORD_SUFFIX;
        count++;
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
            socket.put(',').putQuoted("count").put(':').put(count);
            if (timings) {
                socket.put(',').putQuoted("timings").put(':').put('{');
                socket.putQuoted("compiler").put(':').put(compilerNanos).put(',');
                socket.putQuoted("execute").put(':').put(nanosecondClock.getTicks() - executeStartNanos).put(',');
                socket.putQuoted("count").put(':').put(recordCountNanos);
                socket.put('}');
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

    private long getFd() {
        return httpConnectionContext.getFd();
    }

    boolean noCursor() {
        return cursor == null;
    }

    boolean of(RecordCursorFactory factory, SqlExecutionContextImpl sqlExecutionContext) throws PeerDisconnectedException, PeerIsSlowToReadException {
        this.recordCursorFactory = factory;
        queryCacheable = true;
        this.cursor = factory.getCursor(sqlExecutionContext);
        final RecordMetadata metadata = factory.getMetadata();
        HttpRequestHeader header = httpConnectionContext.getRequestHeader();
        DirectByteCharSequence columnNames = header.getUrlParam("cols");
        this.valueWriters.clear();
        int columnCount;
        if (columnNames != null) {
            columnsQueryParameter.clear();
            try {
                TextUtil.utf8Decode(columnNames.getLo(), columnNames.getHi(), columnsQueryParameter);
            } catch (Utf8Exception e) {
                info().$("utf8 error when decoding column list '").$(columnNames).$('\'').$();
                HttpChunkedResponseSocket socket = getHttpConnectionContext().getChunkedResponseSocket();
                JsonQueryProcessor.header(socket, "");
                socket.put('{').
                        putQuoted("error").put(':').putQuoted("utf8 error in column list");
                socket.put('}');
                socket.sendChunk(true);
                return false;
            }

            this.columnSkewList.clear();
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
                int columnType = metadata.getColumnType(i);
                if (columnType == ColumnType.NULL) {
                    columnType = ColumnType.STRING;
                }
                this.columnTypes.add(columnType);
                this.valueWriters.add(allValueWriters.getQuick(columnType));
                this.columnNames.add(metadata.getColumnName(i));
            }
        }
        this.columnCount = columnCount;
        return true;
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

    private void putDoubleValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getDouble(col), doubleScale);
    }

    private void putFloatValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        socket.put(rec.getFloat(col), floatScale);
    }

    private void putSkewedBinValue(HttpChunkedResponseSocket socket, Record record, int col) {
        putBinValue(socket, record, columnSkewList.getQuick(col));
    }

    private void putSkewedBooleanValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putBooleanValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedByteValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putByteValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedCharValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putCharValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedDateValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putDateValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedDoubleValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putDoubleValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedFloatValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putFloatValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedIntValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putIntValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedLong256Value(HttpChunkedResponseSocket socket, Record rec, int col) {
        putLong256Value(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedLongValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putLongValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedShortValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putShortValue(socket, rec, col);
    }

    private void putSkewedStrValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putStrValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedSymValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putSymValue(socket, rec, columnSkewList.getQuick(col));
    }

    private void putSkewedTimestampValue(HttpChunkedResponseSocket socket, Record rec, int col) {
        putTimestampValue(socket, rec, columnSkewList.getQuick(col));
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
