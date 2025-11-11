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

package io.questdb.cutlass.http.processors;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.http.HttpResponseArrayWriteState;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.SCSequence;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;

public class JsonQueryProcessorState implements Mutable, Closeable {
    public static final String HIDDEN = "hidden";
    static final int QUERY_SETUP_FIRST_RECORD = 0; // 0
    static final int QUERY_PREFIX = QUERY_SETUP_FIRST_RECORD + 1; // 1
    static final int QUERY_METADATA = QUERY_PREFIX + 1; //2
    static final int QUERY_METADATA_SUFFIX = QUERY_METADATA + 1; // 3
    static final int QUERY_RECORD_START = QUERY_METADATA_SUFFIX + 1; // 4
    static final int QUERY_RECORD = QUERY_RECORD_START + 1; // 5
    static final int QUERY_RECORD_SUFFIX = QUERY_RECORD + 1; // 6
    static final int QUERY_SUFFIX = QUERY_RECORD_SUFFIX + 1; // 7
    static final int QUERY_SEND_RECORDS_LOOP = QUERY_SUFFIX + 1; // 8
    static final int QUERY_RECORD_PREFIX = QUERY_SEND_RECORDS_LOOP + 1; // 9

    // only used in Parquet export
    static final int QUERY_PARQUET_EXPORT_INIT = QUERY_RECORD_PREFIX + 1; // 10
    static final int QUERY_PARQUET_EXPORT_WAIT = QUERY_PARQUET_EXPORT_INIT + 1; // 11
    static final int QUERY_PARQUET_SEND_HEADER = QUERY_PARQUET_EXPORT_WAIT + 1; // 12
    static final int QUERY_PARQUET_TO_PARQUET_FILE = QUERY_PARQUET_SEND_HEADER + 1; // 14
    static final int QUERY_PARQUET_FILE_SEND_INIT = QUERY_PARQUET_TO_PARQUET_FILE + 1; // 13
    static final int QUERY_PARQUET_FILE_SEND_CHUNK = QUERY_PARQUET_FILE_SEND_INIT + 1; // 15
    static final int QUERY_PARQUET_FILE_SEND_COMPLETE = QUERY_PARQUET_FILE_SEND_CHUNK + 1; // 15

    private static final byte DEFAULT_API_VERSION = 1;
    private static final Log LOG = LogFactory.getLog(JsonQueryProcessorState.class);
    private final HttpResponseArrayWriteState arrayState = new HttpResponseArrayWriteState();
    private final ObjList<String> columnNames = new ObjList<>();
    private final IntList columnSkewList = new IntList();
    private final IntList columnTypesAndFlags = new IntList();
    private final RecordCursor.Counter counter = new RecordCursor.Counter();
    private final SCSequence eventSubSequence = new SCSequence();
    private final HttpConnectionContext httpConnectionContext;
    private final CharSequence keepAliveHeader;
    private final Clock nanosecondClock;
    private final StringSink query = new StringSink();
    private final ObjList<StateResumeAction> resumeActions = new ObjList<>();
    private final StringSink stringSink = new StringSink();
    private final long statementTimeout;
    private byte apiVersion = DEFAULT_API_VERSION;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private int columnCount;
    private int columnIndex;
    // indicates to the state machine that column value was fully sent to
    // the client, as opposed to being partially send
    private boolean columnValueFullySent;
    private long compilerNanos;
    private boolean containsSecret;
    private long count;
    private boolean countRows = false;
    private RecordCursor cursor;
    private boolean cursorHasRows;
    private long executeStartNanos;
    private boolean explain = false;
    private boolean noMeta = false;
    // Operation is stored here to be retried
    private Operation operation;
    private OperationFuture operationFuture;
    private boolean pausedQuery = false;
    private boolean queryCacheable = false;
    private boolean queryJitCompiled = false;
    private int queryState = QUERY_SETUP_FIRST_RECORD;
    private int queryTimestampIndex;
    private short queryType;
    private boolean quoteLargeNum = false;
    private Record record;
    private long recordCountNanos;
    private RecordCursorFactory recordCursorFactory;
    private Rnd rnd;
    private long skip;
    private SqlExecutionContext sqlExecutionContext;
    private long stop;
    private boolean timings = false;

    public JsonQueryProcessorState(
            HttpConnectionContext httpConnectionContext,
            Clock nanosecondClock,
            CharSequence keepAliveHeader
    ) {
        this.httpConnectionContext = httpConnectionContext;
        resumeActions.extendAndSet(QUERY_SETUP_FIRST_RECORD, this::onSetupFirstRecord);
        resumeActions.extendAndSet(QUERY_PREFIX, this::onQueryPrefix);
        resumeActions.extendAndSet(QUERY_METADATA, this::onQueryMetadata);
        resumeActions.extendAndSet(QUERY_METADATA_SUFFIX, this::onQueryMetadataSuffix);
        resumeActions.extendAndSet(QUERY_SEND_RECORDS_LOOP, this::onSendRecordsLoop);
        resumeActions.extendAndSet(QUERY_RECORD_PREFIX, this::onQueryRecordPrefix);
        resumeActions.extendAndSet(QUERY_RECORD, this::onQueryRecord);
        resumeActions.extendAndSet(QUERY_RECORD_SUFFIX, this::onQueryRecordSuffix);
        resumeActions.extendAndSet(QUERY_SUFFIX, this::doQuerySuffix);
        this.nanosecondClock = nanosecondClock;
        this.statementTimeout = httpConnectionContext.getRequestHeader().getStatementTimeout();
        this.keepAliveHeader = keepAliveHeader;
    }

    @Override
    public void clear() {
        apiVersion = DEFAULT_API_VERSION;
        columnCount = 0;
        columnSkewList.clear();
        columnTypesAndFlags.clear();
        columnNames.clear();
        queryTimestampIndex = -1;
        cursor = Misc.free(cursor);
        circuitBreaker = null;
        sqlExecutionContext = null;
        record = null;
        if (recordCursorFactory != null) {
            if (queryCacheable) {
                httpConnectionContext.getSelectCache().put(query, recordCursorFactory);
            } else {
                recordCursorFactory.close();
            }
            recordCursorFactory = null;
        }
        query.clear();
        stringSink.clear();
        queryState = QUERY_SETUP_FIRST_RECORD;
        columnIndex = 0;
        columnValueFullySent = true;
        arrayState.clear();
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
        counter.clear();
        stop = 0;
        containsSecret = false;
    }

    public void clearFactory() {
        columnSkewList.clear();
        columnTypesAndFlags.clear();
        recordCursorFactory = Misc.free(recordCursorFactory);
    }

    @Override
    public void close() {
        cursor = Misc.free(cursor);
        clearFactory();
        circuitBreaker = null;
        sqlExecutionContext = null;
        freeAsyncOperation();
    }

    public void configure(
            HttpRequestHeader request,
            DirectUtf8Sequence query,
            long skip,
            long stop
    ) throws Utf8Exception {
        this.query.clear();
        if (query != null) {
            if (!Utf8s.utf8ToUtf16(query.lo(), query.hi(), this.query)) {
                throw Utf8Exception.INSTANCE;
            }
        }
        this.skip = skip;
        this.stop = stop;
        count = 0L;
        counter.clear();
        noMeta = HttpKeywords.isTrue(request.getUrlParam(URL_PARAM_NM));
        countRows = HttpKeywords.isTrue(request.getUrlParam(URL_PARAM_COUNT));
        timings = HttpKeywords.isTrue(request.getUrlParam(URL_PARAM_TIMINGS));
        explain = HttpKeywords.isTrue(request.getUrlParam(URL_PARAM_EXPLAIN));
        quoteLargeNum = HttpKeywords.isTrue(request.getUrlParam(URL_PARAM_QUOTE_LARGE_NUM))
                || HttpKeywords.isCon(request.getUrlParam(URL_PARAM_SRC));
        apiVersion = parseApiVersion(request);
    }

    public LogRecord critical() {
        return LOG.critical().$('[').$(getFd()).$("] ");
    }

    public LogRecord error() {
        return LOG.error().$('[').$(getFd()).$("] ");
    }

    public void freeAsyncOperation() {
        operationFuture = Misc.free(operationFuture);
        operation = Misc.free(operation);
    }

    public byte getApiVersion() {
        return apiVersion;
    }

    public int getCurrentColumnIndex() {
        return columnIndex;
    }

    public String getCurrentColumnName() {
        if (columnIndex > -1 && columnIndex < columnNames.size()) {
            return columnNames.getQuick(columnIndex);
        }
        return "undefined";
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
        info().$("response buffer is too small, state=").$(queryState).$();
    }

    public void logTimings() {
        info().$("timings ")
                .$("[compiler: ").$(compilerNanos)
                .$(", count: ").$(recordCountNanos)
                .$(", execute: ").$(nanosecondClock.getTicks() - executeStartNanos)
                .$(", q=`").$safe(getQueryOrHidden())
                .$("`]").$();
    }

    public void setCompilerNanos(long compilerNanos) {
        this.compilerNanos = compilerNanos;
    }

    public void setContainsSecret(boolean containsSecret) {
        this.containsSecret = containsSecret;
    }

    public void setCursor(RecordCursor cursor) {
        this.cursor = cursor;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
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

    private static byte parseApiVersion(HttpRequestHeader header) {
        DirectUtf8Sequence versionStr = header.getUrlParam(URL_PARAM_VERSION);
        if (versionStr == null) {
            return DEFAULT_API_VERSION;
        } else {
            try {
                return (byte) Numbers.parseInt(versionStr);
            } catch (NumericException e) {
                return DEFAULT_API_VERSION;
            }
        }
    }

    private static void putBooleanValue(HttpChunkedResponse response, Record rec, int col) {
        response.put(rec.getBool(col));
    }

    private static void putByteValue(HttpChunkedResponse response, Record rec, int col) {
        response.put((int) rec.getByte(col));
    }

    private static void putCharValue(HttpChunkedResponse response, Record rec, int col) {
        char c = rec.getChar(col);
        if (c == 0) {
            response.putAscii("\"\"");
        } else {
            response.putAscii('"').put(c).putAscii('"');
        }
    }

    private static void putDateValue(HttpChunkedResponse response, Record rec, int col) {
        final long d = rec.getDate(col);
        if (d == Long.MIN_VALUE) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"').putISODateMillis(d).putAscii('"');
    }

    private static void putDecimal16Value(HttpChunkedResponse response, Record rec, int col, int type) {
        short l = rec.getDecimal16(col);
        if (l == Decimals.DECIMAL16_NULL) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"');
        Decimals.append(l, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), response);
        response.putAscii('"');
    }

    private static void putDecimal32Value(HttpChunkedResponse response, Record rec, int col, int type) {
        int l = rec.getDecimal32(col);
        if (l == Decimals.DECIMAL32_NULL) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"');
        Decimals.append(l, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), response);
        response.putAscii('"');
    }

    private static void putDecimal64Value(HttpChunkedResponse response, Record rec, int col, int type) {
        long l = rec.getDecimal64(col);
        if (l == Decimals.DECIMAL64_NULL) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"');
        Decimals.append(l, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), response);
        response.putAscii('"');
    }

    private static void putDecimal8Value(HttpChunkedResponse response, Record rec, int col, int type) {
        byte l = rec.getDecimal8(col);
        if (l == Decimals.DECIMAL8_NULL) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"');
        Decimals.append(l, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), response);
        response.putAscii('"');
    }

    private static void putGeoHashStringByteValue(HttpChunkedResponse response, Record rec, int col, int bitFlags) {
        byte l = rec.getGeoByte(col);
        GeoHashes.append(l, bitFlags, response);
    }

    private static void putGeoHashStringIntValue(HttpChunkedResponse response, Record rec, int col, int bitFlags) {
        int l = rec.getGeoInt(col);
        GeoHashes.append(l, bitFlags, response);
    }

    private static void putGeoHashStringLongValue(HttpChunkedResponse response, Record rec, int col, int bitFlags) {
        long l = rec.getGeoLong(col);
        GeoHashes.append(l, bitFlags, response);
    }

    private static void putGeoHashStringShortValue(HttpChunkedResponse response, Record rec, int col, int bitFlags) {
        short l = rec.getGeoShort(col);
        GeoHashes.append(l, bitFlags, response);
    }

    private static void putIPv4Value(HttpChunkedResponse response, Record rec, int col) {
        final int i = rec.getIPv4(col);
        if (i == Numbers.IPv4_NULL) {
            response.putAscii("null");
        } else {
            response.putAscii('"');
            Numbers.intToIPv4Sink(response, i);
            response.putAscii('"');
        }
    }

    private static void putIntValue(HttpChunkedResponse response, Record rec, int col) {
        final int i = rec.getInt(col);
        if (i == Integer.MIN_VALUE) {
            response.putAscii("null");
        } else {
            response.put(i);
        }
    }

    private static void putIntervalValue(HttpChunkedResponse response, Record rec, int col, int intervalType) {
        final Interval interval = rec.getInterval(col);
        if (Interval.NULL.equals(interval)) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"').put(interval, intervalType).putAscii('"');
    }

    private static void putLong256Value(HttpChunkedResponse response, Record rec, int col) {
        response.putAscii('"');
        rec.getLong256(col, response);
        response.putAscii('"');
    }

    private static void putLongValue(HttpChunkedResponse response, Record rec, int col, boolean quoteLargeNum) {
        final long l = rec.getLong(col);
        if (l == Long.MIN_VALUE) {
            response.putAscii("null");
        } else if (quoteLargeNum) {
            response.putAscii('"').put(l).putAscii('"');
        } else {
            response.put(l);
        }
    }

    private static void putRecValue(HttpChunkedResponse response) {
        putStringOrNull(response, null);
    }

    private static void putShortValue(HttpChunkedResponse response, Record rec, int col) {
        response.put(rec.getShort(col));
    }

    private static void putStrValue(HttpChunkedResponse response, Record rec, int col) {
        putStringOrNull(response, rec.getStrA(col));
    }

    private static void putStringOrNull(HttpChunkedResponse response, CharSequence str) {
        if (str == null) {
            response.putAscii("null");
        } else {
            response.putQuote().escapeJsonStr(str).putQuote();
        }
    }

    private static void putSymValue(HttpChunkedResponse response, Record rec, int col) {
        putStringOrNull(response, rec.getSymA(col));
    }

    private static void putTimestampValue(HttpChunkedResponse response, Record rec, int col, TimestampDriver driver) {
        final long t = rec.getTimestamp(col);
        if (t == Long.MIN_VALUE) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"').putISODate(driver, t).putAscii('"');
    }

    private static void putUuidValue(HttpChunkedResponse response, Record rec, int col) {
        long lo = rec.getLong128Lo(col);
        long hi = rec.getLong128Hi(col);
        if (Uuid.isNull(lo, hi)) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"');
        Numbers.appendUuid(lo, hi, response);
        response.putAscii('"');
    }

    private void addColumnTypeAndName(RecordMetadata metadata, int i) {
        int columnType = metadata.getColumnType(i);
        String columnName = metadata.getColumnName(i);

        switch (ColumnType.tagOf(columnType)) {
            // list of explicitly supported types, to be keep in sync with doQueryRecord()

            // we use a while-list since if we add a new type to QuestDB
            // the support has to be explicitly added to the JSON REST API
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
            case ColumnType.DOUBLE:
            case ColumnType.FLOAT:
            case ColumnType.INT:
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
            case ColumnType.SHORT:
            case ColumnType.CHAR:
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
            case ColumnType.SYMBOL:
            case ColumnType.BINARY:
            case ColumnType.LONG256:
            case ColumnType.GEOBYTE:
            case ColumnType.GEOSHORT:
            case ColumnType.GEOINT:
            case ColumnType.GEOLONG:
            case ColumnType.RECORD:
            case ColumnType.NULL:
            case ColumnType.UUID:
            case ColumnType.IPv4:
            case ColumnType.INTERVAL:
            case ColumnType.ARRAY:
            case ColumnType.DECIMAL8:
            case ColumnType.DECIMAL16:
            case ColumnType.DECIMAL32:
            case ColumnType.DECIMAL64:
            case ColumnType.DECIMAL128:
            case ColumnType.DECIMAL256:
                break;
            default:
                throw CairoException.nonCritical().put("column type not supported [column=").put(columnName).put(", type=").put(ColumnType.nameOf(columnType)).put(']');
        }

        int flags = GeoHashes.getBitFlags(columnType);
        this.columnTypesAndFlags.add(columnType);
        this.columnTypesAndFlags.add(flags);
        this.columnNames.add(columnName);
    }

    private boolean addSunkColumnToOutput(RecordMetadata metadata) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int columnIndex = metadata.getColumnIndexQuiet(stringSink);
        if (columnIndex == RecordMetadata.COLUMN_NOT_FOUND) {
            info().$("column not found: '").$safe(stringSink).$('\'').$();
            HttpChunkedResponse response = getHttpConnectionContext().getChunkedResponse();
            JsonQueryProcessor.header(response, getHttpConnectionContext(), "", 400);
            response.putAscii('{')
                    .putAsciiQuoted("query").putAscii(':').putQuote().escapeJsonStr(query).putQuote().putAscii(',')
                    .putAsciiQuoted("error").putAscii(':').putAscii('\"').putAscii("column not found: '").escapeJsonStr(stringSink).putAscii("'\"")
                    .putAscii('}');
            response.sendChunk(true);
            return true;
        }

        addColumnTypeAndName(metadata, columnIndex);
        this.columnSkewList.add(columnIndex);
        return false;
    }

    private void doNextRecordLoop(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (doQueryNextRecord()) {
            doRecordFetchLoop(response, columnCount);
        } else {
            doQuerySuffix(response, columnCount);
        }
    }

    private void doQueryMetadata(HttpChunkedResponse response, int columnCount) {
        queryState = QUERY_METADATA;
        for (; columnIndex < columnCount; columnIndex++) {
            response.bookmark();
            if (columnIndex > 0) {
                response.putAscii(',');
            }
            int columnType = columnTypesAndFlags.getQuick(2 * columnIndex);
            response.putAscii('{')
                    .putAsciiQuoted("name").putAscii(':').putQuote().escapeJsonStr(columnNames.getQuick(columnIndex)).putQuote().putAscii(',');
            if (ColumnType.tagOf(columnType) == ColumnType.ARRAY) {
                response.putAsciiQuoted("type").putAscii(':').putAsciiQuoted("ARRAY").put(',');
                response.putAsciiQuoted("dim").putAscii(':').put(ColumnType.decodeWeakArrayDimensionality(columnType)).put(',');
                response.putAsciiQuoted("elemType").putAscii(':').putAsciiQuoted(ColumnType.nameOf(ColumnType.decodeArrayElementType(columnType)));
            } else {
                response.putAsciiQuoted("type").putAscii(':').putAsciiQuoted(ColumnType.nameOf(columnType == ColumnType.NULL ? ColumnType.STRING : columnType));
            }
            response.putAscii('}');
        }
    }

    private void doQueryMetadataSuffix(HttpChunkedResponse response) {
        queryState = QUERY_METADATA_SUFFIX;
        response.bookmark();
        response.putAscii("],\"timestamp\":").put(queryTimestampIndex);
        response.putAscii(",\"dataset\":[");
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

    private boolean doQueryPrefix(HttpChunkedResponse response) {
        if (noMeta) {
            response.bookmark();
            response.putAscii('{').putAsciiQuoted("dataset").putAscii(":[");
            return false;
        }
        response.bookmark();
        response.putAscii('{')
                .putAsciiQuoted("query").putAscii(':').putQuote().escapeJsonStr(query).putQuote().putAscii(',')
                .putAsciiQuoted("columns").putAscii(':').putAscii('[');
        columnIndex = 0;
        columnValueFullySent = true;
        return true;
    }

    private void doQueryRecord(HttpChunkedResponse response, int columnCount) {
        queryState = QUERY_RECORD;
        for (; columnIndex < columnCount; columnIndex++) {
            response.bookmark();
            if (columnIndex > 0 && columnValueFullySent) {
                response.putAscii(',');
            }

            int columnIdx = columnSkewList.size() > 0 ? columnSkewList.getQuick(columnIndex) : columnIndex;
            int columnType = columnTypesAndFlags.getQuick(2 * columnIndex);
            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.BOOLEAN:
                    putBooleanValue(response, record, columnIdx);
                    break;
                case ColumnType.BYTE:
                    putByteValue(response, record, columnIdx);
                    break;
                case ColumnType.DOUBLE:
                    putDoubleValue(response, record, columnIdx);
                    break;
                case ColumnType.FLOAT:
                    putFloatValue(response, record, columnIdx);
                    break;
                case ColumnType.INT:
                    putIntValue(response, record, columnIdx);
                    break;
                case ColumnType.LONG:
                    putLongValue(response, record, columnIdx, quoteLargeNum);
                    break;
                case ColumnType.DATE:
                    putDateValue(response, record, columnIdx);
                    break;
                case ColumnType.TIMESTAMP:
                    putTimestampValue(response, record, columnIdx, ColumnType.getTimestampDriver(columnType));
                    break;
                case ColumnType.SHORT:
                    putShortValue(response, record, columnIdx);
                    break;
                case ColumnType.CHAR:
                    putCharValue(response, record, columnIdx);
                    break;
                case ColumnType.STRING:
                    putStrValue(response, record, columnIdx);
                    break;
                case ColumnType.VARCHAR:
                    putVarcharValue(response, columnIdx);
                    break;
                case ColumnType.SYMBOL:
                    putSymValue(response, record, columnIdx);
                    break;
                case ColumnType.BINARY:
                    putBinValue(response);
                    break;
                case ColumnType.LONG256:
                    putLong256Value(response, record, columnIdx);
                    break;
                case ColumnType.GEOBYTE:
                    putGeoHashStringByteValue(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex + 1));
                    break;
                case ColumnType.GEOSHORT:
                    putGeoHashStringShortValue(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex + 1));
                    break;
                case ColumnType.GEOINT:
                    putGeoHashStringIntValue(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex + 1));
                    break;
                case ColumnType.GEOLONG:
                    putGeoHashStringLongValue(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex + 1));
                    break;
                case ColumnType.RECORD:
                    putRecValue(response);
                    break;
                case ColumnType.NULL:
                    response.putAscii("null");
                    break;
                case ColumnType.UUID:
                    putUuidValue(response, record, columnIdx);
                    break;
                case ColumnType.IPv4:
                    putIPv4Value(response, record, columnIdx);
                    break;
                case ColumnType.INTERVAL:
                    putIntervalValue(response, record, columnIdx, columnType);
                    break;
                case ColumnType.ARRAY:
                    putArrayValue(response, columnIdx, columnType);
                    break;
                case ColumnType.DECIMAL8:
                    putDecimal8Value(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex));
                    break;
                case ColumnType.DECIMAL16:
                    putDecimal16Value(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex));
                    break;
                case ColumnType.DECIMAL32:
                    putDecimal32Value(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex));
                    break;
                case ColumnType.DECIMAL64:
                    putDecimal64Value(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex));
                    break;
                case ColumnType.DECIMAL128:
                    putDecimal128Value(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex));
                    break;
                case ColumnType.DECIMAL256:
                    putDecimal256Value(response, record, columnIdx, columnTypesAndFlags.getQuick(2 * columnIndex));
                    break;
                default:
                    // this should never happen since metadata are already validated
                    throw CairoException.nonCritical().put("column type not supported [type=").put(ColumnType.nameOf(columnType)).put(']');
            }
        }
    }

    private void doQueryRecordPrefix(HttpChunkedResponse response) {
        queryState = QUERY_RECORD_PREFIX;
        response.bookmark();
        if (count > skip) {
            response.putAscii(',');
        }
        response.putAscii('[');
        columnIndex = 0;
        count++;
        counter.inc();
    }

    private void doQueryRecordSuffix(HttpChunkedResponse response) {
        queryState = QUERY_RECORD_SUFFIX;
        response.bookmark();
        response.putAscii(']');
    }

    private void doQuerySuffix(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        querySuffixWithError(response, 0, null, 0);
    }

    private void doRecordFetchLoop(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        try {
            do {
                doQueryRecordPrefix(response);
                doQueryRecord(response, columnCount);
                doQueryRecordSuffix(response);
            } while (doQueryNextRecord());
        } catch (DataUnavailableException | EntryUnavailableException | NoSpaceLeftInResponseBufferException e) {
            throw e;
        } catch (Throwable e) {
            response.resetToBookmark();
            throw e;
        }
        doQuerySuffix(response, columnCount);
    }

    private long getFd() {
        return httpConnectionContext.getFd();
    }

    private void onNoMoreData() {
        long nanos = nanosecondClock.getTicks();
        if (countRows) {
            // this is the tail end of the cursor
            // we don't need to read records, just round up record count
            final RecordCursor cursor = this.cursor;
            long size = cursor.size();
            counter.clear();
            if (size < 0) {
                try {
                    cursor.calculateSize(circuitBreaker, counter);
                    this.count += counter.get() + 1;
                } catch (DataUnavailableException e) {
                    this.count += counter.get();
                    throw e;
                }
            } else {
                this.count = size;
            }
        }
        recordCountNanos = nanosecondClock.getTicks() - nanos;
    }

    private void onQueryMetadata(HttpChunkedResponse response, int columnCount) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryMetadata(response, columnCount);
        onQueryMetadataSuffix(response, columnCount);
    }

    private void onQueryMetadataSuffix(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryMetadataSuffix(response);
        onSendRecordsLoop(response, columnCount);
    }

    private void onQueryPrefix(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (doQueryPrefix(response)) {
            doQueryMetadata(response, columnCount);
            doQueryMetadataSuffix(response);
        }
        onSendRecordsLoop(response, columnCount);
    }

    private void onQueryRecord(HttpChunkedResponse response, int columnCount)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryRecord(response, columnCount);
        onQueryRecordSuffix(response, columnCount);
    }

    private void onQueryRecordPrefix(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryRecordPrefix(response);
        onQueryRecord(response, columnCount);
    }

    private void onQueryRecordSuffix(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryRecordSuffix(response);
        doNextRecordLoop(response, columnCount);
    }

    private void onSendRecordsLoop(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (cursorHasRows) {
            doRecordFetchLoop(response, columnCount);
        } else {
            doQuerySuffix(response, columnCount);
        }
    }

    private void onSetupFirstRecord(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerIsSlowToReadException, PeerDisconnectedException {
        // If there is an exception in the first record setup then upper layers will handle it:
        // Either they will send error or pause execution on DataUnavailableException
        setupFirstRecord();
        // If we make it past setup then we optimistically send HTTP 200 header.
        // There is still a risk of exception while iterating over cursor, but there is not much we can do about it.
        // Trying to access the first record before sending HTTP headers will already catch many errors.
        // So we can send an appropriate HTTP error code. If there is an error past the first record then
        // we have no choice but to disconnect :( - because we have already sent HTTP 200 header.

        // We assume HTTP headers will always fit into our buffer => a state transition to the QUERY_PREFIX
        // before actually sending HTTP headers. Otherwise, we would have to make JsonQueryProcessor.header() idempotent
        queryState = QUERY_PREFIX;
        JsonQueryProcessor.header(response, getHttpConnectionContext(), keepAliveHeader, 200);
        onQueryPrefix(response, columnCount);
    }

    // parses comma-separated column names, handling double quotes and escape char while converting from utf8 to utf16 encoding.
    private long parseNextColumnName(long rawLo, long rawHi) {
        boolean quoted = false;
        boolean escaped = false;
        while (rawLo < rawHi) {
            byte b = Unsafe.getUnsafe().getByte(rawLo);
            if (b < 0) {
                int n = Utf8s.utf8DecodeMultiByte(rawLo, rawHi, b, stringSink);
                if (n == -1) {
                    // Invalid code point
                    return 0;
                }
                escaped = false;
                rawLo += n;
            } else {
                rawLo++;
                if (escaped) {
                    escaped = false;
                    stringSink.put((char) b);
                    continue;
                }

                if (b == '\\') {
                    escaped = true;
                } else if (b == '"') {
                    quoted = !quoted;
                } else if (!quoted && b == ',') {
                    return rawLo;
                } else {
                    stringSink.put((char) b);
                }
            }
        }
        return rawLo;
    }

    private void putArrayValue(HttpChunkedResponse response, int columnIdx, int columnType) {
        arrayState.of(response);
        var arrayView = arrayState.getArrayView() == null ? record.getArray(columnIdx, columnType) : arrayState.getArrayView();
        try {
            ArrayTypeDriver.arrayToJson(arrayView, response, arrayState);
            arrayState.clear();
            columnValueFullySent = true;
        } catch (Throwable e) {
            // we have to disambiguate here if this is the first attempt to send the value, which failed,
            // and we have any partial value we can send to the clint, or our state did not bookmark anything?
            columnValueFullySent = arrayState.isNothingWritten();
            arrayState.reset(arrayView);
            throw e;
        }
    }

    private void putBinValue(HttpChunkedResponse response) {
        response.putAscii('[');
        response.putAscii(']');
    }

    private void putDecimal128Value(HttpChunkedResponse response, Record rec, int col, int type) {
        var decimal128 = sqlExecutionContext.getDecimal128();
        rec.getDecimal128(col, decimal128);
        if (decimal128.isNull()) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"');
        Decimals.appendNonNull(decimal128, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), response);
        response.putAscii('"');
    }

    private void putDecimal256Value(HttpChunkedResponse response, Record rec, int col, int type) {
        var decimal256 = sqlExecutionContext.getDecimal256();
        rec.getDecimal256(col, decimal256);
        if (decimal256.isNull()) {
            response.putAscii("null");
            return;
        }
        response.putAscii('"');
        Decimals.appendNonNull(decimal256, ColumnType.getDecimalPrecision(type), ColumnType.getDecimalScale(type), response);
        response.putAscii('"');
    }

    private void putDoubleValue(HttpChunkedResponse response, Record rec, int col) {
        response.put(rec.getDouble(col));
    }

    private void putFloatValue(HttpChunkedResponse response, Record rec, int col) {
        response.put(rec.getFloat(col));
    }

    private void putVarcharValue(HttpChunkedResponse response, int columnIdx) {
        Utf8Sequence str = record.getVarcharA(columnIdx);
        if (str == null) {
            response.putAscii("null");
        } else {
            response.putQuote().escapeJsonStr(str).putQuote();
        }
    }

    private void setupFirstRecord() {
        if (skip > 0) {
            final RecordCursor cursor = this.cursor;
            long target = skip + 1;
            while (target > 0 && cursor.hasNext()) {
                target--;
            }
            if (target > 0) {
                cursorHasRows = false;
                return;
            }
            count = skip;
            counter.set(skip);
        } else {
            if (!cursor.hasNext()) {
                cursorHasRows = false;
                return;
            }
        }

        columnIndex = 0;
        columnValueFullySent = true;
        record = cursor.getRecord();
        cursorHasRows = true;
    }

    static void prepareExceptionJson(
            HttpChunkedResponse response,
            int position,
            CharSequence message,
            CharSequence query
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.putAscii('{')
                .putAsciiQuoted("query").putAscii(':').putQuote().escapeJsonStr(query != null ? query : "").putQuote().putAscii(',')
                .putAsciiQuoted("error").putAscii(':').putQuote().escapeJsonStr(message != null ? message : "").putQuote().putAscii(',')
                .putAsciiQuoted("position").putAscii(':').put(position)
                .putAscii('}');
        response.sendChunk(true);
    }

    boolean of(RecordCursorFactory factory, boolean queryCacheable, SqlExecutionContextImpl sqlExecutionContext)
            throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        this.recordCursorFactory = factory;
        this.queryCacheable = queryCacheable;
        this.queryJitCompiled = factory.usesCompiledFilter();
        this.circuitBreaker = sqlExecutionContext.getCircuitBreaker();
        this.sqlExecutionContext = sqlExecutionContext;
        final RecordMetadata metadata = factory.getMetadata();
        this.queryTimestampIndex = metadata.getTimestampIndex();
        HttpRequestHeader header = httpConnectionContext.getRequestHeader();
        DirectUtf8Sequence columnNames = header.getUrlParam(URL_PARAM_COLS);

        int columnCount;
        this.columnNames.clear();
        columnSkewList.clear();
        this.columnTypesAndFlags.clear();
        if (columnNames != null) {
            columnCount = 0;
            long rawLo = columnNames.lo();
            final long rawHi = columnNames.hi();
            while (rawLo < rawHi) {
                stringSink.clear();
                rawLo = parseNextColumnName(rawLo, rawHi);
                if (rawLo <= 0) {
                    info().$("utf8 error when decoding column list '").$safe(columnNames).$('\'').$();
                    HttpChunkedResponse response = getHttpConnectionContext().getChunkedResponse();
                    JsonQueryProcessor.header(response, getHttpConnectionContext(), "", 400);
                    response.putAscii('{')
                            .putAsciiQuoted("error").putAscii(':').putAsciiQuoted("utf8 error in column list")
                            .putAscii('}');
                    response.sendChunk(true);
                    return false;
                }

                if (stringSink.isEmpty()) {
                    info().$("empty column in query parameter '").$(URL_PARAM_COLS).$(": ").$safe(columnNames).$('\'').$();
                    HttpChunkedResponse response = getHttpConnectionContext().getChunkedResponse();
                    JsonQueryProcessor.header(response, getHttpConnectionContext(), "", 400);
                    response.putAscii('{')
                            .putAsciiQuoted("query").putAscii(':').putQuote().escapeJsonStr(query).putQuote().putAscii(',')
                            .putAsciiQuoted("error").putAscii(':').putAsciiQuoted("empty column in query parameter")
                            .putAscii('}');
                    response.sendChunk(true);
                    return false;
                }

                if (addSunkColumnToOutput(metadata)) {
                    return false;
                }
                columnCount++;
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

    void querySuffixWithError(
            HttpChunkedResponse response,
            int code,
            @Nullable CharSequence message,
            int messagePosition
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        // we no longer need cursor when we reached query suffix
        // closing cursor here guarantees that by the time http client finished reading response the table
        // is released
        cursor = Misc.free(cursor);
        circuitBreaker = null;
        queryState = QUERY_SUFFIX;
        if (count > -1) {
            logTimings();
            response.bookmark();
            if (code > 0) {
                // closing the failed record to make the JSON response parsable
                response.putAscii(']');
            }
            // always close the dataset
            response.putAscii(']');
            response.putAscii(',').putAsciiQuoted("count").putAscii(':').put(count);
            if (code > 0) {
                response.putAscii(',')
                        .putAsciiQuoted("error").putAscii(':')
                        .putQuote().escapeJsonStr(message != null ? message : "Internal server error").putQuote()
                        .putAscii(", \"errorPos\"").putAscii(':').put(messagePosition);
            }
            if (timings) {
                response.putAscii(',').putAsciiQuoted("timings").putAscii(':')
                        .putAscii('{')
                        .putAsciiQuoted("authentication").putAscii(':').put(httpConnectionContext.getAuthenticationNanos()).putAscii(',')
                        .putAsciiQuoted("compiler").putAscii(':').put(compilerNanos).putAscii(',')
                        .putAsciiQuoted("execute").putAscii(':').put(nanosecondClock.getTicks() - executeStartNanos).putAscii(',')
                        .putAsciiQuoted("count").putAscii(':').put(recordCountNanos)
                        .putAscii('}');
            }
            if (explain) {
                response.putAscii(',').putAsciiQuoted("explain").putAscii(':')
                        .putAscii('{')
                        .putAsciiQuoted("jitCompiled").putAscii(':').putAscii(queryJitCompiled ? "true" : "false")
                        .putAscii('}');
            }
            response.putAscii('}');
            count = -1;
            counter.set(-1);
            response.sendChunk(true);
            return;
        }
        response.done();
    }

    void resume(HttpChunkedResponse response)
            throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        resumeActions.getQuick(queryState).onResume(response, columnCount);
    }

    void setQueryCacheable(boolean queryCacheable) {
        this.queryCacheable = queryCacheable;
    }

    @FunctionalInterface
    interface StateResumeAction {
        void onResume(
                HttpChunkedResponse response,
                int columnCount
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException;
    }
}
