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
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.HttpChunkedResponse;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_NM;
import static io.questdb.cutlass.http.HttpConstants.URL_PARAM_VERSION;

public class SqlValidationProcessorState implements Mutable, Closeable {
    public static final String HIDDEN = "hidden";
    static final int QUERY_DONE = 5;
    static final int QUERY_METADATA = 2;
    static final int QUERY_METADATA_SUFFIX = 3;
    static final int QUERY_PREFIX = 1;
    static final int QUERY_SETUP_FIRST_RECORD = 0;
    static final int QUERY_SUFFIX = 4;
    private static final byte DEFAULT_API_VERSION = 1;
    private static final Log LOG = LogFactory.getLog(SqlValidationProcessorState.class);
    private final ObjList<String> columnNames = new ObjList<>();
    private final IntList columnSkewList = new IntList();
    private final IntList columnTypesAndFlags = new IntList();
    private final HttpConnectionContext httpConnectionContext;
    private final CharSequence keepAliveHeader;
    private final StringSink query = new StringSink();
    private final ObjList<StateResumeAction> resumeActions = new ObjList<>();
    private byte apiVersion = DEFAULT_API_VERSION;
    private int columnCount;
    private int columnIndex;
    private long compilerNanos;
    private boolean containsSecret;
    private boolean noMeta = false;
    private int queryState = QUERY_SETUP_FIRST_RECORD;
    private int queryTimestampIndex;
    private short queryType;
    private Rnd rnd;

    public SqlValidationProcessorState(
            HttpConnectionContext httpConnectionContext,
            CharSequence keepAliveHeader
    ) {
        this.httpConnectionContext = httpConnectionContext;
        resumeActions.extendAndSet(QUERY_SETUP_FIRST_RECORD, this::onSetupFirstRecord);
        resumeActions.extendAndSet(QUERY_PREFIX, this::onQueryPrefix);
        resumeActions.extendAndSet(QUERY_METADATA, this::onQueryMetadata);
        resumeActions.extendAndSet(QUERY_METADATA_SUFFIX, this::onQueryMetadataSuffix);
        resumeActions.extendAndSet(QUERY_SUFFIX, (response, columnCount1) -> querySuffixWithError(response, 0, null, 0));
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
        query.clear();
        queryState = QUERY_SETUP_FIRST_RECORD;
        columnIndex = 0;
        noMeta = false;
        containsSecret = false;
    }

    public void clearFactory() {
        columnSkewList.clear();
        columnTypesAndFlags.clear();
    }

    @Override
    public void close() {
        clearFactory();
    }

    public void configure(
            HttpRequestHeader request,
            DirectUtf8Sequence query
    ) throws Utf8Exception {
        this.query.clear();
        if (query != null) {
            if (!Utf8s.utf8ToUtf16(query.lo(), query.hi(), this.query)) {
                throw Utf8Exception.INSTANCE;
            }
        }
        noMeta = HttpKeywords.isTrue(request.getUrlParam(URL_PARAM_NM));
        apiVersion = parseApiVersion(request);
    }

    public LogRecord critical() {
        return LOG.critical().$('[').$(getFd()).$("] ");
    }

    public LogRecord error() {
        return LOG.error().$('[').$(getFd()).$("] ");
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

    public HttpConnectionContext getHttpConnectionContext() {
        return httpConnectionContext;
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

    public LogRecord info() {
        return LOG.info().$('[').$(getFd()).$("] ");
    }

    public void logBufferTooSmall() {
        info().$("response buffer is too small, state=").$(queryState).$();
    }

    public void logTimings() {
        info().$("timings ")
                .$("[compiler: ").$(compilerNanos)
                .$(", q=`").$safe(getQueryOrHidden())
                .$("`]").$();
    }

    public void setCompilerNanos(long compilerNanos) {
        this.compilerNanos = compilerNanos;
    }

    public void setContainsSecret(boolean containsSecret) {
        this.containsSecret = containsSecret;
    }

    public void setQueryType(short type) {
        queryType = type;
    }

    public void setRnd(Rnd rnd) {
        this.rnd = rnd;
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
                break;
            default:
                throw CairoException.nonCritical().put("column type not supported [column=").put(columnName).put(", type=").put(ColumnType.nameOf(columnType)).put(']');
        }

        int flags = GeoHashes.getBitFlags(columnType);
        this.columnTypesAndFlags.add(columnType);
        this.columnTypesAndFlags.add(flags);
        this.columnNames.add(columnName);
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
                response.putAsciiQuoted("dim").putAscii(':').put(ColumnType.decodeArrayDimensionality(columnType)).put(',');
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
    }

    private boolean doQueryPrefix(HttpChunkedResponse response) {
        if (noMeta) {
            response.bookmark();
            response.putAscii('{');
            return false;
        }
        response.bookmark();
        response.putAscii('{')
                .putAsciiQuoted("query").putAscii(':').putQuote().escapeJsonStr(query).putQuote().putAscii(',')
                .putAsciiQuoted("columns").putAscii(':').putAscii('[');
        columnIndex = 0;
        return true;
    }

    private long getFd() {
        return httpConnectionContext.getFd();
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
        querySuffixWithError(response, 0, null, 0);
    }

    private void onQueryPrefix(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (doQueryPrefix(response)) {
            doQueryMetadata(response, columnCount);
            doQueryMetadataSuffix(response);
        }
        querySuffixWithError(response, 0, null, 0);
    }

    private void onSetupFirstRecord(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerIsSlowToReadException, PeerDisconnectedException {
        columnIndex = 0;
        queryState = QUERY_PREFIX;
        JsonQueryProcessor.header(response, getHttpConnectionContext(), keepAliveHeader, 200);
        onQueryPrefix(response, columnCount);
    }

    private void queryDone(HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.done();
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

    boolean of(RecordCursorFactory factory)
            throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        try (factory) {
            // Enable column pre-touch in REST API only when LIMIT K,N is not specified since when limit is defined
            // we do a no-op loop over the cursor to calculate the total row count and pre-touch only slows things down.
            // Make sure to use the override flag to avoid affecting the explain plan.
            final RecordMetadata metadata = factory.getMetadata();
            this.queryTimestampIndex = metadata.getTimestampIndex();
            this.columnNames.clear();
            columnSkewList.clear();
            this.columnTypesAndFlags.clear();
            int columnCount = metadata.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                addColumnTypeAndName(metadata, i);
            }
            this.columnCount = columnCount;
            return true;
        }
    }

    void querySuffixWithError(
            HttpChunkedResponse response,
            int code,
            @Nullable CharSequence message,
            int messagePosition
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        queryState = QUERY_SUFFIX;
        logTimings();
        response.bookmark();
        if (code > 0) {
            response.putAscii(',')
                    .putAsciiQuoted("error").putAscii(':')
                    .putQuote().escapeJsonStr(message != null ? message : "Internal server error").putQuote()
                    .putAscii(", \"errorPos\"").putAscii(':').put(messagePosition);
        }
        response.putAscii('}');
        queryState = QUERY_DONE;
        response.sendChunk(true);
    }

    void resume(HttpChunkedResponse response)
            throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        resumeActions.getQuick(queryState).onResume(response, columnCount);
    }

    @FunctionalInterface
    interface StateResumeAction {
        void onResume(
                HttpChunkedResponse response,
                int columnCount
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException;
    }
}
