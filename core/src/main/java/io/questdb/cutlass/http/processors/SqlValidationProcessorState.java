/*******************************************************************************
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

package io.questdb.cutlass.http.processors;

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
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.http.processors.SqlValidationProcessor.readyForNextRequest;

public class SqlValidationProcessorState implements Mutable, Closeable {
    public static final String HIDDEN = "hidden";
    static final int VALIDATION_BAD_UTF8 = 8;
    static final int VALIDATION_CONFIRMATION = 7;
    static final int VALIDATION_DONE = 5;
    static final int VALIDATION_EMPTY_QUERY = 9;
    static final int VALIDATION_ERROR = 6;
    static final int VALIDATION_METADATA = 2;
    static final int VALIDATION_METADATA_SUFFIX = 3;
    static final int VALIDATION_PREFIX = 1;
    static final int VALIDATION_RESPONSE_BEGIN = 0;
    static final int VALIDATION_SUFFIX = 4;
    private static final byte DEFAULT_API_VERSION = 1;
    private static final Log LOG = LogFactory.getLog(SqlValidationProcessorState.class);
    private final ObjList<String> columnNames = new ObjList<>();
    private final IntList columnSkewList = new IntList();
    private final IntList columnTypesAndFlags = new IntList();
    private final StringSink errorMessage = new StringSink();
    private final HttpConnectionContext httpConnectionContext;
    private final CharSequence keepAliveHeader;
    private final StringSink query = new StringSink();
    private final ObjList<StateResumeAction> resumeActions = new ObjList<>();
    private byte apiVersion = DEFAULT_API_VERSION;
    private int columnCount;
    private int columnIndex;
    private long compilerNanos;
    private boolean containsSecret;
    private int errorPosition;
    private boolean noMeta = false;
    private int queryTimestampIndex;
    private short queryType;
    private String queryTypeStringConfirmation = null;
    private Rnd rnd;
    private int validationState = VALIDATION_RESPONSE_BEGIN;

    public SqlValidationProcessorState(
            HttpConnectionContext httpConnectionContext,
            CharSequence keepAliveHeader
    ) {
        this.httpConnectionContext = httpConnectionContext;
        resumeActions.extendAndSet(VALIDATION_RESPONSE_BEGIN, this::onSetupFirstRecord);
        resumeActions.extendAndSet(VALIDATION_PREFIX, this::onResumePrefix);
        resumeActions.extendAndSet(VALIDATION_METADATA, this::onResumeMetadata);
        resumeActions.extendAndSet(VALIDATION_METADATA_SUFFIX, this::onResumeMetadataSuffix);
        resumeActions.extendAndSet(VALIDATION_SUFFIX, (response, columnCount) -> resumeValidationSuffix(response));
        resumeActions.extendAndSet(VALIDATION_DONE, (response, columnCount) -> response.done());
        resumeActions.extendAndSet(VALIDATION_ERROR, (response, columnCount) -> onResumeError(response));
        resumeActions.extendAndSet(VALIDATION_CONFIRMATION, (response, columnCount) -> onResumeSendConfirmation(response));
        resumeActions.extendAndSet(VALIDATION_BAD_UTF8, (response, columnCount) -> onResumeBadUtf8(response));
        resumeActions.extendAndSet(VALIDATION_EMPTY_QUERY, (response, columnCount) -> onResumeEmptyQuery(response));
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
        validationState = VALIDATION_RESPONSE_BEGIN;
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
        info().$("response buffer is too small, state=").$(validationState).$();
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

    public void storeError(int errorPosition, CharSequence errorMessage) {
        this.validationState = VALIDATION_ERROR;
        this.errorPosition = errorPosition;
        this.errorMessage.clear();
        this.errorMessage.put(errorMessage);
    }

    public void storeQueryTypeStringConfirmation(String queryTypeString) {
        this.validationState = VALIDATION_CONFIRMATION;
        this.queryTypeStringConfirmation = queryTypeString;
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
        final int columnType = metadata.getColumnType(i);
        this.columnTypesAndFlags.add(columnType);
        this.columnTypesAndFlags.add(GeoHashes.getBitFlags(columnType));
        this.columnNames.add(metadata.getColumnName(i));
    }

    private void doQueryMetadata(HttpChunkedResponse response, int columnCount) {
        validationState = VALIDATION_METADATA;
        for (; columnIndex < columnCount; columnIndex++) {
            response.bookmark();
            if (columnIndex > 0) {
                response.putAscii(',');
            }
            int columnType = columnTypesAndFlags.getQuick(2 * columnIndex);
            response.putAscii('{')
                    .putAsciiQuoted("name").putAscii(':').putQuote().escapeJsonStr(columnNames.getQuick(columnIndex)).putQuote().putAscii(',');
            if (ColumnType.tagOf(columnType) == ColumnType.ARRAY) {
                response.putAsciiQuoted("type").putAscii(':').putAsciiQuoted("ARRAY").putAscii(',');
                response.putAsciiQuoted("dim").putAscii(':').put(ColumnType.decodeArrayDimensionality(columnType)).putAscii(',');
                response.putAsciiQuoted("elemType").putAscii(':').putAsciiQuoted(ColumnType.nameOf(ColumnType.decodeArrayElementType(columnType)));
            } else {
                response.putAsciiQuoted("type").putAscii(':').putAsciiQuoted(ColumnType.nameOf(columnType == ColumnType.NULL ? ColumnType.STRING : columnType));
            }
            response.putAscii('}');
        }
    }

    private void doQueryMetadataSuffix(HttpChunkedResponse response) {
        validationState = VALIDATION_METADATA_SUFFIX;
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

    private void onResumeMetadata(HttpChunkedResponse response, int columnCount) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryMetadata(response, columnCount);
        onResumeMetadataSuffix(response, columnCount);
    }

    private void onResumeMetadataSuffix(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        doQueryMetadataSuffix(response);
        resumeValidationSuffix(response);
    }

    private void onResumePrefix(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (doQueryPrefix(response)) {
            doQueryMetadata(response, columnCount);
            doQueryMetadataSuffix(response);
        }
        resumeValidationSuffix(response);
    }

    private void onSetupFirstRecord(
            HttpChunkedResponse response,
            int columnCount
    ) throws PeerIsSlowToReadException, PeerDisconnectedException {
        columnIndex = 0;
        validationState = VALIDATION_PREFIX;
        JsonQueryProcessor.header(response, getHttpConnectionContext(), keepAliveHeader, 200);
        onResumePrefix(response, columnCount);
    }

    boolean of(RecordCursorFactory factory) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        try (factory) {
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

    void onResumeBadUtf8(HttpChunkedResponse response) throws PeerIsSlowToReadException, PeerDisconnectedException {
        Utf8Sequence query = getHttpConnectionContext().getRequestHeader().getUrlParam(URL_PARAM_QUERY);
        response.bookmark();
        response.putAscii('{')
                .putAsciiQuoted("query").putAscii(':').putQuoted(query.asAsciiCharSequence()).putAscii(',')
                .putAsciiQuoted("error").putAscii(':').putQuoted("Bad UTF8 encoding in query text").putAscii(',')
                .putAsciiQuoted("position").putAscii(':').put(0)
                .putAscii('}');
        validationState = VALIDATION_DONE;
        readyForNextRequest(getHttpConnectionContext());
        response.sendChunk(true);
    }

    void onResumeEmptyQuery(HttpChunkedResponse response) throws PeerIsSlowToReadException, PeerDisconnectedException {
        response.bookmark();
        String noticeOrError = getApiVersion() >= 2 ? "notice" : "error";
        response.putAscii('{')
                .putAsciiQuoted(noticeOrError).putAscii(':').putAsciiQuoted("empty query")
                .putAscii(",")
                .putAsciiQuoted("query").putAscii(':').putQuote().escapeJsonStr(getQuery()).putQuote()
                .putAscii(",")
                .putAsciiQuoted("position").putAscii(':').putAsciiQuoted("0")
                .putAscii('}');
        validationState = VALIDATION_DONE;
        readyForNextRequest(getHttpConnectionContext());
        response.sendChunk(true);
    }

    void onResumeError(HttpChunkedResponse response) throws PeerIsSlowToReadException, PeerDisconnectedException {
        response.bookmark();
        response.putAscii('{')
                .putAsciiQuoted("query").putAscii(':').putQuote().escapeJsonStr(query).putQuote().putAscii(',')
                .putAsciiQuoted("error").putAscii(':').putQuote().escapeJsonStr(errorMessage).putQuote().putAscii(',')
                .putAsciiQuoted("position").putAscii(':').put(errorPosition)
                .putAscii('}');
        validationState = VALIDATION_DONE;
        readyForNextRequest(getHttpConnectionContext());
        response.sendChunk(true);
    }

    void onResumeSendConfirmation(HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        response.bookmark();
        response.putAscii('{')
                .putAsciiQuoted("queryType").putAscii(':').putAsciiQuoted(queryTypeStringConfirmation)
                .putAscii('}');
        validationState = VALIDATION_DONE;
        response.sendChunk(true);
    }

    void resume(HttpChunkedResponse response)
            throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException {
        resumeActions.getQuick(validationState).onResume(response, columnCount);
    }

    void resumeValidationSuffix(HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        validationState = VALIDATION_SUFFIX;
        logTimings();
        response.bookmark();
        response.putAscii('}');
        validationState = VALIDATION_DONE;
        response.sendChunk(true);
    }

    void storeBadUtf8() {
        validationState = VALIDATION_BAD_UTF8;
    }

    void storeEmptyQuery() {
        validationState = VALIDATION_EMPTY_QUERY;
    }

    @FunctionalInterface
    interface StateResumeAction {
        void onResume(
                HttpChunkedResponse response,
                int columnCount
        ) throws PeerDisconnectedException, PeerIsSlowToReadException, SqlException;
    }
}
