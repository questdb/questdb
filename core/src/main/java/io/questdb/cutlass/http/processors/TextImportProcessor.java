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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.cutlass.text.TextException;
import io.questdb.cutlass.text.TextLoadWarning;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.*;
import io.questdb.std.str.*;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.cutlass.text.TextLoadWarning.*;

public class TextImportProcessor implements HttpRequestProcessor, HttpMultipartContentListener, Closeable {
    static final int MESSAGE_UNKNOWN = 3;
    static final int RESPONSE_PREFIX = 1;
    private static final Log LOG = LogFactory.getLog(TextImportProcessor.class);
    // Local value has to be static because each thread will have its own instance of
    // processor. For different threads to lookup the same value from local value map the key,
    // which is LV, has to be the same between processor instances
    private static final LocalValue<TextImportProcessorState> LV = new LocalValue<>();
    private static final int MESSAGE_DATA = 2;
    private static final int MESSAGE_SCHEMA = 1;
    private static final String OVERRIDDEN_FROM_TABLE = "From Table";
    private static final Utf8String PARTITION_BY_NONE = new Utf8String("NONE");
    private static final int RESPONSE_COLUMN = 2;
    private static final int RESPONSE_COMPLETE = 6;
    private static final int RESPONSE_DONE = 5;
    private static final int RESPONSE_ERROR = 4;
    private static final int RESPONSE_SUFFIX = 3;
    private static final int TO_STRING_COL1_PAD = 15;
    private static final int TO_STRING_COL2_PAD = 50;
    private static final int TO_STRING_COL3_PAD = 15;
    private static final int TO_STRING_COL4_PAD = 7;
    private static final int TO_STRING_COL5_PAD = 12;
    private static final Utf8SequenceIntHashMap atomicityParamMap = new Utf8SequenceIntHashMap();
    private final CairoEngine engine;
    private final byte requiredAuthType;
    private HttpConnectionContext transientContext;
    private TextImportProcessorState transientState;

    public TextImportProcessor(CairoEngine cairoEngine, JsonQueryProcessorConfiguration configuration) {
        this.engine = cairoEngine;
        requiredAuthType = configuration.getRequiredAuthType();
    }

    @Override
    public void close() {
    }

    @Override
    public void failRequest(HttpConnectionContext context, HttpException e) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        sendErrorAndThrowDisconnect(e.getFlyweightMessage());
    }

    @Override
    public byte getRequiredAuthType() {
        return requiredAuthType;
    }

    @Override
    public void onChunk(long lo, long hi)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (hi > lo) {
            try {
                transientState.lo = lo;
                transientState.hi = hi;
                transientState.textLoader.parse(lo, hi, transientContext.getSecurityContext());
                if (transientState.messagePart == MESSAGE_DATA && !transientState.analysed) {
                    transientState.analysed = true;
                    transientState.textLoader.setState(TextLoader.LOAD_DATA);
                }
            } catch (EntryUnavailableException e) {
                throw RetryOperationException.INSTANCE;
            } catch (TextException | CairoException | CairoError e) {
                sendErrorAndThrowDisconnect(e.getFlyweightMessage());
            }
        }
    }

    @Override
    public void onPartBegin(HttpRequestHeader partHeader) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        final DirectUtf8Sequence contentDisposition = partHeader.getContentDispositionName();
        LOG.debug().$("part begin [name=").$(contentDisposition).$(']').$();
        if (Utf8s.equalsNcAscii("data", contentDisposition)) {
            final HttpRequestHeader rh = transientContext.getRequestHeader();
            DirectUtf8Sequence name = rh.getUrlParam(URL_PARAM_NAME);
            if (name == null) {
                name = partHeader.getContentDispositionFilename();
            }

            if (name == null) {
                sendErrorAndThrowDisconnect("no file name given");
            }

            Utf8Sequence partitionedBy = rh.getUrlParam(URL_PARAM_PARTITION_BY);
            if (partitionedBy == null) {
                partitionedBy = PARTITION_BY_NONE;
            }
            int partitionBy = PartitionBy.fromUtf8String(partitionedBy);
            if (partitionBy == -1) {
                sendErrorAndThrowDisconnect("invalid partitionBy");
            }

            DirectUtf8Sequence timestampColumn = rh.getUrlParam(URL_PARAM_TIMESTAMP);
            if (PartitionBy.isPartitioned(partitionBy) && timestampColumn == null) {
                sendErrorAndThrowDisconnect("when specifying partitionBy you must also specify timestamp");
            }

            transientState.analysed = false;
            transientState.textLoader.configureDestination(
                    name,
                    Utf8s.equalsNcAscii("true", rh.getUrlParam(URL_PARAM_OVERWRITE)),
                    getAtomicity(rh.getUrlParam(URL_PARAM_ATOMICITY)),
                    partitionBy,
                    timestampColumn,
                    null
            );

            DirectUtf8Sequence o3MaxLagChars = rh.getUrlParam(URL_PARAM_O3_MAX_LAG);
            if (o3MaxLagChars != null) {
                try {
                    long o3MaxLag = Numbers.parseLong(o3MaxLagChars);
                    if (o3MaxLag >= 0) {
                        transientState.textLoader.setO3MaxLag(o3MaxLag);
                    }
                } catch (NumericException e) {
                    sendErrorAndThrowDisconnect("invalid o3MaxLag value, must be a long");
                }
            }

            DirectUtf8Sequence maxUncommittedRowsChars = rh.getUrlParam(URL_PARAM_MAX_UNCOMMITTED_ROWS);
            if (maxUncommittedRowsChars != null) {
                try {
                    int maxUncommittedRows = Numbers.parseInt(maxUncommittedRowsChars);
                    if (maxUncommittedRows >= 0) {
                        transientState.textLoader.setMaxUncommittedRows(maxUncommittedRows);
                    }
                } catch (NumericException e) {
                    sendErrorAndThrowDisconnect("invalid maxUncommittedRows, must be an int");
                }
            }

            boolean create = !Utf8s.equalsNcAscii("false", rh.getUrlParam(URL_PARAM_CREATE));
            transientState.textLoader.setCreate(create);

            boolean forceHeader = Utf8s.equalsNcAscii("true", rh.getUrlParam(URL_PARAM_FORCE_HEADER));
            transientState.textLoader.setForceHeaders(forceHeader);
            transientState.textLoader.setSkipLinesWithExtraValues(Utf8s.equalsNcAscii("true", rh.getUrlParam(URL_PARAM_SKIP_LEV)));
            DirectUtf8Sequence delimiter = rh.getUrlParam(URL_PARAM_DELIMITER);
            if (delimiter != null && delimiter.size() == 1) {
                transientState.textLoader.configureColumnDelimiter(delimiter.byteAt(0));
            }
            transientState.textLoader.setState(TextLoader.ANALYZE_STRUCTURE);

            transientState.forceHeader = forceHeader;
            transientState.messagePart = MESSAGE_DATA;
        } else if (Utf8s.equalsNcAscii("schema", contentDisposition)) {
            transientState.textLoader.setState(TextLoader.LOAD_JSON_METADATA);
            transientState.messagePart = MESSAGE_SCHEMA;
        } else {
            if (partHeader.getContentDisposition() == null) {
                sendErrorAndThrowDisconnect("'Content-Disposition' multipart header missing'");
            } else {
                sendErrorAndThrowDisconnect("invalid value in 'Content-Disposition' multipart header");
            }
        }
    }

    @Override
    public void onPartEnd() throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            LOG.debug().$("part end").$();
            transientState.textLoader.wrapUp();
            if (transientState.messagePart == MESSAGE_DATA) {
                sendResponse(transientContext);
            }
        } catch (TextException | CairoException | CairoError e) {
            sendErrorAndThrowDisconnect(e.getFlyweightMessage());
        }
    }

    // This processor implements HttpMultipartContentListener, methods of which
    // have neither context nor dispatcher. During "chunk" processing we may need
    // to send something back to client, or disconnect them. To do that we need
    // these transient references. resumeRecv() will set them and they will remain
    // valid during multipart events.

    @Override
    public void onRequestComplete(HttpConnectionContext context) {
        if (transientState != null) {
            transientState.clear();
        }
    }

    @Override
    public void onRequestRetry(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        this.transientContext = context;
        this.transientState = LV.get(context);
        onChunk(transientState.lo, transientState.hi);
    }

    @Override
    public void resumeRecv(HttpConnectionContext context) {
        this.transientContext = context;
        this.transientState = LV.get(context);
        if (transientState == null) {
            LOG.debug().$("new text state").$();
            LV.set(context, this.transientState = new TextImportProcessorState(engine));
        }
        transientState.json = isJson(context);
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        context.resumeResponseSend();
        doResumeSend(LV.get(context), context.getChunkedResponse());
    }

    private static int getAtomicity(Utf8Sequence name) {
        if (name == null) {
            return Atomicity.SKIP_COL;
        }

        int atomicity = atomicityParamMap.get(name);
        return atomicity == -1 ? Atomicity.SKIP_COL : atomicity;
    }

    private static void pad(Utf8Sink b, int w, long value) {
        int len = (int) Math.log10(value);
        if (len < 0) {
            len = 0;
        }
        replicate(b, ' ', w - len - 1);
        b.put(value);
        b.putAscii("  |");
    }

    private static Utf8Sink pad(Utf8Sink b, int w, CharSequence value) {
        int pad = value == null ? w : w - value.length();
        replicate(b, ' ', pad);
        if (value != null) {
            if (pad < 0) {
                b.putAscii("...").put(value.subSequence(-pad + 3, value.length()));
            } else {
                b.put(value);
            }
        }
        b.putAscii("  |");
        return b;
    }

    private static void replicate(Utf8Sink b, char c, int times) {
        for (int i = 0; i < times; i++) {
            b.put(c);
        }
    }

    private static void resumeJson(TextImportProcessorState state, HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final TextLoaderCompletedState completeState = state.completeState;
        final RecordMetadata metadata = completeState.getMetadata();
        final LongList errors = completeState.getColumnErrorCounts();

        switch (state.responseState) {
            case RESPONSE_PREFIX:
                long totalRows = completeState.getParsedLineCount();
                long importedRows = completeState.getWrittenLineCount();
                response.putAscii('{')
                        .putAsciiQuoted("status").putAscii(':').putAsciiQuoted("OK").putAscii(',')
                        .putAsciiQuoted("location").putAscii(':').putQuoted(completeState.getTableName()).putAscii(',')
                        .putAsciiQuoted("rowsRejected").putAscii(':').put(totalRows - importedRows + completeState.getErrorLineCount()).putAscii(',')
                        .putAsciiQuoted("rowsImported").putAscii(':').put(importedRows).putAscii(',')
                        .putAsciiQuoted("header").putAscii(':').put(completeState.isHeaderDetected()).putAscii(',')
                        .putAsciiQuoted("partitionBy").putAscii(':').putAsciiQuoted(PartitionBy.toString(completeState.getPartitionBy())).putAscii(',');

                int tsIdx = metadata.getTimestampIndex();
                if (tsIdx != -1) {
                    response.putAsciiQuoted("timestamp").putAscii(':').putQuoted(metadata.getColumnName(tsIdx)).putAscii(',');
                }
                if (completeState.getWarnings() != TextLoadWarning.NONE) {
                    final int warningFlags = completeState.getWarnings();
                    response.putAsciiQuoted("warnings").putAscii(':').putAscii('[');
                    boolean isFirst = true;
                    if ((warningFlags & TextLoadWarning.TIMESTAMP_MISMATCH) != TextLoadWarning.NONE) {
                        isFirst = false;
                        response.putAsciiQuoted("Existing table timestamp column is used");
                    }
                    if ((warningFlags & PARTITION_TYPE_MISMATCH) != TextLoadWarning.NONE) {
                        if (!isFirst) response.putAscii(',');
                        response.putAsciiQuoted("Existing table PartitionBy is used");
                    }
                    response.putAscii(']').putAscii(',');
                }
                response.putAsciiQuoted("columns").putAscii(':').putAscii('[');
                state.responseState = RESPONSE_COLUMN;
                // fall through
            case RESPONSE_COLUMN:
                if (metadata != null) {
                    final int columnCount = metadata.getColumnCount();
                    for (; state.columnIndex < columnCount; state.columnIndex++) {
                        response.bookmark();
                        if (state.columnIndex > 0) {
                            response.putAscii(',');
                        }
                        response.putAscii('{')
                                .putAsciiQuoted("name").putAscii(':').putQuoted(metadata.getColumnName(state.columnIndex)).putAscii(',')
                                .putAsciiQuoted("type").putAscii(':').putAsciiQuoted(ColumnType.nameOf(metadata.getColumnType(state.columnIndex))).putAscii(',')
                                .putAsciiQuoted("size").putAscii(':').put(ColumnType.sizeOf(metadata.getColumnType(state.columnIndex))).putAscii(',')
                                .putAsciiQuoted("errors").putAscii(':').put(errors.getQuick(state.columnIndex));
                        response.putAscii('}');
                    }
                }
                state.responseState = RESPONSE_SUFFIX;
                // fall through
            case RESPONSE_SUFFIX:
                response.bookmark();
                response.putAscii(']').putAscii('}');
                state.responseState = RESPONSE_COMPLETE;
                response.sendChunk(true);
                break;
            case RESPONSE_DONE:
                state.responseState = RESPONSE_COMPLETE;
                response.done();
                break;
            default:
                break;
        }
    }

    private static void resumeText(TextImportProcessorState state, HttpChunkedResponse socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final TextLoaderCompletedState textLoaderCompletedState = state.completeState;
        final RecordMetadata metadata = textLoaderCompletedState.getMetadata();
        LongList errors = textLoaderCompletedState.getColumnErrorCounts();

        switch (state.responseState) {
            case RESPONSE_PREFIX:
                sep(socket);
                socket.putAscii('|');
                pad(socket, TO_STRING_COL1_PAD, "Location:");
                pad(socket, TO_STRING_COL2_PAD, textLoaderCompletedState.getTableName());
                pad(socket, TO_STRING_COL3_PAD, "Pattern");
                pad(socket, TO_STRING_COL4_PAD, "Locale");
                pad(socket, TO_STRING_COL5_PAD, "Errors").putEOL();

                socket.putAscii('|');
                pad(socket, TO_STRING_COL1_PAD, "Partition by");
                pad(socket, TO_STRING_COL2_PAD, PartitionBy.toString(textLoaderCompletedState.getPartitionBy()));
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                if (hasFlag(textLoaderCompletedState.getWarnings(), PARTITION_TYPE_MISMATCH)) {
                    pad(socket, TO_STRING_COL5_PAD, OVERRIDDEN_FROM_TABLE);
                } else {
                    pad(socket, TO_STRING_COL5_PAD, "");
                }
                socket.putEOL();

                socket.putAscii('|');
                pad(socket, TO_STRING_COL1_PAD, "Timestamp");
                pad(socket, TO_STRING_COL2_PAD, textLoaderCompletedState.getTimestampCol() == null ? "NONE" : textLoaderCompletedState.getTimestampCol());
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                if (hasFlag(textLoaderCompletedState.getWarnings(), TIMESTAMP_MISMATCH)) {
                    pad(socket, TO_STRING_COL5_PAD, OVERRIDDEN_FROM_TABLE);
                } else {
                    pad(socket, TO_STRING_COL5_PAD, "");
                }
                socket.putEOL();

                sep(socket);

                socket.putAscii('|');
                pad(socket, TO_STRING_COL1_PAD, "Rows handled");
                pad(socket, TO_STRING_COL2_PAD, textLoaderCompletedState.getParsedLineCount() + textLoaderCompletedState.getErrorLineCount());
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                pad(socket, TO_STRING_COL5_PAD, "").putEOL();
                socket.putAscii('|');
                pad(socket, TO_STRING_COL1_PAD, "Rows imported");
                pad(socket, TO_STRING_COL2_PAD, textLoaderCompletedState.getWrittenLineCount());
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                pad(socket, TO_STRING_COL5_PAD, "").putEOL();
                sep(socket);

                state.responseState = RESPONSE_COLUMN;
                // fall through
            case RESPONSE_COLUMN:

                if (metadata != null) {
                    final int columnCount = metadata.getColumnCount();

                    for (; state.columnIndex < columnCount; state.columnIndex++) {
                        socket.bookmark();
                        socket.putAscii('|');
                        pad(socket, TO_STRING_COL1_PAD, state.columnIndex);
                        pad(socket, TO_STRING_COL2_PAD, metadata.getColumnName(state.columnIndex));
                        if (!metadata.isColumnIndexed(state.columnIndex)) {
                            pad(socket, TO_STRING_COL3_PAD + TO_STRING_COL4_PAD + 3, ColumnType.nameOf(metadata.getColumnType(state.columnIndex)));
                        } else {
                            StringSink sink = Misc.getThreadLocalSink();
                            sink.put("(idx/").put(metadata.getIndexValueBlockCapacity(state.columnIndex)).put(") ");
                            sink.put(ColumnType.nameOf(metadata.getColumnType(state.columnIndex)));
                            pad(socket, TO_STRING_COL3_PAD + TO_STRING_COL4_PAD + 3, sink);
                        }
                        pad(socket, TO_STRING_COL5_PAD, errors.getQuick(state.columnIndex));
                        socket.putEOL();
                    }
                }
                state.responseState = RESPONSE_SUFFIX;
                // fall through
            case RESPONSE_SUFFIX:
                socket.bookmark();
                sep(socket);
                state.responseState = RESPONSE_COMPLETE;
                socket.sendChunk(true);
                break;
            case RESPONSE_DONE:
                state.responseState = RESPONSE_COMPLETE;
                socket.done();
                break;
            default:
                break;
        }
    }

    private static void sep(Utf8Sink b) {
        b.putAscii('+');
        replicate(b, '-', TO_STRING_COL1_PAD + TO_STRING_COL2_PAD + TO_STRING_COL3_PAD + TO_STRING_COL4_PAD + TO_STRING_COL5_PAD + 14);
        b.putAscii("+\r\n");
    }

    private void doResumeSend(
            TextImportProcessorState state,
            HttpChunkedResponse socket
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {
            if (state.errorMessage != null) {
                resumeError(state, socket);
            } else if (state.json) {
                resumeJson(state, socket);
            } else {
                resumeText(state, socket);
            }
        } catch (NoSpaceLeftInResponseBufferException ignored) {
            if (socket.resetToBookmark()) {
                socket.sendChunk(false);
            } else {
                // what we have here is out unit of data, column value or query
                // is larger that response content buffer
                // all we can do in this scenario is to log appropriately
                // and disconnect socket
                socket.shutdownWrite();
                throw ServerDisconnectException.INSTANCE;
            }
        }

        state.clear();
    }

    private boolean isJson(HttpConnectionContext transientContext) {
        return Utf8s.equalsNcAscii("json", transientContext.getRequestHeader().getUrlParam(URL_PARAM_FMT));
    }

    private void resumeError(TextImportProcessorState state, HttpChunkedResponse socket) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (state.responseState == RESPONSE_ERROR) {
            socket.bookmark();
            if (state.json) {
                socket.putAscii('{').putAsciiQuoted("status").putAscii(':').putQuoted(state.errorMessage).putAscii('}');
            } else {
                socket.put(state.errorMessage);
            }
            state.responseState = RESPONSE_DONE;
            socket.sendChunk(true);
        }
        socket.shutdownWrite();
        throw ServerDisconnectException.INSTANCE;
    }

    private void sendErr(HttpConnectionContext context, CharSequence message, HttpChunkedResponse response) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        final TextImportProcessorState state = LV.get(context);
        state.responseState = RESPONSE_ERROR;
        state.errorMessage = message;
        response.status(200, state.json ? CONTENT_TYPE_JSON : CONTENT_TYPE_TEXT);
        response.sendHeader();
        response.sendChunk(false);
        resumeError(state, response);
    }

    private void sendErrorAndThrowDisconnect(CharSequence message)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        transientState.snapshotStateAndCloseWriter();
        final HttpChunkedResponse response = transientContext.getChunkedResponse();
        sendErr(transientContext, message, response);
    }

    private void sendResponse(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        final TextImportProcessorState state = LV.get(context);
        final HttpChunkedResponse response = context.getChunkedResponse();

        // Copy written state to state, text loader, parser can be closed before re-attempt to send the response
        state.snapshotStateAndCloseWriter();
        if (state.state == TextImportProcessorState.STATE_OK) {
            response.status(200, state.json ? CONTENT_TYPE_JSON : CONTENT_TYPE_TEXT);
            response.sendHeader();
            doResumeSend(state, response);
        } else {
            sendErr(context, state.stateMessage, context.getChunkedResponse());
        }
    }

    static {
        atomicityParamMap.put(new Utf8String("skipRow"), Atomicity.SKIP_ROW);
        atomicityParamMap.put(new Utf8String("abort"), Atomicity.SKIP_ALL);
    }
}
