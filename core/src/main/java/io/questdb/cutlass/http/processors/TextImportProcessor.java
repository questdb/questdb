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
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cutlass.text.TextLoadWarning.*;

public class TextImportProcessor implements HttpRequestProcessor, HttpMultipartContentListener, Closeable {
    public static final CharSequence CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
    static final int MESSAGE_UNKNOWN = 3;
    static final int RESPONSE_PREFIX = 1;
    private static final CharSequence CONTENT_TYPE_JSON = "application/json; charset=utf-8";
    private final static Log LOG = LogFactory.getLog(TextImportProcessor.class);
    // Local value has to be static because each thread will have its own instance of
    // processor. For different threads to lookup the same value from local value map the key,
    // which is LV, has to be the same between processor instances
    private static final LocalValue<TextImportProcessorState> LV = new LocalValue<>();
    private static final int MESSAGE_DATA = 2;
    private static final int MESSAGE_SCHEMA = 1;
    private static final String OVERRIDDEN_FROM_TABLE = "From Table";
    private static final int RESPONSE_COLUMN = 2;
    private static final int RESPONSE_COMPLETE = 6;
    private static final int RESPONSE_DONE = 5;
    private static final int RESPONSE_SUFFIX = 3;
    private static final int TO_STRING_COL1_PAD = 15;
    private static final int TO_STRING_COL2_PAD = 50;
    private static final int TO_STRING_COL3_PAD = 15;
    private static final int TO_STRING_COL4_PAD = 7;
    private static final int TO_STRING_COL5_PAD = 12;
    private static final LowerCaseCharSequenceIntHashMap atomicityParamMap = new LowerCaseCharSequenceIntHashMap();
    private final CairoEngine engine;
    private HttpConnectionContext transientContext;
    private TextImportProcessorState transientState;

    public TextImportProcessor(CairoEngine cairoEngine) {
        this.engine = cairoEngine;
    }

    @Override
    public void close() {
    }

    @Override
    public void notifyRetryFailed(HttpConnectionContext context, HttpException e) {
        transientState.snapshotStateAndCloseWriter();
    }

    @Override
    public void onChunk(long lo, long hi) throws PeerIsSlowToReadException, PeerDisconnectedException {
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
                transientState.textLoader.setIgnoreEverything(true);
                sendResponse(transientContext);
            }
        }
    }

    @Override
    public void onPartBegin(HttpRequestHeader partHeader) {
        final CharSequence contentDisposition = partHeader.getContentDispositionName();
        LOG.debug().$("part begin [name=").$(contentDisposition).$(']').$();
        if (Chars.equalsNc("data", contentDisposition)) {

            final HttpRequestHeader rh = transientContext.getRequestHeader();
            CharSequence tableName = rh.getUrlParam("tableName");
            if (Chars.isBlank(tableName)) {
                // "name" query parameter name is handled for backward compatibility
                tableName = rh.getUrlParam("name");
                if (Chars.isBlank(tableName)) {
                    tableName = partHeader.getContentDispositionFilename();

                    if (Chars.isBlank(tableName)) {
                        throwHttpException("no file name given");
                    }
                }
            }

            final CharSequence partitionByColumnName = rh.getUrlParam("partitionBy");
            final int partitionBy;
            if (Chars.isBlank(partitionByColumnName)) {
                partitionBy = PartitionBy.NONE;
            } else {
                partitionBy = PartitionBy.fromString(partitionByColumnName);
                if (partitionBy == -1) {
                    throwHttpException("invalid partitionBy");
                }
            }

            final CharSequence timestampColumnName = rh.getUrlParam("timestamp");
            final boolean isTimestampColumnNameBlank = Chars.isBlank(timestampColumnName);
            if (PartitionBy.isPartitioned(partitionBy) && isTimestampColumnNameBlank) {
                throwHttpException("when specifying partitionBy you must also specify timestamp");
            }

            transientState.analysed = false;
            transientState.textLoader.configureDestination(
                    tableName,
                    !Chars.equalsIgnoreCaseNc("false", rh.getUrlParam("wal")),
                    Chars.equalsIgnoreCaseNc("true", rh.getUrlParam("overwrite")),
                    getAtomicity(rh.getUrlParam("atomicity")),
                    partitionBy,
                    isTimestampColumnNameBlank ? null : timestampColumnName,
                    null
            );

            CharSequence o3MaxLagChars = rh.getUrlParam("o3MaxLag");
            if (o3MaxLagChars != null) {
                try {
                    long o3MaxLag = Numbers.parseLong(o3MaxLagChars);
                    if (o3MaxLag >= 0) {
                        transientState.textLoader.setO3MaxLag(o3MaxLag);
                    }
                } catch (NumericException e) {
                    throwHttpException("invalid o3MaxLag value, must be a long");
                }
            }

            CharSequence maxUncommittedRowsChars = rh.getUrlParam("maxUncommittedRows");
            if (maxUncommittedRowsChars != null) {
                try {
                    int maxUncommittedRows = Numbers.parseInt(maxUncommittedRowsChars);
                    if (maxUncommittedRows >= 0) {
                        transientState.textLoader.setMaxUncommittedRows(maxUncommittedRows);
                    }
                } catch (NumericException e) {
                    throwHttpException("invalid maxUncommittedRows, must be an int");
                }
            }

            transientState.textLoader.setForceHeaders(Chars.equalsIgnoreCaseNc("true", rh.getUrlParam("forceHeader")));
            transientState.textLoader.setSkipLinesWithExtraValues(Chars.equalsIgnoreCaseNc("true", rh.getUrlParam("skipLev")));
            CharSequence delimiter = rh.getUrlParam("delimiter");
            if (delimiter != null && delimiter.length() == 1) {
                transientState.textLoader.configureColumnDelimiter((byte) delimiter.charAt(0));
            }
            transientState.textLoader.setState(TextLoader.ANALYZE_STRUCTURE);

            transientState.forceHeader = Chars.equalsIgnoreCaseNc("true", rh.getUrlParam("forceHeader"));
            transientState.messagePart = MESSAGE_DATA;
        } else if (Chars.equalsIgnoreCaseNc("schema", contentDisposition)) {
            transientState.textLoader.setState(TextLoader.LOAD_JSON_METADATA);
            transientState.messagePart = MESSAGE_SCHEMA;
        } else {
            if (partHeader.getContentDisposition() == null) {
                throwHttpException("'Content-Disposition' multipart header missing'");
            } else {
                throwHttpException("invalid value in 'Content-Disposition' multipart header");
            }
        }
    }

    @Override
    public void onPartEnd() throws PeerDisconnectedException, PeerIsSlowToReadException {
        try {
            LOG.debug().$("part end").$();
            transientState.textLoader.wrapUp();
            if (transientState.messagePart == MESSAGE_DATA) {
                sendResponse(transientContext);
            }
        } catch (TextException | CairoException | CairoError e) {
            throwHttpException(e.getFlyweightMessage());
        }
    }

    // This processor implements HttpMultipartContentListener, methods of which
    // have neither context nor dispatcher. During "chunk" processing we may need
    // to send something back to client, or disconnect them. To do that we need
    // these transient references. resumeRecv() will set them and they will remain
    // valid during multipart events.

    @Override
    public void onRequestComplete(HttpConnectionContext context) {
        transientState.clear();
    }

    @Override
    public void onRequestRetry(HttpConnectionContext context) throws PeerIsSlowToReadException, PeerDisconnectedException {
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
    public void resumeSend(HttpConnectionContext context) throws PeerDisconnectedException, PeerIsSlowToReadException {
        context.resumeResponseSend();
        doResumeSend(LV.get(context), context.getChunkedResponseSocket());
    }

    private static int getAtomicity(CharSequence name) {
        if (name == null) {
            return Atomicity.SKIP_COL;
        }

        int atomicity = atomicityParamMap.get(name);
        return atomicity == -1 ? Atomicity.SKIP_COL : atomicity;
    }

    private static void pad(CharSink b, int w, long value) {
        int len = (int) Math.log10(value);
        if (len < 0) {
            len = 0;
        }
        replicate(b, ' ', w - len - 1);
        b.put(value);
        b.put("  |");
    }

    private static CharSink pad(CharSink b, int w, CharSequence value) {
        int pad = value == null ? w : w - value.length();
        replicate(b, ' ', pad);

        if (value != null) {
            if (pad < 0) {
                b.put("...").put(value.subSequence(-pad + 3, value.length()));
            } else {
                b.put(value);
            }
        }

        b.put("  |");

        return b;
    }

    private static void replicate(CharSink b, char c, int times) {
        for (int i = 0; i < times; i++) {
            b.put(c);
        }
    }

    private static void resumeJson(TextImportProcessorState state, HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final TextLoaderCompletedState completeState = state.completeState;
        final RecordMetadata metadata = completeState.getMetadata();
        final LongList errors = completeState.getColumnErrorCounts();

        switch (state.responseState) {
            case RESPONSE_PREFIX:
                long totalRows = completeState.getParsedLineCount();
                long importedRows = completeState.getWrittenLineCount();
                socket.put('{')
                        .putQuoted("status").put(':').putQuoted("OK").put(',')
                        .putQuoted("location").put(':').encodeUtf8AndQuote(completeState.getTableName()).put(',')
                        .putQuoted("rowsRejected").put(':').put(totalRows - importedRows + completeState.getErrorLineCount()).put(',')
                        .putQuoted("rowsImported").put(':').put(importedRows).put(',')
                        .putQuoted("header").put(':').put(completeState.isHeaderDetected()).put(',')
                        .putQuoted("partitionBy").put(':').putQuoted(PartitionBy.toString(completeState.getPartitionBy())).put(',');

                final int tsIdx = metadata != null ? metadata.getTimestampIndex() : -1;
                if (tsIdx != -1) {
                    socket.putQuoted("timestamp").put(':').encodeUtf8AndQuote(metadata.getColumnName(tsIdx)).put(',');
                }
                if (completeState.getWarnings() != TextLoadWarning.NONE) {
                    final int warningFlags = completeState.getWarnings();
                    socket.putQuoted("warnings").put(':').put('[');
                    boolean isFirst = true;
                    if ((warningFlags & TextLoadWarning.TIMESTAMP_MISMATCH) != TextLoadWarning.NONE) {
                        isFirst = false;
                        socket.putQuoted("Existing table timestamp column is used");
                    }
                    if ((warningFlags & PARTITION_TYPE_MISMATCH) != TextLoadWarning.NONE) {
                        if (!isFirst) socket.put(',');
                        socket.putQuoted("Existing table PartitionBy is used");
                    }
                    socket.put(']').put(',');
                }
                socket.putQuoted("columns").put(':').put('[');
                state.responseState = RESPONSE_COLUMN;
                // fall through
            case RESPONSE_COLUMN:
                if (metadata != null) {
                    final int columnCount = metadata.getColumnCount();
                    for (; state.columnIndex < columnCount; state.columnIndex++) {
                        socket.bookmark();
                        if (state.columnIndex > 0) {
                            socket.put(',');
                        }
                        socket.put('{').
                                putQuoted("name").put(':').putQuoted(metadata.getColumnName(state.columnIndex)).put(',').
                                putQuoted("type").put(':').putQuoted(ColumnType.nameOf(metadata.getColumnType(state.columnIndex))).put(',').
                                putQuoted("size").put(':').put(ColumnType.sizeOf(metadata.getColumnType(state.columnIndex))).put(',').
                                putQuoted("errors").put(':').put(errors.getQuick(state.columnIndex));
                        socket.put('}');
                    }
                }
                state.responseState = RESPONSE_SUFFIX;
                // fall through
            case RESPONSE_SUFFIX:
                socket.bookmark();
                socket.put(']').put('}');
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

    private static void resumeText(TextImportProcessorState state, HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final TextLoaderCompletedState textLoaderCompletedState = state.completeState;
        final RecordMetadata metadata = textLoaderCompletedState.getMetadata();
        LongList errors = textLoaderCompletedState.getColumnErrorCounts();

        switch (state.responseState) {
            case RESPONSE_PREFIX:
                sep(socket);
                socket.put('|');
                pad(socket, TO_STRING_COL1_PAD, "Location:");
                pad(socket, TO_STRING_COL2_PAD, textLoaderCompletedState.getTableName());
                pad(socket, TO_STRING_COL3_PAD, "Pattern");
                pad(socket, TO_STRING_COL4_PAD, "Locale");
                pad(socket, TO_STRING_COL5_PAD, "Errors").put(Misc.EOL);

                socket.put('|');
                pad(socket, TO_STRING_COL1_PAD, "Partition by");
                pad(socket, TO_STRING_COL2_PAD, PartitionBy.toString(textLoaderCompletedState.getPartitionBy()));
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                if (hasFlag(textLoaderCompletedState.getWarnings(), PARTITION_TYPE_MISMATCH)) {
                    pad(socket, TO_STRING_COL5_PAD, OVERRIDDEN_FROM_TABLE);
                } else {
                    pad(socket, TO_STRING_COL5_PAD, "");
                }
                socket.put(Misc.EOL);

                socket.put('|');
                pad(socket, TO_STRING_COL1_PAD, "Timestamp");
                pad(socket, TO_STRING_COL2_PAD, textLoaderCompletedState.getTimestampCol() == null ? "NONE" : textLoaderCompletedState.getTimestampCol());
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                if (hasFlag(textLoaderCompletedState.getWarnings(), TIMESTAMP_MISMATCH)) {
                    pad(socket, TO_STRING_COL5_PAD, OVERRIDDEN_FROM_TABLE);
                } else {
                    pad(socket, TO_STRING_COL5_PAD, "");
                }
                socket.put(Misc.EOL);

                sep(socket);

                socket.put('|');
                pad(socket, TO_STRING_COL1_PAD, "Rows handled");
                pad(socket, TO_STRING_COL2_PAD, textLoaderCompletedState.getParsedLineCount() + textLoaderCompletedState.getErrorLineCount());
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                pad(socket, TO_STRING_COL5_PAD, "").put(Misc.EOL);
                socket.put('|');
                pad(socket, TO_STRING_COL1_PAD, "Rows imported");
                pad(socket, TO_STRING_COL2_PAD, textLoaderCompletedState.getWrittenLineCount());
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                pad(socket, TO_STRING_COL5_PAD, "").put(Misc.EOL);
                sep(socket);

                state.responseState = RESPONSE_COLUMN;
                // fall through
            case RESPONSE_COLUMN:

                if (metadata != null) {
                    final int columnCount = metadata.getColumnCount();

                    for (; state.columnIndex < columnCount; state.columnIndex++) {
                        socket.bookmark();
                        socket.put('|');
                        pad(socket, TO_STRING_COL1_PAD, state.columnIndex);
                        pad(socket, TO_STRING_COL2_PAD, metadata.getColumnName(state.columnIndex));
                        if (!metadata.isColumnIndexed(state.columnIndex)) {
                            pad(socket, TO_STRING_COL3_PAD + TO_STRING_COL4_PAD + 3, ColumnType.nameOf(metadata.getColumnType(state.columnIndex)));
                        } else {
                            StringSink sink = Misc.getThreadLocalBuilder();
                            sink.put("(idx/").put(metadata.getIndexValueBlockCapacity(state.columnIndex)).put(") ");
                            sink.put(ColumnType.nameOf(metadata.getColumnType(state.columnIndex)));
                            pad(socket, TO_STRING_COL3_PAD + TO_STRING_COL4_PAD + 3, sink);
                        }
                        pad(socket, TO_STRING_COL5_PAD, errors.getQuick(state.columnIndex));
                        socket.put(Misc.EOL);
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

    private static void sep(CharSink b) {
        b.put('+');
        replicate(b, '-', TO_STRING_COL1_PAD + TO_STRING_COL2_PAD + TO_STRING_COL3_PAD + TO_STRING_COL4_PAD + TO_STRING_COL5_PAD + 14);
        b.put("+\r\n");
    }

    private void doResumeSend(
            TextImportProcessorState state,
            HttpChunkedResponseSocket socket
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        try {
            if (state.json) {
                resumeJson(state, socket);
            } else {
                resumeText(state, socket);
            }
        } catch (NoSpaceLeftInResponseBufferException ignored) {
            socket.sendChunk(true);
        }
        state.clear();
    }

    private boolean isJson(HttpConnectionContext transientContext) {
        return Chars.equalsNc("json", transientContext.getRequestHeader().getUrlParam("fmt"));
    }

    private void throwHttpException(CharSequence message) {
        transientState.snapshotStateAndCloseWriter();
        throw HttpException.instance(message);
    }

    private void sendResponse(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException {
        final TextImportProcessorState state = LV.get(context);
        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();

        // Copy written state to state, text loader, parser can be closed before re-attempt to send the response
        state.snapshotStateAndCloseWriter();
        if (state.json) {
            socket.status(200, CONTENT_TYPE_JSON);
        } else {
            socket.status(200, CONTENT_TYPE_TEXT);
        }
        socket.sendHeader();
        doResumeSend(state, socket);
    }

    static {
        atomicityParamMap.put("skipRow", Atomicity.SKIP_ROW);
        atomicityParamMap.put("abort", Atomicity.SKIP_ALL);
    }
}
