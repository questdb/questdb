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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.*;
import io.questdb.cutlass.text.Atomicity;
import io.questdb.cutlass.text.TextException;
import io.questdb.cutlass.text.TextLoader;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.str.CharSink;

import java.io.Closeable;


public class TextImportProcessor implements HttpRequestProcessor, HttpMultipartContentListener, Closeable {
    static final int RESPONSE_PREFIX = 1;
    static final int MESSAGE_UNKNOWN = 3;
    private final static Log LOG = LogFactory.getLog(TextImportProcessor.class);
    private static final int RESPONSE_COLUMN = 2;
    private static final int RESPONSE_SUFFIX = 3;
    private static final int MESSAGE_SCHEMA = 1;
    private static final int MESSAGE_DATA = 2;
    private static final int TO_STRING_COL1_PAD = 15;
    private static final int TO_STRING_COL2_PAD = 50;
    private static final int TO_STRING_COL3_PAD = 15;
    private static final int TO_STRING_COL4_PAD = 7;
    private static final int TO_STRING_COL5_PAD = 10;
    private static final CharSequence CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
    private static final CharSequence CONTENT_TYPE_JSON = "application/json; charset=utf-8";
    private static final CharSequenceIntHashMap atomicityParamMap = new CharSequenceIntHashMap();
    // Local value has to be static because each thread will have its own instance of
    // processor. For different threads to lookup the same value from local value map the key,
    // which is LV, has to be the same between processor instances
    private static final LocalValue<TextImportProcessorState> LV = new LocalValue<>();

    static {
        atomicityParamMap.put("skipRow", Atomicity.SKIP_ROW);
        atomicityParamMap.put("abort", Atomicity.SKIP_ALL);
    }

    private final CairoEngine engine;
    private HttpConnectionContext transientContext;
    private TextImportProcessorState transientState;

    public TextImportProcessor(CairoEngine cairoEngine) {
        this.engine = cairoEngine;
    }

    private static void resumeJson(TextImportProcessorState state, HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final TextLoader textLoader = state.textLoader;
        final RecordMetadata metadata = textLoader.getMetadata();
        final LongList errors = textLoader.getColumnErrorCounts();


        switch (state.responseState) {
            case RESPONSE_PREFIX:
                long totalRows = state.textLoader.getParsedLineCount();
                long importedRows = state.textLoader.getWrittenLineCount();
                socket.put('{')
                        .putQuoted("status").put(':').putQuoted("OK").put(',')
                        .putQuoted("location").put(':').encodeUtf8AndQuote(textLoader.getTableName()).put(',')
                        .putQuoted("rowsRejected").put(':').put(totalRows - importedRows + textLoader.getErrorLineCount()).put(',')
                        .putQuoted("rowsImported").put(':').put(importedRows).put(',')
                        .putQuoted("header").put(':').put(textLoader.isForceHeaders()).put(',')
                        .putQuoted("columns").put(':').put('[');
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
                socket.sendChunk();
                socket.done();
                break;
            default:
                break;
        }
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

    // This processor implements HttpMultipartContentListener, methods of which
    // have neither context nor dispatcher. During "chunk" processing we may need
    // to send something back to client, or disconnect them. To do that we need
    // these transient references. resumeRecv() will set them and they will remain
    // valid during multipart events.

    private static void pad(CharSink b, int w, long value) {
        int len = (int) Math.log10(value);
        if (len < 0) {
            len = 0;
        }
        replicate(b, ' ', w - len - 1);
        b.put(value);
        b.put("  |");
    }

    private static void replicate(CharSink b, char c, int times) {
        for (int i = 0; i < times; i++) {
            b.put(c);
        }
    }

    private static void sep(CharSink b) {
        b.put('+');
        replicate(b, '-', TO_STRING_COL1_PAD + TO_STRING_COL2_PAD + TO_STRING_COL3_PAD + TO_STRING_COL4_PAD + TO_STRING_COL5_PAD + 14);
        b.put("+\r\n");
    }

    private static void resumeText(TextImportProcessorState state, HttpChunkedResponseSocket socket) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final TextLoader textLoader = state.textLoader;
        final RecordMetadata metadata = textLoader.getMetadata();
        LongList errors = textLoader.getColumnErrorCounts();


        switch (state.responseState) {
            case RESPONSE_PREFIX:
                sep(socket);
                socket.put('|');
                pad(socket, TO_STRING_COL1_PAD, "Location:");
                pad(socket, TO_STRING_COL2_PAD, textLoader.getTableName());
                pad(socket, TO_STRING_COL3_PAD, "Pattern");
                pad(socket, TO_STRING_COL4_PAD, "Locale");
                pad(socket, TO_STRING_COL5_PAD, "Errors").put(Misc.EOL);


                socket.put('|');
                pad(socket, TO_STRING_COL1_PAD, "Partition by");
                pad(socket, TO_STRING_COL2_PAD, PartitionBy.toString(textLoader.getPartitionBy()));
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                pad(socket, TO_STRING_COL5_PAD, "").put(Misc.EOL);
                sep(socket);

                socket.put('|');
                pad(socket, TO_STRING_COL1_PAD, "Rows handled");
                pad(socket, TO_STRING_COL2_PAD, textLoader.getParsedLineCount() + textLoader.getErrorLineCount());
                pad(socket, TO_STRING_COL3_PAD, "");
                pad(socket, TO_STRING_COL4_PAD, "");
                pad(socket, TO_STRING_COL5_PAD, "").put(Misc.EOL);

                socket.put('|');
                pad(socket, TO_STRING_COL1_PAD, "Rows imported");
                pad(socket, TO_STRING_COL2_PAD, textLoader.getWrittenLineCount());
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
                        pad(socket, TO_STRING_COL3_PAD + TO_STRING_COL4_PAD + 3, ColumnType.nameOf(metadata.getColumnType(state.columnIndex)));
                        pad(socket, TO_STRING_COL5_PAD, errors.getQuick(state.columnIndex));
                        socket.put(Misc.EOL);
                    }
                }
                state.responseState = RESPONSE_SUFFIX;
                // fall through
            case RESPONSE_SUFFIX:
                socket.bookmark();
                sep(socket);
                socket.sendChunk();
                socket.done();
                break;
            default:
                break;
        }
    }

    private static int getAtomicity(CharSequence name) {
        if (name == null) {
            return Atomicity.SKIP_COL;
        }

        int atomicity = atomicityParamMap.get(name);
        return atomicity == -1 ? Atomicity.SKIP_COL : atomicity;
    }

    @Override
    public void close() {
    }

    @Override
    public void onChunk(long lo, long hi)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        if (hi > lo) {
            try {
                transientState.textLoader.parse(lo, hi, transientContext.getCairoSecurityContext());
                if (transientState.messagePart == MESSAGE_DATA && !transientState.analysed) {
                    transientState.analysed = true;
                    transientState.textLoader.setState(TextLoader.LOAD_DATA);
                }
            } catch (TextException e) {
                handleTextException(e);
            }
        }
    }

    @Override
    public void onPartBegin(HttpRequestHeader partHeader) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        final CharSequence contentDisposition = partHeader.getContentDispositionName();
        LOG.debug().$("part begin [name=").$(contentDisposition).$(']').$();
        if (Chars.equalsNc("data", contentDisposition)) {

            final HttpRequestHeader rh = transientContext.getRequestHeader();
            CharSequence name = rh.getUrlParam("name");
            if (name == null) {
                name = partHeader.getContentDispositionFilename();
            }
            if (name == null) {
                transientContext.simpleResponse().sendStatus(400, "no file name given");
                throw ServerDisconnectException.INSTANCE;
            }

            CharSequence partitionedBy = rh.getUrlParam("partitionBy");
            if (partitionedBy == null) {
                partitionedBy = "NONE";
            }
            int partitionBy = PartitionBy.fromString(partitionedBy);
            if (partitionBy == -1) {
                transientContext.simpleResponse().sendStatus(400, "invalid partitionBy");
                throw ServerDisconnectException.INSTANCE;
            }

            CharSequence timestampIndexCol = rh.getUrlParam("timestamp");
            if (partitionBy != PartitionBy.NONE && timestampIndexCol == null) {
                transientContext.simpleResponse().sendStatus(400, "when specifying partitionBy you must also specify timestamp");
                throw ServerDisconnectException.INSTANCE;
            }

            transientState.analysed = false;
            transientState.textLoader.configureDestination(
                    name,
                    Chars.equalsNc("true", rh.getUrlParam("overwrite")),
                    Chars.equalsNc("true", rh.getUrlParam("durable")),
                    getAtomicity(rh.getUrlParam("atomicity")),
                    partitionBy,
                    timestampIndexCol
            );
            transientState.textLoader.setForceHeaders(Chars.equalsNc("true", rh.getUrlParam("forceHeader")));
            transientState.textLoader.setSkipRowsWithExtraValues(Chars.equalsNc("true", rh.getUrlParam("skipLev")));
            transientState.textLoader.setState(TextLoader.ANALYZE_STRUCTURE);

            transientState.forceHeader = Chars.equalsNc("true", rh.getUrlParam("forceHeader"));
            transientState.messagePart = MESSAGE_DATA;
        } else if (Chars.equalsNc("schema", contentDisposition)) {
            transientState.textLoader.setState(TextLoader.LOAD_JSON_METADATA);
            transientState.messagePart = MESSAGE_SCHEMA;
        } else {
            if (partHeader.getContentDisposition() == null) {
                transientContext.simpleResponse().sendStatus(400, "'Content-Disposition' multipart header missing'");
            } else {
                transientContext.simpleResponse().sendStatus(400, "invalid value in 'Content-Disposition' multipart header");
            }
            throw ServerDisconnectException.INSTANCE;
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
        } catch (TextException e) {
            handleTextException(e);
        }
    }

    @Override
    public void onHeadersReady(HttpConnectionContext context) {

    }

    @Override
    public void onRequestComplete(HttpConnectionContext context) {
        transientState.clear();
        context.clear();
        context.getDispatcher().registerChannel(context, IOOperation.READ);
    }

    @Override
    public void resumeRecv(HttpConnectionContext context) {
        this.transientContext = context;
        this.transientState = LV.get(context);
        if (this.transientState == null) {
            LOG.debug().$("new text state").$();
            LV.set(context, this.transientState = new TextImportProcessorState(engine));
        }
    }

    @Override
    public void resumeSend(
            HttpConnectionContext context
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        doResumeSend(LV.get(context), context.getChunkedResponseSocket());
    }

    private void doResumeSend(
            TextImportProcessorState state,
            HttpChunkedResponseSocket socket
    ) throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        try {

            if (state.json) {
                resumeJson(state, socket);
            } else {
                resumeText(state, socket);
            }
        } catch (NoSpaceLeftInResponseBufferException ignored) {
            if (socket.resetToBookmark()) {
                socket.sendChunk();
            } else {
                // what we have here is out unit of data, column value or query
                // is larger that response content buffer
                // all we can do in this scenario is to log appropriately
                // and disconnect socket
                throw ServerDisconnectException.INSTANCE;
            }
        }

        state.clear();
    }

    private void handleTextException(TextException e)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        sendError(transientContext, e.getFlyweightMessage(), Chars.equalsNc("json", transientContext.getRequestHeader().getUrlParam("fmt")));
        throw ServerDisconnectException.INSTANCE;
    }

    private void sendError(
            HttpConnectionContext context,
            CharSequence message,
            boolean json
    ) throws PeerDisconnectedException, PeerIsSlowToReadException {
        final HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();
        if (json) {
            socket.status(400, CONTENT_TYPE_JSON);
            socket.sendHeader();
            socket.put('{').putQuoted("status").put(':').encodeUtf8AndQuote(message).put('}');
        } else {
            socket.status(400, CONTENT_TYPE_TEXT);
            socket.sendHeader();
            socket.encodeUtf8(message);
        }
        socket.sendChunk();
        socket.done();
    }

    private void sendResponse(HttpConnectionContext context)
            throws PeerDisconnectedException, PeerIsSlowToReadException, ServerDisconnectException {
        TextImportProcessorState state = LV.get(context);
        // todo: may be set this up when headers are ready?
        state.json = Chars.equalsNc("json", context.getRequestHeader().getUrlParam("fmt"));
        HttpChunkedResponseSocket socket = context.getChunkedResponseSocket();

        if (state.state == TextImportProcessorState.STATE_OK) {
            if (state.json) {
                socket.status(200, CONTENT_TYPE_JSON);
            } else {
                socket.status(200, CONTENT_TYPE_TEXT);
            }
            socket.sendHeader();
            doResumeSend(state, socket);
        } else {
            sendError(context, state.stateMessage, state.json);
        }
    }
}
