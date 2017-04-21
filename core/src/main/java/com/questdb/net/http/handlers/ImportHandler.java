/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.net.http.handlers;

import com.questdb.BootstrapEnv;
import com.questdb.PartitionBy;
import com.questdb.ex.DisconnectedChannelException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.ex.ResponseContentBufferTooSmallException;
import com.questdb.ex.SlowWritableChannelException;
import com.questdb.factory.Factory;
import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.Chars;
import com.questdb.misc.Misc;
import com.questdb.net.http.*;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.LocalValue;
import com.questdb.std.LongList;
import com.questdb.std.Mutable;
import com.questdb.std.str.ByteSequence;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.FileNameExtractorCharSequence;
import com.questdb.store.ColumnType;
import com.questdb.txt.parser.DelimitedTextParser;
import com.questdb.txt.parser.FormatParser;
import com.questdb.txt.parser.listener.JournalImportListener;

import java.io.Closeable;
import java.io.IOException;


public class ImportHandler extends AbstractMultipartHandler {
    private static final int RESPONSE_PREFIX = 1;
    private static final int RESPONSE_COLUMN = 2;
    private static final int RESPONSE_SUFFIX = 3;
    private static final int MESSAGE_SCHEMA = 1;
    private static final int MESSAGE_DATA = 2;
    private static final int MESSAGE_UNKNOWN = 3;
    private static final int TO_STRING_COL1_PAD = 15;
    private static final int TO_STRING_COL2_PAD = 50;
    private static final int TO_STRING_COL3_PAD = 10;
    private static final CharSequence CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
    private static final CharSequence CONTENT_TYPE_JSON = "application/json; charset=utf-8";
    private static final ThreadLocal<FormatParser> PARSER = new ThreadLocal<>();
    private static final CharSequenceIntHashMap atomicityParamMap = new CharSequenceIntHashMap();
    private final Factory factory;
    private final LocalValue<ImportHandlerContext> lvContext = new LocalValue<>();
    private final ServerConfiguration configuration;

    public ImportHandler(BootstrapEnv env) {
        this.factory = env.factory;
        this.configuration = env.configuration;
    }

    @Override
    public void resume(IOContext context) throws IOException {
        super.resume(context);
        final ImportHandlerContext ctx = lvContext.get(context);
        final ChunkedResponse r = context.chunkedResponse();
        try {

            if (ctx.json) {
                resumeJson(ctx, r);
            } else {
                resumeText(ctx, r);
            }
        } catch (ResponseContentBufferTooSmallException ignored) {
            if (r.resetToBookmark()) {
                r.sendChunk();
            } else {
                // what we have here is out unit of data, column value or query
                // is larger that response content buffer
                // all we can do in this scenario is to log appropriately
                // and disconnect socket
                throw DisconnectedChannelException.INSTANCE;
            }
        }

        ctx.clear();
    }

    @Override
    protected void onComplete0(IOContext context) throws IOException {
    }

    @Override
    protected void onData(IOContext context, ByteSequence data) throws IOException {
        int len;

        ImportHandlerContext h = lvContext.get(context);
        if ((len = data.length()) < 1) {
            return;
        }

        switch (h.messagePart) {
            case MESSAGE_DATA:
                if (h.state != ImportHandlerContext.STATE_OK) {
                    break;
                }

                long lo = ((DirectByteCharSequence) data).getLo();
                if (h.analysed) {
                    h.textParser.parse(lo, len, Integer.MAX_VALUE, h.importer);
                } else {
                    analyseFormat(h, lo, len);
                    if (h.state == ImportHandlerContext.STATE_OK) {
                        try {
                            h.textParser.analyseStructure(lo, len, 100, h.importer, h.forceHeader);
                            h.textParser.parse(lo, len, Integer.MAX_VALUE, h.importer);
                        } catch (JournalRuntimeException e) {

                            if (configuration.isHttpAbortBrokenUploads()) {
                                sendError(context, e.getMessage());
                                throw e;
                            }
                            h.state = ImportHandlerContext.STATE_DATA_ERROR;
                            h.stateMessage = e.getMessage();
                        }
                    }
                    h.analysed = true;
                }
                break;
            case MESSAGE_SCHEMA:
                h.textParser.setSchemaText((DirectByteCharSequence) data);
                break;
            default:
                break;

        }
    }

    @Override
    protected void onPartBegin(IOContext context, RequestHeaderBuffer hb) throws IOException {
        ImportHandlerContext h = lvContext.get(context);
        if (Chars.equals("data", hb.getContentDispositionName())) {

            CharSequence name = context.request.getUrlParam("name");
            if (name == null) {
                name = hb.getContentDispositionFilename();
            }
            if (name == null) {
                context.simpleResponse().send(400, "no name given");
                throw DisconnectedChannelException.INSTANCE;
            }
            h.analysed = false;
            h.importer.of(
                    FileNameExtractorCharSequence.get(name).toString(),
                    Chars.equalsNc("true", context.request.getUrlParam("overwrite")),
                    Chars.equalsNc("true", context.request.getUrlParam("durable")),
                    getAtomicity(context.request.getUrlParam("atomicity"))
            );
            h.forceHeader = Chars.equalsNc("true", context.request.getUrlParam("forceHeader"));
            h.messagePart = MESSAGE_DATA;
        } else if (Chars.equals("schema", hb.getContentDispositionName())) {
            h.messagePart = MESSAGE_SCHEMA;
        } else {
            h.messagePart = MESSAGE_UNKNOWN;
        }
    }

    @Override
    protected void onPartEnd(IOContext context) throws IOException {
        ImportHandlerContext h = lvContext.get(context);
        if (h != null) {
            switch (h.messagePart) {
                case MESSAGE_DATA:
                    h.textParser.parseLast();
                    h.importer.commit();
                    sendResponse(context);
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void setup(IOContext context) {
        ImportHandlerContext h = lvContext.get(context);
        if (h == null) {
            lvContext.set(context, new ImportHandlerContext(factory));
        }
    }

    @Override
    public void setupThread() {
        PARSER.set(FormatParser.FACTORY.newInstance());
    }

    private static void resumeJson(ImportHandlerContext ctx, ChunkedResponse r) throws DisconnectedChannelException, SlowWritableChannelException {
        final JournalMetadata m = ctx.importer.getMetadata();
        final int columnCount = m.getColumnCount();
        final LongList errors = ctx.importer.getErrors();

        switch (ctx.responseState) {
            case RESPONSE_PREFIX:
                long totalRows = ctx.textParser.getLineCount();
                long importedRows = ctx.importer.getImportedRowCount();
                r.put('{')
                        .putQuoted("status").put(':').putQuoted("OK").put(',')
                        .putQuoted("location").put(':').putUtf8EscapedAndQuoted(FileNameExtractorCharSequence.get(m.getName())).put(',')
                        .putQuoted("rowsRejected").put(':').put(totalRows - importedRows).put(',')
                        .putQuoted("rowsImported").put(':').put(importedRows).put(',')
                        .putQuoted("header").put(':').put(ctx.importer.isHeader()).put(',')
                        .putQuoted("columns").put(':').put('[');
                ctx.responseState = RESPONSE_COLUMN;
                // fall through
            case RESPONSE_COLUMN:
                for (; ctx.columnIndex < columnCount; ctx.columnIndex++) {
                    RecordColumnMetadata cm = m.getColumnQuick(ctx.columnIndex);
                    r.bookmark();
                    if (ctx.columnIndex > 0) {
                        r.put(',');
                    }
                    r.put('{').
                            putQuoted("name").put(':').putQuoted(cm.getName()).put(',').
                            putQuoted("type").put(':').putQuoted(ColumnType.nameOf(cm.getType())).put(',').
                            putQuoted("size").put(':').put(ColumnType.sizeOf(cm.getType())).put(',').
                            putQuoted("errors").put(':').put(errors.getQuick(ctx.columnIndex)).put('}');
                }
                ctx.responseState = RESPONSE_SUFFIX;
                // fall through
            case RESPONSE_SUFFIX:
                r.bookmark();
                r.put(']').put('}');
                r.sendChunk();
                r.done();
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
        replicate(b, '-', TO_STRING_COL1_PAD + TO_STRING_COL2_PAD + TO_STRING_COL3_PAD + 8);
        b.put("+\n");
    }

    private static void col(CharSink b, ColumnMetadata m) {
        pad(
                b,
                TO_STRING_COL2_PAD,
                (m.distinctCountHint > 0 ? m.distinctCountHint + " ~ " : "")
                        + (m.indexed ? '#' : "")
                        + m.name
                        + (m.sameAs != null ? " -> " + m.sameAs : "")
                        + ' '
                        + ColumnType.nameOf(m.type)
                        + '('
                        + m.size
                        + ')'
        );
    }

    private static void resumeText(ImportHandlerContext h, ChunkedResponse r) throws IOException {
        JournalMetadata m = h.importer.getMetadata();
        LongList errors = h.importer.getErrors();

        final int columnCount = m.getColumnCount();

        switch (h.responseState) {
            case RESPONSE_PREFIX:
                sep(r);
                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Location:");
                pad(r, TO_STRING_COL2_PAD, m.getName());
                pad(r, TO_STRING_COL3_PAD, "Errors").put(Misc.EOL);


                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Partition by");
                pad(r, TO_STRING_COL2_PAD, PartitionBy.toString(m.getPartitionBy()));
                pad(r, TO_STRING_COL3_PAD, "").put(Misc.EOL);
                sep(r);

                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Rows handled");
                pad(r, TO_STRING_COL2_PAD, h.textParser.getLineCount());
                pad(r, TO_STRING_COL3_PAD, "").put(Misc.EOL);

                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Rows imported");
                pad(r, TO_STRING_COL2_PAD, h.importer.getImportedRowCount());
                pad(r, TO_STRING_COL3_PAD, "").put(Misc.EOL);
                sep(r);

                h.responseState = RESPONSE_COLUMN;
                // fall through
            case RESPONSE_COLUMN:
                for (; h.columnIndex < columnCount; h.columnIndex++) {
                    r.bookmark();
                    r.put('|');
                    pad(r, TO_STRING_COL1_PAD, h.columnIndex);
                    col(r, m.getColumnQuick(h.columnIndex));
                    pad(r, TO_STRING_COL3_PAD, errors.getQuick(h.columnIndex));
                    r.put(Misc.EOL);
                }
                h.responseState = RESPONSE_SUFFIX;
                // fall through
            case RESPONSE_SUFFIX:
                r.bookmark();
                sep(r);
                r.sendChunk();
                r.done();
                break;
            default:
                break;
        }
    }

    private static int getAtomicity(CharSequence name) {
        if (name == null) {
            return JournalImportListener.ATOMICITY_RELAXED;
        }

        int atomicity = atomicityParamMap.get(name);
        return atomicity == -1 ? JournalImportListener.ATOMICITY_RELAXED : atomicity;
    }

    private void analyseFormat(ImportHandlerContext context, long address, int len) {
        final FormatParser fmtParser = PARSER.get();

        fmtParser.of(address, len);
        if (fmtParser.getDelimiter() != 0 && fmtParser.getStdDev() < 0.5) {
            context.state = ImportHandlerContext.STATE_OK;
            context.textParser.of(fmtParser.getDelimiter());
        } else {
            context.state = ImportHandlerContext.STATE_INVALID_FORMAT;
            context.stateMessage = "Unsupported Data Format";
        }
    }

    private void sendError(IOContext context, String message) throws IOException {
        ResponseSink sink = context.responseSink();
        if (Chars.equalsNc("json", context.request.getUrlParam("fmt"))) {
            sink.status(200, CONTENT_TYPE_JSON);
            sink.put('{').putQuoted("status").put(':').putUtf8EscapedAndQuoted(message).put('}');
        } else {
            sink.status(400, CONTENT_TYPE_TEXT);
            sink.putUtf8(message);
        }
        sink.flush();
        throw DisconnectedChannelException.INSTANCE;
    }

    private void sendResponse(IOContext context) throws IOException {
        ImportHandlerContext h = lvContext.get(context);
        h.json = Chars.equalsNc("json", context.request.getUrlParam("fmt"));
        ChunkedResponse r = context.chunkedResponse();

        switch (h.state) {
            case ImportHandlerContext.STATE_OK:
                if (h.json) {
                    r.status(200, CONTENT_TYPE_JSON);
                } else {
                    r.status(200, CONTENT_TYPE_TEXT);
                }
                r.sendHeader();
                resume(context);
                break;
            default:
                sendError(context, h.stateMessage);
                break;
        }
    }

    private static class ImportHandlerContext implements Mutable, Closeable {
        public static final int STATE_OK = 0;
        public static final int STATE_INVALID_FORMAT = 1;
        public static final int STATE_DATA_ERROR = 2;
        public int columnIndex = 0;
        private int state;
        private String stateMessage;
        private boolean analysed = false;
        private DelimitedTextParser textParser = new DelimitedTextParser();
        private JournalImportListener importer;
        private int messagePart = MESSAGE_UNKNOWN;
        private int responseState = RESPONSE_PREFIX;
        private boolean json = false;
        private boolean forceHeader = false;

        private ImportHandlerContext(Factory factory) {
            this.importer = new JournalImportListener(factory);
        }

        @Override
        public void clear() {
            responseState = RESPONSE_PREFIX;
            columnIndex = 0;
            messagePart = MESSAGE_UNKNOWN;
            analysed = false;
            state = STATE_OK;
            textParser.clear();
            importer.clear();
        }

        @Override
        public void close() {
            clear();
            textParser = Misc.free(textParser);
            importer = Misc.free(importer);
        }
    }

    static {
        atomicityParamMap.put("relaxed", JournalImportListener.ATOMICITY_RELAXED);
        atomicityParamMap.put("strict", JournalImportListener.ATOMICITY_STRICT);
    }
}
