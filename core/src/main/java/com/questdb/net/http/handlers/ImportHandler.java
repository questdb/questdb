/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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
import com.questdb.common.ColumnType;
import com.questdb.common.JournalRuntimeException;
import com.questdb.common.PartitionBy;
import com.questdb.common.RecordColumnMetadata;
import com.questdb.ex.ImportSchemaException;
import com.questdb.ex.ResponseContentBufferTooSmallException;
import com.questdb.net.http.ChunkedResponse;
import com.questdb.net.http.IOContext;
import com.questdb.net.http.RequestHeaderBuffer;
import com.questdb.net.http.ResponseSink;
import com.questdb.parser.ImportedColumnMetadata;
import com.questdb.parser.JsonSchemaParser;
import com.questdb.parser.json.JsonException;
import com.questdb.parser.json.JsonLexer;
import com.questdb.parser.plaintext.PlainTextDelimiterLexer;
import com.questdb.parser.plaintext.PlainTextLexer;
import com.questdb.parser.plaintext.PlainTextStoringParser;
import com.questdb.std.*;
import com.questdb.std.ex.DisconnectedChannelException;
import com.questdb.std.ex.SlowWritableChannelException;
import com.questdb.std.str.ByteSequence;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.FileNameExtractorCharSequence;
import com.questdb.store.factory.configuration.ColumnMetadata;
import com.questdb.store.factory.configuration.JournalMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ThreadLocal;


public class ImportHandler extends AbstractMultipartHandler {
    private static final int RESPONSE_PREFIX = 1;
    private static final int RESPONSE_COLUMN = 2;
    private static final int RESPONSE_SUFFIX = 3;
    private static final int MESSAGE_SCHEMA = 1;
    private static final int MESSAGE_DATA = 2;
    private static final int MESSAGE_UNKNOWN = 3;
    private static final int TO_STRING_COL1_PAD = 15;
    private static final int TO_STRING_COL2_PAD = 50;
    private static final int TO_STRING_COL3_PAD = 15;
    private static final int TO_STRING_COL4_PAD = 7;
    private static final int TO_STRING_COL5_PAD = 10;
    private static final CharSequence CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
    private static final CharSequence CONTENT_TYPE_JSON = "application/json; charset=utf-8";
    private static final ThreadLocal<PlainTextDelimiterLexer> PARSER = new ThreadLocal<>();
    private static final CharSequenceIntHashMap atomicityParamMap = new CharSequenceIntHashMap();
    private final LocalValue<ImportHandlerContext> lvContext = new LocalValue<>();
    private final BootstrapEnv env;

    public ImportHandler(BootstrapEnv env) {
        this.env = env;
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
    protected void onComplete0(IOContext context) {
    }

    @Override
    protected void onData(IOContext context, ByteSequence data) throws IOException {
        int len;

        ImportHandlerContext h = lvContext.get(context);
        if ((len = data.length()) < 1) {
            return;
        }

        long lo = ((DirectByteCharSequence) data).getLo();

        switch (h.messagePart) {
            case MESSAGE_DATA:
                if (h.state != ImportHandlerContext.STATE_OK) {
                    break;
                }
                parseData(context, h, lo, len);
                break;
            case MESSAGE_SCHEMA:
                parseSchema(context, h, lo, len);
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
                case MESSAGE_SCHEMA:
                    try {
                        h.jsonLexer.parseLast();
                    } catch (JsonException e) {
                        handleJsonException(context, h, e);
                    }
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
            lvContext.set(context, new ImportHandlerContext(env));
        }
    }

    @Override
    public void setupThread() {
        PARSER.set(PlainTextDelimiterLexer.FACTORY.newInstance());
    }

    private static void resumeJson(ImportHandlerContext ctx, ChunkedResponse r) throws DisconnectedChannelException, SlowWritableChannelException {
        final JournalMetadata m = ctx.importer.getJournalMetadata();
        final ObjList<ImportedColumnMetadata> importedMetadata = ctx.importer.getImportedMetadata();
        final int columnCount = m.getColumnCount();
        final LongList errors = ctx.importer.getErrors();

        switch (ctx.responseState) {
            case RESPONSE_PREFIX:
                long totalRows = ctx.textParser.getLineCount();
                long importedRows = ctx.importer.getImportedRowCount();
                r.put('{')
                        .putQuoted("status").put(':').putQuoted("OK").put(',')
                        .putQuoted("location").put(':').encodeUtf8AndQuote(FileNameExtractorCharSequence.get(m.getName())).put(',')
                        .putQuoted("rowsRejected").put(':').put(totalRows - importedRows).put(',')
                        .putQuoted("rowsImported").put(':').put(importedRows).put(',')
                        .putQuoted("header").put(':').put(ctx.importer.isHeader()).put(',')
                        .putQuoted("columns").put(':').put('[');
                ctx.responseState = RESPONSE_COLUMN;
                // fall through
            case RESPONSE_COLUMN:
                for (; ctx.columnIndex < columnCount; ctx.columnIndex++) {
                    RecordColumnMetadata cm = m.getColumnQuick(ctx.columnIndex);
                    ImportedColumnMetadata im = importedMetadata.getQuick(ctx.columnIndex);

                    r.bookmark();
                    if (ctx.columnIndex > 0) {
                        r.put(',');
                    }
                    r.put('{').
                            putQuoted("name").put(':').putQuoted(cm.getName()).put(',').
                            putQuoted("type").put(':').putQuoted(ColumnType.nameOf(cm.getType())).put(',').
                            putQuoted("size").put(':').put(ColumnType.sizeOf(cm.getType())).put(',').
                            putQuoted("errors").put(':').put(errors.getQuick(ctx.columnIndex));

                    if (im.pattern != null) {
                        r.put(',').putQuoted("pattern").put(':').putQuoted(im.pattern);
                    }

                    if (im.dateLocale != null) {
                        r.put(',').putQuoted("locale").put(':').putQuoted(im.dateLocale.getId());
                    }

                    r.put('}');
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
        replicate(b, '-', TO_STRING_COL1_PAD + TO_STRING_COL2_PAD + TO_STRING_COL3_PAD + TO_STRING_COL4_PAD + TO_STRING_COL5_PAD + 14);
        b.put("+\n");
    }

    private static void col(CharSink b, ColumnMetadata m, ImportedColumnMetadata im) {
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

        pad(b, TO_STRING_COL3_PAD, im.pattern);
        pad(b, TO_STRING_COL4_PAD, im.dateLocale != null ? im.dateLocale.getId() : null);
    }

    private static void resumeText(ImportHandlerContext h, ChunkedResponse r) throws IOException {
        JournalMetadata m = h.importer.getJournalMetadata();
        LongList errors = h.importer.getErrors();
        ObjList<ImportedColumnMetadata> importedMetadata = h.importer.getImportedMetadata();

        final int columnCount = m.getColumnCount();

        switch (h.responseState) {
            case RESPONSE_PREFIX:
                sep(r);
                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Location:");
                pad(r, TO_STRING_COL2_PAD, m.getName());
                pad(r, TO_STRING_COL3_PAD, "Pattern");
                pad(r, TO_STRING_COL4_PAD, "Locale");
                pad(r, TO_STRING_COL5_PAD, "Errors").put(Misc.EOL);


                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Partition by");
                pad(r, TO_STRING_COL2_PAD, PartitionBy.toString(m.getPartitionBy()));
                pad(r, TO_STRING_COL3_PAD, "");
                pad(r, TO_STRING_COL4_PAD, "");
                pad(r, TO_STRING_COL5_PAD, "").put(Misc.EOL);
                sep(r);

                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Rows handled");
                pad(r, TO_STRING_COL2_PAD, h.textParser.getLineCount());
                pad(r, TO_STRING_COL3_PAD, "");
                pad(r, TO_STRING_COL4_PAD, "");
                pad(r, TO_STRING_COL5_PAD, "").put(Misc.EOL);

                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Rows imported");
                pad(r, TO_STRING_COL2_PAD, h.importer.getImportedRowCount());
                pad(r, TO_STRING_COL3_PAD, "");
                pad(r, TO_STRING_COL4_PAD, "");
                pad(r, TO_STRING_COL5_PAD, "").put(Misc.EOL);
                sep(r);

                h.responseState = RESPONSE_COLUMN;
                // fall through
            case RESPONSE_COLUMN:
                for (; h.columnIndex < columnCount; h.columnIndex++) {
                    r.bookmark();
                    r.put('|');
                    pad(r, TO_STRING_COL1_PAD, h.columnIndex);
                    col(r, m.getColumnQuick(h.columnIndex), importedMetadata.getQuick(h.columnIndex));
                    pad(r, TO_STRING_COL5_PAD, errors.getQuick(h.columnIndex));
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
            return PlainTextStoringParser.ATOMICITY_RELAXED;
        }

        int atomicity = atomicityParamMap.get(name);
        return atomicity == -1 ? PlainTextStoringParser.ATOMICITY_RELAXED : atomicity;
    }

    private void analyseFormat(ImportHandlerContext context, long address, int len) {
        final PlainTextDelimiterLexer plainTextDelimiterLexer = PARSER.get();

        plainTextDelimiterLexer.of(address, len);
        if (plainTextDelimiterLexer.getDelimiter() != 0 && plainTextDelimiterLexer.getStdDev() < 0.5) {
            context.state = ImportHandlerContext.STATE_OK;
            context.textParser.of(plainTextDelimiterLexer.getDelimiter());
        } else {
            context.state = ImportHandlerContext.STATE_INVALID_FORMAT;
            context.stateMessage = "Unsupported Data Format";
        }
    }

    private void handleJsonException(IOContext context, ImportHandlerContext h, JsonException e) throws IOException {
        if (env.configuration.isHttpAbortBrokenUploads()) {
            sendError(context, e.getMessage());
            throw ImportSchemaException.INSTANCE;
        }
        h.state = ImportHandlerContext.STATE_DATA_ERROR;
        h.stateMessage = e.getMessage();
    }

    private void parseData(IOContext context, ImportHandlerContext h, long lo, int len) throws IOException {
        if (h.analysed) {
            h.textParser.parse(lo, len, Integer.MAX_VALUE, h.importer);
        } else {
            analyseFormat(h, lo, len);
            if (h.state == ImportHandlerContext.STATE_OK) {
                try {
                    h.textParser.analyseStructure(lo, len, env.configuration.getHttpImportSampleSize(), h.importer, h.forceHeader, h.jsonSchemaParser.getMetadata());
                    h.textParser.parse(lo, len, Integer.MAX_VALUE, h.importer);
                } catch (JournalRuntimeException e) {
                    if (env.configuration.isHttpAbortBrokenUploads()) {
                        sendError(context, e.getMessage());
                        throw e;
                    }
                    h.state = ImportHandlerContext.STATE_DATA_ERROR;
                    h.stateMessage = e.getMessage();
                }
            }
            h.analysed = true;
        }
    }

    private void parseSchema(IOContext context, ImportHandlerContext h, long lo, int len) throws IOException {
        try {
            h.jsonLexer.parse(lo, len, h.jsonSchemaParser);
        } catch (JsonException e) {
            handleJsonException(context, h, e);
        }
    }

    private void sendError(IOContext context, String message) throws IOException {
        ResponseSink sink = context.responseSink();
        if (Chars.equalsNc("json", context.request.getUrlParam("fmt"))) {
            sink.status(200, CONTENT_TYPE_JSON);
            sink.put('{').putQuoted("status").put(':').encodeUtf8AndQuote(message).put('}');
        } else {
            sink.status(400, CONTENT_TYPE_TEXT);
            sink.encodeUtf8(message);
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
        private final PlainTextLexer textParser;
        private final PlainTextStoringParser importer;
        private final JsonSchemaParser jsonSchemaParser;
        private final JsonLexer jsonLexer;
        public int columnIndex = 0;
        private int state;
        private String stateMessage;
        private boolean analysed = false;
        private int messagePart = MESSAGE_UNKNOWN;
        private int responseState = RESPONSE_PREFIX;
        private boolean json = false;
        private boolean forceHeader = false;

        private ImportHandlerContext(BootstrapEnv env) {
            this.importer = new PlainTextStoringParser(env);
            this.textParser = new PlainTextLexer(env);
            this.jsonSchemaParser = new JsonSchemaParser(env);
            this.jsonLexer = new JsonLexer(
                    1024,
                    env.configuration.getHttpImportMaxJsonStringLen()
            );
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
            jsonLexer.clear();
            jsonSchemaParser.clear();
        }

        @Override
        public void close() {
            clear();
            Misc.free(textParser);
            Misc.free(importer);
            Misc.free(jsonLexer);
        }
    }

    static {
        atomicityParamMap.put("relaxed", PlainTextStoringParser.ATOMICITY_RELAXED);
        atomicityParamMap.put("strict", PlainTextStoringParser.ATOMICITY_STRICT);
    }
}
