/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net.http.handlers;

import com.nfsdb.ex.DisconnectedChannelException;
import com.nfsdb.ex.ResponseContentBufferTooSmallException;
import com.nfsdb.ex.SlowWritableChannelException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.io.parser.DelimitedTextParser;
import com.nfsdb.io.parser.FormatParser;
import com.nfsdb.io.parser.TextParser;
import com.nfsdb.io.parser.listener.JournalImportListener;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Misc;
import com.nfsdb.net.http.ChunkedResponse;
import com.nfsdb.net.http.IOContext;
import com.nfsdb.net.http.RequestHeaderBuffer;
import com.nfsdb.std.*;
import com.nfsdb.std.ThreadLocal;

import java.io.Closeable;
import java.io.IOException;

public class ImportHandler extends AbstractMultipartHandler {
    private static final int TO_STRING_COL1_PAD = 15;
    private static final int TO_STRING_COL2_PAD = 50;
    private static final int TO_STRING_COL3_PAD = 10;
    private final static ThreadLocal<FileNameExtractorCharSequence> nameExtractor = new ThreadLocal<>(FileNameExtractorCharSequence.FACTORY);
    private final JournalFactory factory;
    private final ThreadLocal<FormatParser> tlFormatParser = new ThreadLocal<>(FormatParser.FACTORY);
    private final LocalValue<ImportHandlerContext> lvContext = new LocalValue<>();

    public ImportHandler(JournalFactory factory) {
        this.factory = factory;
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
    protected void onData(IOContext context, ByteSequence data) throws DisconnectedChannelException, SlowWritableChannelException {
        int len;

        ImportHandlerContext h = lvContext.get(context);
        if ((len = data.length()) < 1) {
            return;
        }

        switch (h.part) {
            case DATA:
                long lo = ((DirectByteCharSequence) data).getLo();
                if (!h.analysed) {
                    analyseFormat(h, lo, len);
                    if (h.dataFormatValid) {
                        h.textParser.analyseStructure(lo, len, 100, h.importer);
                        h.analysed = true;
                    }
                }

                if (h.dataFormatValid) {
                    h.textParser.parse(lo, len, Integer.MAX_VALUE, h.importer);
                } else {
                    context.simpleResponse().send(400, "Invalid data format");
                    throw DisconnectedChannelException.INSTANCE;
                }
                break;
            case SCHEMA:
                h.textParser.putSchema((DirectByteCharSequence) data);
                break;
            default:
                break;

        }
    }

    @Override
    protected void onPartBegin(IOContext context, RequestHeaderBuffer hb) throws IOException {
        ImportHandlerContext h = lvContext.get(context);
        if (Chars.equals("data", hb.getContentDispositionName())) {

            if (hb.getContentDispositionFilename() == null) {
                context.simpleResponse().send(400, "data field should be of file type");
                throw DisconnectedChannelException.INSTANCE;
            }
            h.analysed = false;
            h.importer.of(hb.getContentDispositionFilename().toString());
            h.part = MessagePart.DATA;
        } else if (Chars.equals("schema", hb.getContentDispositionName())) {
            h.part = MessagePart.SCHEMA;
        } else {
            h.part = MessagePart.UNKNOWN;
        }
    }

    @Override
    protected void onPartEnd(IOContext context) throws IOException {
        ImportHandlerContext h = lvContext.get(context);
        if (h != null) {
            switch (h.part) {
                case DATA:
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

    private static void resumeJson(ImportHandlerContext ctx, ChunkedResponse r) throws DisconnectedChannelException, SlowWritableChannelException {
        final JournalMetadata m = ctx.importer.getMetadata();
        final int columnCount = m.getColumnCount();
        final LongList errors = ctx.importer.getErrors();

        switch (ctx.responseState) {
            case PREFIX:
                long totalRows = ctx.textParser.getLineCount();
                long importedRows = ctx.importer.getImportedRowCount();
                r.put('{')
                        .putQuoted("location").put(':').putUtf8EscapedAndQuoted(nameExtractor.get().of(m.getLocation())).put(',')
                        .putQuoted("rowsRejected").put(':').put(totalRows - importedRows).put(',')
                        .putQuoted("rowsImported").put(':').put(importedRows).put(',')
                        .putQuoted("columns").put(':').put('[');
                ctx.responseState = ResponseState.COLUMN;
                // fall through
            case COLUMN:
                for (; ctx.columnIndex < columnCount; ctx.columnIndex++) {
                    RecordColumnMetadata cm = m.getColumnQuick(ctx.columnIndex);
                    r.bookmark();
                    if (ctx.columnIndex > 0) {
                        r.put(',');
                    }
                    r.put('{').
                            putQuoted("name").put(':').putQuoted(cm.getName()).put(',').
                            putQuoted("type").put(':').putQuoted(cm.getType().name()).put(',').
                            putQuoted("size").put(':').put(cm.getType().size()).put(',').
                            putQuoted("errors").put(':').put(errors.getQuick(ctx.columnIndex)).put('}');
                }
                ctx.responseState = ResponseState.SUFFIX;
                // fall through
            case SUFFIX:
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
                        + m.type.name()
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
            case PREFIX:
                sep(r);
                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Location:");
                pad(r, TO_STRING_COL2_PAD, m.getLocation());
                pad(r, TO_STRING_COL3_PAD, "Errors").put(Misc.EOL);


                r.put('|');
                pad(r, TO_STRING_COL1_PAD, "Partition by");
                pad(r, TO_STRING_COL2_PAD, m.getPartitionType().name());
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

                h.responseState = ResponseState.COLUMN;
                // fall through
            case COLUMN:
                for (; h.columnIndex < columnCount; h.columnIndex++) {
                    r.bookmark();
                    r.put('|');
                    pad(r, TO_STRING_COL1_PAD, h.columnIndex);
                    col(r, m.getColumnQuick(h.columnIndex));
                    pad(r, TO_STRING_COL3_PAD, errors.getQuick(h.columnIndex));
                    r.put(Misc.EOL);
                }
                h.responseState = ResponseState.SUFFIX;
                // fall through
            case SUFFIX:
                r.bookmark();
                sep(r);
                r.sendChunk();
                r.done();
                break;
            default:
                break;
        }
    }

    private void analyseFormat(ImportHandlerContext context, long address, int len) {
        final FormatParser fmtParser = tlFormatParser.get();

        fmtParser.of(address, len);
        context.dataFormatValid = fmtParser.getFormat() != null && fmtParser.getStdDev() < 0.5;

        if (context.dataFormatValid) {
            context.textParser.of(fmtParser.getFormat().getDelimiter());
        }
    }

    private void sendResponse(IOContext context) throws IOException {
        ImportHandlerContext h = lvContext.get(context);
        CharSequence fmt = context.request.getUrlParam("fmt");
        h.json = fmt != null && Chars.equals("json", fmt);
        ChunkedResponse r = context.chunkedResponse();

        if (h.json) {
            r.status(200, "application/json; charset=utf-8");
        } else {
            r.status(200, "text/plain; charset=utf-8");
        }
        r.sendHeader();
        resume(context);
    }

    private enum MessagePart {
        SCHEMA, DATA, UNKNOWN
    }

    private enum ResponseState {
        PREFIX, COLUMN, SUFFIX
    }

    private static class ImportHandlerContext implements Mutable, Closeable {
        public int columnIndex = 0;
        private boolean analysed = false;
        private boolean dataFormatValid = false;
        private TextParser textParser = new DelimitedTextParser();
        private JournalImportListener importer;
        private MessagePart part = MessagePart.UNKNOWN;
        private ResponseState responseState = ResponseState.PREFIX;
        private boolean json = false;

        private ImportHandlerContext(JournalWriterFactory factory) {
            this.importer = new JournalImportListener(factory);
        }

        @Override
        public void clear() {
            responseState = ResponseState.PREFIX;
            columnIndex = 0;
            part = MessagePart.UNKNOWN;
            analysed = false;
            dataFormatValid = false;
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
}
