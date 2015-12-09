/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.http.handlers;

import com.nfsdb.collections.ByteSequence;
import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.LongList;
import com.nfsdb.exceptions.DisconnectedChannelException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.http.IOContext;
import com.nfsdb.http.RequestHeaderBuffer;
import com.nfsdb.http.Response;
import com.nfsdb.io.parser.CsvParser;
import com.nfsdb.io.parser.FormatParser;
import com.nfsdb.io.parser.PipeParser;
import com.nfsdb.io.parser.TabParser;
import com.nfsdb.io.parser.listener.JournalImportListener;
import com.nfsdb.io.parser.listener.MetadataExtractorListener;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Misc;

import java.io.IOException;

public class ImportHandler extends AbstractMultipartHandler {
    private static final int TO_STRING_COL1_PAD = 15;
    private static final int TO_STRING_COL2_PAD = 50;
    private static final int TO_STRING_COL3_PAD = 10;

    private final JournalFactory factory;

    public ImportHandler(JournalFactory factory) {
        this.factory = factory;
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

    private static CharSink pad(CharSink b, int w, long value) {
        int len = (int) Math.log10(value);
        if (len < 0) {
            len = 0;
        }
        replicate(b, ' ', w - len - 1);
        b.put(value);
        b.put("  |");
        return b;
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

    private static void sendSummary(IOContext context) throws IOException {
        if (context.importer != null) {
            Response r = context.response;

            r.status(200, "text/plain; charset=utf-8");

            JournalMetadata m = context.importer.getMetadata();
            LongList errors = context.importer.getErrors();

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
            pad(r, TO_STRING_COL2_PAD, context.textParser.getLineCount());
            pad(r, TO_STRING_COL3_PAD, "").put(Misc.EOL);

            r.put('|');
            pad(r, TO_STRING_COL1_PAD, "Rows imported");
            pad(r, TO_STRING_COL2_PAD, context.importer.getImportedRowCount());
            pad(r, TO_STRING_COL3_PAD, "").put(Misc.EOL);
            sep(r);

            for (int i = 0, n = m.getColumnCount(); i < n; i++) {
                r.put('|');
                pad(r, TO_STRING_COL1_PAD, i);
                col(r, m.getColumnQuick(i));
                pad(r, TO_STRING_COL3_PAD, errors.getQuick(i));
                r.put(Misc.EOL);
            }
            sep(r);
            r.end();
        }
    }

    private void analyseColumns(IOContext context, long address, int len) {
        // analyse columns and their types
        int sampleSize = 100;
        MetadataExtractorListener lsnr = (MetadataExtractorListener) context.threadContext.getCache().get(IOWorkerContextKey.ME.name());
        if (lsnr == null) {
            lsnr = new MetadataExtractorListener();
            context.threadContext.getCache().put(IOWorkerContextKey.ME.name(), lsnr);
        }

        context.textParser.parse(address, len, sampleSize, lsnr);
        lsnr.onLineCount(context.textParser.getLineCount());
        context.importer.onMetadata(lsnr.getMetadata());
        context.textParser.setHeader(lsnr.isHeader());
        context.textParser.restart();
        context.analysed = true;
    }

    private void analyseFormat(IOContext context, long address, int len) {

        FormatParser parser = (FormatParser) context.threadContext.getCache().get(IOWorkerContextKey.FP.name());
        if (parser == null) {
            context.threadContext.getCache().put(IOWorkerContextKey.FP.name(), parser = new FormatParser());
        }

        parser.of(address, len);
        context.dataFormatValid = parser.getFormat() != null && parser.getStdDev() < 0.5;

        if (context.dataFormatValid) {
            if (context.textParser != null) {
                context.textParser.clear();
            } else {
                switch (parser.getFormat()) {
                    case CSV:
                        context.textParser = new CsvParser();
                        break;
                    case TAB:
                        context.textParser = new TabParser();
                        break;
                    case PIPE:
                        context.textParser = new PipeParser();
                        break;
                    default:
                        context.dataFormatValid = false;
                }
            }
        }
    }

    @Override
    protected void onComplete0(IOContext context) throws IOException {
    }

    @Override
    protected void onData(IOContext context, RequestHeaderBuffer hb, ByteSequence data) {
        int len;
        if (hb.getContentDispositionFilename() != null && (len = data.length()) > 0) {
            long lo = ((DirectByteCharSequence) data).getLo();
            if (!context.analysed) {
                analyseFormat(context, lo, len);
                if (context.dataFormatValid) {
                    analyseColumns(context, lo, len);
                }
            }

            if (context.dataFormatValid) {
                context.textParser.parse(lo, len, Integer.MAX_VALUE, context.importer);
            } else {
                context.response.simple(400, "Invalid data format");
            }
        }
    }

    @Override
    protected void onPartBegin(IOContext context, RequestHeaderBuffer hb) throws IOException {
        if (hb.getContentDispositionFilename() != null) {
            if (Chars.equals(hb.getContentDispositionName(), "data")) {
                context.analysed = false;
                context.importer = new JournalImportListener(factory, hb.getContentDispositionFilename().toString());
            } else {
                context.response.simple(400, "Unrecognised field");
                throw DisconnectedChannelException.INSTANCE;
            }
        }
    }

    @Override
    protected void onPartEnd(IOContext context) throws IOException {
        if (context.textParser != null) {
//            System.out.println("Closing importer for \n\r" + context.importer.getMetadata());
            context.textParser.parseLast();
//            System.out.println(context.importer.getImportedRowCount());
            context.importer.commit();
            sendSummary(context);
            context.textParser = Misc.free(context.textParser);
            context.importer = Misc.free(context.importer);
        }
    }
}
