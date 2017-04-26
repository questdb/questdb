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

package com.questdb.txt.parser;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Unsafe;
import com.questdb.std.Mutable;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectPool;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.txt.ImportedColumnMetadata;
import com.questdb.txt.SchemaImpl;
import com.questdb.txt.parser.listener.InputAnalysisListener;
import com.questdb.txt.parser.listener.Listener;
import com.questdb.txt.parser.listener.MetadataExtractorListener;
import com.questdb.txt.parser.listener.probe.TypeProbeCollection;

import java.io.Closeable;

public class DelimitedTextParser implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(DelimitedTextParser.class);
    private final ObjList<DirectByteCharSequence> fields = new ObjList<>();
    private final ObjectPool<DirectByteCharSequence> csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 16);
    private final ObjectPool<ImportedColumnMetadata> mPool = new ObjectPool<>(ImportedColumnMetadata.FACTORY, 256);
    private final MetadataExtractorListener mel;
    private final SchemaImpl schema = new SchemaImpl(csPool, mPool);
    private boolean ignoreEolOnce;
    private char separator;
    private boolean inQuote;
    private boolean delayedOutQuote;
    private boolean eol;
    private int fieldIndex;
    private long fieldLo;
    private long fieldHi;
    private int lineCount;
    private boolean useLineRollBuf = false;
    private long lineRollBufCur;
    private Listener listener;
    private boolean calcFields;
    private long lastLineStart;
    private long lineRollBufLen = 4 * 1024L;
    private long lineRollBufPtr = Unsafe.malloc(lineRollBufLen);
    private boolean header;
    private long lastQuotePos = -1;

    public DelimitedTextParser(TypeProbeCollection typeProbeCollection) {
        this.mel = new MetadataExtractorListener(mPool, typeProbeCollection);
    }

    public void analyseStructure(long addr, int len, int lineCountLimit, InputAnalysisListener ial, boolean forceHeader) {
        this.schema.parse();
        mel.of(schema, forceHeader);
        parse(addr, len, lineCountLimit, mel);
        mel.onLineCount(lineCount);
        ial.onMetadata(mel.getMetadata(), mel.isHeader());
        setHeader(mel.isHeader());
        restart();
    }

    @Override
    public final void clear() {
        restart();
        this.fields.clear();
        this.calcFields = true;
        this.csPool.clear();
        this.mPool.clear();
        this.mel.clear();
    }

    @Override
    public void close() {
        if (lineRollBufPtr != 0) {
            Unsafe.free(lineRollBufPtr, lineRollBufLen);
            lineRollBufPtr = 0;
        }
        schema.close();
    }

    public int getLineCount() {
        return lineCount;
    }

    public DelimitedTextParser of(char separator) {
        clear();
        this.separator = separator;
        return this;
    }

    public void parse(long lo, long len, int lineCountLimit, Listener listener) {
        this.listener = listener;
        this.fieldHi = useLineRollBuf ? lineRollBufCur : (this.fieldLo = lo);
        parse(lo, len, lineCountLimit);
    }

    public void parseLast() {
        if (useLineRollBuf) {
            if (inQuote) {
                listener.onError(lineCount);
            } else {
                this.fieldHi++;
                stashField();
                triggerLine(0);
            }
        }
    }

    public final void restart() {
        this.fieldLo = 0;
        this.eol = false;
        this.fieldIndex = 0;
        this.inQuote = false;
        this.delayedOutQuote = false;
        this.lineCount = 0;
        this.lineRollBufCur = lineRollBufPtr;
        this.useLineRollBuf = false;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    public void setSchemaText(CharSequence schema) {
        if (schema != null) {
            this.schema.setText(schema);
        }
    }

    private void calcField() {
        if (fields.size() == fieldIndex) {
            fields.add(csPool.next());
        }
    }

    private void growRollBuf(long len) {
        LOG.info().$("Resizing line roll buffer: ").$(lineRollBufLen).$(" -> ").$(len).$();
        long p = Unsafe.malloc(len);
        long l = lineRollBufCur - lineRollBufPtr;
        if (l > 0) {
            Unsafe.getUnsafe().copyMemory(lineRollBufPtr, p, l);
        }
        Unsafe.free(lineRollBufPtr, lineRollBufLen);
        shift(lineRollBufPtr - p);
        lineRollBufCur = p + l;
        lineRollBufPtr = p;
        lineRollBufLen = len;
    }

    private void ignoreEolOnce() {
        eol = true;
        fieldIndex = 0;
        ignoreEolOnce = false;
    }

    private void parse(long lo, long len, int lineCountLimit) {
        long hi = lo + len;
        long ptr = lo;

        OUT:
        while (ptr < hi) {
            byte c = Unsafe.getUnsafe().getByte(ptr++);

            if (useLineRollBuf) {
                putToRollBuf(c);
            }

            this.fieldHi++;

            if (delayedOutQuote && c != '"') {
                inQuote = delayedOutQuote = false;
            }

            if (c == separator) {
                if (eol) {
                    uneol(lo);
                }

                if (inQuote || ignoreEolOnce) {
                    continue;
                }
                stashField();
                fieldIndex++;
            } else {
                switch (c) {
                    case '"':
                        quote();
                        break;
                    case '\r':
                    case '\n':

                        if (inQuote) {
                            break;
                        }

                        if (eol) {
                            this.fieldLo = this.fieldHi;
                            break;
                        }

                        stashField();

                        if (ignoreEolOnce) {
                            ignoreEolOnce();
                            break;
                        }

                        triggerLine(ptr);

                        if (lineCount > lineCountLimit) {
                            break OUT;
                        }
                        break;
                    default:
                        if (eol) {
                            uneol(lo);
                        }
                        break;
                }
            }
        }

        if (useLineRollBuf) {
            return;
        }

        if (eol) {
            this.fieldLo = 0;
        } else {
            rollLine(lo, hi);
            useLineRollBuf = true;
        }
    }

    private void putToRollBuf(byte c) {
        if (lineRollBufCur - lineRollBufPtr == lineRollBufLen) {
            growRollBuf(lineRollBufLen << 2);
        }
        Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
    }

    private void quote() {
        if (inQuote) {
            delayedOutQuote = !delayedOutQuote;
            lastQuotePos = this.fieldHi;
        } else if (fieldHi - fieldLo == 1) {
            inQuote = true;
            this.fieldLo = this.fieldHi;
        }
    }

    private void rollLine(long lo, long hi) {
        long l = hi - lo - lastLineStart;
        if (l >= lineRollBufLen) {
            growRollBuf(l << 2);
        }
        assert lo + lastLineStart + l <= hi;
        Unsafe.getUnsafe().copyMemory(lo + lastLineStart, lineRollBufPtr, l);
        lineRollBufCur = lineRollBufPtr + l;
        shift(lo + lastLineStart - lineRollBufPtr);
    }

    private void shift(long d) {
        for (int i = 0; i < fieldIndex; i++) {
            fields.getQuick(i).lshift(d);
        }
        this.fieldLo -= d;
        this.fieldHi -= d;
        if (lastQuotePos > -1) {
            this.lastQuotePos -= d;
        }
    }

    private void stashField() {
        if (calcFields) {
            calcField();
        }

        if (fieldIndex >= fields.size()) {
            listener.onError(lineCount++);
            ignoreEolOnce = true;
            fieldIndex = 0;
            return;
        }

        DirectByteCharSequence seq = fields.getQuick(fieldIndex);

        if (lastQuotePos > -1) {
            seq.of(this.fieldLo, lastQuotePos - 1);
            lastQuotePos = -1;
        } else {
            seq.of(this.fieldLo, this.fieldHi - 1);
        }

        this.fieldLo = this.fieldHi;
    }

    private void triggerLine(long ptr) {
        if (calcFields) {
            calcFields = false;
            listener.onFieldCount(fields.size());
        }

        int hi = fieldIndex + 1;

        if (header) {
            listener.onHeader(fields, hi);
            header = false;
            fieldIndex = 0;
            eol = true;
            return;
        }

        listener.onFields(lineCount++, fields, hi);
        fieldIndex = 0;
        eol = true;

        if (useLineRollBuf) {
            useLineRollBuf = false;
            lineRollBufCur = lineRollBufPtr;
            this.fieldLo = this.fieldHi = ptr;
        }
    }

    private void uneol(long lo) {
        eol = false;
        this.lastLineStart = this.fieldLo - lo;
    }
}
