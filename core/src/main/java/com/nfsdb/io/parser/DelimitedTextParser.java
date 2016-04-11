/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.io.parser;

import com.nfsdb.io.ImportedColumnMetadata;
import com.nfsdb.io.SchemaImpl;
import com.nfsdb.io.parser.listener.InputAnalysisListener;
import com.nfsdb.io.parser.listener.Listener;
import com.nfsdb.io.parser.listener.MetadataExtractorListener;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.DirectByteCharSequence;
import com.nfsdb.std.ObjList;
import com.nfsdb.std.ObjectPool;

public class DelimitedTextParser implements TextParser {
    private final static Log LOG = LogFactory.getLog(DelimitedTextParser.class);
    private final ObjList<DirectByteCharSequence> fields = new ObjList<>();
    private final ObjectPool<DirectByteCharSequence> csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 16);
    private final ObjectPool<ImportedColumnMetadata> mPool = new ObjectPool<>(ImportedColumnMetadata.FACTORY, 256);
    private final MetadataExtractorListener mel = new MetadataExtractorListener(mPool);
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
    private long lineRollBufPtr = Unsafe.getUnsafe().allocateMemory(lineRollBufLen);
    private boolean header;
    private long lastQuotePos = -1;

    public void analyseStructure(long addr, int len, int sampleSize, InputAnalysisListener ial) {
        this.schema.parse();
        mel.of(schema);
        parse(addr, len, sampleSize, mel);
        mel.onLineCount(lineCount);
        ial.onMetadata(mel.getMetadata());
        setHeader(mel.isHeader());
        restart();
    }

    @Override
    public int getLineCount() {
        return lineCount;
    }

    @Override
    public TextParser of(char separator) {
        clear();
        this.separator = separator;
        return this;
    }

    @Override
    public void parse(long lo, long len, int lim, Listener listener) {
        this.listener = listener;
        this.fieldHi = useLineRollBuf ? lineRollBufCur : (this.fieldLo = lo);
        parse(lo, len, lim);
    }

    @Override
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

    public void putSchema(CharSequence schema) {
        if (schema != null) {
            this.schema.put(schema);
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

    @Override
    public void setHeader(boolean header) {
        this.header = header;
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
            Unsafe.getUnsafe().freeMemory(lineRollBufPtr);
            lineRollBufPtr = 0;
        }
        schema.close();
    }

    private void calcField() {
        if (fields.size() == fieldIndex) {
            fields.add(csPool.next());
        }
    }

    private void growRollBuf(long len) {
        LOG.info().$("Resizing line roll buffer: ").$(lineRollBufLen).$(" -> ").$(len).$();
        long p = Unsafe.getUnsafe().allocateMemory(len);
        long l = lineRollBufCur - lineRollBufPtr;
        if (l > 0) {
            Unsafe.getUnsafe().copyMemory(lineRollBufPtr, p, l);
        }
        Unsafe.getUnsafe().freeMemory(lineRollBufPtr);
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

    private void parse(long lo, long len, int maxLine) {
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

                        if (lineCount > maxLine) {
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
        } else {
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
