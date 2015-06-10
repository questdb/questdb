/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.io.parser;

import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.io.parser.listener.Listener;
import com.nfsdb.logging.Logger;
import com.nfsdb.utils.Unsafe;

public abstract class AbstractTextParser implements TextParser {
    private final static Logger LOGGER = Logger.getLogger(AbstractTextParser.class);
    protected boolean inQuote;
    protected boolean delayedOutQuote;
    protected boolean eol;
    protected int fieldIndex;
    protected long fieldLo;
    protected long fieldHi;
    protected int lineCount;
    protected boolean useLineRollBuf = false;
    protected long lineRollBufCur;
    protected boolean ignoreEolOnce;
    private Listener listener;
    private DirectByteCharSequence fields[];
    private boolean calcFields;
    private long lastLineStart;
    private long lineRollBufLen = 4 * 1024L;
    private long lineRollBufPtr = Unsafe.getUnsafe().allocateMemory(lineRollBufLen);
    private boolean header;
    private long lastQuotePos = -1;

    public AbstractTextParser() {
        reset();
    }

    @Override
    public void close() {
        Unsafe.getUnsafe().freeMemory(lineRollBufPtr);
    }

    @Override
    public int getLineCount() {
        return lineCount;
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

    @Override
    public final void reset() {
        restart();
        this.fields = null;
        this.calcFields = true;
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

    protected void ignoreEolOnce() {
        eol = true;
        fieldIndex = 0;
        ignoreEolOnce = false;
    }

    protected abstract void parse(long lo, long len, int lim);

    protected void putToRollBuf(byte c) {
        if (lineRollBufCur - lineRollBufPtr == lineRollBufLen) {
            growRollBuf(lineRollBufLen << 2);
        }
        Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
    }

    protected void quote() {
        if (inQuote) {
            delayedOutQuote = !delayedOutQuote;
            lastQuotePos = this.fieldHi;
        } else {
            inQuote = true;
            this.fieldLo = this.fieldHi;
        }
    }

    protected void rollLine(long lo, long hi) {
        long l = hi - lo - lastLineStart;
        if (l >= lineRollBufLen) {
            growRollBuf(l << 2);
        }
        assert lo + lastLineStart + l <= hi;
        Unsafe.getUnsafe().copyMemory(lo + lastLineStart, lineRollBufPtr, l);
        lineRollBufCur = lineRollBufPtr + l;
        shift(lo + lastLineStart - lineRollBufPtr);
    }

    protected void stashField() {
        if (calcFields) {
            calcField();
        }

        if (fieldIndex >= fields.length) {
            listener.onError(lineCount++);
            ignoreEolOnce = true;
            fieldIndex = 0;
            return;
        }

        DirectByteCharSequence seq = fields[fieldIndex];

        if (lastQuotePos > -1) {
            seq.init(this.fieldLo, lastQuotePos - 1);
            lastQuotePos = -1;
        } else {
            seq.init(this.fieldLo, this.fieldHi - 1);
        }

        this.fieldLo = this.fieldHi;
    }

    protected void triggerLine(long ptr) {
        if (calcFields) {
            calcFields = false;
            listener.onFieldCount(fields.length);
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

    protected void uneol(long lo) {
        eol = false;
        this.lastLineStart = this.fieldLo - lo;
    }

    private void calcField() {
        if (fields == null || fields.length == fieldIndex) {
            DirectByteCharSequence sa[] = new DirectByteCharSequence[fieldIndex + 1];
            if (fields != null) {
                System.arraycopy(fields, 0, sa, 0, fieldIndex);
            }
            sa[fieldIndex] = new DirectByteCharSequence();
            fields = sa;
        }
    }

    private void growRollBuf(long len) {
        LOGGER.warn("Resizing line roll buffer: " + lineRollBufLen + " -> " + len);
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

    private void shift(long d) {
        for (int i = 0; i < fieldIndex; i++) {
            fields[i].lshift(d);
        }
        this.fieldLo -= d;
        this.fieldHi -= d;
        if (lastQuotePos > -1) {
            this.lastQuotePos -= d;
        }
    }
}
