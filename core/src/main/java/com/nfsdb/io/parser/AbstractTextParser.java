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

package com.nfsdb.io.parser;

import com.nfsdb.io.parser.listener.Listener;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.DirectByteCharSequence;

public abstract class AbstractTextParser implements TextParser {
    private final static Log LOG = LogFactory.getLog(AbstractTextParser.class);
    boolean inQuote;
    boolean delayedOutQuote;
    boolean eol;
    int fieldIndex;
    long fieldLo;
    long fieldHi;
    int lineCount;
    boolean useLineRollBuf = false;
    long lineRollBufCur;
    boolean ignoreEolOnce;
    private Listener listener;
    private DirectByteCharSequence fields[];
    private boolean calcFields;
    private long lastLineStart;
    private long lineRollBufLen = 4 * 1024L;
    private long lineRollBufPtr = Unsafe.getUnsafe().allocateMemory(lineRollBufLen);
    private boolean header;
    private long lastQuotePos = -1;

    AbstractTextParser() {
        clear();
    }

    @Override
    public final void clear() {
        restart();
        this.fields = null;
        this.calcFields = true;
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

    void ignoreEolOnce() {
        eol = true;
        fieldIndex = 0;
        ignoreEolOnce = false;
    }

    protected abstract void parse(long lo, long len, int lim);

    void putToRollBuf(byte c) {
        if (lineRollBufCur - lineRollBufPtr == lineRollBufLen) {
            growRollBuf(lineRollBufLen << 2);
        }
        Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
    }

    void quote() {
        if (inQuote) {
            delayedOutQuote = !delayedOutQuote;
            lastQuotePos = this.fieldHi;
        } else {
            inQuote = true;
            this.fieldLo = this.fieldHi;
        }
    }

    void rollLine(long lo, long hi) {
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
            Unsafe.arrayGet(fields, i).lshift(d);
        }
        this.fieldLo -= d;
        this.fieldHi -= d;
        if (lastQuotePos > -1) {
            this.lastQuotePos -= d;
        }
    }

    void stashField() {
        if (calcFields) {
            calcField();
        }

        if (fieldIndex >= fields.length) {
            listener.onError(lineCount++);
            ignoreEolOnce = true;
            fieldIndex = 0;
            return;
        }

        DirectByteCharSequence seq = Unsafe.arrayGet(fields, fieldIndex);

        if (lastQuotePos > -1) {
            seq.of(this.fieldLo, lastQuotePos - 1);
            lastQuotePos = -1;
        } else {
            seq.of(this.fieldLo, this.fieldHi - 1);
        }

        this.fieldLo = this.fieldHi;
    }

    void triggerLine(long ptr) {
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

    void uneol(long lo) {
        eol = false;
        this.lastLineStart = this.fieldLo - lo;
    }
}
