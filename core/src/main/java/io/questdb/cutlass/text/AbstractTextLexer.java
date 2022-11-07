/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.text;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public abstract class AbstractTextLexer implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(AbstractTextLexer.class);
    private final ObjList<DirectByteCharSequence> fields = new ObjList<>();
    private final ObjectPool<DirectByteCharSequence> csPool;
    private final int lineRollBufLimit;
    private CharSequence tableName;
    private boolean ignoreEolOnce;
    private long lineRollBufCur;
    private Listener textLexerListener;
    private long lastLineStart;
    private int lineRollBufSize;
    private long lineRollBufPtr;
    private boolean header;
    private long lastQuotePos = -1;
    private long errorCount = 0;
    private int lineCountLimit;
    private int fieldMax = -1;
    private int fieldIndex;
    private long lineCount;
    private boolean eol;
    private boolean useLineRollBuf = false;
    private boolean rollBufferUnusable = false;
    private boolean inQuote;
    private boolean delayedOutQuote;
    private long fieldLo;
    private long fieldHi;
    private boolean skipLinesWithExtraValues;

    public AbstractTextLexer(TextConfiguration textConfiguration) {
        this.csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, textConfiguration.getTextLexerStringPoolCapacity());
        this.lineRollBufSize = textConfiguration.getRollBufferSize();
        this.lineRollBufLimit = textConfiguration.getRollBufferLimit();
        this.lineRollBufPtr = Unsafe.malloc(lineRollBufSize, MemoryTag.NATIVE_TEXT_PARSER_RSS);
    }

    @Override
    public void clear() {
        restart(false);
        this.fields.clear();
        this.csPool.clear();
        errorCount = 0;
        fieldMax = -1;
    }

    @Override
    public void close() {
        if (lineRollBufPtr != 0) {
            Unsafe.free(lineRollBufPtr, lineRollBufSize, MemoryTag.NATIVE_TEXT_PARSER_RSS);
            lineRollBufPtr = 0;
        }
    }

    public long getErrorCount() {
        return errorCount;
    }

    public long getLineCount() {
        return lineCount;
    }

    public void parse(long lo, long hi, int lineCountLimit, Listener textLexerListener) {
        setupLimits(lineCountLimit, textLexerListener);
        parse(lo, hi);
    }

    public void parse(long lo, long hi) {
        this.fieldHi = useLineRollBuf ? lineRollBufCur : (this.fieldLo = lo);
        parse0(lo, hi);
    }

    public void parseExactLines(long lo, long hi) {
        this.fieldHi = this.fieldLo = lo;
        long ptr = lo;

        try {
            while (ptr < hi) {
                final byte c = Unsafe.getUnsafe().getByte(ptr++);
                this.fieldHi++;
                if (delayedOutQuote && c != '"') {
                    inQuote = delayedOutQuote = false;
                }
                doSwitch(lo, hi, c);
            }
        } catch (LineLimitException ignore) {
            // loop exit
        }
    }

    public void parseLast() {
        if (useLineRollBuf) {
            if (inQuote && lastQuotePos < fieldHi) {
                errorCount++;
                LOG.info().$("quote is missing [table=").$(tableName).$(']').$();
            } else {
                this.fieldHi++;
                stashField(fieldIndex);
                triggerLine(0);
            }
        }
    }

    public final void restart(boolean header) {
        this.fieldLo = 0;
        this.eol = false;
        this.fieldIndex = 0;
        this.fieldMax = -1;
        this.inQuote = false;
        this.delayedOutQuote = false;
        this.lineCount = 0;
        this.lineRollBufCur = lineRollBufPtr;
        this.useLineRollBuf = false;
        this.rollBufferUnusable = false;
        this.header = header;
        fields.clear();
        csPool.clear();
    }

    public void setSkipLinesWithExtraValues(boolean skipLinesWithExtraValues) {
        this.skipLinesWithExtraValues = skipLinesWithExtraValues;
    }

    public void setupBeforeExactLines(Listener textLexerListener) {
        this.textLexerListener = textLexerListener;
        this.lineCountLimit = Integer.MAX_VALUE;
    }

    public void setupLimits(int lineCountLimit, Listener textLexerListener) {
        this.lineCountLimit = lineCountLimit;
        this.textLexerListener = textLexerListener;
    }

    private void addField() {
        fields.add(csPool.next());
        fieldMax++;
    }

    protected void checkEol(long lo) {
        if (eol) {
            uneol(lo);
        }
    }

    private boolean checkState(long ptr, byte c) {
        if (!rollBufferUnusable && !useLineRollBuf && !delayedOutQuote) {
            this.fieldHi++;
            return true;
        }
        return checkStateSlow(ptr, c);
    }

    private boolean checkStateSlow(long ptr, byte c) {
        if (rollBufferUnusable) {
            eol(ptr, c);
            return false;
        }

        if (useLineRollBuf) {
            putToRollBuf(c);
            if (rollBufferUnusable) {
                return false;
            }
        }

        this.fieldHi++;

        if (delayedOutQuote && c != '"') {
            inQuote = delayedOutQuote = false;
        }
        return true;
    }

    private void clearRollBuffer(long ptr) {
        useLineRollBuf = false;
        lineRollBufCur = lineRollBufPtr;
        this.fieldLo = this.fieldHi = ptr;
    }

    protected abstract void doSwitch(long lo, long hi, byte c) throws LineLimitException;

    private void eol(long ptr, byte c) {
        if (c == '\n' || c == '\r') {
            eol = true;
            rollBufferUnusable = false;
            clearRollBuffer(ptr);
            fieldIndex = 0;
            lineCount++;
        }
    }

    private void extraField(int fieldIndex) {
        LogRecord logRecord = LOG.error()
                .$("extra fields [table=").$(tableName)
                .$(", fieldIndex=").$(fieldIndex)
                .$(", fieldMax=").$(fieldMax)
                .$("]\n\t").$(lineCount)
                .$(" -> ");
        for (int i = 0, n = fields.size(); i < n; i++) {
            if (i > 0) {
                logRecord.$(',');
            }
            logRecord.$(fields.getQuick(i));
        }
        logRecord.$(" ...").$();

        if (skipLinesWithExtraValues) {
            errorCount++;
            ignoreEolOnce = true;
            this.fieldIndex = 0;
        } else {
            // prepare for next field
            if (lastQuotePos > -1) {
                lastQuotePos = -1;
            }
            this.fieldLo = this.fieldHi;
        }
    }

    private boolean growRollBuf(int requiredLength, boolean updateFields) {
        if (requiredLength > lineRollBufLimit) {
            LOG.info()
                    .$("too long [table=").$(tableName)
                    .$(", line=").$(lineCount)
                    .$(", requiredLen=").$(requiredLength)
                    .$(", rollLimit=").$(lineRollBufLimit)
                    .$(']').$();
            errorCount++;
            rollBufferUnusable = true;
            return false;
        }

        final int len = Math.min(lineRollBufLimit, requiredLength << 1);
        LOG.info().$("resizing ").$(lineRollBufSize).$(" -> ").$(len).$(" [table=").$(tableName).$(']').$();
        long p = Unsafe.malloc(len, MemoryTag.NATIVE_TEXT_PARSER_RSS);
        long l = lineRollBufCur - lineRollBufPtr;
        if (l > 0) {
            Vect.memcpy(p, lineRollBufPtr, l);
        }
        Unsafe.free(lineRollBufPtr, lineRollBufSize, MemoryTag.NATIVE_TEXT_PARSER_RSS);
        if (updateFields) {
            shift(lineRollBufPtr - p);
        }
        lineRollBufCur = p + l;
        lineRollBufPtr = p;
        lineRollBufSize = len;
        return true;
    }

    private void growRollBufAndPut(byte c) {
        if (growRollBuf(lineRollBufSize + 1, true)) {
            Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
        }
    }

    private void ignoreEolOnce() {
        eol = true;
        fieldIndex = 0;
        ignoreEolOnce = false;
    }

    protected void onColumnDelimiter(long lo) {
        if (!eol && !inQuote && !ignoreEolOnce && lineCount > 0 && fieldIndex < fieldMax && lastQuotePos < 0) {
            fields.getQuick(fieldIndex++).of(this.fieldLo, this.fieldHi - 1);
            this.fieldLo = this.fieldHi;
        } else {
            onColumnDelimiterSlow(lo);
        }
    }

    private void onColumnDelimiterSlow(long lo) {
        checkEol(lo);

        if (inQuote || ignoreEolOnce) {
            return;
        }
        stashFieldSlow(fieldIndex++);
    }

    protected void onLineEnd(long ptr) throws LineLimitException {
        if (inQuote) {
            return;
        }

        if (eol) {
            this.fieldLo = this.fieldHi;
            return;
        }

        stashField(fieldIndex);

        if (ignoreEolOnce) {
            ignoreEolOnce();
            return;
        }

        triggerLine(ptr);

        if (lineCount > lineCountLimit) {
            throw LineLimitException.INSTANCE;
        }
    }

    protected void onQuote() {
        if (inQuote) {
            delayedOutQuote = !delayedOutQuote;
            lastQuotePos = this.fieldHi;
        } else if (fieldHi - fieldLo == 1) {
            inQuote = true;
            this.fieldLo = this.fieldHi;
        }
    }

    private void parse0(long lo, long hi) {
        long ptr = lo;

        try {
            while (ptr < hi) {
                final byte c = Unsafe.getUnsafe().getByte(ptr++);

                if (checkState(ptr, c)) {
                    doSwitch(lo, ptr, c);
                }
            }
        } catch (LineLimitException ignore) {
            // loop exit
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
        if (lineRollBufCur - lineRollBufPtr == lineRollBufSize) {
            growRollBufAndPut(c);
        } else {
            Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
        }
    }

    private void rollLine(long lo, long hi) {
        // lastLineStart is an offset from 'lo'
        // 'lo' is the address of incoming buffer
        int l = (int) (hi - lo - lastLineStart);
        if (l < lineRollBufSize || growRollBuf(l, false)) {
            assert lo + lastLineStart + l <= hi;
            Vect.memcpy(lineRollBufPtr, lo + lastLineStart, l);
            lineRollBufCur = lineRollBufPtr + l;
            shift(lo + lastLineStart - lineRollBufPtr);
        }
    }

    void setTableName(CharSequence tableName) {
        this.tableName = tableName;
    }

    private void shift(long d) {
        for (int i = 0; i < fieldIndex; i++) {
            fields.getQuick(i).shl(d);
        }
        this.fieldLo -= d;
        this.fieldHi -= d;
        if (lastQuotePos > -1) {
            this.lastQuotePos -= d;
        }
    }

    private void stashField(int fieldIndex) {
        if (lineCount > 0 && fieldIndex <= fieldMax && lastQuotePos < 0) {
            fields.getQuick(fieldIndex).of(this.fieldLo, this.fieldHi - 1);
            this.fieldLo = this.fieldHi;
        } else {
            stashFieldSlow(fieldIndex);
        }
    }

    private void stashFieldSlow(int fieldIndex) {
        if (lineCount == 0 && fieldIndex >= fieldMax) {
            addField();
        }

        if (fieldIndex > fieldMax) {
            extraField(fieldIndex);
            return;
        }

        if (lastQuotePos > -1) {
            fields.getQuick(fieldIndex).of(this.fieldLo, lastQuotePos - 1);
            lastQuotePos = -1;
        } else {
            fields.getQuick(fieldIndex).of(this.fieldLo, this.fieldHi - 1);
        }

        this.fieldLo = this.fieldHi;
    }

    private void triggerLine(long ptr) {
        eol = true;
        fieldIndex = 0;
        if (useLineRollBuf) {
            clearRollBuffer(ptr);
        }

        if (header) {
            header = false;
            return;
        }

        textLexerListener.onFields(lineCount++, fields, fieldMax + 1);
    }

    private void uneol(long lo) {
        eol = false;
        this.lastLineStart = this.fieldLo - lo;
    }

    @FunctionalInterface
    public interface Listener {
        void onFields(long line, ObjList<DirectByteCharSequence> fields, int hi);
    }

    protected static final class LineLimitException extends Exception {
        private static final LineLimitException INSTANCE = new LineLimitException();
    }
}
