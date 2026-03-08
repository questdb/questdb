/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.SwarUtils;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

public abstract class AbstractTextLexer implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(AbstractTextLexer.class);
    private static final long MASK_CR = SwarUtils.broadcast((byte) '\r');
    private static final long MASK_NEW_LINE = SwarUtils.broadcast((byte) '\n');
    private static final long MASK_QUOTE = SwarUtils.broadcast((byte) '"');

    private final ObjectPool<DirectUtf8String> csPool;
    private final ObjList<DirectUtf8String> fields = new ObjList<>();
    private final int lineRollBufLimit;
    private boolean ascii;
    private boolean delayedOutQuote;
    private boolean eol;
    private long errorCount = 0;
    private long fieldHi;
    private int fieldIndex;
    private long fieldLo;
    private int fieldMax = -1;
    private boolean header;
    private boolean ignoreEolOnce;
    private boolean inQuote;
    private long lastLineStart;
    private long lastQuotePos = -1;
    private long lineCount;
    private int lineCountLimit;
    private long lineRollBufCur;
    private long lineRollBufPtr;
    private int lineRollBufSize;
    private boolean rollBufferUnusable = false;
    private boolean skipLinesWithExtraValues;
    private CharSequence tableName;
    private Listener textLexerListener;
    private boolean useLineRollBuf = false;

    public AbstractTextLexer(TextConfiguration textConfiguration) {
        this.csPool = new ObjectPool<>(DirectUtf8String.FACTORY, textConfiguration.getTextLexerStringPoolCapacity());
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

    public void parseLast() {
        if (useLineRollBuf) {
            if (inQuote && lastQuotePos < fieldHi) {
                errorCount++;
                LOG.info().$("quote is missing [table=").$safe(tableName).$(']').$();
            } else {
                this.fieldHi++;
                stashField(fieldIndex);
                triggerLine(0);
            }
        }
    }

    public final void restart(boolean header) {
        nextField(0);
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
        nextField(ptr);
    }

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
                .$("extra fields [table=").$safe(tableName)
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
            nextField();
        }
    }

    private boolean growRollBuf(int requiredLength, boolean updateFields) {
        if (requiredLength > lineRollBufLimit) {
            LOG.info()
                    .$("too long [table=").$safe(tableName)
                    .$(", line=").$(lineCount)
                    .$(", requiredLen=").$(requiredLength)
                    .$(", rollLimit=").$(lineRollBufLimit)
                    .$(']').$();
            errorCount++;
            rollBufferUnusable = true;
            return false;
        }

        final int len = Math.min(lineRollBufLimit, requiredLength << 1);
        LOG.info().$("resizing ").$(lineRollBufSize).$(" -> ").$(len).$(" [table=").$safe(tableName).$(']').$();
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

    private void nextField() {
        this.ascii = true;
        this.fieldLo = this.fieldHi;
    }

    private void nextField(long ptr) {
        this.ascii = true;
        this.fieldLo = this.fieldHi = ptr;
    }

    private void onColumnDelimiterSlow(long lo) {
        checkEol(lo);

        if (inQuote || ignoreEolOnce) {
            return;
        }
        stashFieldSlow(fieldIndex++);
    }

    private void parse0(long lo, long hi) {
        long ptr = lo;

        try {
            while (ptr < hi) {
                if (!eol && !rollBufferUnusable && !useLineRollBuf && !delayedOutQuote && ptr < hi - 7) {
                    long word = Unsafe.getUnsafe().getLong(ptr);
                    long zeroBytesWord = SwarUtils.markZeroBytes(word ^ MASK_NEW_LINE)
                            | SwarUtils.markZeroBytes(word ^ MASK_CR)
                            | SwarUtils.markZeroBytes(word ^ MASK_QUOTE)
                            | SwarUtils.markZeroBytes(word ^ getDelimiterMask());
                    if (zeroBytesWord == 0) {
                        ptr += 7;
                        this.fieldHi += 7;
                        this.ascii &= Utf8s.isAscii(word);
                        continue;
                    } else {
                        int firstIndex = SwarUtils.indexOfFirstMarkedByte(zeroBytesWord);
                        if (firstIndex > 0) {
                            // The firstIndex returns the byte count we have to "trust"
                            // the assumption is that these bytes will become a part of the existing fields
                            // These bytes come on LOW bits of the "word". To check that these bytes are
                            // positive, we need to isolate them. We do that by masking out the entire
                            // word, save for the bytes we intend to keep.
                            this.ascii &= Utf8s.isAscii(word & (0xffffffffffffffffL >>> (64 - firstIndex * 8)));
                            ptr += firstIndex;
                        }
                        this.fieldHi += firstIndex;
                    }
                }

                final byte b = Unsafe.getUnsafe().getByte(ptr++);
                this.ascii &= b > 0;

                if (checkState(ptr, b)) {
                    doSwitch(lo, ptr, b);
                }
            }
        } catch (LineLimitException ignore) {
            // loop exit
        }

        if (useLineRollBuf) {
            return;
        }

        if (eol) {
            nextField(0);
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
            fields.getQuick(fieldIndex).of(this.fieldLo, this.fieldHi - 1, ascii);
            nextField();
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
            fields.getQuick(fieldIndex).of(this.fieldLo, lastQuotePos - 1, ascii);
            lastQuotePos = -1;
        } else {
            fields.getQuick(fieldIndex).of(this.fieldLo, this.fieldHi - 1, ascii);
        }
        nextField();
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

    protected void checkEol(long lo) {
        if (eol) {
            uneol(lo);
        }
    }

    protected abstract void doSwitch(long lo, long hi, byte b) throws LineLimitException;

    protected abstract long getDelimiterMask();

    protected void onColumnDelimiter(long lo) {
        if (!eol && !inQuote && !ignoreEolOnce && lineCount > 0 && fieldIndex < fieldMax && lastQuotePos < 0) {
            fields.getQuick(fieldIndex++).of(fieldLo, fieldHi - 1, ascii);
            nextField();
        } else {
            onColumnDelimiterSlow(lo);
        }
    }

    protected void onLineEnd(long ptr) throws LineLimitException {
        if (inQuote) {
            return;
        }

        if (eol) {
            nextField();
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
            lastQuotePos = fieldHi;
        } else if (fieldHi - fieldLo == 1) {
            inQuote = true;
            nextField();
        }
    }

    void setTableName(CharSequence tableName) {
        this.tableName = tableName;
    }

    @FunctionalInterface
    public interface Listener {
        void onFields(long line, ObjList<DirectUtf8String> fields, int hi);
    }

    protected static final class LineLimitException extends Exception {
        private static final LineLimitException INSTANCE = new LineLimitException();
    }
}
