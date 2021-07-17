/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class TextLexer implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(TextLexer.class);
    private final ObjList<DirectByteCharSequence> fields = new ObjList<>();
    private final ObjectPool<DirectByteCharSequence> csPool;
    private final TextMetadataDetector metadataDetector;
    private final int lineRollBufLimit;
    private CharSequence tableName;
    private boolean ignoreEolOnce;
    private long lineRollBufCur;
    private Listener textLexerListener;
    private long lastLineStart;
    private int lineRollBufLen;
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
    private byte columnDelimiter;
    private boolean inQuote;
    private boolean delayedOutQuote;
    private long fieldLo;
    private long fieldHi;
    private boolean skipLinesWithExtraValues;

    public TextLexer(TextConfiguration textConfiguration, TypeManager typeManager) {
        this.metadataDetector = new TextMetadataDetector(typeManager, textConfiguration);
        this.csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, textConfiguration.getTextLexerStringPoolCapacity());
        this.lineRollBufLen = textConfiguration.getRollBufferSize();
        this.lineRollBufLimit = textConfiguration.getRollBufferLimit();
        this.lineRollBufPtr = Unsafe.malloc(lineRollBufLen);
    }

    public void analyseStructure(
            long lo,
            long hi,
            int lineCountLimit,
            boolean forceHeader,
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> types
    ) {
        metadataDetector.of(names, types, forceHeader);
        parse(lo, hi, lineCountLimit, metadataDetector);
        metadataDetector.evaluateResults(lineCount, errorCount);
        restart(isHeaderDetected());
    }

    @Override
    public final void clear() {
        restart(false);
        this.fields.clear();
        this.csPool.clear();
        this.metadataDetector.clear();
        errorCount = 0;
        fieldMax = -1;
    }

    @Override
    public void close() {
        if (lineRollBufPtr != 0) {
            Unsafe.free(lineRollBufPtr, lineRollBufLen);
            lineRollBufPtr = 0;
        }
        metadataDetector.close();
    }

    public long getErrorCount() {
        return errorCount;
    }

    public long getLineCount() {
        return lineCount;
    }

    public boolean isSkipLinesWithExtraValues() {
        return skipLinesWithExtraValues;
    }

    public void setSkipLinesWithExtraValues(boolean skipLinesWithExtraValues) {
        this.skipLinesWithExtraValues = skipLinesWithExtraValues;
    }

    public void of(byte columnDelimiter) {
        clear();
        this.columnDelimiter = columnDelimiter;
    }

    public void parse(long lo, long hi, int lineCountLimit, Listener textLexerListener) {
        this.textLexerListener = textLexerListener;
        this.fieldHi = useLineRollBuf ? lineRollBufCur : (this.fieldLo = lo);
        this.lineCountLimit = lineCountLimit;
        parse(lo, hi);
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

    private void addField() {
        fields.add(csPool.next());
        fieldMax++;
    }

    private void checkEol(long lo) {
        if (eol) {
            uneol(lo);
        }
    }

    private void clearRollBuffer(long ptr) {
        useLineRollBuf = false;
        lineRollBufCur = lineRollBufPtr;
        this.fieldLo = this.fieldHi = ptr;
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
        LogRecord logRecord = LOG.error().$("extra fields [table=").$(tableName).$(", fieldIndex=").$(fieldIndex).$(", fieldMax=").$(fieldMax).$("]\n\t").$(lineCount).$(" -> ");
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

    ObjList<CharSequence> getColumnNames() {
        return metadataDetector.getColumnNames();
    }

    ObjList<TypeAdapter> getColumnTypes() {
        return metadataDetector.getColumnTypes();
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
        LOG.info().$("resizing ").$(lineRollBufLen).$(" -> ").$(len).$(" [table=").$(tableName).$(']').$();
        long p = Unsafe.malloc(len);
        long l = lineRollBufCur - lineRollBufPtr;
        if (l > 0) {
            Vect.memcpy(lineRollBufPtr, p, l);
        }
        Unsafe.free(lineRollBufPtr, lineRollBufLen);
        if (updateFields) {
            shift(lineRollBufPtr - p);
        }
        lineRollBufCur = p + l;
        lineRollBufPtr = p;
        lineRollBufLen = len;
        return true;
    }

    private void growRollBufAndPut(byte c) {
        if (growRollBuf(lineRollBufLen + 1, true)) {
            Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
        }
    }

    private void ignoreEolOnce() {
        eol = true;
        fieldIndex = 0;
        ignoreEolOnce = false;
    }

    boolean isHeaderDetected() {
        return metadataDetector.isHeader();
    }

    private void onColumnDelimiter(long lo) {
        checkEol(lo);

        if (inQuote || ignoreEolOnce) {
            return;
        }
        stashField(fieldIndex++);
    }

    private void onLineEnd(long ptr) throws LineLimitException {
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

    private void onQuote() {
        if (inQuote) {
            delayedOutQuote = !delayedOutQuote;
            lastQuotePos = this.fieldHi;
        } else if (fieldHi - fieldLo == 1) {
            inQuote = true;
            this.fieldLo = this.fieldHi;
        }
    }

    private void parse(long lo, long hi) {
        long ptr = lo;

        try {
            while (ptr < hi) {
                final byte c = Unsafe.getUnsafe().getByte(ptr++);

                if (rollBufferUnusable) {
                    eol(ptr, c);
                    continue;
                }

                if (useLineRollBuf) {
                    putToRollBuf(c);
                    if (rollBufferUnusable) {
                        continue;
                    }
                }

                this.fieldHi++;

                if (delayedOutQuote && c != '"') {
                    inQuote = delayedOutQuote = false;
                }

                if (c == columnDelimiter) {
                    onColumnDelimiter(lo);
                } else if (c == '"') {
                    onQuote();
                } else if (c == '\n' || c == '\r') {
                    onLineEnd(ptr);
                } else {
                    checkEol(lo);
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
        if (lineRollBufCur - lineRollBufPtr == lineRollBufLen) {
            growRollBufAndPut(c);
        } else {
            Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
        }
    }

    private void rollLine(long lo, long hi) {
        // lastLineStart is an offset from 'lo'
        // 'lo' is the address of incoming buffer
        int l = (int) (hi - lo - lastLineStart);
        if (l < lineRollBufLen || growRollBuf(l, false)) {
            assert lo + lastLineStart + l <= hi;
            Vect.memcpy(lo + lastLineStart, lineRollBufPtr, l);
            lineRollBufCur = lineRollBufPtr + l;
            shift(lo + lastLineStart - lineRollBufPtr);
        }
    }

    void setTableName(CharSequence tableName) {
        this.tableName = tableName;
        this.metadataDetector.setTableName(tableName);
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
        if (lineCount == 0 && fieldIndex >= fields.size()) {
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

    private static final class LineLimitException extends Exception {
        private static final LineLimitException INSTANCE = new LineLimitException();
    }
}
