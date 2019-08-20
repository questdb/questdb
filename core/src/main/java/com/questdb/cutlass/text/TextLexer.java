/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cutlass.text;

import com.questdb.cutlass.text.types.TypeAdapter;
import com.questdb.cutlass.text.types.TypeManager;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;
import com.questdb.std.Mutable;
import com.questdb.std.ObjList;
import com.questdb.std.ObjectPool;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class TextLexer implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(TextLexer.class);
    private final ObjList<DirectByteCharSequence> fields = new ObjList<>();
    private final ObjectPool<DirectByteCharSequence> csPool;
    private final TextMetadataDetector metadataDetector;
    private final int lineRollBufLimit;
    private boolean ignoreEolOnce;
    private byte columnDelimiter;
    private boolean inQuote;
    private boolean delayedOutQuote;
    private boolean eol;
    private int fieldIndex;
    private int fieldMax = -1;
    private long fieldLo;
    private long fieldHi;
    private long lineCount;
    private boolean useLineRollBuf = false;
    private long lineRollBufCur;
    private Listener textLexerListener;
    private long lastLineStart;
    private int lineRollBufLen;
    private long lineRollBufPtr;
    private boolean header;
    private long lastQuotePos = -1;
    private long errorCount = 0;
    private boolean rollBufferUnusable = false;
    private CharSequence tableName;

    public TextLexer(TextConfiguration textConfiguration, TypeManager typeManager) {
        this.metadataDetector = new TextMetadataDetector(typeManager, textConfiguration);
        this.csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, textConfiguration.getTextLexerStringPoolCapacity());
        this.lineRollBufLen = textConfiguration.getRollBufferSize();
        this.lineRollBufLimit = textConfiguration.getRollBufferLimit();
        this.lineRollBufPtr = Unsafe.malloc(lineRollBufLen);
    }

    public void analyseStructure(
            long address,
            int len,
            int lineCountLimit,
            boolean forceHeader,
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> types
    ) {
        metadataDetector.of(names, types, forceHeader);
        parse(address, len, lineCountLimit, metadataDetector);
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

    public long getLineCount() {
        return lineCount;
    }

    public void of(byte columnDelimiter) {
        clear();
        this.columnDelimiter = columnDelimiter;
    }

    public void parse(long lo, long len, int lineCountLimit, Listener textLexerListener) {
        this.textLexerListener = textLexerListener;
        this.fieldHi = useLineRollBuf ? lineRollBufCur : (this.fieldLo = lo);
        parse(lo, len, lineCountLimit);
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

    private void clearRollBuffer(long ptr) {
        useLineRollBuf = false;
        lineRollBufCur = lineRollBufPtr;
        this.fieldLo = this.fieldHi = ptr;
    }

    ObjList<CharSequence> getColumnNames() {
        return metadataDetector.getColumnNames();
    }

    ObjList<TypeAdapter> getColumnTypes() {
        return metadataDetector.getColumnTypes();
    }

    private boolean growRollBuf(int requiredLength, boolean updateFields) {
        if (requiredLength > lineRollBufLimit) {
            // todo: log content of roll buffer
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
            Unsafe.getUnsafe().copyMemory(lineRollBufPtr, p, l);
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

    private void ignoreEolOnce() {
        eol = true;
        fieldIndex = 0;
        ignoreEolOnce = false;
    }

    boolean isHeaderDetected() {
        return metadataDetector.isHeader();
    }

    private void parse(long lo, long len, int lineCountLimit) {
        long hi = lo + len;
        long ptr = lo;

        OUT:
        while (ptr < hi) {
            byte c = Unsafe.getUnsafe().getByte(ptr++);

            if (rollBufferUnusable) {
                if (c == '\n' || c == '\r') {
                    eol = true;
                    rollBufferUnusable = false;
                    clearRollBuffer(ptr);
                    fieldIndex = 0;
                    lineCount++;
                }
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
                if (eol) {
                    uneol(lo);
                }

                if (inQuote || ignoreEolOnce) {
                    continue;
                }
                stashField(fieldIndex++);
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

                        stashField(fieldIndex);

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
            if (growRollBuf(lineRollBufLen + 1, true)) {
                Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
            }
        } else {
            Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
        }
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

    private void reportExtraFields() {
        LogRecord logRecord = LOG.error().$("extra fields [table=").$(tableName).$("]\n\t").$(lineCount).$(" -> ");
        for (int i = 0, n = fields.size(); i < n; i++) {
            if (i > 0) {
                logRecord.$(',');
            }
            logRecord.$(fields.getQuick(i));
        }
        logRecord.$(" ...").$();

        errorCount++;
        ignoreEolOnce = true;
        fieldIndex = 0;
    }

    private void rollLine(long lo, long hi) {
        // lastLineStart is an offset from 'lo'
        // 'lo' is the address of incoming buffer
        int l = (int) (hi - lo - lastLineStart);
        if (l < lineRollBufLen || growRollBuf(l, false)) {
            assert lo + lastLineStart + l <= hi;
            Unsafe.getUnsafe().copyMemory(lo + lastLineStart, lineRollBufPtr, l);
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
            fields.getQuick(i).lshift(d);
        }
        this.fieldLo -= d;
        this.fieldHi -= d;
        if (lastQuotePos > -1) {
            this.lastQuotePos -= d;
        }
    }

    private void stashField(int fieldIndex) {
        if (lineCount == 0) {
            fields.add(csPool.next());
            fieldMax++;
        }

        if (fieldIndex > fieldMax) {
            reportExtraFields();
            return;
        }

        final DirectByteCharSequence seq = fields.getQuick(fieldIndex);

        if (lastQuotePos > -1) {
            seq.of(this.fieldLo, lastQuotePos - 1);
            lastQuotePos = -1;
        } else {
            seq.of(this.fieldLo, this.fieldHi - 1);
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
}
