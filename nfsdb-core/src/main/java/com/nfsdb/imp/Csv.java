/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.imp;

import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Unsafe;
import org.jetbrains.annotations.NotNull;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class Csv {
    private final List<ColumnMetadata> meta = new ArrayList<>();
    private Listener listener;
    private boolean inQuote;
    private boolean delayedOutQuote;
    private boolean eol;
    private int fieldIndex;
    private DirectCharSequence fields[];
    private boolean calcFields;
    private long lo;
    private long hi;
    private int lineCount;
    private boolean useLineRollBuf = false;
    private long lastLineStart;
    private long lineRollBufLen = 1024;
    private final long lineRollBufPtr = Unsafe.getUnsafe().allocateMemory(lineRollBufLen);
    private long lineRollBufCur;
    private boolean header;
    private boolean ignoreEolOnce;
    private FieldTypeDetector typeDetector;

    public int getLineCount() {
        return lineCount;
    }

    public void parse(File file, long bufSize, boolean detect, boolean header, Listener listener) throws IOException {
        this.header = header;
        this.listener = listener;
        this.reset();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                long size = channel.size();
                long p = 0;
                while (p < size) {
                    MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, p, size - p < bufSize ? size - p : bufSize);
                    try {
                        if (p == 0 && detect) {
                            detect(buf);
                        }
                        p += buf.remaining();
                        parse(((DirectBuffer) buf).address(), buf.remaining(), Integer.MAX_VALUE);
                    } finally {
                        ByteBuffers.release(buf);
                    }
                }
                listener.onFieldCount(lineCount);
            }
        }
    }

    public void parse(long lo, long len, int maxLine) {
        this.hi = useLineRollBuf ? lineRollBufCur : (this.lo = lo);
        long hi = lo + len;
        long ptr = lo;

        OUT:
        while (ptr < hi) {
            byte c = Unsafe.getUnsafe().getByte(ptr++);

            if (useLineRollBuf) {
                Unsafe.getUnsafe().putByte(lineRollBufCur++, c);
            }

            this.hi++;

            if (delayedOutQuote && c != '"') {
                inQuote = delayedOutQuote = false;
            }

            switch (c) {
                case '"':
                    quote();
                    break;
                case ',':
                case '\t':

                    if (eol) {
                        uneol(lo);
                    }

                    if (inQuote || ignoreEolOnce) {
                        break;
                    }
                    stashField();
                    fieldIndex++;
                    break;
                case '\r':
                case '\n':

                    if (inQuote) {
                        break;
                    }

                    if (eol) {
                        this.lo = this.hi;
                        break;
                    }

                    stashField();

                    if (ignoreEolOnce) {
                        ignoreEolOnce();
                        break;
                    }

                    triggerLine();

                    if (useLineRollBuf) {
                        useLineRollBuf = false;
                        lineRollBufCur = lineRollBufPtr;
                        this.lo = this.hi = ptr;
                    }

                    fieldIndex = 0;
                    eol = true;
                    if (lineCount > maxLine) {
                        break OUT;
                    }
                    break;
                default:
                    if (eol) {
                        uneol(lo);
                    }
            }
        }

        if (useLineRollBuf) {
            return;
        }

        if ((fieldIndex > 0 && fieldIndex < fields.length - 1) || this.lo < this.hi) {
            rollLine(lo, hi);
            useLineRollBuf = true;
        } else {
            this.lo = 0;
        }
    }

    public void reset() {
        reset0();
        this.typeDetector = null;
        this.fields = null;
        this.calcFields = true;
        meta.clear();
    }

    private void calcField() {
        if (fields == null || fields.length == fieldIndex) {
            DirectCharSequence sa[] = new DirectCharSequence[fieldIndex + 1];
            if (fields != null) {
                System.arraycopy(fields, 0, sa, 0, fieldIndex);
            }
            sa[fieldIndex] = new DirectCharSequence();
            fields = sa;
        }
    }

    private void detect(ByteBuffer buf) {

        this.typeDetector = new FieldTypeDetector();
        Listener lsnr = listener;
        try {
            listener = typeDetector;
            parse(((DirectBuffer) buf).address(), buf.remaining(), 100);
            buf.clear();
            listener.onLineCount(lineCount);
            this.header = typeDetector.isHeader();
            reset0();
        } finally {
            this.listener = lsnr;
        }
    }

    private void growRollBuf(long len) {
        Unsafe.getUnsafe().freeMemory(lineRollBufPtr);
        lineRollBufCur = Unsafe.getUnsafe().allocateMemory(lineRollBufLen = len << 2);
    }

    private void ignoreEolOnce() {
        eol = true;
        fieldIndex = 0;
        ignoreEolOnce = false;
    }

    private void quote() {
        if (inQuote) {
            delayedOutQuote = !delayedOutQuote;
        } else {
            inQuote = true;
        }
    }

    private void reset0() {
        this.lo = 0;
        this.eol = false;
        this.fieldIndex = 0;
        this.inQuote = false;
        this.delayedOutQuote = false;
        this.lineCount = 0;
        this.lineRollBufCur = lineRollBufPtr;
        this.useLineRollBuf = false;
    }

    private void rollLine(long lo, long hi) {
        long l = hi - lo - lastLineStart;
        if (l > lineRollBufLen) {
            growRollBuf(l);
        }
        Unsafe.getUnsafe().copyMemory(lo + lastLineStart, lineRollBufPtr, l);
        lineRollBufCur = lineRollBufPtr + l;
        long d = lo + lastLineStart - lineRollBufPtr;
        for (int i = 0; i < fieldIndex; i++) {
            fields[i].lo -= d;
            fields[i].hi -= d;
        }
        this.lo -= d;
    }

    private void stashField() {
        if (calcFields) {
            calcField();
        }

        if (fieldIndex >= fields.length) {
            listener.onError(lineCount++);
            ignoreEolOnce = true;
            fieldIndex = 0;
            return;
        }

        DirectCharSequence seq = fields[fieldIndex];
        seq.lo = this.lo;
        seq.hi = this.hi - 1;
        this.lo = this.hi;

        if (header) {
            stashHeaderField(seq);
        }
    }

    private void stashHeaderField(CharSequence seq) {
        char[] chars = new char[seq.length()];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = seq.charAt(i);
        }
        ColumnMetadata m = new ColumnMetadata();
        m.name = new String(chars);
        if (typeDetector != null) {
            m.type = typeDetector.getTypes(fieldIndex);
        }
        meta.add(m);
    }

    private void triggerLine() {
        if (calcFields) {
            calcFields = false;
            listener.onFieldCount(fields.length);
        }

        if (header) {
            triggerNames();
            return;
        }

        for (int i = 0; i <= fieldIndex; i++) {
            listener.onField(i, fields[i], lineCount, i == fieldIndex);
        }

        lineCount++;
    }

    private void triggerNames() {
        listener.onNames(meta);
        header = false;
    }

    private void uneol(long lo) {
        eol = false;
        this.lastLineStart = this.lo - lo;
    }

    public interface Listener {
        void onError(int line);

        void onField(int index, CharSequence value, int line, boolean eol);

        void onFieldCount(int count);

        void onLineCount(int count);

        void onNames(List<ColumnMetadata> meta);
    }

    public class DirectCharSequence implements CharSequence {
        private long lo;
        private long hi;
        private StringBuilder builder;

        @Override
        public int length() {
            return (int) (hi - lo);
        }

        @Override
        public char charAt(int index) {
            return (char) Unsafe.getUnsafe().getByte(lo + index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            DirectCharSequence seq = new DirectCharSequence();
            seq.lo = this.lo + start;
            seq.hi = this.lo + end;
            return seq;
        }


        @NotNull
        @Override
        public String toString() {
            if (builder == null) {
                builder = new StringBuilder();
            } else {
                builder.setLength(0);
            }
            return builder.append(this).toString();
        }
    }
}
