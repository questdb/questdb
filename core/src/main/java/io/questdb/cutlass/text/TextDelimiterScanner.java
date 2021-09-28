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

import io.questdb.cutlass.http.ex.NotEnoughLinesException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import java.io.Closeable;
import java.util.Arrays;

public class TextDelimiterScanner implements Closeable {
    private static final Log LOG = LogFactory.getLog(TextDelimiterScanner.class);
    private static final byte[] priorities = new byte[Byte.MAX_VALUE + 1];
    private static final double DOUBLE_TOLERANCE = 0.05d;
    private static final byte[] potentialDelimiterBytes = new byte[256];

    static {
        configurePriority((byte) ',', (byte) 10);
        configurePriority((byte) '\t', (byte) 10);
        configurePriority((byte) '|', (byte) 10);
        configurePriority((byte) ':', (byte) 9);
        configurePriority((byte) ' ', (byte) 8);
        configurePriority((byte) ';', (byte) 8);

        Arrays.fill(potentialDelimiterBytes, (byte) 0);
        for (int i = 1; i < '0'; i++) {
            potentialDelimiterBytes[i] = 1;
        }
        for (int i = '9' + 1; i < 'A'; i++) {
            potentialDelimiterBytes[i] = 1;
        }

        for (int i = 'Z'; i < 'a'; i++) {
            potentialDelimiterBytes[i] = 1;
        }

        for (int i = 'z' + 1; i < 255; i++) {
            potentialDelimiterBytes[i] = 1;
        }
    }

    private final long matrix;
    private final int matrixSize;
    private final int matrixRowSize;
    private final int lineCountLimit;
    private final double maxRequiredDelimiterStdDev;
    private final double maxRequiredLineLengthStdDev;
    private CharSequence tableName;

    public TextDelimiterScanner(TextConfiguration configuration) {
        this.lineCountLimit = configuration.getTextAnalysisMaxLines();
        this.matrixRowSize = 256 * Integer.BYTES;
        this.matrixSize = matrixRowSize * lineCountLimit;
        this.matrix = Unsafe.malloc(this.matrixSize, MemoryTag.NATIVE_DEFAULT);
        this.maxRequiredDelimiterStdDev = configuration.getMaxRequiredDelimiterStdDev();
        this.maxRequiredLineLengthStdDev = configuration.getMaxRequiredLineLengthStdDev();
    }

    private static void configurePriority(byte value, byte priority) {
        assert value > -1;
        priorities[value] = priority;
    }

    private static byte getPriority(byte value) {
        return priorities[value];
    }

    @Override
    public void close() {
        Unsafe.free(matrix, matrixSize, MemoryTag.NATIVE_DEFAULT);
    }

    private void bumpCountAt(int line, byte bytePosition, int increment) {
        if (bytePosition > 0) {
            final long pos = matrix + ((long) line * matrixRowSize + bytePosition * Integer.BYTES);
            Unsafe.getUnsafe().putInt(pos, Unsafe.getUnsafe().getInt(pos) + increment);
        }
    }

    byte scan(long address, long hi) throws TextException {
        int lineCount = 0;
        boolean quotes = false;
        long cursor = address;
        boolean delayedClosingQuote = false;

        // bit field that has bit set for bytes
        // that occurred in this text
        long byteBitFieldLo = 0;
        long byteBitFieldHi = 0;

        int lineLen = 0;

        Vect.memset(matrix, matrixSize, 0);
        while (cursor < hi && lineCount < lineCountLimit) {
            byte b = Unsafe.getUnsafe().getByte(cursor++);

            if (delayedClosingQuote) {
                delayedClosingQuote = false;
                // this is double quote '""' situation
                if (b == '"') {
                    lineLen++;
                    continue;
                } else {
                    // last quote was genuine closing one
                    quotes = false;
                }
            }

            // ignore everything that is in quotes
            if (quotes) {
                if (b == '"') {
                    delayedClosingQuote = true;
                }
                lineLen++;
                continue;
            }

            switch (b) {
                case '\n':
                case '\r':
                    // if line doesn't have content we just ignore
                    // line end, thus skipping empty lines as well
                    if (lineLen > 0) {
                        // we are going to be storing this line length in 'A' byte
                        // we don't collect frequencies on printable characters when trying to determine
                        // column delimiter, therefore 'A' is free to use.
                        bumpCountAt(lineCount, (byte) 'A', lineLen);
                        byteBitFieldHi = byteBitFieldHi | (1L << (Byte.MAX_VALUE - 'A'));
                        lineCount++;
                        lineLen = 0;
                    }
                    continue;
                case '"':
                    lineLen++;
                    quotes = true;
                    continue;
                default:
                    lineLen++;
                    if (potentialDelimiterBytes[b & 0xff] == 1) {
                        break;
                    }
                    continue;
            }

            bumpCountAt(lineCount, b, 1);

            if (b < 64) {
                byteBitFieldLo = byteBitFieldLo | (1L << b);
            } else {
                byteBitFieldHi = byteBitFieldHi | (1L << (Byte.MAX_VALUE - b));
            }
        }

        // calculate standard deviation for every byte in the matrix
        byte delimiter = Byte.MIN_VALUE;

        if (lineCount < 2) {
            LOG.info().$("not enough lines [table=").$(tableName).$(']').$();
            throw NotEnoughLinesException.$("not enough lines [table=").put(tableName).put(']');
        }

        double lastDelimiterStdDev = Double.POSITIVE_INFINITY;
        byte lastDelimiterPriority = Byte.MIN_VALUE;
        double lastDelimiterMean = 0;
        double lineLengthStdDev = 0;

        for (int i = 0, n = Byte.MAX_VALUE + 1; i < n; i++) {
            boolean set;

            if (i < 64) {
                set = (byteBitFieldLo & (1L << i)) != 0;
            } else {
                set = (byteBitFieldHi & (1L << (Byte.MAX_VALUE - i))) != 0;
            }

            if (set) {
                long offset = i * 4L;

                // calculate mean
                long sum = 0;
                for (int l = 0; l < lineCount; l++) {
                    sum += Unsafe.getUnsafe().getInt(matrix + offset);
                    offset += matrixRowSize;
                }

                offset = i * 4L;
                final double mean = (double) sum / lineCount;
                if (mean > 0) {
                    double squareSum = 0.0;
                    for (int l = 0; l < lineCount; l++) {
                        double x = Unsafe.getUnsafe().getInt(matrix + offset) - mean;
                        squareSum += x * x;
                        offset += matrixRowSize;
                    }

                    final double stdDev = Math.sqrt(squareSum / lineCount);
                    if (i == 'A') {
                        lineLengthStdDev = stdDev;
                        continue;
                    }

                    final byte thisPriority = getPriority((byte) i);

                    // when stddev of this is less than last - use this
                    // when stddev of this is the same as last then
                    //    choose on priority (higher is better)
                    //    when priority is the same choose on mean (higher is better
                    if (stdDev < lastDelimiterStdDev
                            || (
                            (Math.abs(stdDev - lastDelimiterStdDev) < DOUBLE_TOLERANCE)
                                    &&
                                    (lastDelimiterPriority < thisPriority || lastDelimiterPriority == thisPriority && lastDelimiterMean > mean)
                    )) {
                        lastDelimiterStdDev = stdDev;
                        lastDelimiterPriority = thisPriority;
                        lastDelimiterMean = mean;
                        delimiter = (byte) i;
                    }
                }
            }
        }

        // exclude '.' as delimiter
        if (delimiter != '.' && lastDelimiterStdDev < maxRequiredDelimiterStdDev) {
            LOG.info()
                    .$("scan result [table=`").$(tableName)
                    .$("`, delimiter='").$((char) delimiter)
                    .$("', priority=").$(lastDelimiterPriority)
                    .$(", mean=").$(lastDelimiterMean)
                    .$(", stddev=").$(lastDelimiterStdDev)
                    .$(']').$();

            return delimiter;
        }

        // it appears we could not establish delimiter
        // we could treat input as single column of data if line length change stays within tolerance
        if (lineLengthStdDev < maxRequiredLineLengthStdDev) {
            return ',';
        }

        LOG.info()
                .$("min deviation is too high [delimiterStdDev=").$(lastDelimiterStdDev)
                .$(", delimiterMaxStdDev=").$(maxRequiredDelimiterStdDev)
                .$(", lineLengthStdDev=").$(lineLengthStdDev)
                .$(", lineLengthMaxStdDev=").$(maxRequiredLineLengthStdDev)
                .$(", lineCountLimit=").$(lineCountLimit)
                .$(", lineCount=").$(lineCount)
                .$(']').$();

        throw TextException.$("min deviation is too high [delimiterStdDev=")
                .put(lastDelimiterStdDev)
                .put(", delimiterMaxStdDev=").put(maxRequiredDelimiterStdDev)
                .put(", lineLengthStdDev=").put(lineLengthStdDev)
                .put(", lineLengthMaxStdDev=").put(maxRequiredLineLengthStdDev)
                .put(", lineCountLimit=").put(lineCountLimit)
                .put(", lineCount=").put(lineCount)
                .put(']');
    }

    void setTableName(CharSequence tableName) {
        this.tableName = tableName;
    }
}
