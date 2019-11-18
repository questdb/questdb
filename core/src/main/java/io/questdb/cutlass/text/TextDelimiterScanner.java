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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;

import java.io.Closeable;

public class TextDelimiterScanner implements Closeable {
    private static final Log LOG = LogFactory.getLog(TextDelimiterScanner.class);
    private static final byte[] priorities = new byte[Byte.MAX_VALUE + 1];
    private static final double DOUBLE_TOLERANCE = 0.00000001d;

    static {
        configurePriority((byte) ',', (byte) 10);
        configurePriority((byte) '\t', (byte) 10);
        configurePriority((byte) '|', (byte) 10);
        configurePriority((byte) ':', (byte) 9);
        configurePriority((byte) ' ', (byte) 8);
        configurePriority((byte) ';', (byte) 8);
    }

    private final long matrix;
    private final int matrixSize;
    private final int matrixRowSize;
    private final int lineCountLimit;
    private final double maxRequiredDelimiterStdDev;
    private CharSequence tableName;

    public TextDelimiterScanner(TextConfiguration configuration) {
        this.lineCountLimit = configuration.getTextAnalysisMaxLines();
        this.matrixRowSize = (Byte.MAX_VALUE + 1) * Integer.BYTES;
        this.matrixSize = matrixRowSize * lineCountLimit;
        this.matrix = Unsafe.malloc(this.matrixSize);
        this.maxRequiredDelimiterStdDev = configuration.getMaxRequiredDelimiterStdDev();
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
        Unsafe.free(matrix, matrixSize);
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

        boolean lineHasContent = false;

        Unsafe.getUnsafe().setMemory(matrix, matrixSize, (byte) 0);
        while (cursor < hi && lineCount < lineCountLimit) {
            byte b = Unsafe.getUnsafe().getByte(cursor++);

            if (delayedClosingQuote) {
                delayedClosingQuote = false;
                // this is double quote '""' situation
                if (b == '"') {
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
                continue;
            }

            switch (b) {
                case '\n':
                case '\r':
                    // if line doesn't have content we just ignore
                    // line end, thus skipping empty lines as well
                    if (lineHasContent) {
                        lineCount++;
                        lineHasContent = false;
                    }
                    continue;
                case '"':
                    quotes = true;
                    continue;
                default:
                    if ((b > 0 && b < '0') || (b > '9' && b < 'A') || (b > 'Z' && b < 'a') || (b > 'z')) {
                        break;
                    }
                    continue;
            }

            lineHasContent = true;
            long pos = matrix + (lineCount * matrixRowSize + b * Integer.BYTES);
            Unsafe.getUnsafe().putInt(pos, Unsafe.getUnsafe().getInt(pos) + 1);

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
            throw TextException.$("not enough lines [table=").put(tableName).put(']');
        }

        double lastStdDev = Double.MAX_VALUE;
        byte lastDelimiterPriority = Byte.MIN_VALUE;
        double lastDelimiterMean = 0;

        for (int i = 0, n = Byte.MAX_VALUE + 1; i < n; i++) {
            boolean set;

            if (i < 64) {
                set = (byteBitFieldLo & (1L << i)) != 0;
            } else {
                set = (byteBitFieldHi & (1L << (Byte.MAX_VALUE - i))) != 0;
            }

            if (set) {
                long offset = i * Integer.BYTES;

                // calculate mean
                long sum = 0;
                for (int l = 0; l < lineCount; l++) {
                    sum += Unsafe.getUnsafe().getInt(matrix + offset);
                    offset += matrixRowSize;
                }

                offset = i * Integer.BYTES;
                final double mean = (double) sum / lineCount;
                if (mean > 0) {
                    double squareSum = 0.0;
                    for (int l = 0; l < lineCount; l++) {
                        double x = Unsafe.getUnsafe().getInt(matrix + offset) - mean;
                        squareSum += x * x;
                        offset += matrixRowSize;
                    }

                    double stdDev = Math.sqrt(squareSum / lineCount);
                    final byte thisPriority = getPriority((byte) i);

                    // when stddev of this is less than last - use this
                    // when stddev of this is the same as last then
                    //    choose on priority (higher is better)
                    //    when priority is the same choose on mean (higher is better
                    if (stdDev < lastStdDev
                            || (
                            (Math.abs(stdDev - lastStdDev) < DOUBLE_TOLERANCE)
                                    &&
                                    (lastDelimiterPriority < thisPriority || lastDelimiterPriority == thisPriority && lastDelimiterMean > mean)
                    )) {
                        lastStdDev = stdDev;
                        lastDelimiterPriority = thisPriority;
                        lastDelimiterMean = mean;
                        delimiter = (byte) i;
                    }
                }
            }
        }

        assert delimiter > 0;

        if (lastStdDev < maxRequiredDelimiterStdDev) {
            LOG.info()
                    .$("scan result [table=`").$(tableName)
                    .$("`, delimiter='").$((char) delimiter)
                    .$("', priority=").$(lastDelimiterPriority)
                    .$(", mean=").$(lastDelimiterMean)
                    .$(", stddev=").$(lastStdDev)
                    .$(']').$();

            return delimiter;
        }

        LOG.info()
                .$("min deviation is too high [stddev=").$(lastStdDev)
                .$(", max=").$(maxRequiredDelimiterStdDev)
                .$(']').$();

        throw TextException.$("min deviation is too high [stddev=")
                .put(lastStdDev)
                .put(", max=").put(maxRequiredDelimiterStdDev)
                .put(']');
    }

    void setTableName(CharSequence tableName) {
        this.tableName = tableName;
    }
}
