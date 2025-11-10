/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode.DECIMAL_INVALID_SCALE;

/**
 * Parses the binary decimal format used in ILP.
 * <br>
 * A decimal is laid out as follows:
 * <pre>
 * +--------+--------+------------+
 * | scale  |  len   |   values   |
 * +--------+--------+------------+
 * | 1 byte | 1 byte | $len bytes |
 * </pre>
 * <p>
 * Values is the unscaled value of the decimal in big-endian two's complement format.
 */
public class DecimalBinaryFormatParser implements QuietCloseable {
    private final IntList unscaledValues = new IntList();
    private int len;
    private int nextBinaryPartExpectSize = 1;
    private int scale;
    private ParserState state;

    @Override
    public void close() {
        nextBinaryPartExpectSize = 1;
        state = ParserState.SCALE;
    }

    public int getNextExpectSize() {
        return nextBinaryPartExpectSize;
    }

    /**
     * Load the parsed decimal into a Decimal256
     *
     * @return true if decimal is fully loaded, false if the unscaled value was too big for Decimal256
     */
    public boolean load(Decimal256 decimal256) {
        // Fast path when no values are present in the unscaled value
        if (len == 0) {
            decimal256.ofNull();
            return true;
        }

        int size = unscaledValues.size();
        // A Decimal256 can only hold 256 bits, so we can't load more than that
        for (int i = 0, n = size - 8; i < n; i++) {
            if (unscaledValues.getQuick(i) != 0) {
                return false;
            }
        }

        long sign = unscaledValues.getQuick(0) < 0 ? -1 : 0;
        long hh = sign;
        long hl = sign;
        long lh = sign;
        long ll = sign;

        // The values are stored in big-endian format, so we need to reverse the order
        switch ((size - 1) & 0b111) {
            case 7:
                hh = (long) unscaledValues.getQuick(size - 8) << 32;
                // fall through
            case 6:
                hh = (hh & 0xFFFFFFFF00000000L) | (unscaledValues.getQuick(size - 7) & 0xFFFFFFFFL);
                // fall through
            case 5:
                hl = (long) unscaledValues.getQuick(size - 6) << 32;
                // fall through
            case 4:
                hl = (hl & 0xFFFFFFFF00000000L) | (unscaledValues.getQuick(size - 5) & 0xFFFFFFFFL);
                // fall through
            case 3:
                lh = (long) unscaledValues.getQuick(size - 4) << 32;
                // fall through
            case 2:
                lh = (lh & 0xFFFFFFFF00000000L) | (unscaledValues.getQuick(size - 3) & 0xFFFFFFFFL);
                // fall through
            case 1:
                ll = (long) unscaledValues.getQuick(size - 2) << 32;
                // fall through
            case 0:
                ll = (ll & 0xFFFFFFFF00000000L) | (unscaledValues.getQuick(size - 1) & 0xFFFFFFFFL);
        }

        decimal256.of(hh, hl, lh, ll, scale);
        return true;
    }

    public boolean processNextBinaryPart(long addr) throws ParseException {
        switch (state) {
            case SCALE -> {
                scale = Unsafe.getUnsafe().getByte(addr);
                if (scale < 0 || scale > Decimals.MAX_SCALE) {
                    throw ParseException.invalidScale();
                }
                state = ParserState.LEN;
                nextBinaryPartExpectSize = 1;
                return false;
            }
            case LEN -> {
                len = Unsafe.getUnsafe().getByte(addr);
                if (len > 0) {
                    state = ParserState.VALUES;
                    nextBinaryPartExpectSize = len;
                    return false;
                }
                state = ParserState.FINISH;
                nextBinaryPartExpectSize = 1;
                return true;
            }
            case VALUES -> {
                final int nints = (len + 3) / 4;
                unscaledValues.clear(nints);
                unscaledValues.extendAndSet(nints - 1, 0);
                int intIndex = nints - 1;
                for (int i = len; i >= 4; i -= 4) {
                    int value = Unsafe.getUnsafe().getInt(addr + i - 4);
                    // Convert from little endian to big endian
                    value = Integer.reverseBytes(value);
                    unscaledValues.setQuick(intIndex--, value);
                }
                int remaining = len & 0b11;
                if (remaining != 0) {
                    switch (remaining) {
                        case 1:
                            unscaledValues.setQuick(0, Unsafe.getUnsafe().getByte(addr));
                            break;
                        case 2: {
                            short s = Short.reverseBytes(Unsafe.getUnsafe().getShort(addr));
                            unscaledValues.setQuick(0, s);
                            break;
                        }
                        case 3: {
                            short s = Short.reverseBytes(Unsafe.getUnsafe().getShort(addr));
                            int value = s << 8 | (Unsafe.getUnsafe().getByte(addr + 2) & 0xFF);
                            unscaledValues.setQuick(0, value);
                            break;
                        }
                    }
                }
                state = ParserState.FINISH;
                nextBinaryPartExpectSize = 1;
                return true;
            }
            default -> {
                assert false;
                return false;
            }
        }
    }

    public void reset() {
        nextBinaryPartExpectSize = 1;
        state = ParserState.SCALE;
        unscaledValues.clear();
    }

    private enum ParserState {
        SCALE,
        LEN,
        VALUES,
        FINISH
    }

    public static class ParseException extends Exception {
        private static final ThreadLocal<ParseException> tlException = new ThreadLocal<>(ParseException::new);
        private LineTcpParser.ErrorCode errorCode;

        public static @NotNull ParseException invalidScale() {
            return tlException.get().errorCode(DECIMAL_INVALID_SCALE);
        }

        public ParseException errorCode(LineTcpParser.ErrorCode errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public LineTcpParser.ErrorCode errorCode() {
            return errorCode;
        }
    }
}
