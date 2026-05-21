/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BinarySequence;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

/**
 * Cast BINARY to VARCHAR, validating that the bytes are well-formed UTF-8.
 * Throws {@link CairoException} on the first row whose bytes do not decode
 * as UTF-8 — silently replacing invalid bytes with {@code U+FFFD} would
 * corrupt the data and mask writer bugs.
 * <p>
 * Motivating use case: parquet writers that emit string columns as
 * {@code BYTE_ARRAY} without the {@code STRING} logical type. QuestDB's
 * conservative inference assigns those columns to {@code BINARY}, which
 * blocks {@code LIKE} / {@code REGEXP_REPLACE} / aggregate functions
 * declared on {@code VARCHAR}. An explicit cast lets operators opt into
 * string semantics, with a hard error on the first non-UTF-8 row so a
 * legitimately-binary payload can't be silently misinterpreted.
 */
public class CastBinaryToVarcharFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Uø)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0), argPositions.getQuick(0));
    }

    public static class Func extends AbstractCastToVarcharFunction {
        private final int argPosition;
        private final Utf8StringSink sinkA = new Utf8StringSink();
        private final Utf8StringSink sinkB = new Utf8StringSink();

        public Func(Function arg, int argPosition) {
            super(arg);
            this.argPosition = argPosition;
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return load(rec, sinkA);
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return load(rec, sinkB);
        }

        private Utf8Sequence load(Record rec, Utf8StringSink sink) {
            final BinarySequence bin = arg.getBin(rec);
            if (bin == null) {
                return null;
            }
            final long len = bin.length();
            if (len == 0) {
                sink.clear();
                return sink;
            }
            // Reject sequences too large to fit in an int-sized sink. UTF-8
            // validation works on bytes; a 2 GB column value isn't a real
            // workload here.
            if (len > Integer.MAX_VALUE) {
                throw CairoException.nonCritical().position(argPosition)
                        .put("binary value too large to cast to varchar [len=").put(len).put(']');
            }
            sink.clear();
            // Stream-validate UTF-8 byte-by-byte using a DFA-style state
            // machine. The validator advances `expectedContinuation` between
            // 0..3 bytes; the lead byte resets it. Any byte that violates
            // the position class for its expected state throws. Catches:
            //   - bare continuation bytes (no prior lead)
            //   - truncated multi-byte sequences (lead without continuation)
            //   - overlong encodings (validated via the explicit min/max
            //     codepoint check at the end of each multi-byte sequence)
            //   - UTF-16 surrogate codepoints U+D800..U+DFFF (forbidden in
            //     UTF-8)
            //   - codepoints beyond U+10FFFF
            int expectedContinuation = 0;
            int codepoint = 0;
            int minCodepoint = 0;
            for (long i = 0; i < len; i++) {
                final int b = bin.byteAt(i) & 0xff;
                sink.put((byte) b);
                if (expectedContinuation == 0) {
                    if ((b & 0x80) == 0) {
                        // 1-byte ASCII
                        continue;
                    }
                    if ((b & 0xe0) == 0xc0) {
                        // 2-byte lead 110xxxxx
                        expectedContinuation = 1;
                        codepoint = b & 0x1f;
                        minCodepoint = 0x80;
                    } else if ((b & 0xf0) == 0xe0) {
                        // 3-byte lead 1110xxxx
                        expectedContinuation = 2;
                        codepoint = b & 0x0f;
                        minCodepoint = 0x800;
                    } else if ((b & 0xf8) == 0xf0) {
                        // 4-byte lead 11110xxx
                        expectedContinuation = 3;
                        codepoint = b & 0x07;
                        minCodepoint = 0x10000;
                    } else {
                        throw CairoException.nonCritical().position(argPosition)
                                .put("binary value is not valid UTF-8 [byteOffset=").put(i)
                                .put(", byte=0x").put(toHex(b)).put(']');
                    }
                } else {
                    if ((b & 0xc0) != 0x80) {
                        // Expected 10xxxxxx continuation, got something else.
                        throw CairoException.nonCritical().position(argPosition)
                                .put("binary value is not valid UTF-8 [byteOffset=").put(i)
                                .put(", byte=0x").put(toHex(b))
                                .put(", expected=continuation]");
                    }
                    codepoint = (codepoint << 6) | (b & 0x3f);
                    expectedContinuation--;
                    if (expectedContinuation == 0) {
                        if (codepoint < minCodepoint) {
                            throw CairoException.nonCritical().position(argPosition)
                                    .put("binary value is not valid UTF-8 [byteOffset=").put(i)
                                    .put(", codepoint=").put(codepoint)
                                    .put(", reason=overlong]");
                        }
                        if (codepoint >= 0xd800 && codepoint <= 0xdfff) {
                            throw CairoException.nonCritical().position(argPosition)
                                    .put("binary value is not valid UTF-8 [byteOffset=").put(i)
                                    .put(", codepoint=0x").put(toHex(codepoint))
                                    .put(", reason=surrogate]");
                        }
                        if (codepoint > 0x10ffff) {
                            throw CairoException.nonCritical().position(argPosition)
                                    .put("binary value is not valid UTF-8 [byteOffset=").put(i)
                                    .put(", codepoint=0x").put(toHex(codepoint))
                                    .put(", reason=beyond-u10ffff]");
                        }
                    }
                }
            }
            if (expectedContinuation != 0) {
                throw CairoException.nonCritical().position(argPosition)
                        .put("binary value is not valid UTF-8 [reason=truncated, missing=")
                        .put(expectedContinuation).put(" continuation bytes]");
            }
            return sink;
        }

        // Tiny hex helper. Avoids pulling in Numbers.appendHex for the
        // single-byte / single-int formatting we need here.
        private static CharSequence toHex(int v) {
            return Integer.toHexString(v);
        }
    }
}
