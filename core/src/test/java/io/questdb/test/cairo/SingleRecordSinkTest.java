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

package io.questdb.test.cairo;

import io.questdb.cairo.SingleRecordSink;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class SingleRecordSinkTest extends AbstractTest {
    public static void runWithSink(WithNewSink code) throws Exception {
        assertMemoryLeak(() -> {
            try (SingleRecordSink sink = new SingleRecordSink(1024, MemoryTag.NATIVE_DEFAULT)) {
                code.runWithSink(sink);
            }
        });
    }

    public static void runWithSinks(WithNewSinks code, int maxHeap) throws Exception {
        assertMemoryLeak(() -> {
            try (SingleRecordSink left = new SingleRecordSink(maxHeap, MemoryTag.NATIVE_DEFAULT);
                 SingleRecordSink right = new SingleRecordSink(maxHeap, MemoryTag.NATIVE_DEFAULT)) {
                code.runWithSink(left, right);
            }
        });
    }

    public static void runWithSinks(WithNewSinks code) throws Exception {
        runWithSinks(code, 1024);
    }

    @Test
    public void testFuzz() throws Exception {
        Rnd rnd = TestUtils.generateRandom(null);
        testFuzz0(rnd, false);
        testFuzz0(rnd, true);
    }

    @Test(expected = LimitOverflowException.class)
    public void testPutIntExceedsMaxSize() throws Exception {
        runWithSink(sink -> {
            for (int i = 0; i < 300; i++) {
                sink.putInt(i);
            }
        });
    }

    @Test
    public void testReleaseMemory() throws Exception {
        runWithSink(sink -> {
            sink.putInt(123);
            sink.close();
        });
    }

    @Test
    public void testReleaseMemoryAfterResize() throws Exception {
        runWithSink(sink -> {
            for (int i = 0; i < 100; i++) {
                sink.putInt(i);
            }
            sink.close();
        });
    }

    @Test
    public void testReopen() throws Exception {
        runWithSinks((sinkLeft, sinkRight) -> {
            sinkLeft.putInt(123);
            sinkLeft.close();
            sinkLeft.reopen();
            sinkLeft.putInt(42);

            sinkRight.putInt(321);
            sinkRight.close();
            sinkRight.reopen();
            sinkRight.putInt(42);

            Assert.assertTrue(sinkLeft.memeq(sinkRight));
        });
    }

    @Test
    public void testReopenAfterInstantiation() throws Exception {
        runWithSinks((sinkLeft, sinkRight) -> {
            sinkLeft.reopen();
            sinkLeft.putInt(42);

            sinkRight.reopen();
            sinkRight.putInt(42);

            Assert.assertTrue(sinkLeft.memeq(sinkRight));
        });
    }

    private static void testFuzz0(Rnd rnd, boolean negativeCase) throws Exception {
        runWithSinks((sinkLeft, sinkRight) -> {
            // generate ops
            int opsCount = rnd.nextInt(100) + 1; // at least one op
            PUT_OP[] ops = new PUT_OP[opsCount];
            for (int i = 0; i < opsCount; i++) {
                int op = rnd.nextInt(PUT_OP.values().length);
                ops[i] = PUT_OP.values()[op];
            }
            int badValueAt = -1;
            if (negativeCase) {
                badValueAt = rnd.nextInt(opsCount);
            }

            long seed0 = rnd.getSeed0();
            long seed1 = rnd.getSeed1();

            // populate left sink
            rnd.reset(seed0, seed1);
            for (int i = 0; i < opsCount; i++) {
                ops[i].put(sinkLeft, rnd, false);
            }

            // populate right sink
            rnd.reset(seed0, seed1);
            for (int i = 0; i < opsCount; i++) {
                ops[i].put(sinkRight, rnd, i == badValueAt);
            }


            Assert.assertTrue(sinkLeft.memeq(sinkRight) != negativeCase);
        }, 1024 * 1024);
    }

    private enum PUT_OP {
        PUT_INT,
        PUT_LONG,
        PUT_DOUBLE,
        PUT_STR,
        PUT_BIN,
        PUT_BOOL,
        PUT_BYTE,
        PUT_CHAR,
        PUT_DATE,
        PUT_FLOAT,
        PUT_IPV4,
        PUT_LONG128,
        PUT_SHORT,
        PUT_TIMESTAMP,
        PUT_VARCHAR,
        PUT_LONG256_DIRECT,
        PUT_LONG256_WRAPPED,
        PUT_DECIMAL128,
        PUT_DECIMAL256;

        private void put(SingleRecordSink sink, Rnd rnd, boolean badValue) {
            switch (this) {
                case PUT_LONG256_WRAPPED:
                    Long256Impl long256wrapper = new Long256Impl();
                    long256wrapper.setAll(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());

                    if (badValue) {
                        long seed0 = rnd.getSeed0();
                        long seed1 = rnd.getSeed1();
                        Long256Impl long256wrapperB = new Long256Impl();
                        long256wrapperB.copyFrom(long256wrapper);
                        do {
                            long256wrapper.setAll(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                        } while (long256wrapper.equals(long256wrapperB));

                        // reset seeds, as if we didn't generated the bad value
                        rnd.reset(seed0, seed1);
                    }
                    sink.putLong256(long256wrapper);
                    break;
                case PUT_LONG256_DIRECT:
                    long long256A = rnd.nextLong();
                    long long256B = rnd.nextLong();
                    long long256C = rnd.nextLong();
                    long long256D = rnd.nextLong();

                    if (badValue) {
                        long long256A2 = long256A;
                        long long256B2 = long256B;
                        long long256C2 = long256C;
                        long long256D2 = long256D;
                        do {
                            long256A = rnd.nextLong();
                            long256B = rnd.nextLong();
                            long256C = rnd.nextLong();
                            long256D = rnd.nextLong();
                        } while (long256A == long256A2 && long256B == long256B2 && long256C == long256C2 && long256D == long256D2);
                    }
                    sink.putLong256(rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                    break;
                case PUT_VARCHAR:
                    boolean varcharNull = rnd.nextInt(5) == 0; // 20% chance of null
                    if (varcharNull && !badValue) {
                        sink.putVarchar((Utf8Sequence) null);
                        break;
                    }

                    if (varcharNull) {
                        long seed0 = rnd.getSeed0();
                        long seed1 = rnd.getSeed1();
                        sink.putVarchar(rnd.nextChars(10));
                        rnd.reset(seed0, seed1);
                        break;
                    }

                    CharSequence charSequence = rnd.nextChars(10);
                    if (!badValue) {
                        sink.putVarchar(charSequence);
                    } else {
                        long seed0 = rnd.getSeed0();
                        long seed1 = rnd.getSeed1();

                        CharSequence charSequenceB = String.valueOf(charSequence);
                        do {
                            charSequence = rnd.nextChars(10);
                        } while (charSequence.equals(charSequenceB));
                        sink.putVarchar(charSequence);
                        rnd.reset(seed0, seed1);
                    }
                    break;
                case PUT_TIMESTAMP:
                    long rndTimestamp = rnd.nextLong();
                    if (badValue) {
                        rndTimestamp++;
                    }
                    sink.putTimestamp(rndTimestamp);
                    break;
                case PUT_SHORT:
                    short rndShort = rnd.nextShort();
                    if (badValue) {
                        rndShort++;
                    }
                    sink.putShort(rndShort);
                    break;
                case PUT_LONG128:
                    long rndLong128A = rnd.nextLong();
                    long rndLong128B = rnd.nextLong();
                    if (badValue) {
                        rndLong128A++;
                    }
                    sink.putLong128(rndLong128A, rndLong128B);
                    break;
                case PUT_IPV4:
                    int rndIPv4 = rnd.nextInt();
                    if (badValue) {
                        rndIPv4++;
                    }
                    sink.putIPv4(rndIPv4);
                    break;
                case PUT_FLOAT:
                    float rndFloat = rnd.nextFloat();
                    if (badValue) {
                        rndFloat *= 2;
                        rndFloat += 1;
                    }
                    sink.putFloat(rndFloat);
                    break;
                case PUT_DATE:
                    long rndDate = rnd.nextLong();
                    if (badValue) {
                        rndDate++;
                    }
                    sink.putDate(rndDate);
                    break;
                case PUT_CHAR:
                    char rndChar = rnd.nextChar();
                    if (badValue) {
                        rndChar++;
                    }
                    sink.putChar(rndChar);
                    break;
                case PUT_INT:
                    int rndInt = rnd.nextInt();
                    if (badValue) {
                        rndInt++;
                    }
                    sink.putInt(rndInt);
                    break;
                case PUT_LONG:
                    long rndLong = rnd.nextLong();
                    if (badValue) {
                        rndLong++;
                    }
                    sink.putLong(rndLong);
                    break;
                case PUT_DOUBLE:
                    double rndDouble = rnd.nextDouble();
                    if (badValue) {
                        rndDouble *= 2;
                        rndDouble += 1;
                    }
                    sink.putDouble(rndDouble);
                    break;
                case PUT_STR:
                    boolean strNull = rnd.nextInt(5) == 0; // 20% chance of null
                    if (strNull && !badValue) {
                        sink.putStr(null);
                        break;
                    }
                    if (strNull) {
                        long seed0 = rnd.getSeed0();
                        long seed1 = rnd.getSeed1();
                        sink.putStr(rnd.nextChars(10));
                        rnd.reset(seed0, seed1);
                        break;
                    }

                    CharSequence str = rnd.nextChars(10);
                    if (!badValue) {
                        sink.putStr(str);
                    } else {
                        long seed0 = rnd.getSeed0();
                        long seed1 = rnd.getSeed1();

                        CharSequence strB = String.valueOf(str);
                        do {
                            str = rnd.nextChars(10);
                        } while (str.equals(strB));
                        sink.putStr(str);
                        rnd.reset(seed0, seed1);
                    }
                    break;
                case PUT_BOOL:
                    boolean rndBool = rnd.nextBoolean();
                    if (badValue) {
                        rndBool = !rndBool;
                    }
                    sink.putBool(rndBool);
                    break;
                case PUT_BYTE:
                    byte rndByte = rnd.nextByte();
                    if (badValue) {
                        rndByte++;
                    }
                    sink.putByte(rndByte);
                    break;
                case PUT_BIN:
                    boolean binNull = rnd.nextInt(5) == 0; // 20% chance of null
                    if (binNull && !badValue) {
                        sink.putBin(null);
                        break;
                    }


                    int len = rnd.nextInt(10);
                    BinarySequence seq;
                    long s0 = 0;
                    long s1 = 0;
                    if (!badValue) {
                        seq = new BinarySequence() {
                            @Override
                            public byte byteAt(long index) {
                                return rnd.nextByte();
                            }

                            @Override
                            public long length() {
                                return len;
                            }
                        };
                    } else {
                        for (int i = 0; i < len; i++) {
                            rnd.nextByte();
                        }
                        s0 = rnd.getSeed0();
                        s1 = rnd.getSeed1();
                        seq = new BinarySequence() {
                            @Override
                            public byte byteAt(long index) {
                                return rnd.nextByte();
                            }

                            @Override
                            public long length() {
                                return len + 1;
                            }
                        };
                    }
                    sink.putBin(seq);
                    if (badValue) {
                        rnd.reset(s0, s1);
                    }
                    break;
                case PUT_DECIMAL128:
                    long rndDecimal128Hi = rnd.nextPositiveLong() % Decimal128.MAX_VALUE.getHigh();
                    long rndDecimal128Lo = rnd.nextLong();
                    if (badValue) {
                        rndDecimal128Hi++;
                    }
                    sink.putDecimal128(new Decimal128(rndDecimal128Hi, rndDecimal128Lo, 0));
                    break;
                case PUT_DECIMAL256:
                    long rndDecimal256HH = rnd.nextPositiveLong() % Decimal256.MAX_VALUE.getHh();
                    long rndDecimal256HL = rnd.nextLong();
                    long rndDecimal256LH = rnd.nextLong();
                    long rndDecimal256LL = rnd.nextLong();
                    if (badValue) {
                        rndDecimal256HH++;
                    }
                    var decimal256 = new Decimal256(rndDecimal256HH, rndDecimal256HL, rndDecimal256LH, rndDecimal256LL, 0);
                    sink.putDecimal256(decimal256);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    interface WithNewSink {
        void runWithSink(SingleRecordSink sink);
    }

    interface WithNewSinks {
        void runWithSink(SingleRecordSink left, SingleRecordSink right);
    }
}
