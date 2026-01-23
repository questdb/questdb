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

package io.questdb.test.std.fastdouble;

import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.fastdouble.FastDoubleParser;
import io.questdb.std.str.Utf8s;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests class {@link FastDoubleParser}
 */
public class FastDoubleParserTest extends AbstractFastXParserTest {
    @Test
    public void testParseDoubleBitsMemIntInt() {
        createAllTestDataForDouble()
                .forEach(t -> testBits(t, u -> {
                                    final String s = u.input();
                                    final int len = s.length();
                                    long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                                    try {
                                        // the function under the test cannot validate length of
                                        // the memory pointer. This validation has to be done externally.
                                        if (s.length() < u.charOffset() + u.charLength()) {
                                            throw NumericException.INSTANCE;
                                        }
                                        Utf8s.strCpyAscii(s, len, mem);
                                        return FastDoubleParser.parseDouble(mem, u.charOffset(), u.charLength(), false);
                                    } finally {
                                        Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
                                    }
                                }
                        )
                );
    }

    @Test
    public void testParseDoubleByteArray() {
        createAllTestDataForDouble().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .forEach(t -> test(t, u -> FastDoubleParser.parseDouble(u.input().getBytes(StandardCharsets.UTF_8), false)));
    }

    @Test
    public void testParseDoubleByteArrayIntInt() {
        createAllTestDataForDouble()
                .forEach(t -> test(t, u -> FastDoubleParser.parseDouble(u.input().getBytes(StandardCharsets.UTF_8), u.byteOffset(), u.byteLength(), false)));
    }

    @Test
    public void testParseDoubleCharArray() {
        createAllTestDataForDouble().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .forEach(t -> test(t, u -> FastDoubleParser.parseDouble(u.input().toCharArray(), false)));
    }

    @Test
    public void testParseDoubleCharArrayIntInt() {
        createAllTestDataForDouble()
                .forEach(t -> test(t, u -> FastDoubleParser.parseDouble(u.input().toCharArray(), u.charOffset(), u.charLength(), false)));
    }

    @Test
    public void testParseDoubleCharSequence() {
        createAllTestDataForDouble().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .forEach(t -> test(t, u -> FastDoubleParser.parseDouble(u.input(), false)));
    }

    @Test
    public void testParseDoubleCharSequenceIntInt() {
        createAllTestDataForDouble()
                .forEach(t -> test(t, u -> FastDoubleParser.parseDouble(u.input(), u.charOffset(), u.charLength())));
    }

    private void test(TestData d, ToDoubleFunction<TestData> f) {
        if (!d.valid()) {
            try {
                f.applyAsDouble(d);
                fail();
            } catch (Exception e) {
                //success
            }
        } else {
            try {
                assertEquals(d.expectedDoubleValue(), f.applyAsDouble(d), 0.001);
            } catch (NumericException e) {
                throw new NumberFormatException();
            }
        }
    }

    private void testBits(TestData d, ToDoubleFunction<TestData> f) {
        if (!d.valid()) {
            try {
                f.applyAsDouble(d);
                fail();
            } catch (NumericException e) {
                // good
            }
        } else {
            try {
                assertEquals(d.expectedDoubleValue(), f.applyAsDouble(d), 0.001);
            } catch (NumericException e) {
                throw new NumberFormatException();
            }
        }
    }

    @FunctionalInterface
    public interface ToDoubleFunction<T> {
        double applyAsDouble(T value) throws NumericException;
    }
}
