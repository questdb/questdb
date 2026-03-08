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

import io.questdb.std.NumericException;
import io.questdb.std.fastdouble.FastDoubleParser;
import io.questdb.std.fastdouble.FastFloatParser;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests class {@link FastDoubleParser}
 */
public class FastFloatParserTest extends AbstractFastXParserTest {
    @Test
    public void testParseDoubleByteArray() {
        createAllTestDataForFloat().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .forEach(t -> test(t, u -> {
                    try {
                        return FastFloatParser.parseFloat(u.input().getBytes(StandardCharsets.UTF_8), false);
                    } catch (NumericException e) {
                        throw new NumberFormatException();
                    }
                }));
    }

    @Test
    public void testParseDoubleByteArrayIntInt() {
        createAllTestDataForFloat()
                .forEach(t -> test(t, u -> {
                    try {
                        return FastFloatParser.parseFloat(u.input().getBytes(StandardCharsets.UTF_8), u.byteOffset(), u.byteLength(), false);
                    } catch (NumericException e) {
                        throw new NumberFormatException();
                    }
                }));
    }

    @Test
    public void testParseDoubleCharArray() {
        createAllTestDataForFloat().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .forEach(t -> test(t, u -> {
                    try {
                        return FastFloatParser.parseFloat(u.input().toCharArray(), false);
                    } catch (NumericException e) {
                        throw new NumberFormatException();
                    }
                }));
    }

    @Test
    public void testParseDoubleCharArrayIntInt() {
        createAllTestDataForFloat()
                .forEach(t -> test(t, u -> {
                    try {
                        return FastFloatParser.parseFloat(u.input().toCharArray(), u.charOffset(), u.charLength(), false);
                    } catch (NumericException e) {
                        throw new NumberFormatException();
                    }
                }));
    }

    @Test
    public void testParseDoubleCharSequence() {
        createAllTestDataForFloat().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .forEach(t -> test(t, u -> {
                    try {
                        return FastFloatParser.parseFloat(u.input(), false);
                    } catch (NumericException e) {
                        throw new NumberFormatException();
                    }
                }));
    }

    @Test
    public void testParseDoubleCharSequenceIntInt() {
        createAllTestDataForFloat()
                .forEach(t -> test(t, u -> {
                    try {
                        return FastFloatParser.parseFloat(u.input(), u.charOffset(), u.charLength(), false);
                    } catch (NumericException e) {
                        throw new NumberFormatException();
                    }
                }));
    }

    private void test(TestData d, ToFloatFunction<TestData> f) {
        if (!d.valid()) {
            try {
                f.applyAsFloat(d);
                fail();
            } catch (Exception e) {
                //success
            }
        } else {
            assertEquals(d.title(), d.expectedFloatValue(), f.applyAsFloat(d), 0.001);
        }
    }

    @FunctionalInterface
    public interface ToFloatFunction<T> {
        float applyAsFloat(T value);
    }
}
