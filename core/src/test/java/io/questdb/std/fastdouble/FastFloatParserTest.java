/*
 * @(#)FastDoubleParserTest.java
 * Copyright © 2022. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;
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
            try {
                assertEquals(d.title(), d.expectedFloatValue(), f.applyAsFloat(d), 0.001);
            } catch (NumericException e) {
                throw new NumberFormatException();
            }
        }
    }

    @FunctionalInterface
    public interface ToFloatFunction<T> {
        float applyAsFloat(T value) throws NumericException;
    }
}
