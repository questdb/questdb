/*
 * @(#)FastDoubleParserTest.java
 * Copyright © 2022. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.NumericException;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * Tests class {@link FastDoubleParser}
 */
public class FastFloatParserTest extends AbstractFastXParserTest {
    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleCharSequence() {
        return createAllTestDataForFloat().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> {
                            try {
                                return FastFloatParser.parseFloat(u.input());
                            } catch (NumericException e) {
                                throw new NumberFormatException();
                            }
                        })));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleCharSequenceIntInt() {
        return createAllTestDataForFloat().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> {
                            try {
                                return FastFloatParser.parseFloat(u.input(), u.charOffset(), u.charLength());
                            } catch (NumericException e) {
                                throw new NumberFormatException();
                            }
                        })));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleByteArray() {
        return createAllTestDataForFloat().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> {
                            try {
                                return FastFloatParser.parseFloat(u.input().getBytes(StandardCharsets.UTF_8));
                            } catch (NumericException e) {
                                throw new NumberFormatException();
                            }
                        })));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleByteArrayIntInt() {
        return createAllTestDataForFloat().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> {
                            try {
                                return FastFloatParser.parseFloat(u.input().getBytes(StandardCharsets.UTF_8), u.byteOffset(), u.byteLength());
                            } catch (NumericException e) {
                                throw new NumberFormatException();
                            }
                        })));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleCharArray() {
        return createAllTestDataForFloat().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> {
                            try {
                                return FastFloatParser.parseFloat(u.input().toCharArray());
                            } catch (NumericException e) {
                                throw new NumberFormatException();
                            }
                        })));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleCharArrayIntInt() {
        return createAllTestDataForFloat().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> {
                            try {
                                return FastFloatParser.parseFloat(u.input().toCharArray(), u.charOffset(), u.charLength());
                            } catch (NumericException e) {
                                throw new NumberFormatException();
                            }
                        })));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleBitsCharSequenceIntInt() {
        return createAllTestDataForFloat().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> testBits(t, u -> {
                            try {
                                return FastFloatParser.parseFloatBits(u.input(), u.charOffset(), u.charLength());
                            } catch (NumericException e) {
                                throw new NumberFormatException();
                            }
                        })));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleBitsByteArrayIntInt() {
        return createAllTestDataForFloat().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> testBits(t, u -> FastFloatParser.parseFloatBits(u.input().getBytes(StandardCharsets.UTF_8), u.byteOffset(), u.byteLength()))));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleBitsCharArrayIntInt() {
        return createAllTestDataForFloat().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> testBits(t, u -> FastFloatParser.parseFloatBits(u.input().toCharArray(), u.charOffset(), u.charLength()))));
    }

    private void test(TestData d, ToFloatFunction<TestData> f) throws NumericException {
        if (!d.valid()) {
            try {
                f.applyAsFloat(d);
                fail();
            } catch (Exception e) {
                //success
            }
        } else {
            double actual = f.applyAsFloat(d);
            assertEquals(d.expectedFloatValue(), actual);
        }
    }

    private void testBits(TestData d, ToFloatFunction<TestData> f) throws NumericException {
        if (!d.valid()) {
            try {
                f.applyAsFloat(d);
                fail();
            } catch (Exception e) {
                //success
            }
        } else {
            assertEquals(d.expectedFloatValue(), f.applyAsFloat(d));
        }
    }

    @FunctionalInterface
    public interface ToFloatFunction<T> {
        float applyAsFloat(T value) throws NumericException;
    }
}
