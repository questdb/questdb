/*
 * @(#)FastDoubleParserTest.java
 * Copyright © 2022. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
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
public class FastDoubleParserTest extends AbstractFastXParserTest {
    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleBitsByteArrayIntInt() {
        return createAllTestDataForDouble().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> testBits(t, u -> FastDoubleParser.parseDoubleBits(u.input().getBytes(StandardCharsets.UTF_8), u.byteOffset(), u.byteLength()))));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleBitsCharArrayIntInt() {
        return createAllTestDataForDouble().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> testBits(t, u -> FastDoubleParser.parseDoubleBits(u.input().toCharArray(), u.charOffset(), u.charLength()))));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleBitsCharSequenceIntInt() {
        return createAllTestDataForDouble().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> testBits(t, u -> FastDoubleParser.parseDoubleBits(u.input(), u.charOffset(), u.charLength()))));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleBitsMemIntInt() {
        return createAllTestDataForDouble().stream()
                .map(t -> dynamicTest(t.title(),
                                () -> testBits(t, u -> {
                                            final String s = u.input();
                                            final int len = s.length();
                                            long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                                            try {
                                                // the function under the test cannot validate length of
                                                // the memory pointer. This validations has to be done externally.
                                                if (s.length() < u.charOffset() + u.charLength()) {
                                                    throw NumericException.INSTANCE;
                                                }
                                                Chars.asciiStrCpy(s, len, mem);
                                                return FastDoubleParser.parseDouble(mem, u.charOffset(), u.charLength());
                                            } finally {
                                                Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
                                            }
                                        }
                                )
                        )
                );
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleByteArray() {
        return createAllTestDataForDouble().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> FastDoubleParser.parseDouble(u.input().getBytes(StandardCharsets.UTF_8)))));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleByteArrayIntInt() {
        return createAllTestDataForDouble().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> FastDoubleParser.parseDouble(u.input().getBytes(StandardCharsets.UTF_8), u.byteOffset(), u.byteLength()))));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleCharArray() {
        return createAllTestDataForDouble().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> FastDoubleParser.parseDouble(u.input().toCharArray()))));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleCharArrayIntInt() {
        return createAllTestDataForDouble().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> FastDoubleParser.parseDouble(u.input().toCharArray(), u.charOffset(), u.charLength()))));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleCharSequence() {
        return createAllTestDataForDouble().stream()
                .filter(t -> t.charLength() == t.input().length()
                        && t.charOffset() == 0)
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> FastDoubleParser.parseDouble(u.input()))));
    }

    @TestFactory
    Stream<DynamicNode> dynamicTestsParseDoubleCharSequenceIntInt() {
        return createAllTestDataForDouble().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> test(t, u -> FastDoubleParser.parseDouble(u.input(), u.charOffset(), u.charLength()))));
    }

    private void test(TestData d, ToDoubleFunction<TestData> f) throws NumericException {
        if (!d.valid()) {
            try {
                f.applyAsDouble(d);
                fail();
            } catch (Exception e) {
                //success
            }
        } else {
            double actual = f.applyAsDouble(d);
            assertEquals(d.expectedDoubleValue(), actual);
        }
    }

    private void testBits(TestData d, ToDoubleFunction<TestData> f) throws NumericException {
        if (!d.valid()) {
            try {
                f.applyAsDouble(d);
                fail();
            } catch (NumericException e) {
                // good
            }
        } else {
            assertEquals(d.expectedDoubleValue(), f.applyAsDouble(d));
        }
    }

    @FunctionalInterface
    public interface ToDoubleFunction<T> {
        double applyAsDouble(T value) throws NumericException;
    }
}
