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
public class FastDoubleParserTest extends AbstractFastXParserTest {
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
    Stream<DynamicNode> dynamicTestsParseDoubleBitsCharSequenceIntInt() {
        return createAllTestDataForDouble().stream()
                .map(t -> dynamicTest(t.title(),
                        () -> testBits(t, u -> FastDoubleParser.parseDoubleBits(u.input(), u.charOffset(), u.charLength()))));
    }

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
