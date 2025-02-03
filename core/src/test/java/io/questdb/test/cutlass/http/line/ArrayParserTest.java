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

package io.questdb.test.cutlass.http.line;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayJsonSerializer;
import io.questdb.cairo.arr.ArrayRowMajorTraversal;
import io.questdb.cairo.arr.ArrayValuesSlice;
import io.questdb.cairo.arr.ArrayViewImpl;
import io.questdb.cutlass.line.tcp.ArrayParser;
import io.questdb.cutlass.line.tcp.ArrayParser.ParseException;
import io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode;
import io.questdb.std.DirectIntSlice;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ErrorCode.*;
import static java.util.Collections.nCopies;
import static org.junit.Assert.*;

@Ignore
// todo: array parser should not be required at all if ILP sender sends data in the same format that we store array as
//    it will be both not requiring parsing overhead and on the storage fast path
public class ArrayParserTest {

    private ArrayParser parser;
    private DirectUtf8Sink sink;

    @Before
    public void setUp() {
        sink = new DirectUtf8Sink(1024);
        parser = new ArrayParser();
    }

    @After
    public void tearDown() {
        parser.close();
        sink.close();
    }

    @Test
    public void testBoolean1d() throws ParseException {
        testByteLiteral("[0u1]", new int[]{1}, new int[]{1});
        testByteLiteral("[0u0,1]", new int[]{2}, new int[]{0, 1});
        testByteLiteral("[0u1,0,1]", new int[]{3}, new int[]{1, 0, 1});
    }

    @Test
    public void testBoolean1dInvalid() {
        testInvalidLiteral("[0u2]", ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral("[0u-1]", ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral("[0u.]", ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral("[0u0,1,2]", ND_ARR_UNEXPECTED_TOKEN, 7);
    }

    @Test
    public void testByte1d() throws ParseException {
        testByteLiteral("[3i1]", new int[]{1}, new int[]{1});
        testByteLiteral(String.format("[3i%d]", Byte.MAX_VALUE), new int[]{1}, new int[]{Byte.MAX_VALUE});
        testByteLiteral(String.format("[3i%d]", Byte.MIN_VALUE), new int[]{1}, new int[]{Byte.MIN_VALUE});
        testByteLiteral("[3i1,2]", new int[]{2}, new int[]{1, 2});
        testByteLiteral("[3i1,2,3]", new int[]{3}, new int[]{1, 2, 3});
    }

    @Test
    public void testDimensionalityLimit() throws ParseException {
        int maxNesting = ArrayParser.DIM_COUNT_LIMIT - 1;
        String validArray = String.format("[5i%s1%s]", repeat("[", maxNesting), repeat("]", maxNesting));
        int[] maxShape = new int[ArrayParser.DIM_COUNT_LIMIT];
        Arrays.fill(maxShape, 1);
        testIntLiteral(validArray, maxShape, new int[]{1});

        int tooMuchNesting = maxNesting + 1;
        String invalidArray = String.format("[5i%s1%s]", repeat("[", tooMuchNesting), repeat("]", tooMuchNesting));
        testInvalidLiteral(invalidArray, ND_ARR_UNEXPECTED_TOKEN, 3 + maxNesting);
    }

    @Test
    public void testDouble1d() throws ParseException {
        testDoubleLiteral("[6f1]", new int[]{1}, new double[]{1.0});
        testDoubleLiteral("[6f1.1]", new int[]{1}, new double[]{1.1});
        testDoubleLiteral("[6f1e1]", new int[]{1}, new double[]{1e1});
        testDoubleLiteral("[6f1.1e1]", new int[]{1}, new double[]{1.1e1});
        testDoubleLiteral("[6f1.1e-1]", new int[]{1}, new double[]{1.1e-1});
        testDoubleLiteral("[6f-1.1e-1]", new int[]{1}, new double[]{-1.1e-1});
        testDoubleLiteral("[6f1.1,2.2]", new int[]{2}, new double[]{1.1, 2.2});
        testDoubleLiteral("[6f1.1,2.2,3.3]", new int[]{3}, new double[]{1.1, 2.2, 3.3});
    }

    @Test
    public void testFloat1d() throws ParseException {
        testFloatLiteral("[5f1]", new int[]{1}, new float[]{1f});
        testFloatLiteral("[5f1e1]", new int[]{1}, new float[]{1e1f});
        testFloatLiteral("[5f1.1e1]", new int[]{1}, new float[]{1.1e1f});
        testFloatLiteral("[5f1.1e-1]", new int[]{1}, new float[]{1.1e-1f});
        testFloatLiteral("[5f-1.1e-1]", new int[]{1}, new float[]{-1.1e-1f});
        testFloatLiteral("[5f1.1]", new int[]{1}, new float[]{1.1f});
        testFloatLiteral("[5f1.1,2.2]", new int[]{2}, new float[]{1.1f, 2.2f});
        testFloatLiteral("[5f1.1,2.2,3.3]", new int[]{3}, new float[]{1.1f, 2.2f, 3.3f});
    }

    @Test
    public void testInt1d() throws ParseException {
        testIntLiteral("[5i1]", new int[]{1}, new int[]{1});
        testIntLiteral(String.format("[5i%d]", Integer.MAX_VALUE), new int[]{1}, new int[]{Integer.MAX_VALUE});
        testIntLiteral(String.format("[5i%d]", Integer.MIN_VALUE), new int[]{1}, new int[]{Integer.MIN_VALUE});
        testIntLiteral("[5i1,2]", new int[]{2}, new int[]{1, 2});
        testIntLiteral("[5i1,2,3]", new int[]{3}, new int[]{1, 2, 3});
    }

    @Test
    public void testInt1dInvalid() {
        testInvalidLiteral("[5i]", ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral("[5i,]", ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral("[5ia]", ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral("[5i1.1]", ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral("[5i1,,1]", ND_ARR_UNEXPECTED_TOKEN, 5);
        testInvalidLiteral("[5i1,]", ND_ARR_UNEXPECTED_TOKEN, 5);
        String veryLongInt = repeat("1", ArrayParser.LEAF_LENGTH_LIMIT - 1);
        String dosAttack = veryLongInt + "1";
        testInvalidLiteral(String.format("[5i%s", veryLongInt), ND_ARR_PREMATURE_END, 3);
        testInvalidLiteral(String.format("[5i%s", dosAttack), ND_ARR_UNEXPECTED_TOKEN, 3);
    }

    @Test
    public void testInt2d() throws ParseException {
        testIntLiteral("[5i[1]]", new int[]{1, 1}, new int[]{1});
        testIntLiteral("[5i[1],[2]]", new int[]{2, 1}, new int[]{1, 2});
        testIntLiteral("[5i[1,2],[3,4]]", new int[]{2, 2}, new int[]{1, 2, 3, 4});
    }

    @Test
    public void testInt2dInvalid() {
        testInvalidLiteral("[5i[]]", ND_ARR_UNEXPECTED_TOKEN, 4);
        testInvalidLiteral("[5i[,]]", ND_ARR_UNEXPECTED_TOKEN, 4);
        testInvalidLiteral("[5i[a]]", ND_ARR_UNEXPECTED_TOKEN, 4);
        testInvalidLiteral("[5i[1.1]]", ND_ARR_UNEXPECTED_TOKEN, 4);
        testInvalidLiteral("[5i[1,]]", ND_ARR_UNEXPECTED_TOKEN, 6);
        testInvalidLiteral("[5i[1],,[1]]", ND_ARR_UNEXPECTED_TOKEN, 7);
        testInvalidLiteral("[5i[1,,1]]", ND_ARR_UNEXPECTED_TOKEN, 6);
        testInvalidLiteral("[5i[1],]", ND_ARR_UNEXPECTED_TOKEN, 7);
    }

    @Test
    public void testInt3d() throws ParseException {
        testIntLiteral("[5i[[1]]]", new int[]{1, 1, 1}, new int[]{1});
        testIntLiteral("[5i[[1],[2]]]", new int[]{1, 2, 1}, new int[]{1, 2});
        testIntLiteral("[5i[[1,2],[3,4]]]", new int[]{1, 2, 2}, new int[]{1, 2, 3, 4});
        testIntLiteral("[5i[[1,2],[3,4]],[[5,6],[7,8]]]", new int[]{2, 2, 2}, new int[]{1, 2, 3, 4, 5, 6, 7, 8});
    }

    @Test
    public void testInvalidJagged() {
        testInvalidLiteral("[5i[1],[1,2]]", ND_ARR_IRREGULAR_SHAPE, 10);
        testInvalidLiteral("[5i[1,2],[1]]", ND_ARR_IRREGULAR_SHAPE, 11);
        testInvalidLiteral("[5i[[1,2],[1]]]", ND_ARR_IRREGULAR_SHAPE, 12);
        testInvalidLiteral("[5i[[1],[2]],[[3],[4],[5]]]", ND_ARR_IRREGULAR_SHAPE, 22);
    }

    @Test
    public void testJsonSerializer() throws ParseException {
        testLiteralToJson("[5i1]");
        testLiteralToJson("[5i1,2]");
        testLiteralToJson("[5i[1]]");
        testLiteralToJson("[5i[1,2]]");
        testLiteralToJson("[5i[1,2],[3,4]]");
    }

    @Test
    public void testLong1d() throws ParseException {
        testLongLiteral("[6i1]", new int[]{1}, new long[]{1});
        testLongLiteral("[6i1,2]", new int[]{2}, new long[]{1, 2});
        testLongLiteral("[6i1,2,3]", new int[]{3}, new long[]{1, 2, 3});
    }

    @Test
    public void testNullArray() throws ParseException {
        ArrayValuesSlice values = parseAndGetValues("[]", new int[0]);
        assertEquals("values should be empty", 0, values.size());
    }

    @Test
    public void testShort1d() throws ParseException {
        testShortLiteral("[4i1]", new int[]{1}, new int[]{1});
        testShortLiteral(String.format("[4i%d]", Short.MAX_VALUE), new int[]{1}, new int[]{Short.MAX_VALUE});
        testShortLiteral(String.format("[4i%d]", Short.MIN_VALUE), new int[]{1}, new int[]{Short.MIN_VALUE});
        testShortLiteral("[4i1,2]", new int[]{2}, new int[]{1, 2});
        testShortLiteral("[4i1,2,3]", new int[]{3}, new int[]{1, 2, 3});
    }

    @Test
    public void testValuesOutOfRange() {
        testInvalidLiteral(String.format("[3i%d]", Byte.MIN_VALUE - 1), ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral(String.format("[3i%d]", Byte.MAX_VALUE + 1), ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral(String.format("[4i%d]", Short.MIN_VALUE - 1), ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral(String.format("[4i%d]", Short.MAX_VALUE + 1), ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral(String.format("[5i%d]", Integer.MIN_VALUE - 1L), ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral(String.format("[5i%d]", Integer.MAX_VALUE + 1L), ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral("[6i-9223372036854775809]", ND_ARR_UNEXPECTED_TOKEN, 3);
        testInvalidLiteral("[6i9223372036854775808]", ND_ARR_UNEXPECTED_TOKEN, 3);
    }

    private static String repeat(String s, int n) {
        return String.join("", nCopies(n, s));
    }

    private void assertSliceEquals(DirectIntSlice actual, int[] expected) {
        assertArrayEquals(expected, actual.toArray());
    }

    private ArrayValuesSlice parseAndGetValues(String literal, int[] expectedShape) throws ParseException {
        DirectUtf8String arrayStr = utf8String(sink, literal);
        parser.parse(arrayStr);
        ArrayViewImpl view = parser.getView();
        assertSliceEquals((DirectIntSlice) view.getShape(), expectedShape);
        return view.getValues();
    }

    private void testByteLiteral(String literal, int[] expectedShape, int[] expectedValues) throws ParseException {
        ArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size", expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], values.getByte(i));
        }
    }

    private void testDoubleLiteral(String literal, int[] expectedShape, double[] expectedValues) throws ParseException {
        ArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size",
                Double.BYTES * expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], values.getDouble(i), 0.0);
        }
    }

    private void testFloatLiteral(String literal, int[] expectedShape, float[] expectedValues) throws ParseException {
        ArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size",
                Float.BYTES * expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], values.getFloat(i), 0.0);
        }
    }

    private void testIntLiteral(String literal, int[] expectedShape, int[] expectedValues) throws ParseException {
        ArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size",
                Integer.BYTES * expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], values.getInt(i));
        }
    }

    private void testInvalidLiteral(@NotNull String literal, @NotNull ErrorCode expectedErrorCode, int expectedPosition) {
        try {
            parser.parse(utf8String(sink, literal));
            fail("Parsing was supposed to fail");
        } catch (ParseException e) {
            assertEquals(expectedErrorCode, e.errorCode());
            assertEquals(expectedPosition, e.position());
        }
    }

    private void testLiteralToJson(String literal) throws ParseException {
        int columnType = ColumnType.encodeArrayType(ColumnType.INT, 1);
        DirectUtf8String arrayStr = utf8String(sink, literal);
        parser.parse(arrayStr);
        ArrayViewImpl array = parser.getView();
        try (ArrayRowMajorTraversal traversal = new ArrayRowMajorTraversal()) {
            traversal.of(array);
            sink.clear();
            ArrayJsonSerializer.serialize(sink, array, traversal, columnType);
        }
        String expectedJson = "[" + literal.substring(3);
        assertEquals("JSON doesn't match the array literal", expectedJson, sink.toString());
    }

    private void testLongLiteral(String literal, int[] expectedShape, long[] expectedValues) throws ParseException {
        ArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size",
                Long.BYTES * expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], values.getLong(i));
        }
    }

    private void testShortLiteral(String literal, int[] expectedShape, int[] expectedValues) throws ParseException {
        ArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size",
                Short.BYTES * expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], values.getShort(i));
        }
    }

    private DirectUtf8String utf8String(DirectUtf8Sink sink, String literal) {
        sink.clear();
        sink.putAscii(literal);
        return new DirectUtf8String().of(sink.ptr(), sink.ptr() + sink.size(), true);
    }
}
