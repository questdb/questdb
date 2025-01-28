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

import io.questdb.cutlass.line.tcp.NdArrayParser;
import io.questdb.cutlass.line.tcp.NdArrayParser.ParseException;
import io.questdb.std.DirectIntSlice;
import io.questdb.std.ndarr.NdArrayValuesSlice;
import io.questdb.std.ndarr.NdArrayView;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class NdArrayParserTest {

    private NdArrayParser parser;
    private DirectUtf8Sink sink;

    @Before
    public void setUp() {
        sink = new DirectUtf8Sink(1024);
        parser = new NdArrayParser();
    }

    @After
    public void tearDown() {
        parser.close();
        sink.close();
    }

    @Test
    public void testDouble1d() throws ParseException {
        testDoubleLiteral("{6f1}", new int[]{1}, new double[]{1.0});
        testDoubleLiteral("{6f1.1}", new int[]{1}, new double[]{1.1});
        testDoubleLiteral("{6f1.1,2.2}", new int[]{2}, new double[]{1.1, 2.2});
        testDoubleLiteral("{6f1.1,2.2,3.3}", new int[]{3}, new double[]{1.1, 2.2, 3.3});
    }

    @Test
    public void testFloat1d() throws ParseException {
        testFloatLiteral("{5f1}", new int[]{1}, new float[]{1f});
        testFloatLiteral("{5f1.1}", new int[]{1}, new float[]{1.1f});
        testFloatLiteral("{5f1.1,2.2}", new int[]{2}, new float[]{1.1f, 2.2f});
        testFloatLiteral("{5f1.1,2.2,3.3}", new int[]{3}, new float[]{1.1f, 2.2f, 3.3f});
    }

    @Test
    public void testInt1d() throws ParseException {
        testIntLiteral("{5i1}", new int[]{1}, new int[]{1});
        testIntLiteral("{5i1,2}", new int[]{2}, new int[]{1, 2});
        testIntLiteral("{5i1,2,3}", new int[]{3}, new int[]{1, 2, 3});
    }

    @Test
    public void testInt2d() throws ParseException {
        testIntLiteral("{5i{1},{2}}", new int[]{2, 1}, new int[]{1, 2});
    }

    @Test
    public void testLong1d() throws ParseException {
        testLongLiteral("{6i1}", new int[]{1}, new long[]{1});
        testLongLiteral("{6i1,2}", new int[]{2}, new long[]{1, 2});
        testLongLiteral("{6i1,2,3}", new int[]{3}, new long[]{1, 2, 3});
    }

    private void assertSliceEquals(DirectIntSlice actual, int[] expected) {
        assertArrayEquals(expected, actual.toArray());
    }

    private NdArrayValuesSlice parseAndGetValues(String literal, int[] expectedShape) throws ParseException {
        DirectUtf8String arrayStr = utf8String(sink, literal);
        parser.parse(arrayStr);
        NdArrayView view = parser.getView();
        assertSliceEquals(view.getShape(), expectedShape);
        return view.getValues();
    }

    private void testDoubleLiteral(String literal, int[] expectedShape, double[] expectedValues) throws ParseException {
        NdArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size",
                Double.BYTES * expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(0.0, expectedValues[i], values.getDouble(i));
        }
    }

    private void testFloatLiteral(String literal, int[] expectedShape, float[] expectedValues) throws ParseException {
        NdArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size",
                Float.BYTES * expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(0.0, expectedValues[i], values.getFloat(i));
        }
    }

    private void testIntLiteral(String literal, int[] expectedShape, int[] expectedValues) throws ParseException {
        NdArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size",
                Integer.BYTES * expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], values.getInt(i));
        }
    }

    private void testLongLiteral(String literal, int[] expectedShape, long[] expectedValues) throws ParseException {
        NdArrayValuesSlice values = parseAndGetValues(literal, expectedShape);
        assertEquals("values don't have the expected size",
                Long.BYTES * expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], values.getLong(i));
        }
    }

    private DirectUtf8String utf8String(DirectUtf8Sink sink, String literal) {
        sink.putAscii(literal);
        return new DirectUtf8String().of(sink.ptr(), sink.ptr() + sink.size(), true);
    }
}
