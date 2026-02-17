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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.DoubleArrayParser;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DoubleArrayParserTest extends AbstractTest {

    @Test
    public void parseEmptyArray() {
        try (DoubleArrayParser parser = new DoubleArrayParser()) {
            parser.of("{}", 1);
            Assert.assertEquals(1, parser.getDimCount());
            Assert.assertEquals(0, parser.getDimLen(0));
            Assert.assertEquals(0, parser.length());
            Assert.assertEquals(0, parser.getFlatViewLength());

            parser.of("{{}}", 2);
            Assert.assertEquals(2, parser.getDimCount());
            Assert.assertEquals(1, parser.getDimLen(0));
            Assert.assertEquals(0, parser.length());
            Assert.assertEquals(0, parser.getFlatViewLength());
        }
    }

    @Test
    public void testInconsistentArray() {
        String input = "{{\"1\",\"2.0\"},{\"3.1\"}}";
        try (DoubleArrayParser parser = new DoubleArrayParser()) {
            try {
                parser.of(input);
                Assert.fail();
            } catch (IllegalArgumentException e) {
                TestUtils.assertContains(e.getMessage(),
                        "element counts in sub-arrays don't match [depth=1, currentCount=1, alreadyObservedCount=2, position=19]");
            }
        }
    }

    @Test
    public void testParseNull() {
        try (DoubleArrayParser parser = new DoubleArrayParser()) {
            parser.of(null);

            Assert.assertEquals(0, parser.getDimCount());
            Assert.assertEquals(0, parser.getFlatViewLength());
        }
    }

    @Test
    public void testRepeatedParseWithDifferentLengthArrays() {
        try (DoubleArrayParser parser = new DoubleArrayParser()) {
            parser.of("[[1, 2], [3, 4]]");
            Assert.assertEquals(2, parser.getDimLen(0));
            parser.of("[[1, 2], [3, 4], [5, 6]]");
            Assert.assertEquals(3, parser.getDimLen(0));
        }
    }

    @Test
    public void testSmoke() {
        String input = "{{\"1\",\"2.0\"},{\"3.1\",\"0.4\"}}";

        int expectedType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
        try (DoubleArrayParser parser = new DoubleArrayParser()) {
            parser.of(input);
            Assert.assertEquals(4, parser.getFlatViewLength());
            Assert.assertEquals(0, parser.getFlatViewOffset());
            Assert.assertEquals(2, parser.getStride(0));
            Assert.assertEquals(1, parser.getStride(1));
            Assert.assertEquals(expectedType, parser.getType());
            Assert.assertEquals(2, parser.getDimCount());
            Assert.assertEquals(1, parser.getDouble(0), 0.0001);
            Assert.assertEquals(2, parser.getDouble(1), 0.0001);
            Assert.assertEquals(3.1, parser.getDouble(2), 0.0001);
            Assert.assertEquals(0.4, parser.getDouble(3), 0.0001);
        }

        input = "[[\"1\",\"2.0\"],[\"3.1\",\"0.4\"]]";
        try (DoubleArrayParser parser = new DoubleArrayParser()) {
            parser.of(input);
            Assert.assertEquals(4, parser.getFlatViewLength());
            Assert.assertEquals(0, parser.getFlatViewOffset());
            Assert.assertEquals(2, parser.getStride(0));
            Assert.assertEquals(1, parser.getStride(1));
            Assert.assertEquals(expectedType, parser.getType());
            Assert.assertEquals(2, parser.getDimCount());
            Assert.assertEquals(1, parser.getDouble(0), 0.0001);
            Assert.assertEquals(2, parser.getDouble(1), 0.0001);
            Assert.assertEquals(3.1, parser.getDouble(2), 0.0001);
            Assert.assertEquals(0.4, parser.getDouble(3), 0.0001);
        }
    }

    @Test
    public void testSmokeNoQuotes() {
        String input = "{\r{1,2.0}, {3.1,\n0.4}}";
        int expectedType = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);

        try (DoubleArrayParser parser = new DoubleArrayParser()) {
            parser.of(input);
            Assert.assertEquals(4, parser.getFlatViewLength());
            Assert.assertEquals(0, parser.getFlatViewOffset());
            Assert.assertEquals(2, parser.getStride(0));
            Assert.assertEquals(1, parser.getStride(1));
            Assert.assertEquals(expectedType, parser.getType());
            Assert.assertEquals(2, parser.getDimCount());
            Assert.assertEquals(1, parser.getDouble(0), 0.0001);
            Assert.assertEquals(2, parser.getDouble(1), 0.0001);
            Assert.assertEquals(3.1, parser.getDouble(2), 0.0001);
            Assert.assertEquals(0.4, parser.getDouble(3), 0.0001);
        }
    }
}
