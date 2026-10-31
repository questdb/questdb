/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.rnd;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndBinCCCFunctionFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

public class RndBinCCCFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testBadMinimum() {
        assertFailure(8, "minimum has to be grater than 0", 0L, 10L, 2);
    }

    @Test
    public void testDefaultLengthReadDoesNotGeneratePayload() throws Exception {
        assertLengthReadDoesNotGeneratePayload("rnd_bin()");
    }

    @Test
    public void testFixedLength() throws Exception {
        assertQuery("x\n" +
                        "00000000 ee 41 1d 15 55 8a\n" +
                        "\n" +
                        "00000000 d8 cc 14 ce f1 59\n" +
                        "00000000 c4 91 3b 72 db f3\n" +
                        "00000000 1b c7 88 de a0 79\n" +
                        "00000000 77 15 68 61 26 af\n" +
                        "00000000 c4 95 94 36 53 49\n" +
                        "\n" +
                        "\n" +
                        "00000000 3b 08 a1 1e 38 8d\n",
                "select to_char(rnd_bin(6,6,2)) x from long_sequence(10)");
    }

    @Test
    public void testFixedLengthNoNulls() throws Exception {
        assertQuery("x\n" +
                        "00000000 ee 41 1d 15 55\n" +
                        "00000000 17 fa d8 cc 14\n" +
                        "00000000 f1 59 88 c4 91\n" +
                        "00000000 72 db f3 04 1b\n" +
                        "00000000 88 de a0 79 3c\n" +
                        "00000000 15 68 61 26 af\n" +
                        "00000000 c4 95 94 36 53\n" +
                        "00000000 b4 59 7e 3b 08\n" +
                        "00000000 1e 38 8d 1b 9e\n" +
                        "00000000 c8 39 09 fe d8\n",
                "select to_char(rnd_bin(5,5,0)) x from long_sequence(10)");
    }

    @Test
    public void testFixedLengthReadDoesNotGeneratePayload() throws Exception {
        assertLengthReadDoesNotGeneratePayload("rnd_bin(65_536, 65_536, 2)");
    }

    @Test
    public void testInvalidRange() {
        assertFailure(0, "invalid range", 150L, 140L, 3);
    }

    @Test
    public void testNegativeNullRate() {
        assertFailure(14, "invalid null rate", 20L, 30L, -1);
    }

    @Test
    public void testVarLength() throws Exception {
        assertQuery("x\n" +
                        "00000000 41 1d 15\n" +
                        "00000000 17 fa d8 cc 14\n" +
                        "\n" +
                        "\n" +
                        "\n" +
                        "00000000 91 3b 72 db f3\n" +
                        "00000000 c7 88 de a0 79 3c 77 15\n" +
                        "00000000 26 af 19 c4 95 94 36 53\n" +
                        "\n" +
                        "\n",
                "select to_char(rnd_bin(3,8,2)) x from long_sequence(10)");
    }

    @Test
    public void testVarLengthIsEvaluatedOncePerRowWhenOrdered() throws Exception {
        // rnd_bin reads the unseeded context random, so a second cursor pass yields different values
        assertQuery("SELECT x, to_char(rnd_bin(3, 8, 0)) b FROM long_sequence(3) ORDER BY x DESC")
                .returnsOnce("""
                        x\tb
                        3\t00000000 59 88 c4 91 3b 72
                        2\t00000000 17 fa d8 cc 14
                        1\t00000000 41 1d 15
                        """);
    }

    @Test
    public void testVarLengthNoNulls() throws Exception {
        assertQuery("x\n" +
                        "00000000 41 1d 15\n" +
                        "00000000 17 fa d8 cc 14\n" +
                        "00000000 59 88 c4 91 3b 72\n" +
                        "00000000 04 1b c7 88 de a0\n" +
                        "00000000 77 15 68\n" +
                        "00000000 af 19 c4 95 94 36 53\n" +
                        "00000000 59 7e 3b 08 a1\n" +
                        "00000000 8d 1b 9e f4 c8 39 09\n" +
                        "00000000 9d 30 78\n" +
                        "00000000 32 de e4\n",
                "select to_char(rnd_bin(3,8,0)) x from long_sequence(10)");
    }

    @Test
    public void testVarLengthReadDoesNotGeneratePayload() throws Exception {
        assertLengthReadDoesNotGeneratePayload("rnd_bin(1, 65_536, 2)");
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndBinCCCFunctionFactory();
    }

    private void assertLengthReadDoesNotGeneratePayload(String expression) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd() {
                @Override
                public byte nextByte() {
                    throw new AssertionError("length read generated binary payload");
                }
            };
            sqlExecutionContext.setRandom(rnd);
            try {
                final LongList expected = new LongList();
                try (
                        RecordCursorFactory factory = select("SELECT " + expression + " FROM long_sequence(100)");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        final BinarySequence sequence = record.getBin(0);
                        expected.add(sequence == null ? -1 : sequence.length());
                    }
                }
                rnd.reset();
                try (
                        RecordCursorFactory factory = select("SELECT length(" + expression + ") FROM long_sequence(100)");
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    final Record record = cursor.getRecord();
                    for (int i = 0; i < expected.size(); i++) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(expected.getQuick(i), record.getLong(0));
                    }
                    Assert.assertFalse(cursor.hasNext());
                }
            } finally {
                sqlExecutionContext.setRandom(null);
            }
        });
    }
}
