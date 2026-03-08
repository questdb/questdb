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

package io.questdb.test.griffin;

import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TrailingDigitsTest extends AbstractCairoTest {
    /**
     * Test if output contains trailing digits with a simple test case.
     */
    @Test
    public void simpleDoubleConversionTest() throws Exception {
        assertMemoryLeak(
                () -> {
                    TestUtils.printSql(
                            engine,
                            sqlExecutionContext,
                            "select * from (select 111.1111111 as val)",
                            sink
                    );
                    TestUtils.assertEquals("val\n111.1111111\n", sink);
                }
        );
    }

    /**
     * Generate multiple test cases to test if trailing digits exists.
     */
    @Test
    public void testDoubleConversion() throws SqlException {
        int cases = 1000;
        for (int digits = 11; digits <= 14; digits++)
            Assert.assertEquals(passCount(cases, digits), cases);
    }

    /**
     * Check trailing digits for the conversion of input with given digits.
     *
     * @param cases  the number of generated cases
     * @param digits the digits count of generated input, should be greater than 5
     */
    private int passCount(int cases, int digits) throws SqlException {
        Assert.assertTrue(digits > 5);
        int pass = 0;
        char[] s = new char[digits];
        Rnd rnd = new Rnd();
        for (int t = 0; t < cases; t++) {
            for (int i = 0; i < s.length; i++) {
                s[i] = (char) (rnd.nextPositiveInt() % 9 + '1');
            }
            int dp = rnd.nextPositiveInt() % (5) + 1;
            s[dp] = '.';
            String numStr = new String(s);
            String expected = "val\n" + numStr + "\n";
            TestUtils.printSql(
                    engine,
                    sqlExecutionContext,
                    "select * from (select " + numStr + " as val)",
                    sink
            );
            if (Chars.equals(sink, expected)) {
                pass++;
            }
        }
        return pass;
    }
}
