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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class BitwiseTest extends AbstractCairoTest {

    @Test
    public void testIntAnd() throws Exception {
        assertBitwiseOp("select 6 & 4", "4\n");
    }

    @Test
    public void testIntAndLeftNull() throws Exception {
        assertBitwiseOp("select NaN & 4", "null\n");
    }

    @Test
    public void testIntAndRightNull() throws Exception {
        assertBitwiseOp("select 2 & NaN", "null\n");
    }

    @Test
    public void testIntNot() throws Exception {
        assertBitwiseOp("select ~1024", "-1025\n");
    }

    @Test
    public void testIntNotNull() throws Exception {
        assertBitwiseOp("select ~cast(NaN as int)", "null\n");
    }

    @Test
    public void testIntOr() throws Exception {
        assertBitwiseOp("select 2 | 4", "6\n");
    }

    @Test
    public void testIntOrLeftNull() throws Exception {
        assertBitwiseOp("select NaN | 4", "null\n");
    }

    @Test
    public void testIntOrRightNull() throws Exception {
        assertBitwiseOp("select 2 | NaN", "null\n");
    }

    @Test
    public void testIntXor() throws Exception {
        assertBitwiseOp("select 6 ^ 4", "2\n");
    }

    @Test
    public void testIntXorLeftNull() throws Exception {
        assertBitwiseOp("select NaN ^ 4", "null\n");
    }

    @Test
    public void testIntXorRightNull() throws Exception {
        assertBitwiseOp("select 2 ^ NaN", "null\n");
    }

    @Test
    public void testLongAnd() throws Exception {
        assertBitwiseOp("select 6L & 4", "4\n");
    }

    @Test
    public void testLongAndLeftNull() throws Exception {
        assertBitwiseOp("select NaN & 4L", "null\n");
    }

    @Test
    public void testLongAndRightNull() throws Exception {
        assertBitwiseOp("select 2L & NaN", "null\n");
    }

    @Test
    public void testLongNot() throws Exception {
        assertBitwiseOp("select ~255L", "-256\n");
    }

    @Test
    public void testLongNotNull() throws Exception {
        assertBitwiseOp("select ~cast(NaN as long)", "null\n");
    }

    @Test
    public void testLongOr() throws Exception {
        assertBitwiseOp("select 2L | 4", "6\n");
    }

    @Test
    public void testLongOrLeftNull() throws Exception {
        assertBitwiseOp("select NaN | 4L", "null\n");
    }

    @Test
    public void testLongOrRightNull() throws Exception {
        assertBitwiseOp("select 2L | NaN", "null\n");
    }

    @Test
    public void testLongXor() throws Exception {
        assertBitwiseOp("select 6L ^ 4", "2\n");
    }

    @Test
    public void testLongXorLeftNull() throws Exception {
        assertBitwiseOp("select NaN ^ 4L", "null\n");
    }

    @Test
    public void testLongXorRightNull() throws Exception {
        assertBitwiseOp("select 2L ^ NaN", "null\n");
    }

    private void assertBitwiseOp(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> assertSql(
                "column\n" +
                        expected, sql
        ));
    }
}
