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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.math.MulIntFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class MulIntFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testBothNan() throws Exception {
        assertQuery("column\nnull\n", "SELECT null*null");
    }

    @Test
    public void testIntOverflow() throws Exception {
        assertQuery("column\n1720468802000000\n", "SELECT 1720468802 * 1000000");
    }

    @Test
    public void testLeftNan() throws Exception {
        assertQuery("column\nnull\n", "SELECT null*5");
    }

    @Test
    public void testMulByZero() throws Exception {
        assertQuery("column\n0\n", "SELECT 10*0");
    }

    @Test
    public void testRightNan() throws Exception {
        assertQuery("column\nnull\n", "SELECT 123*null");
    }

    @Test
    public void testSimple() throws Exception {
        assertQuery("column\n810\n", "SELECT 10*81");
    }

    @Test
    public void testTimestamp() throws Exception {
        assertQuery("to_utc\n1969-12-31T23:00:00.000810Z\n", "SELECT to_utc(10*81, 'Europe/Berlin')");
    }

    @Test
    public void testTimestampIntOverflow() throws Exception {
        assertQuery("to_utc\n2024-07-08T18:00:02.000000Z\n", "SELECT to_utc(1720468802 * 1000000, 'Europe/Berlin')");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new MulIntFunctionFactory();
    }
}