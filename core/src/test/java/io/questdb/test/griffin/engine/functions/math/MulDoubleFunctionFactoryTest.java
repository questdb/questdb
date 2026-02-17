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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class MulDoubleFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testBothNan() throws SqlException {
        assertSqlWithTypes("x\nnull:DOUBLE\n", "select NaN::DOUBLE * NaN::DOUBLE x");
    }

    @Test
    public void testInfinite() throws SqlException {
        assertSqlWithTypes("x\nnull:DOUBLE\n", "select " + Double.MAX_VALUE + "d * 2d x");
    }

    @Test
    public void testNegativeInfinite() throws SqlException {
        assertSqlWithTypes("x\nnull:DOUBLE\n", "select -" + Double.MAX_VALUE + "d * 2d x");
    }

    @Test
    public void testMulByZero() throws SqlException {
        assertSqlWithTypes("x\n0.0:DOUBLE\n", "select 10d * 0d x");
    }

    @Test
    public void testSimple() throws SqlException {
        assertSqlWithTypes("x\n20.0:DOUBLE\n", "select 10d * 2d x");
    }
}