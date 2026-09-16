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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.math.Atan2DoubleFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

public class Atan2DoubleFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testArgumentsAreClosed() throws SqlException {
        final CascadeSpy y = new CascadeSpy();
        final CascadeSpy x = new CascadeSpy();
        final ObjList<Function> args = new ObjList<>();
        args.add(y);
        args.add(x);
        final IntList argPositions = new IntList();
        argPositions.add(0);
        argPositions.add(0);
        new Atan2DoubleFunctionFactory()
                .newInstance(0, args, argPositions, configuration, sqlExecutionContext)
                .close();
        Assert.assertTrue(y.isClosed);
        Assert.assertTrue(x.isClosed);
    }

    @Test
    public void testArgumentsReceiveInit() throws Exception {
        // rnd_double() only assigns its Rnd in init(); without the BinaryFunction cascade this NPEs.
        // atan2(y, 1.0) with y in [0, 1) is in [0, PI/4], so the count is deterministic.
        assertQuery("""
                SELECT count(*) cnt FROM (
                  SELECT atan2(rnd_double(), 1.0) v FROM long_sequence(100)
                ) WHERE v >= 0.0 AND v <= 0.7853981634""")
                .noRandomAccess()
                .expectSize()
                .returns("cnt\n100\n");
    }

    @Test
    public void testNaN() throws SqlException {
        call(Double.NaN, Double.NaN).andAssert(Double.NaN, DELTA);
    }

    @Test
    public void testNegative() throws SqlException {
        call(-5.0, -5.0).andAssert(-Math.PI * 3 / 4, DELTA);
        call(-10.0, -10.0).andAssert(-Math.PI * 3 / 4, DELTA);
        call(-0.0000000000000001, -10.0).andAssert(-Math.PI, DELTA);
        call(-10.0, -0.0000000000000001).andAssert(-Math.PI / 2, DELTA);
    }

    @Test
    public void testPositive() throws SqlException {
        call(5.0, 5.0).andAssert(Math.PI / 4, DELTA);
        call(10.0, 10.0).andAssert(Math.PI / 4, DELTA);
        call(0.0, 10.0).andAssert(0.0, DELTA);
        call(10.0, 0.0).andAssert(Math.PI / 2, DELTA);
    }

    @Test
    public void testZero() throws SqlException {
        call(-0.0000000000000001, -0.0000000000000001).andAssert(-Math.PI * 3 / 4, DELTA);
        call(0.0, 0.0).andAssert(0.0, DELTA);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new Atan2DoubleFunctionFactory();
    }

    private static class CascadeSpy extends DoubleFunction {
        private boolean isClosed;

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public double getDouble(Record rec) {
            return 1.0;
        }
    }
}
