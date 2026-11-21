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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.eq.EqDateFunctionFactory;
import io.questdb.std.NumericException;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class EqDateFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testEquals() throws SqlException, NumericException {
        long t1 = MillisTimestampDriver.floor("2020-12-31T23:59:59.000Z");
        long t2 = MillisTimestampDriver.floor("2020-12-31T23:59:59.001Z");
        callBySignature("=(MM)", t1, t1).andAssert(true);
        callBySignature("=(MM)", t1, t2).andAssert(false);
    }

    @Test
    public void testNotEquals() throws SqlException, NumericException {
        long t1 = MillisTimestampDriver.floor("2020-12-31T23:59:59.000Z");
        long t2 = MillisTimestampDriver.floor("2020-12-31T23:59:59.001Z");
        callBySignature("<>(MM)", t1, t1).andAssert(false);
        callBySignature("<>(MM)", t1, t2).andAssert(true);
    }

    @Test
    public void testNullComparedWithDate() throws Exception {
        // NULL = x holds exactly when x is NULL and != negates it, whichever side the untyped NULL stands on
        assertMemoryLeak(() -> {
            final String ops = "null != %1$s a, %1$s != null b, null <> %1$s c, %1$s <> null d,"
                    + " null = %1$s e, %1$s = null f, typeOf(null != %1$s) t";
            final String header = "a\tb\tc\td\te\tf\tt\n";
            final String value = "true\ttrue\ttrue\ttrue\tfalse\tfalse\tBOOLEAN\n";
            final String nullValue = "false\tfalse\tfalse\tfalse\ttrue\ttrue\tBOOLEAN\n";
            execute("CREATE TABLE x (d DATE)");
            execute("INSERT INTO x VALUES ('2020-01-01'), (null)");
            assertQuery("SELECT " + String.format(ops, "d") + " FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + value + nullValue);
            assertQuery("x WHERE null != d")
                    .noLeakCheck()
                    .returns("d\n2020-01-01T00:00:00.000Z\n");
            assertQuery("x WHERE NOT (null <> d)")
                    .noLeakCheck()
                    .returns("d\n\n");
            assertQuery("SELECT " + String.format(ops, "'2020-01-01'::DATE"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + value);
            assertQuery("SELECT " + String.format(ops, "null::DATE"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + nullValue);
            bindVariableService.clear();
            bindVariableService.setDate(0, MillisTimestampDriver.floor("2020-01-01"));
            assertQuery("SELECT " + String.format(ops, "$1"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + value);
            bindVariableService.clear();
            bindVariableService.setDate(0);
            assertQuery("SELECT " + String.format(ops, "$1"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + nullValue);
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqDateFunctionFactory();
    }
}
