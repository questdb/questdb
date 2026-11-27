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

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.eq.EqShortFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class EqShortFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testAll() throws SqlException {
        call(10, 20).andAssert(false);
        call(150, 150).andAssert(true);
        call(0, 77).andAssert(false);
        call(0, 0).andAssert(true);
    }

    @Test
    public void testNullComparedWithShort() throws Exception {
        // SHORT has no NULL sentinel, so no SHORT value equals NULL, 0 included, whichever side the NULL stands on
        assertMemoryLeak(() -> {
            final String ops = "null != %1$s a, %1$s != null b, null <> %1$s c, %1$s <> null d,"
                    + " null = %1$s e, %1$s = null f, typeOf(null != %1$s) t";
            final String header = "a\tb\tc\td\te\tf\tt\n";
            final String row = "true\ttrue\ttrue\ttrue\tfalse\tfalse\tBOOLEAN\n";
            execute("CREATE TABLE x (sh SHORT)");
            execute("INSERT INTO x VALUES (0), (5), (null)");
            assertQuery("SELECT sh, " + String.format(ops, "sh") + " FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sh\t" + header + "0\t" + row + "5\t" + row + "0\t" + row);
            assertQuery("x WHERE null != sh")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sh\n0\n5\n0\n");
            assertQuery("SELECT " + String.format(ops, "0::SHORT"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + row);
            assertQuery("SELECT " + String.format(ops, "null::SHORT"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + row);
            bindVariableService.clear();
            bindVariableService.setShort(0, (short) 0);
            assertQuery("SELECT " + String.format(ops, "$1"))
                    .noLeakCheck()
                    .expectSize()
                    .returns(header + row);
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqShortFunctionFactory();
    }

}