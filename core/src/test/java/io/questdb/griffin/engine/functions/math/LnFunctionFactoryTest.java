/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.math;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class LnFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testLnDouble() throws Exception {
        assertLog("select ln(9989.2233)", "9.209262120872339\n");
    }

    @Test
    public void testLnDoubleNull() throws Exception {
        assertLog("select ln(NaN)", "NaN\n");
    }

    @Test
    public void testLnInt() throws Exception {
        assertLog("select ln(11211)", "9.324650718153594\n");
    }

    private void assertLog(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink,
                "ln\n" +
                        expected
        ));
    }
}