/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class LogFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testLogDouble() throws Exception {
        assertLog("select log(9989.2233)", "9.209262120872339\n");
    }

    @Test
    public void testLogDoubleNull() throws Exception {
        assertLog("select log(NaN)", "NaN\n");
    }

    @Test
    public void testLogInt() throws Exception {
        assertLog("select log(8965)", "9.101083386039234\n");
    }

    private void assertLog(String sql, String expected) throws Exception {
        assertMemoryLeak(() -> TestUtils.assertSql(
                compiler,
                sqlExecutionContext,
                sql,
                sink,
                "log\n" +
                        expected
        ));
    }
}