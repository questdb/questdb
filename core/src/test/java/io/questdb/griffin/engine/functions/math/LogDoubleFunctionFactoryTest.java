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

public class LogDoubleFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testLogDouble() throws Exception {
        assertLog("select log(2,9989.2233)", "13.2861567920291\n");
    }

    @Test
    public void testLogDoubleNull() throws Exception {
        assertLog("select log(NaN,NaN)", "NaN\n");
    }

    @Test
    public void testLogInt() throws Exception {
        assertLog("select log(5,8965)", "5.654821050086258\n");
    }

    @Test
    public void testLogPerfect() throws Exception {
        assertLog("select log(100,1000000)", "3.0\n");
    }

    @Test
    public void testLogZeroArgument() throws Exception {
        assertLog("select log(5,0)", "-Infinity\n");
    }

    @Test
    public void testLogZeroBase() throws Exception {
        assertLog("select log(0,5)", "-0.0\n");
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