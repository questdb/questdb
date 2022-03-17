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

package io.questdb.griffin;

import io.questdb.TelemetryJob;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class HideTelemetryTablesTest extends AbstractGriffinTest {

    @Test
    public void testShow() throws Exception {

        assertMemoryLeak(() -> {
            compiler.compile("create table test(a int)", sqlExecutionContext);
            compiler.compile("create table " + TelemetryJob.tableName + "(a int)", sqlExecutionContext);
            compiler.compile("create table " + TelemetryJob.configTableName + "(a int)", sqlExecutionContext);
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "tables order by 2",
                    sink,
                    "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\tcommitLag\n" +
                            "2\ttelemetry\t\tNONE\t1000\t0\n" +
                            "3\ttelemetry_config\t\tNONE\t1000\t0\n" +
                            "1\ttest\t\tNONE\t1000\t0\n"
            );
        });
    }

    @Test
    public void testHide() throws Exception {

        hideTelemetryTable = true;

        assertMemoryLeak(() -> {
            compiler.compile("create table test(a int)", sqlExecutionContext);
            compiler.compile("create table " + TelemetryJob.tableName + "(a int)", sqlExecutionContext);
            compiler.compile("create table " + TelemetryJob.configTableName + "(a int)", sqlExecutionContext);
            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "tables",
                    sink,
                    "id\tname\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\tcommitLag\n" +
                            "1\ttest\t\tNONE\t1000\t0\n"
            );
        });
    }
}
