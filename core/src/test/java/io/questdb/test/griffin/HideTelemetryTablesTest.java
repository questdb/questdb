/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.TelemetryConfigLogger;
import io.questdb.tasks.TelemetryTask;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class HideTelemetryTablesTest extends AbstractCairoTest {

    @Test
    public void testHide() throws Exception {
        node1.getConfigurationOverrides().setIsHidingTelemetryTable(true);

        assertMemoryLeak(() -> {
            ddl("create table test(a int)");
            ddl("create table " + TelemetryTask.TABLE_NAME + "(a int)");
            ddl("create table " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME + "(a int)");
            assertSql(
                    "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                            "1\ttest\t\tNONE\t1000\t300000000\n", "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()"
            );
        });
    }

    @Test
    public void testShow() throws Exception {
        node1.getConfigurationOverrides().setIsHidingTelemetryTable(false);

        assertMemoryLeak(() -> {
            ddl("create table test(a int)");
            ddl("create table " + TelemetryTask.TABLE_NAME + "(a int)");
            ddl("create table " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME + "(a int)");
            assertSql(
                    "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                            "2\ttelemetry\t\tNONE\t1000\t300000000\n" +
                            "3\ttelemetry_config\t\tNONE\t1000\t300000000\n" +
                            "1\ttest\t\tNONE\t1000\t300000000\n", "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables() order by 2"
            );
        });
    }
}
