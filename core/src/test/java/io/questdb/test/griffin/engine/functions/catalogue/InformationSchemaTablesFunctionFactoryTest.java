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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.TelemetryConfigLogger;
import io.questdb.tasks.TelemetryTask;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InformationSchemaTablesFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testSelectWhenTelemetryTablesAreHidden() throws Exception {
        assertMemoryLeak(() -> {
            node1.getConfigurationOverrides().setIsHidingTelemetryTable(true);
            execute("create table " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME + " (i int)");
            execute("create table " + TelemetryTask.TABLE_NAME + " (i int)");


            assertQuery("table_catalog\ttable_schema\ttable_name\ttable_type\tself_referencing_column_name\treference_generation\tuser_defined_type_catalog\tuser_defined_type_schema\tuser_defined_type_name\tis_insertable_into\tis_typed\tcommit_action\n",
                    "select * from information_schema.tables() order by table_name",
                    null, true, false);
        });
    }

    @Test
    public void testSelectWhenThereAreNoTables() throws Exception {
        assertMemoryLeak(() -> assertQuery("table_catalog\ttable_schema\ttable_name\ttable_type\tself_referencing_column_name\treference_generation\tuser_defined_type_catalog\tuser_defined_type_schema\tuser_defined_type_name\tis_insertable_into\tis_typed\tcommit_action\n",
                "select * from information_schema.tables()",
                null, false, false));
    }

    @Test
    public void testSelectWhenThereAreTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table first_table(i int)");
            execute("create table second_table(i int)");

            assertQuery("table_catalog\ttable_schema\ttable_name\ttable_type\tself_referencing_column_name\treference_generation\tuser_defined_type_catalog\tuser_defined_type_schema\tuser_defined_type_name\tis_insertable_into\tis_typed\tcommit_action\n" +
                            "qdb\tpublic\tfirst_table\tBASE TABLE\t\t\t\t\t\ttrue\tfalse\t\n" +
                            "qdb\tpublic\tsecond_table\tBASE TABLE\t\t\t\t\t\ttrue\tfalse\t\n",
                    "select * from information_schema.tables() order by table_name",
                    null, true, false);
        });
    }
}
