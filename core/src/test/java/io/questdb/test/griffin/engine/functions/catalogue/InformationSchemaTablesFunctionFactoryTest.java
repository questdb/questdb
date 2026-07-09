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


            assertQuery("select * from information_schema.tables() order by table_name")
                    .returns("table_catalog\ttable_schema\ttable_name\ttable_type\tself_referencing_column_name\treference_generation\tuser_defined_type_catalog\tuser_defined_type_schema\tuser_defined_type_name\tis_insertable_into\tis_typed\tcommit_action\n");
        });
    }

    @Test
    public void testSelectWhenThereAreNoTables() throws Exception {
        assertMemoryLeak(() -> assertQuery("select * from information_schema.tables()")
                .noRandomAccess()
                .returns("table_catalog\ttable_schema\ttable_name\ttable_type\tself_referencing_column_name\treference_generation\tuser_defined_type_catalog\tuser_defined_type_schema\tuser_defined_type_name\tis_insertable_into\tis_typed\tcommit_action\n"));
    }

    @Test
    public void testSelectWhenThereAreTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table first_table(i int)");
            execute("create table second_table(i int)");

            assertQuery("select * from information_schema.tables() order by table_name")
                    .returns("""
                            table_catalog\ttable_schema\ttable_name\ttable_type\tself_referencing_column_name\treference_generation\tuser_defined_type_catalog\tuser_defined_type_schema\tuser_defined_type_name\tis_insertable_into\tis_typed\tcommit_action
                            qdb\tpublic\tfirst_table\tBASE TABLE\t\t\t\t\t\ttrue\tfalse\t
                            qdb\tpublic\tsecond_table\tBASE TABLE\t\t\t\t\t\ttrue\tfalse\t
                            """);
        });
    }

    @Test
    public void testTableTypesForViewKinds() throws Exception {
        // PG tooling and BI clients key on table_type / is_insertable_into: a
        // materialized view must report "MATERIALIZED VIEW", a view "VIEW", a
        // live view "LIVE VIEW", and none of the three accept INSERTs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mat_v AS (SELECT ts, max(v) FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("CREATE VIEW plain_v AS (SELECT ts, max(v) FROM base SAMPLE BY 1h)");
            execute("CREATE LIVE VIEW live_v FLUSH EVERY 1s AS SELECT ts, v, row_number() OVER () AS rn FROM base");

            assertQuery("SELECT table_name, table_type, is_insertable_into FROM information_schema.tables() ORDER BY table_name")
                    .returns("""
                            table_name\ttable_type\tis_insertable_into
                            base\tBASE TABLE\ttrue
                            live_v\tLIVE VIEW\tfalse
                            mat_v\tMATERIALIZED VIEW\tfalse
                            plain_v\tVIEW\tfalse
                            """);
        });
    }
}
