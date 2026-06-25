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

import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InformationSchemaColumnsFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table A(col0 int, col1 symbol, col2 double)");
            execute("create table B(col0 long, col1 string, col2 float)");
            execute("create table C(col0 double, col1 char, col2 byte)");
            drainWalQueue();
            assertQuery("SELECT * FROM information_schema.columns() ORDER BY table_name")
                    .noLeakCheck()
                    .ddl(null)
                    .returns("""
                            table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type
                            qdb\tpublic\tA\tcol0\t0\t\tyes\tinteger
                            qdb\tpublic\tA\tcol1\t1\t\tyes\tcharacter varying
                            qdb\tpublic\tA\tcol2\t2\t\tyes\tdouble precision
                            qdb\tpublic\tB\tcol0\t0\t\tyes\tbigint
                            qdb\tpublic\tB\tcol1\t1\t\tyes\tcharacter varying
                            qdb\tpublic\tB\tcol2\t2\t\tyes\treal
                            qdb\tpublic\tC\tcol0\t0\t\tyes\tdouble precision
                            qdb\tpublic\tC\tcol1\t1\t\tyes\tcharacter
                            qdb\tpublic\tC\tcol2\t2\t\tyes\tsmallint
                            """);
        });
    }

    @Test
    public void testColumnsCompleteBeforeStartupHydration() throws Exception {
        // Regression for the startup-hydration race shared with tables()/all_tables():
        // information_schema.columns() (and the pg_catalog.pg_attribute it backs)
        // snapshot MetadataCache, which the background onStartupAsyncHydrator() fills
        // lazily. A query in the post-restart / post-backup-restore pre-hydration
        // window used to omit tables and their columns, which breaks PostgreSQL
        // driver introspection (the pg_class -> pg_attribute join renders existing
        // tables with zero columns). getCursor() must reconcile first.
        assertMemoryLeak(() -> {
            execute("create table A(col0 int, col1 symbol)");
            execute("create table B(col0 long, col1 string)");
            drainWalQueue();

            // Simulate the pre-hydration window: the registry knows the tables, but
            // the metadata cache is empty (async hydrator has not run).
            try (MetadataCacheWriter w = engine.getMetadataCache().writeLock()) {
                w.clearCache();
            }

            assertQuery("SELECT table_name, column_name FROM information_schema.columns() ORDER BY table_name, column_name")
                    .noLeakCheck()
                    .ddl(null)
                    .returns("""
                            table_name	column_name
                            A	col0
                            A	col1
                            B	col0
                            B	col1
                            """);
        });
    }

    @Test
    public void testDropAndRecreateWithDifferentColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (old int)");

            // a single RecordCursorFactory is reused before and after the table is
            // dropped and recreated, mimicking the behavior of a query cache
            assertQuery("information_schema.columns()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .sizeMayVary()
                    .mutateWith("drop table x", "create table x (new long)") // recreate with different column type and name
                    .returns(
                            """
                                    table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type
                                    qdb\tpublic\tx\told\t0\t\tyes\tinteger
                                    """,
                            """
                                    table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type
                                    qdb\tpublic\tx\tnew\t0\t\tyes\tbigint
                                    """
                    );
        });
    }

    @Test
    public void testRename() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_rename ( ts timestamp, x int ) timestamp(ts) partition by day wal");
            drainWalQueue();

            assertQuery("show columns from test_rename")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                            x\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            """);

            assertQuery("information_schema.columns()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type
                            qdb\tpublic\ttest_rename\tts\t0\t\tyes\ttimestamp without time zone
                            qdb\tpublic\ttest_rename\tx\t1\t\tyes\tinteger
                            """);

            execute("rename table test_rename to test_renamed");
            drainWalQueue();

            assertQuery("show columns from test_renamed")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                            x\tINT\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t
                            """);

            assertQuery("information_schema.columns()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type
                            qdb\tpublic\ttest_renamed\tts\t0\t\tyes\ttimestamp without time zone
                            qdb\tpublic\ttest_renamed\tx\t1\t\tyes\tinteger
                            """);
        });
    }
}
