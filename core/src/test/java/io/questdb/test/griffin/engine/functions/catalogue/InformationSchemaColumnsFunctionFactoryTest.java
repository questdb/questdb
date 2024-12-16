/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
            assertQueryNoLeakCheck(
                    "table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type\n" +
                            "qdb\tpublic\tA\tcol0\t0\t\tyes\tinteger\n" +
                            "qdb\tpublic\tA\tcol1\t1\t\tyes\tcharacter varying\n" +
                            "qdb\tpublic\tA\tcol2\t2\t\tyes\tdouble precision\n" +
                            "qdb\tpublic\tB\tcol0\t0\t\tyes\tbigint\n" +
                            "qdb\tpublic\tB\tcol1\t1\t\tyes\tcharacter varying\n" +
                            "qdb\tpublic\tB\tcol2\t2\t\tyes\treal\n" +
                            "qdb\tpublic\tC\tcol0\t0\t\tyes\tdouble precision\n" +
                            "qdb\tpublic\tC\tcol1\t1\t\tyes\tcharacter\n" +
                            "qdb\tpublic\tC\tcol2\t2\t\tyes\tsmallint\n",
                    "SELECT * FROM information_schema.columns() ORDER BY table_name",
                    null,
                    null,
                    true
            );
        });
    }

    @Test
    public void testRename() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_rename ( ts timestamp, x int ) timestamp(ts) partition by day wal");
            drainWalQueue();

            assertSql("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                    "x\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n", "show columns from test_rename");

            assertSql("table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type\n" +
                    "qdb\tpublic\ttest_rename\tts\t0\t\tyes\ttimestamp without time zone\n" +
                    "qdb\tpublic\ttest_rename\tx\t1\t\tyes\tinteger\n", "information_schema.columns()");

            execute("rename table test_rename to test_renamed");
            drainWalQueue();

            assertSql("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                    "x\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n", "show columns from test_renamed");

            assertSql("table_catalog\ttable_schema\ttable_name\tcolumn_name\tordinal_position\tcolumn_default\tis_nullable\tdata_type\n" +
                    "qdb\tpublic\ttest_renamed\tts\t0\t\tyes\ttimestamp without time zone\n" +
                    "qdb\tpublic\ttest_renamed\tx\t1\t\tyes\tinteger\n", "information_schema.columns()");
        });
    }
}
