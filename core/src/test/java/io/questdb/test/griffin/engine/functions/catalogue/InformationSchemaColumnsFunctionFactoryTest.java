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
            ddl("create table A(col0 int, col1 symbol, col2 double)");
            ddl("create table B(col0 long, col1 string, col2 float)");
            ddl("create table C(col0 double, col1 char, col2 byte)");
            drainWalQueue();
            assertQuery(
                    "table_name\tordinal_position\tcolumn_name\tdata_type\n" +
                            "A\t0\tcol0\tINT\n" +
                            "A\t1\tcol1\tSYMBOL\n" +
                            "A\t2\tcol2\tDOUBLE\n" +
                            "B\t0\tcol0\tLONG\n" +
                            "B\t1\tcol1\tSTRING\n" +
                            "B\t2\tcol2\tFLOAT\n" +
                            "C\t0\tcol0\tDOUBLE\n" +
                            "C\t1\tcol1\tCHAR\n" +
                            "C\t2\tcol2\tBYTE\n",
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
            ddl("create table test_rename ( ts timestamp, x int ) timestamp(ts) partition by day wal");
            drainWalQueue();

            assertSql("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                    "x\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n", "show columns from test_rename");

            assertSql("table_name\tordinal_position\tcolumn_name\tdata_type\n" +
                    "test_rename\t0\tts\tTIMESTAMP\n" +
                    "test_rename\t1\tx\tINT\n", "information_schema.columns()");

            ddl("rename table test_rename to test_renamed");
            drainWalQueue();

            assertSql("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                    "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\ttrue\tfalse\n" +
                    "x\tINT\tfalse\t0\tfalse\t0\tfalse\tfalse\n", "show columns from test_renamed");

            assertSql("table_name\tordinal_position\tcolumn_name\tdata_type\n" +
                    "test_renamed\t0\tts\tTIMESTAMP\n" +
                    "test_renamed\t1\tx\tINT\n", "information_schema.columns()");
        });
    }
}
