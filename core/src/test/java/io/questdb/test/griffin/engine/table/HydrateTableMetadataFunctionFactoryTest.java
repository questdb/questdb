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

package io.questdb.test.griffin.engine.table;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class HydrateTableMetadataFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testHappyPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE 'a' ( ts timestamp) timestamp(ts) partition by day wal");
            execute("CREATE TABLE 'b' ( ts timestamp) timestamp(ts) partition by day wal");
            assertQuery("select hydrate_table_metadata('a', 'b')")
                    .noLeakCheck()
                    .returnsOnce("hydrate_table_metadata\ntrue\n");
            assertQuery("table_columns('a')")
                    .noLeakCheck()
                    .returnsOnce("""
                            column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude
                            ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t
                            """);
        });
    }

    @Test
    public void testNoArgsGiven() throws Exception {
        assertMemoryLeak(() -> assertException("select hydrate_table_metadata()", 7, "no arguments provided"));
    }

    @Test
    public void testNoValidArgsGiven() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE 'a' ( ts timestamp) timestamp(ts) partition by day wal");
            assertException("select hydrate_table_metadata('foo')", 7, "no valid table names provided");
        });
    }

    @Test
    public void testNotAllTablesAreValid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE 'a' ( ts timestamp) timestamp(ts) partition by day wal");
            assertQuery("select hydrate_table_metadata('a', 'b')")
                    .noLeakCheck()
                    .returnsOnce("hydrate_table_metadata\ntrue\n");
        });
    }

    @Test
    public void testTableNameMustBeNonNullTextConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (bo BOOLEAN, c CHAR, s STRING, sym SYMBOL, vc VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            for (String column : new String[]{"bo", "c", "s", "sym", "vc"}) {
                assertException("SELECT hydrate_table_metadata(" + column + ") FROM a", 30, "STRING constant expected");
            }
            assertException("SELECT hydrate_table_metadata('a', s) FROM a", 35, "STRING constant expected");
            assertException("SELECT hydrate_table_metadata(1)", 30, "STRING constant expected");
            bindVariableService.clear();
            bindVariableService.setStr(0, "a");
            assertException("SELECT hydrate_table_metadata($1)", 30, "STRING constant expected");
            assertException("SELECT hydrate_table_metadata(null)", 30, "table name cannot be NULL");
            assertException("SELECT hydrate_table_metadata('a', null::string)", 39, "table name cannot be NULL");
            assertQuery("SELECT hydrate_table_metadata('a'::varchar, 'a'::symbol)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("hydrate_table_metadata\ntrue\n");
        });
    }

    @Test
    public void testWildcardMustBeSolo() throws Exception {
        assertMemoryLeak(() -> assertException("select hydrate_table_metadata('foo', '*')", 7, "cannot use wildcard alongside other table names"));
    }
}