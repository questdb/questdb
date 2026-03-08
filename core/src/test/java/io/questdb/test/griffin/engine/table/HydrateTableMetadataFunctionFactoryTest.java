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

package io.questdb.test.griffin.engine.table;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class HydrateTableMetadataFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testHappyPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE 'a' ( ts timestamp) timestamp(ts) partition by day wal");
            execute("CREATE TABLE 'b' ( ts timestamp) timestamp(ts) partition by day wal");
            assertSql("hydrate_table_metadata\ntrue\n", "select hydrate_table_metadata('a', 'b')");
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\n",
                    "table_columns('a')"
            );
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
            assertSql("hydrate_table_metadata\ntrue\n", "select hydrate_table_metadata('a', 'b')");
        });
    }

    @Test
    public void testWildcardMustBeSolo() throws Exception {
        assertMemoryLeak(() -> assertException("select hydrate_table_metadata('foo', '*')", 7, "cannot use wildcard alongside other table names"));
    }
}