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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class ReplicaOnlyIndexIntrospectionTest extends AbstractCairoTest {

    @Test
    public void testShowCreateTableEmitsReplicaOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");

            // SHOW CREATE TABLE must re-emit the REPLICA ONLY modifier on the index clause.
            printSql("SHOW CREATE TABLE x;");
            final String ddl = sink.toString().replace("ddl\n", "");
            TestUtils.assertContains(ddl, "INDEX CAPACITY 256 REPLICA ONLY");

            // Round-trip: drop and re-create from the emitted DDL; the index clause
            // (including the REPLICA ONLY modifier) must reproduce exactly.
            execute("drop table x;");
            execute(ddl);
            printSql("SHOW CREATE TABLE x;");
            final String ddl2 = sink.toString().replace("ddl\n", "");
            TestUtils.assertContains(ddl2, "INDEX CAPACITY 256 REPLICA ONLY");
        });
    }

    @Test
    public void testShowCreateTablePostingReplicaOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table z (s symbol index type posting include (p) replica only, p double, ts timestamp) timestamp(ts) partition by day wal");

            // REPLICA ONLY must follow the full INDEX TYPE ... INCLUDE(...) clause.
            printSql("SHOW CREATE TABLE z;");
            final String ddl = sink.toString().replace("ddl\n", "");
            TestUtils.assertContains(ddl, "REPLICA ONLY");

            // Round-trip the rendered DDL: the REPLICA ONLY modifier must persist.
            execute("drop table z;");
            execute(ddl);
            printSql("SHOW CREATE TABLE z;");
            TestUtils.assertContains(sink.toString().replace("ddl\n", ""), "REPLICA ONLY");
        });
    }

    @Test
    public void testTableColumnsExposesReplicaOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");

            // Default config does NOT skip replica-only indexes, so indexed stays true
            // and indexReplicaOnly is true for the flagged symbol column.
            assertQuery("select \"column\", indexed, indexReplicaOnly from table_columns('x')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\tindexed\tindexReplicaOnly
                            s\ttrue\ttrue
                            ts\tfalse\tfalse
                            """);
        });
    }
}
