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

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class AlterTableAddReplicaOnlyIndexTest extends AbstractCairoTest {

    @Test
    public void testAlterAddIndexNotReplicaOnly() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (s symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into y values ('a', 0)");
            drainWalQueue();
            execute("alter table y alter column s add index capacity 256");
            drainWalQueue();
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("y"))) {
                Assert.assertFalse(m.isColumnReplicaOnlyIndex(m.getColumnIndex("s")));
            }
        });
    }

    @Test
    public void testDropIndexClearsReplicaOnlyFlag() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 0), ('b', 1000000)");
            drainWalQueue();

            execute("alter table x alter column s drop index");
            drainWalQueue();

            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("x"))) {
                int i = m.getColumnIndex("s");
                Assert.assertFalse("index dropped -> not indexed", m.isColumnIndexed(i));
                Assert.assertFalse("drop index must clear the replica-only flag", m.isColumnReplicaOnlyIndex(i));
            }

            // table_columns() must reflect the cleared flag
            assertQuery("select \"column\", indexed, indexReplicaOnly from table_columns('x')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            column\tindexed\tindexReplicaOnly
                            s\tfalse\tfalse
                            ts\tfalse\tfalse
                            """);
        });
    }

    @Test
    public void testAlterAddReplicaOnlyIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into x values ('a', 0)");
            drainWalQueue();
            execute("alter table x alter column s add index capacity 256 replica only");
            drainWalQueue();
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("x"))) {
                int i = m.getColumnIndex("s");
                Assert.assertTrue(m.isColumnIndexed(i));
                Assert.assertTrue(m.isColumnReplicaOnlyIndex(i));
            }
        });
    }
}
