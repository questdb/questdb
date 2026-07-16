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

package io.questdb.test.cairo;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Contrast to {@link TableWriterReplicaOnlySkipTest}: under the DEFAULT configuration
 * ({@code skipReplicaOnlyIndexes()} == false, e.g. a replica or standalone node) the REPLICA ONLY
 * modifier is recorded but the bitmap index is still built. This both proves the no-skip path is
 * unchanged AND that the shared {@link ReplicaOnlyIndexTestUtils#indexFilesExist} probe is not vacuously false (it returns
 * TRUE when an index really exists), validating the skip-side assertion in the sibling class.
 */
public class TableWriterReplicaOnlyNoSkipTest extends AbstractCairoTest {

    @Test
    public void testReplicaNodeBuildsReplicaOnlyIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, ts timestamp) timestamp(ts) partition by day wal");
            execute("INSERT INTO x VALUES ('a', 0), ('b', 1_000_000), ('a', 2_000_000), (NULL, 3_000_000)");
            drainWalQueue();

            Assert.assertTrue("index files MUST exist on a non-skipping node", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertQuery("select s, ts from x where s = 'a'").timestamp("ts").returns(
                    "s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t1970-01-01T00:00:02.000000Z\n"
            );
            assertQuery("SELECT s, ts FROM x WHERE s IS NULL")
                    .timestamp("ts")
                    .returns("s\tts\n\t1970-01-01T00:00:03.000000Z\n");
        });
    }

    // touch() reads the replication role LIVE at execution (not captured at construction). On a
    // non-skipping node the replica-only index IS active, so touch() must open its index reader and
    // report a non-zero index_key_pages -- the complement of the skipping-node touch test.
    @Test
    public void testTouchReadsActiveReplicaOnlyIndexOnReplica() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (s SYMBOL INDEX REPLICA ONLY, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('a', 1, 0), ('b', 2, 1_000_000), ('a', 3, 2_000_000)");
            Assert.assertTrue("index must be materialized on a non-skipping node", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));

            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile("SELECT touch((SELECT s, v, ts FROM x))", sqlExecutionContext).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                final CharSequence out = cursor.getRecord().getStrA(0);
                Assert.assertTrue("touch() output must report index pages: " + out, Chars.contains(out, "index_key_pages"));
                Assert.assertFalse(
                        "touch() must have touched the active replica-only index (non-zero key pages): " + out,
                        Chars.contains(out, "index_key_pages\":0")
                );
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }
}
