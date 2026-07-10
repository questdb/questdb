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
            execute("insert into x values ('a', 0), ('b', 1000000), ('a', 2000000)");
            drainWalQueue();

            Assert.assertTrue("index files MUST exist on a non-skipping node", ReplicaOnlyIndexTestUtils.indexFilesExist(engine, "x", "s"));
            assertSql(
                    "s\tts\n" +
                            "a\t1970-01-01T00:00:00.000000Z\n" +
                            "a\t1970-01-01T00:00:02.000000Z\n",
                    "select s, ts from x where s = 'a'"
            );
        });
    }

    // Full-battery result assertion via the QueryAssertion builder (second cursor pass + calculateSize
    // cross-check + variable-column check); sizeMayVary()/inferRandomAccess()/inferTimestamp() adopt each
    // heterogeneous factory's own capabilities.
    private void assertSql(String expected, String query) throws Exception {
        assertQuery(query).noLeakCheck().sizeMayVary().inferRandomAccess().inferTimestamp().returns(expected);
    }
}
