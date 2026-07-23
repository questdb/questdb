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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory;
import io.questdb.test.AbstractOomSweepTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Verifies that a keyed ASOF JOIN backed by {@code AsOfJoinFastRecordCursorFactory}
 * (and its filtered variant) releases its resources when a native allocation fails
 * while the cursor is being opened.
 * <p>
 * The cursor reopens two {@code SingleRecordSink} heaps in {@code of()}; if the
 * second {@code reopen()} trips the RSS memory limit after the first has already
 * allocated, {@code getCursor()} throws and the half-opened cursor is orphaned
 * (the factory's {@code _close()} does not free the reusable cursor). That leaked
 * the first sink's 8-byte heap, tagged {@code NATIVE_RECORD_CHAIN}. The query
 * fuzzer's malloc fault injection surfaced this leak.
 * <p>
 * {@link AbstractOomSweepTest#assertCursorOpenOomSweep} drives the ceiling sweep; the two tests here
 * differ only in which factory they route the fault into.
 */
public class AsOfJoinFastOomTest extends AbstractOomSweepTest {

    @Test
    public void testFilteredKeyedAsOfJoinCleansUpWhenCursorRunsOutOfMemory() throws Exception {
        // The filter has to sit in the slave sub-query. As a top-level WHERE it becomes a post-join
        // Filter over a plain AsOf Join Fast, and this test would silently duplicate the one below.
        assertNoLeakOnCursorOom(
                "SELECT m.k1, m.v, s.v FROM master m ASOF JOIN (SELECT * FROM slave WHERE v > 0) s ON (k1, k2)",
                FilteredAsOfJoinFastRecordCursorFactory.class
        );
    }

    @Test
    public void testKeyedAsOfJoinCleansUpWhenCursorRunsOutOfMemory() throws Exception {
        assertNoLeakOnCursorOom(
                "SELECT m.k1, m.v, s.v FROM master m ASOF JOIN slave s ON (k1, k2)",
                AsOfJoinFastRecordCursorFactory.class
        );
    }

    private void assertNoLeakOnCursorOom(String query, Class<? extends RecordCursorFactory> expectedFactory) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE master AS (" +
                            "  SELECT rnd_symbol('a','b','c') k1, rnd_symbol('x','y') k2, rnd_int() v," +
                            "  timestamp_sequence(0, 60 * 1_000_000L) ts" +
                            "  FROM long_sequence(200)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "CREATE TABLE slave AS (" +
                            "  SELECT rnd_symbol('a','b','c') k1, rnd_symbol('x','y') k2, rnd_int() v," +
                            "  timestamp_sequence(0, 30 * 1_000_000L) ts" +
                            "  FROM long_sequence(200)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Confirm the query really exercises the cursor under test. The EXPLAIN type name cannot
            // do this: "AsOf Join Fast" is emitted by both the keyed and the no-key fast factory, and
            // is a substring of "Filtered AsOf Join Fast", so a name guard passes for four different
            // factories. Match the class instead - these factories are siblings, so no subclass of one
            // can pass for another.
            try (RecordCursorFactory factory = select(query)) {
                TestUtils.assertFactoryInTree(factory, expectedFactory, query);
            }

            assertCursorOpenOomSweep(query);
        });
    }
}
