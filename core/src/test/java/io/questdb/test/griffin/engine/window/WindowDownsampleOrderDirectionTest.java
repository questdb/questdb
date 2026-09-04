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

package io.questdb.test.griffin.engine.window;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Ascending-order contract for the direct time-based downsampling window functions
 * (m4, minmax, lttb, sdt). Their algorithms (M4Algorithm, MinMaxAlgorithm, LttbAlgorithm
 * gap segmentation, SwingingDoor) consume pass1 input in non-decreasing timestamp order;
 * before this contract was enforced, a DESC or mismatched window order produced silently
 * wrong keep flags (m4/minmax collapsed to the newest row, sdt became a keep-everything
 * no-op, gap lttb degraded to no-gap mode).
 * <p>
 * Enforcement, pinned here:
 * <ul>
 *   <li>a descending window ORDER BY is rejected at compile time, on both the sorted path
 *       (initRecordComparator receives the pass1 traversal directions) and the
 *       order-dismissed backward-scan path (WindowContext.getOrderByScanDirection());</li>
 *   <li>for m4/minmax/lttb, whose bucketing has a hard ascending-buffer precondition, an
 *       ascending order key that is not the timestamp argument is additionally policed at
 *       runtime: pass1 throws on the first actual backward timestamp step, and stays legal
 *       when the timestamps happen to arrive ascending;</li>
 *   <li>sdt keeps its documented tolerance for backward steps in the timestamp ARGUMENT
 *       (series-boundary semantics; see the json_extract and nanos-wrap tests in
 *       SdtWindowFunctionTest) - only the window ORDER BY direction is validated.</li>
 * </ul>
 * All rejection errors mention "ascending" and are positioned at the function token.
 */
public class WindowDownsampleOrderDirectionTest extends AbstractCairoTest {

    // 100-row ascending ramp: ts = epoch + x seconds, v = x
    private static final String RAMP_DDL =
            "create table t as (select (x * 1000000)::timestamp ts, x::double v from long_sequence(100)) timestamp(ts) partition by day";

    // ascending ts, but v deliberately NOT aligned with ts: ORDER BY v shuffles the timestamps
    private static final String SHUFFLED_DDL =
            "create table s as (select (x * 1000000)::timestamp ts, ((x * 37) % 100)::double v from long_sequence(100)) timestamp(ts) partition by day";

    // two 50-row clusters, 1s apart inside a cluster, 1 day between clusters
    private static final String CLUSTER_DDL =
            "create table g as (select case when x <= 50 then (x * 1000000)::timestamp else (86400000000L + x * 1000000)::timestamp end ts, x::double v from long_sequence(100)) timestamp(ts) partition by day";

    @Test
    public void testLttbGapRejectsDescendingOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute(CLUSTER_DDL);
            // unguarded, gap detection (currTs > prevTs + threshold) can never fire on DESC
            // input, silently collapsing 2 segments (4 endpoints) into no-gap mode (2 endpoints)
            assertExceptionNoLeakCheck(
                    "select ts, v, lttb(ts, v, 2, '1h') over (order by ts desc) k from g",
                    14,
                    "ascending"
            );
        });
    }

    @Test
    public void testLttbRejectsDescendingOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute(RAMP_DDL);
            assertExceptionNoLeakCheck(
                    "select ts, v, lttb(ts, v, 8) over (order by ts desc) k from t",
                    14,
                    "ascending"
            );
        });
    }

    @Test
    public void testM4RejectsDescendingOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute(RAMP_DDL);
            // unguarded, DESC input collapses the bucketing to keep only the newest row
            assertExceptionNoLeakCheck(
                    "select ts, v, m4(ts, v, 8) over (order by ts desc) k from t",
                    14,
                    "ascending"
            );
        });
    }

    @Test
    public void testM4RejectsDescendingOrderOnBackwardScannedBase() throws Exception {
        assertMemoryLeak(() -> {
            execute(RAMP_DDL);
            // window order may be dismissed against the already-backward base scan
            // (WindowContext.getOrderByScanDirection()); that path must reject too
            assertExceptionNoLeakCheck(
                    "select ts, v, m4(ts, v, 8) over (order by ts desc) k from (select * from t order by ts desc)",
                    14,
                    "ascending"
            );
        });
    }

    @Test
    public void testM4RejectsOrderByMismatchedColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(SHUFFLED_DDL);
            // ascending window order, but on v, not the ts argument: compile-time direction
            // validation cannot see this, so pass1's runtime monotonicity guard rejects it on
            // the first actual backward timestamp step
            assertExceptionNoLeakCheck(
                    "select ts, v, m4(ts, v, 8) over (order by v) k from s",
                    14,
                    "ascending"
            );
        });
    }

    @Test
    public void testMinMaxRejectsDescendingOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute(RAMP_DDL);
            // unguarded, DESC input collapses the bucketing to keep only the newest row
            assertExceptionNoLeakCheck(
                    "select ts, v, minmax(ts, v, 8) over (order by ts desc) k from t",
                    14,
                    "ascending"
            );
        });
    }

    @Test
    public void testSdtRejectsDescendingOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute(RAMP_DDL);
            // unguarded, every backward step re-anchors, keeping 100/100 rows (compression no-op)
            assertExceptionNoLeakCheck(
                    "select ts, v, sdt(ts, v, 0.5) over (order by ts desc) k from t",
                    14,
                    "ascending"
            );
        });
    }

    @Test
    public void testSdtMismatchedOrderKeyKeepsBoundarySemantics() throws Exception {
        // sdt's timestamp argument is deliberately ANY timestamp expression, not necessarily
        // the window order key, and a backward step in it is a documented series boundary -
        // NOT an error (unlike m4/minmax/lttb, sdt has no runtime monotonicity guard, or the
        // json_extract/nanos-wrap behaviors in SdtWindowFunctionTest would be unreachable).
        // Traversal is in k order: ats runs 1000, 2000, 3000, then backward to 500, then 4000.
        // Flat corridor drops the interior point at traversal position 2 (id 3); the backward
        // step re-anchors (keeps id 5) and the pending point before it (id 2) stays flushed.
        assertQuery("select id from (select id, sdt(ats, val, 0.5) over (order by k) keep from m) where keep order by id")
                .ddl("create table m (id int, ats timestamp, k double, val double, ts timestamp) timestamp(ts)",
                        "insert into m values " +
                                "(1, 1000::timestamp, 1.0, 0.0, 1::timestamp)," +
                                "(2, 3000::timestamp, 3.0, 0.0, 2::timestamp)," +
                                "(3, 2000::timestamp, 2.0, 0.0, 3::timestamp)," +
                                "(4, 4000::timestamp, 5.0, 0.0, 4::timestamp)," +
                                "(5, 500::timestamp, 4.0, 0.0, 5::timestamp)")
                .returns("id\n1\n2\n4\n5\n");
    }

    @Test
    public void testM4AlignedNonTimestampOrderKeyStaysAccepted() throws Exception {
        // Precision check on the runtime guard: an ascending ORDER BY on a key that is not the
        // timestamp argument stays legal as long as the timestamps actually arrive ascending -
        // the guard rejects real backward steps, not the shape of the OVER clause. v tracks ts
        // on the ramp, so this is the same selection as ORDER BY ts: 2 buckets x {first,last}.
        assertQuery("select count() c from (select ts, v, m4(ts, v, 8) over (order by v) k from t) where k")
                .ddl(RAMP_DDL)
                .noRandomAccess()
                .expectSize()
                .returns("c\n4\n");
    }
}
