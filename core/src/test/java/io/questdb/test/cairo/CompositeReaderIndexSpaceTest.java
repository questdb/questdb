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
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Follow-up 1 — the reader must name a dimension's HIVE prefix from its DENSE column index, not its
 * WRITER index.
 * <p>
 * {@code PartitionDimension#getColumnIndex()} is a WRITER index: the create-time physical position,
 * persisted in {@code _meta} and deliberately unmoved by a later {@code DROP COLUMN}, which leaves a
 * tombstone behind. {@code TableReader}'s metadata is DENSE — dropped columns are compacted away. The
 * two spaces diverge the instant a lower-index column is dropped.
 * <p>
 * {@code TableReader#renderCellSegment} resolved the dimension VALUE through
 * {@code denseIndexOfDimensionSource} but took the HIVE prefix straight from the writer index, so the
 * two halves of one method disagreed. After a drop it would render {@code px=BTC} where the writer
 * wrote {@code exch=BTC} — a directory the reader cannot find.
 */
public class CompositeReaderIndexSpaceTest extends AbstractCairoTest {

    /**
     * The real proof, and it CANNOT RUN YET: {@code DROP COLUMN} is refused outright on a routed
     * composite table ({@code TableWriter#removeColumn}'s {@code isRoutedComposite()} gate), so the
     * writer and dense index spaces cannot be made to diverge from SQL today.
     * <p>
     * Sub-project 2 (column DDL) lifts that gate. When it does, DELETE the {@code @Ignore} — this test
     * is the reason the fix exists, and lifting the gate without it would silently produce unreadable
     * partitions.
     * <p>
     * An {@code @Ignore} with a precise reason is honest; a test rewritten until it passes against an
     * unreachable condition is not.
     */
    @Test
    @Ignore("blocked by TableWriter#removeColumn's isRoutedComposite() gate; un-ignore when sub-project 2 lifts it")
    public void testHivePrefixSurvivesDropOfLowerIndexColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, foo symbol, exch symbol, px double)"
                    + " timestamp(ts) partition by day, exch wal");
            execute("insert into c values ('2023-01-01T00:00:00.000000Z','f','BTC',1.0)");
            drainWalQueue();

            // foo is writer index 1, exch is 2. Dropping foo shifts exch's DENSE position to 1 while
            // its writer index stays 2.
            execute("alter table c drop column foo");
            drainWalQueue();
            engine.releaseInactive();

            // Must still be exch=, not px=.
            printSql("select name from table_partitions('c')");
            TestUtils.assertContains(sink.toString(), "exch=BTC");

            // ... and the rows must still be readable through that directory.
            printSql("select count() from c");
            TestUtils.assertContains(sink.toString(), "1");
        });
    }

    /**
     * Live guard for what IS reachable: a dimension whose source column is not the first column still
     * renders its own name. This passes both before and after the fix (writer and dense indices
     * coincide while nothing has been dropped), so it is a regression net for the ordinary case
     * rather than a proof of the fix — the proof is the ignored test above.
     */
    @Test
    public void testHivePrefixNamesTheDimensionColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c2 (ts timestamp, foo symbol, exch symbol, px double)"
                    + " timestamp(ts) partition by day, exch wal");
            execute("insert into c2 values ('2023-01-01T00:00:00.000000Z','f','BTC',1.0),"
                    + "('2023-01-01T00:00:01.000000Z','g','ETH',2.0)");
            drainWalQueue();
            engine.releaseInactive();

            printSql("select name from table_partitions('c2') order by name");
            final String names = sink.toString();
            TestUtils.assertContains(names, "exch=BTC");
            TestUtils.assertContains(names, "exch=ETH");
            org.junit.Assert.assertFalse("must not name a different column", names.contains("px="));
            org.junit.Assert.assertFalse("must not name a different column", names.contains("foo="));
        });
    }
}
