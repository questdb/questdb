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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins which plans the QWP egress processor streams through page frames, and
 * in which order: the column-at-a-time copy of a page frame is several times
 * faster than reading the RecordCursor row by row, and a backward scan keeps it
 * by asking for its frames in descending order.
 */
public class QwpEgressPageFrameOrderTest extends AbstractCairoTest {

    @Test
    public void testPageFrameOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x LONG, s SYMBOL, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t SELECT x, 's' || (x % 3), 'v' || x, (x * 3_600_000_000L)::TIMESTAMP FROM long_sequence(100)");
            // {sql, page-frame order without a row window}
            final Object[][] cases = {
                    {"SELECT * FROM t", PartitionFrameCursorFactory.ORDER_ASC},
                    {"SELECT * FROM t ORDER BY ts", PartitionFrameCursorFactory.ORDER_ASC},
                    {"SELECT * FROM t ORDER BY ts DESC", PartitionFrameCursorFactory.ORDER_DESC},
                    {"SELECT v, x FROM t ORDER BY ts DESC", PartitionFrameCursorFactory.ORDER_DESC},
                    {"SELECT * FROM t WHERE ts IN '1970-01-02' ORDER BY ts DESC", PartitionFrameCursorFactory.ORDER_DESC},
                    {"SELECT * FROM t ORDER BY ts DESC LIMIT 5", QwpEgressUpgradeProcessor.NO_PAGE_FRAMES},
                    {"SELECT * FROM t ORDER BY x DESC", QwpEgressUpgradeProcessor.NO_PAGE_FRAMES},
                    {"SELECT s, count() FROM t", QwpEgressUpgradeProcessor.NO_PAGE_FRAMES},
            };
            for (Object[] c : cases) {
                final String sql = (String) c[0];
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertEquals(sql, (int) c[1], QwpEgressUpgradeProcessor.pageFrameOrder(factory, false));
                    // a row window needs the RecordCursor's skipRows and size, whatever the plan
                    Assert.assertEquals(sql, QwpEgressUpgradeProcessor.NO_PAGE_FRAMES,
                            QwpEgressUpgradeProcessor.pageFrameOrder(factory, true));
                }
            }
        });
    }
}
