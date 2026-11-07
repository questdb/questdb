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

import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.griffin.CompiledQuery;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class QwpEgressProcessorStatePageFrameTest extends AbstractCairoTest {

    @Test
    public void testBeginStreamingPageFrameRejectsMetadataOnlyFrames() throws Exception {
        // A single-key covering index scan produces page frames that carry no column data.
        // The columnar emit path reads frame addresses directly, so it would ship NULLs; the
        // upgrade processor streams such a factory through the RecordCursor path instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (sym SYMBOL, price LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('A', 1, '2024-01-01T00:00:00Z'), ('B', 2, '2024-01-01T01:00:00Z')");
            execute("ALTER TABLE t ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            final String sql = "SELECT price FROM t WHERE sym = 'A'";
            try (
                    QwpEgressProcessorState state = new QwpEgressProcessorState(configuration);
                    RecordCursorFactory factory = select(sql);
                    PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)
            ) {
                Assert.assertTrue(factory.supportsPageFrameCursor());
                Assert.assertFalse(factory.producesMaterializedPageFrames());
                Assert.assertNotNull(cursor);
                boolean isRejected = false;
                try {
                    state.beginStreamingPageFrame(1, factory, cursor, 1, 0, sql, CompiledQuery.SELECT, true);
                } catch (AssertionError e) {
                    TestUtils.assertContains(e.getMessage(), "page-frame streaming over metadata-only page frames");
                    isRejected = true;
                }
                Assert.assertTrue(isRejected);
                Assert.assertFalse(state.isStreamingActive());
            }
        });
    }
}
