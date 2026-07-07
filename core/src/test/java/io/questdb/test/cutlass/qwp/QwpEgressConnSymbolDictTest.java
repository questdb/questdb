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

import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class QwpEgressConnSymbolDictTest {

    /**
     * {@code addEntry} commits a new entry to the connection-scoped dict
     * (incrementing {@code size}, growing the heap, populating the dedup map)
     * as soon as it is called. Exposes the contract the next tests rely on.
     */
    @Test
    public void testAddEntryCommitsImmediately() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()) {
                Assert.assertEquals(0, dict.size());
                int id0 = dict.addEntry("foo");
                Assert.assertEquals(0, id0);
                Assert.assertEquals(1, dict.size());
                int id1 = dict.addEntry("bar");
                Assert.assertEquals(1, id1);
                Assert.assertEquals(2, dict.size());
            }
        });
    }

    /**
     * Repeated {@code addEntry} calls for the same byte sequence always return
     * the first-sight id. This is the dedup contract the server's encoder relies
     * on to keep per-row cost to a varint.
     */
    @Test
    public void testAddEntryDedupsIdenticalBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()) {
                int id0 = dict.addEntry("foo");
                int id1 = dict.addEntry("foo");
                int id2 = dict.addEntry("foo");
                Assert.assertEquals(0, id0);
                Assert.assertEquals("dedup must return the original id", 0, id1);
                Assert.assertEquals("dedup must return the original id", 0, id2);
                Assert.assertEquals("dict must hold a single entry after dedup", 1, dict.size());
            }
        });
    }

    /**
     * Models the mid-batch failure sequence on the server's egress path:
     * <ol>
     *   <li>Query A: {@code QwpResultBatchBuffer.beginBatch} snapshots
     *       {@code batchDeltaStart = connDict.size()} (here, 0). The row loop
     *       calls {@code appendCell}, which calls {@code connDict.addEntry} for
     *       each first-sight symbol, committing entries with ids 0 and 1.</li>
     *   <li>Query A fails mid-batch (e.g. {@code CairoException} from
     *       {@code record.getArray}, {@code OutOfMemoryError} from a scratch
     *       grow, or an {@code HttpException} thrown during emit). The frame is
     *       never transmitted; the client never learns about entries 0 or 1.</li>
     *   <li>The server's {@code catch (Throwable)} calls
     *       {@code batchBuffer.rollbackCurrentBatch()} before
     *       {@code state.endStreaming()}, truncating the dict back to
     *       {@code batchDeltaStart}.</li>
     *   <li>Query B starts on the same connection. {@code beginBatch} snapshots
     *       {@code batchDeltaStart = 0}. Query B re-encounters the same symbol,
     *       {@code addEntry} returns a fresh id that sits within
     *       {@code [batchDeltaStart, size())}, and the client receives the
     *       entry in Query B's delta section.</li>
     * </ol>
     * <p>
     * The test pins two invariants:
     * <ul>
     *   <li>{@code rollbackTo(batchDeltaStart)} is applied on aborted batches.</li>
     *   <li>After rollback, dedup returns an id within the new batch's delta
     *       window, i.e. the client always sees every id the server hands out
     *       in row payloads.</li>
     * </ul>
     */
    @Test
    public void testMidBatchAbortDoesNotLeaveOrphans() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()) {
                // ---- Query A's batch commits two entries, then aborts. ----
                int queryABatchDeltaStart = dict.size();
                int fooId = dict.addEntry("foo");
                int barId = dict.addEntry("bar");
                Assert.assertEquals(0, fooId);
                Assert.assertEquals(1, barId);
                Assert.assertEquals(2, dict.size());

                // Server's catch(Throwable) path rolls back the in-flight batch.
                dict.rollbackTo(queryABatchDeltaStart);
                Assert.assertEquals("rollback must undo entries committed by the aborted batch",
                        0, dict.size());

                // ---- Query B begins. beginBatch captures batchDeltaStart. ----
                int queryBBatchDeltaStart = dict.size();
                Assert.assertEquals(0, queryBBatchDeltaStart);

                // Query B re-encounters "foo". The dedup map is clean, so this
                // produces a fresh commit inside the current delta window.
                int fooIdAgain = dict.addEntry("foo");
                Assert.assertEquals("fresh commit after rollback", 0, fooIdAgain);
                Assert.assertTrue(
                        "returned id must sit within the current batch's delta window [start, size)",
                        fooIdAgain >= queryBBatchDeltaStart);
                Assert.assertTrue(fooIdAgain < dict.size());
            }
        });
    }

    /**
     * Spot-checks {@link QwpEgressConnSymbolDict#rollbackTo(int)} on its own,
     * independent of the failure-path scenario:
     * <ul>
     *   <li>truncates {@code size} to the target;</li>
     *   <li>scrubs the dedup map of entries >= target, so the same bytes get a
     *       fresh id on re-insert;</li>
     *   <li>preserves entries below the target, so their bytes still dedup to
     *       their original id.</li>
     * </ul>
     */
    @Test
    public void testRollbackTo() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()) {
                Assert.assertEquals(0, dict.addEntry("aaa"));
                Assert.assertEquals(1, dict.addEntry("bbb"));
                Assert.assertEquals(2, dict.addEntry("ccc"));
                Assert.assertEquals(3, dict.addEntry("ddd"));
                Assert.assertEquals(4, dict.size());

                dict.rollbackTo(2);
                Assert.assertEquals(2, dict.size());

                // Entries below the target stay live; the same bytes dedup back
                // to their original id.
                Assert.assertEquals("aaa preserved", 0, dict.addEntry("aaa"));
                Assert.assertEquals("bbb preserved", 1, dict.addEntry("bbb"));
                Assert.assertEquals(2, dict.size());

                // Rolled-back bytes are gone from the dedup map; re-inserting
                // assigns fresh ids starting at the truncated size.
                Assert.assertEquals("ccc re-inserted at fresh id", 2, dict.addEntry("ccc"));
                Assert.assertEquals("ddd re-inserted at fresh id", 3, dict.addEntry("ddd"));
                Assert.assertEquals(4, dict.size());

                // Rollback to zero clears the whole dict.
                dict.rollbackTo(0);
                Assert.assertEquals(0, dict.size());
                Assert.assertEquals("aaa back to id 0 after full rollback", 0, dict.addEntry("aaa"));
            }
        });
    }

    /**
     * {@link QwpEgressConnSymbolDict#rollbackTo(int)} with {@code targetSize ==
     * size} must be a no-op (common path when an aborted batch hadn't added
     * anything new yet, or when rollback is called defensively).
     */
    @Test
    public void testRollbackToSizeIsNoOp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()) {
                dict.addEntry("foo");
                dict.addEntry("bar");
                int sizeBefore = dict.size();
                dict.rollbackTo(sizeBefore);
                Assert.assertEquals(sizeBefore, dict.size());
                Assert.assertEquals("dedup still returns original id", 0, dict.addEntry("foo"));
            }
        });
    }
}
