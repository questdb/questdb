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

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A result streamed one row per frame reaches the client as bursts of small
 * frames, many of them in one recv, with the last frame of a recv often cut
 * short. The client parses such a burst in place and keeps the unread tail
 * until its next recv; every row must still arrive once and in order.
 */
public class QwpEgressSmallFrameBurstTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testOneRowPerFrameStreamArrivesInOrder() throws Exception {
        final int rows = 20_000;
        TestUtils.assertMemoryLeak(() -> {
            // No forced fragmentation: the server writes whole bursts of frames.
            try (TestServerMain ignored = startWithEnvVariables()) {
                final long[] next = {1};
                final int[] maxRowsPerBatch = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";max_batch_rows=1;")) {
                    client.connect();
                    client.execute("SELECT x FROM long_sequence(" + rows + ")", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            final int n = batch.getRowCount();
                            maxRowsPerBatch[0] = Math.max(maxRowsPerBatch[0], n);
                            for (int r = 0; r < n; r++) {
                                Assert.assertEquals(next[0]++, batch.getLongValue(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(rows, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error status=0x" + Integer.toHexString(status & 0xff)
                                    + " msg=" + message);
                        }
                    });
                }
                Assert.assertEquals("every row must arrive exactly once", rows + 1, next[0]);
                Assert.assertEquals("max_batch_rows=1 must stream one row per frame", 1, maxRowsPerBatch[0]);
            }
        });
    }
}
