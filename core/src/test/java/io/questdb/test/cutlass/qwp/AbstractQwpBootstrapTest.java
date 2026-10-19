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

import io.questdb.PropertyKey;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;

/**
 * Bootstrap test base for QWP tests that need to exercise the HTTP fragmentation
 * code paths. Each test method gets a fresh pair of random chunk sizes derived
 * from the JUnit-managed seed -- recv in [1, 500] and send in [64, 500] -- so
 * failures replay deterministically from the seed log written by
 * {@link TestUtils#generateRandom}. Subclasses launch the server via
 * {@link #startFragmented(String...)}, which threads the chunks through
 * {@code DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE} and
 * {@code DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE} alongside any extra
 * env vars the test wants to set.
 */
public abstract class AbstractQwpBootstrapTest extends AbstractBootstrapTest {

    private static final Log LOG = LogFactory.getLog(AbstractQwpBootstrapTest.class);
    protected int recvChunk;
    protected int sendChunk;

    @Before
    public void setUpFragmentationChunks() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        // recvChunk drives the incremental request parser, which is offset
        // sensitive, and requests are small -- keep the 1-byte draw.
        recvChunk = 1 + rnd.nextInt(500);
        // sendChunk is floored. HttpResponseSink#sendBuffer ships at most
        // sendChunk bytes per event-loop pass and parks in between, so egress
        // pays one dispatcher round trip per chunk. A 1-byte draw made a
        // 50k-row LONG projection (~400 KB) take 62.4 s on a hosted macOS
        // agent, overrunning the 60 s query.timeout and aborting the query
        // mid-stream. The floor still fragments every batch (~2k parks per
        // 131 KB batch), and the send resume path only advances a byte pointer
        // (ChunkUtf8Sink#onRead) rather than driving an offset-sensitive state
        // machine, so a finer split adds no coverage. Tests that need a
        // specific 1-byte split pin sendChunk themselves.
        sendChunk = 64 + rnd.nextInt(437);
    }

    protected long firstBatchTimeoutMs(long baseMs) {
        // HttpResponseSink#sendBuffer parks every sendChunk bytes; a first batch
        // can be ~131 KB (MAX_ROWS_PER_BATCH=16384 LONGs) plus framing, so a small
        // sendChunk needs tens of thousands of park-resume cycles. The default draw
        // is floored at 64 and lands on baseMs; this still scales for a test that
        // pins a smaller sendChunk itself.
        int effectiveChunk = Math.max(1, Math.min(sendChunk, 64));
        return baseMs * 64L / effectiveChunk;
    }

    protected TestServerMain startFragmented(String... extra) {
        String[] all = new String[extra.length + 4];
        all[0] = PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName();
        all[1] = Integer.toString(recvChunk);
        all[2] = PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName();
        all[3] = Integer.toString(sendChunk);
        System.arraycopy(extra, 0, all, 4, extra.length);
        return startWithEnvVariables(all);
    }
}
