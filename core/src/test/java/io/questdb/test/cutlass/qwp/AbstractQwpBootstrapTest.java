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
 * code paths. Each test method gets a fresh pair of random recv / send chunk
 * sizes in [1, 500] derived from the JUnit-managed seed, so failures replay
 * deterministically from the seed log written by
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
        recvChunk = 1 + rnd.nextInt(500);
        sendChunk = 1 + rnd.nextInt(500);
    }

    protected long firstBatchTimeoutMs(long baseMs) {
        // HttpResponseSink#sendBuffer parks every sendChunk bytes; a first batch
        // can be ~131 KB (MAX_ROWS_PER_BATCH=16384 LONGs) plus framing, so at the
        // worst random sendChunk=1 it needs tens of thousands of park-resume cycles.
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
