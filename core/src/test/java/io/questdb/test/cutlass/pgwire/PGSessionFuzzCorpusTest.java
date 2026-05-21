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

package io.questdb.test.cutlass.pgwire;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class PGSessionFuzzCorpusTest extends AbstractCairoTest {

    @Test
    public void testCorpusReplay() throws Exception {
        final int[] replayCount = {0};
        assertMemoryLeak(() -> {
            try (PGFuzzHarness harness = new PGFuzzHarness(engine)) {
                PGSessionFuzz.setHarness(harness);
                try {
                    replayCount[0] = PGFuzzCorpus.replay(
                            PGSessionFuzzCorpusTest.class,
                            "/pgwire-session-corpus",
                            PGSessionFuzz::fuzzerTestOneInput
                    );
                } finally {
                    PGSessionFuzz.clearHarness(harness);
                }
            }
        });
        Assert.assertTrue("empty pgwire session corpus", replayCount[0] > 0);
    }
}
