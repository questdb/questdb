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

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class PGDifferentialFuzzCorpusTest {

    @Test
    public void testCorpusReplayWithPostgresSidecar() throws Exception {
        Assume.assumeTrue("PostgreSQL fuzz oracle is not configured", PGDifferentialFuzzSupport.isPostgresConfigured());
        try {
            final int replayCount = PGFuzzCorpus.replay(
                    PGDifferentialFuzzCorpusTest.class,
                    "/pgwire-differential-corpus",
                    PGDifferentialFuzz::fuzzerTestOneInput
            );
            Assert.assertTrue("empty pgwire differential corpus", replayCount > 0);
        } finally {
            PGDifferentialFuzzSupport.closeForTest();
        }
    }
}
