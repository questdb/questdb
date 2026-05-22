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

public class PGStartupSessionFuzzSmokeTest extends AbstractCairoTest {

    @Test
    public void testStructuredStartupThenTailFrame() throws Exception {
        assertMemoryLeak(() -> {
            try (PGFuzzHarness harness = new PGFuzzHarness(engine, false)) {
                PGStartupSessionFuzz.setHarness(harness);
                try {
                    PGStartupSessionFuzz.fuzzerTestOneInput(newStructuredStartupThenSimpleQuery());
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());
                } finally {
                    PGStartupSessionFuzz.clearHarness(harness);
                }
            }
        });
    }

    private static byte[] newStructuredStartupThenSimpleQuery() {
        return new byte[]{
                (byte) 0x80,
                'Q', 0, 0, 0, 13,
                's', 'e', 'l', 'e', 'c', 't', ' ', '1', 0
        };
    }
}
