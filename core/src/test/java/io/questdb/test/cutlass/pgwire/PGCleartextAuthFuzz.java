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

import io.questdb.cutlass.pgwire.PGMessageProcessingException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.PeerIsSlowToWriteException;
import org.jetbrains.annotations.TestOnly;

public final class PGCleartextAuthFuzz {
    private static final int MAX_INPUT_LEN = 1 << 16;

    private static PGFuzzHarness harness;

    private PGCleartextAuthFuzz() {
    }

    public static void fuzzerTestOneInput(byte[] input) throws Exception {
        final PGFuzzHarness h = harness != null ? harness : PGStandaloneFuzzSupport.cleartextAuthHarness();
        fuzzerTestOneInput(h, input);
    }

    @TestOnly
    static void clearHarness(PGFuzzHarness expectedHarness) {
        if (harness == expectedHarness) {
            harness = null;
        }
    }

    @TestOnly
    static void setHarness(PGFuzzHarness harness) {
        PGCleartextAuthFuzz.harness = harness;
    }

    private static void fuzzerTestOneInput(PGFuzzHarness h, byte[] input) throws Exception {
        if (input == null || input.length == 0 || input.length > MAX_INPUT_LEN) {
            return;
        }

        try {
            h.acceptStartupForFuzz(input);
        } catch (PGMessageProcessingException expected) {
            h.rethrowUnexpectedProcessingError(expected, "cleartext", input);
            // Protocol-level rejection is fine.
        } catch (PeerDisconnectedException
                 | PeerIsSlowToReadException
                 | PeerIsSlowToWriteException expected) {
            // Protocol-level rejection, disconnect, and socket backpressure are fine.
        } finally {
            try {
                h.assertOutputFramesWellFormed(true);
            } finally {
                h.reset();
                h.assertPipelinePoolBalanced();
            }
        }
    }
}
