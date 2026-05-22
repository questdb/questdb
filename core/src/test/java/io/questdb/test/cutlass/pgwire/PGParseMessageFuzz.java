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
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

public final class PGParseMessageFuzz {
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final int HISTORY_SIZE = 64;
    private static final int MAX_HISTORY_HEX_BYTES = 1024;

    private static PGFuzzHarness harness;
    private static final byte[][] recentInputs = new byte[HISTORY_SIZE][];
    private static final int[] recentInputLengths = new int[HISTORY_SIZE];
    private static int recentInputCursor;
    private static long recentInputSequence;

    private PGParseMessageFuzz() {
    }

    public static void fuzzerTestOneInput(byte[] input) throws Exception {
        final PGFuzzHarness h = harness != null ? harness : PGStandaloneFuzzSupport.parseHarness();
        fuzzerTestOneInput(h, input);
    }

    @TestOnly
    static void clearHarness(PGFuzzHarness expectedHarness) {
        if (harness == expectedHarness) {
            harness = null;
            clearRecentInputs();
        }
    }

    @TestOnly
    static void setHarness(PGFuzzHarness harness) {
        clearRecentInputs();
        PGParseMessageFuzz.harness = harness;
    }

    private static void fuzzerTestOneInput(PGFuzzHarness h, byte[] input) throws Exception {
        if (input == null || input.length < 5 || input.length > PGFuzzHarness.INPUT_BUFFER_SIZE) {
            return;
        }

        recordInput(input);
        h.copyInput(input);
        try {
            h.context().parseMessageForFuzz(h.inputBuffer(), input.length);
            PGMessageRoundTripOracle.assertRoundTripIfSupported(input);
        } catch (PGMessageProcessingException expected) {
            h.rethrowUnexpectedProcessingError(expected, "parse", input, -1, (byte) 0, -1, -1, recentInputsDiagnostic());
            // Protocol-level rejection is fine.
        } catch (PeerDisconnectedException | PeerIsSlowToReadException expected) {
            // Protocol-level rejection is fine. Crashes and unexpected exceptions must escape.
        } finally {
            try {
                h.assertOutputFramesWellFormed();
            } finally {
                h.reset();
                h.assertPipelinePoolBalanced();
            }
        }
    }

    private static void appendHex(StringBuilder sink, byte[] bytes, int length) {
        final int hi = Math.min(length, MAX_HISTORY_HEX_BYTES);
        for (int i = 0; i < hi; i++) {
            int value = bytes[i] & 0xff;
            sink.append(HEX[value >>> 4]).append(HEX[value & 0x0f]);
        }
        if (hi < length) {
            sink.append("...");
        }
    }

    private static void clearRecentInputs() {
        Arrays.fill(recentInputs, null);
        Arrays.fill(recentInputLengths, 0);
        recentInputCursor = 0;
        recentInputSequence = 0;
    }

    private static void recordInput(byte[] input) {
        final int storedLength = Math.min(input.length, MAX_HISTORY_HEX_BYTES);
        byte[] copy = recentInputs[recentInputCursor];
        if (copy == null || copy.length < storedLength) {
            copy = Arrays.copyOf(input, storedLength);
            recentInputs[recentInputCursor] = copy;
        } else {
            System.arraycopy(input, 0, copy, 0, storedLength);
        }
        recentInputLengths[recentInputCursor] = input.length;
        recentInputCursor = (recentInputCursor + 1) % HISTORY_SIZE;
        recentInputSequence++;
    }

    private static String recentInputsDiagnostic() {
        StringBuilder sink = new StringBuilder();
        sink.append("recentInputs=[seq=").append(recentInputSequence);
        final int count = (int) Math.min(recentInputSequence, HISTORY_SIZE);
        for (int i = count - 1; i >= 0; i--) {
            final int index = (recentInputCursor + HISTORY_SIZE - 1 - i) % HISTORY_SIZE;
            final byte[] input = recentInputs[index];
            if (input != null) {
                final int length = recentInputLengths[index];
                sink.append(", -").append(i).append(":len=").append(length).append(",hex=");
                appendHex(sink, input, length);
            }
        }
        return sink.append(']').toString();
    }
}
