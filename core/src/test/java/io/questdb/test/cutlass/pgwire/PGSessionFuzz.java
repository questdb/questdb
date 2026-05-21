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

public final class PGSessionFuzz {
    private static final int MAX_BODY_LEN = 4096;
    private static final int MAX_INPUT_LEN = 1 << 16;
    private static final int MAX_STEPS = 32;
    private static final byte[] VALID_TYPES = {'P', 'B', 'D', 'E', 'S', 'Q', 'C', 'X', 'F', 'H'};

    private static PGFuzzHarness harness;

    private PGSessionFuzz() {
    }

    public static void fuzzerTestOneInput(byte[] input) throws Exception {
        final PGFuzzHarness h = harness != null ? harness : PGStandaloneFuzzSupport.sessionHarness();
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
        PGSessionFuzz.harness = harness;
    }

    private static void fuzzerTestOneInput(PGFuzzHarness h, byte[] input) throws Exception {
        if (input == null || input.length == 0 || input.length > MAX_INPUT_LEN) {
            return;
        }

        try {
            final FuzzInput data = new FuzzInput(input);
            int steps = 1 + (data.consumeUnsignedByte() & (MAX_STEPS - 1));
            int step = 0;
            while (steps-- > 0 && data.remaining() >= 3) {
                final byte type = pickMessageType(data);
                if (data.remaining() < Short.BYTES) {
                    break;
                }

                final int maxBodyLen = Math.min(MAX_BODY_LEN, data.remaining() - Short.BYTES);
                final int bodyLen = data.consumeUnsignedShort() % (maxBodyLen + 1);
                final int bodyOffset = data.position();
                data.skip(bodyLen);
                h.copyFrame(type, input, bodyOffset, bodyLen);
                try {
                    h.context().parseMessageForFuzz(h.inputBuffer(), 5 + bodyLen);
                } catch (PeerDisconnectedException expected) {
                    break;
                } catch (PGMessageProcessingException expected) {
                    h.rethrowUnexpectedProcessingError(expected, "session", input, step, type, bodyOffset, bodyLen);
                    // Protocol-level rejection and send backpressure are fine.
                } catch (PeerIsSlowToReadException expected) {
                    // Protocol-level rejection and send backpressure are fine.
                }
                step++;
            }
        } finally {
            try {
                h.assertOutputFramesWellFormed();
            } finally {
                h.reset();
                h.assertPipelinePoolBalanced();
            }
        }
    }

    private static byte pickMessageType(FuzzInput data) {
        final int selector = data.consumeUnsignedByte();
        if (selector < 205) {
            return VALID_TYPES[selector % VALID_TYPES.length];
        }
        if (data.remaining() > 0) {
            return data.consumeByte();
        }
        return (byte) selector;
    }

    private static final class FuzzInput {
        private final byte[] bytes;
        private int offset;

        private FuzzInput(byte[] bytes) {
            this.bytes = bytes;
        }

        private byte consumeByte() {
            return bytes[offset++];
        }

        private int consumeUnsignedByte() {
            return consumeByte() & 0xff;
        }

        private int consumeUnsignedShort() {
            return (consumeUnsignedByte() << 8) | consumeUnsignedByte();
        }

        private int position() {
            return offset;
        }

        private int remaining() {
            return bytes.length - offset;
        }

        private void skip(int n) {
            offset += n;
        }
    }
}
