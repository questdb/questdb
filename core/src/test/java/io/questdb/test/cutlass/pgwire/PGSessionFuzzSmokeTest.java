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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class PGSessionFuzzSmokeTest extends AbstractCairoTest {

    @Test
    public void testSessionFuzzSmoke() throws Exception {
        assertMemoryLeak(() -> {
            try (PGFuzzHarness harness = new PGFuzzHarness(engine)) {
                PGSessionFuzz.setHarness(harness);
                try {
                    PGSessionFuzz.fuzzerTestOneInput(newSession(
                            frame('P', parseBody("select 1")),
                            frame('B', bindBody()),
                            frame('D', describePortalBody()),
                            frame('E', executeBody()),
                            frame('S', new byte[0])
                    ));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGSessionFuzz.fuzzerTestOneInput(newSession(
                            frame('B', bindBody()),
                            frame('E', executeBody()),
                            frame('C', closeBadKindBody()),
                            frame('X', new byte[0])
                    ));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGSessionFuzz.fuzzerTestOneInput(newBindWithNegativeFormatCountSession());
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGSessionFuzz.fuzzerTestOneInput(newSession(
                            frame('P', parseBody("select 3")),
                            frame('B', bindBodyWithNegativeParameterFormatCount())
                    ));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGSessionFuzz.fuzzerTestOneInput(newSession(
                            frame('P', parseBody("select 1")),
                            frame('B', bindBodyWithTruncatedResultFormatCountAfterNamedPortal())
                    ));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGSessionFuzz.fuzzerTestOneInput(newSession(
                            frame('Q', "select 1".getBytes(StandardCharsets.UTF_8))
                    ));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());

                    PGSessionFuzz.fuzzerTestOneInput(newSession(
                            frame('Q', new byte[]{'s', 'e', 'l', 'e', 'c', 't', ' ', '1', 0, 'x', 0})
                    ));
                    Assert.assertEquals(0, harness.context().getPipelineEntryPoolOutieCountForFuzz());
                } finally {
                    PGSessionFuzz.clearHarness(harness);
                }
            }
        });
    }

    private static byte[] bindBody() {
        final byte[] body = new byte[8];
        int p = 0;
        body[p++] = 0; // unnamed portal
        body[p++] = 0; // unnamed prepared statement
        putShort(body, p, 0); // parameter format code count
        p += Short.BYTES;
        putShort(body, p, 0); // parameter value count
        p += Short.BYTES;
        putShort(body, p, 0); // result format code count
        return body;
    }

    private static byte[] bindBodyWithNegativeParameterFormatCount() {
        final byte[] body = new byte[4];
        body[0] = 0; // unnamed portal
        body[1] = 0; // unnamed prepared statement
        putShort(body, 2, 0xff00); // invalid negative parameter format code count
        return body;
    }

    private static byte[] bindBodyWithTruncatedResultFormatCountAfterNamedPortal() {
        return new byte[]{
                0x0c, 0, // named portal
                0, // unnamed prepared statement
                0, 0, // parameter format code count
                0, 0, // parameter value count
                0 // truncated result format code count
        };
    }

    private static byte[] closeBadKindBody() {
        return new byte[]{'Z', 0};
    }

    private static byte[] describePortalBody() {
        return new byte[]{'P', 0};
    }

    private static byte[] executeBody() {
        final byte[] body = new byte[5];
        body[0] = 0; // unnamed portal
        putInt(body, 1, 0); // max rows
        return body;
    }

    private static Frame frame(int type, byte[] body) {
        return new Frame(type, body);
    }

    private static byte[] newBindWithNegativeFormatCountSession() {
        return new byte[]{
                0x04,
                0x00, 0x00, 0x0c, 0x00, 's', 'e', 'l', 'e', 'c', 't', ' ', '3', 0x00, 0x00, 0x00,
                0x01, 0x00, 0x08, 0x01, (byte) 0xfd, 0x00, 0x00, (byte) 0xff, 0x00, (byte) 0xf6, (byte) 0xff,
                0x01, 0x00, 0x02, 'P', 0x00,
                0x03, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x04,
                0x00, 0x00, 0x04
        };
    }

    private static byte[] newSession(Frame... frames) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(frames.length - 1);
        for (int i = 0; i < frames.length; i++) {
            out.write(selectorFor(frames[i].type));
            putShort(out, frames[i].body.length);
            out.write(frames[i].body, 0, frames[i].body.length);
        }
        return out.toByteArray();
    }

    private static byte[] parseBody(String sql) {
        final byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        final byte[] body = new byte[1 + sqlBytes.length + 1 + Short.BYTES];
        int p = 0;
        body[p++] = 0; // unnamed prepared statement
        System.arraycopy(sqlBytes, 0, body, p, sqlBytes.length);
        p += sqlBytes.length;
        body[p++] = 0;
        putShort(body, p, 0); // parameter type count
        return body;
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    private static void putShort(ByteArrayOutputStream out, int value) {
        out.write(value >>> 8);
        out.write(value);
    }

    private static void putShort(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 8);
        bytes[offset + 1] = (byte) value;
    }

    private static int selectorFor(int type) {
        switch (type) {
            case 'P':
                return 0;
            case 'B':
                return 1;
            case 'D':
                return 2;
            case 'E':
                return 3;
            case 'S':
                return 4;
            case 'Q':
                return 5;
            case 'C':
                return 6;
            case 'X':
                return 7;
            case 'F':
                return 8;
            case 'H':
                return 9;
            default:
                return 205;
        }
    }

    private static final class Frame {
        private final byte[] body;
        private final int type;

        private Frame(int type, byte[] body) {
            this.type = type;
            this.body = body;
        }
    }
}
