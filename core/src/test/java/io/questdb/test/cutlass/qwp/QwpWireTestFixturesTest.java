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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Covers the chunked-body reader shared by the QWP upgrade-refusal tests and the ILP-over-HTTP
 * tests in both modules, off a socket: it decodes the reply shapes those tests depend on, and it
 * leaves the stream on the byte after the terminating chunk so a keep-alive caller can reuse it.
 */
public class QwpWireTestFixturesTest {
    private static final String HEADERS = "HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\n\r\n";

    @Test
    public void testReadChunkedBodyJoinsChunksAndStopsAtTheTerminator() throws Exception {
        final InputStream in = stream("5\r\nhello\r\n6\r\n world\r\n0\r\n\r\nleftover");
        Assert.assertEquals("hello world", QwpWireTestFixtures.readChunkedBody(in, HEADERS));
        Assert.assertEquals("leftover", new String(in.readAllBytes(), StandardCharsets.US_ASCII));
    }

    @Test
    public void testReadChunkedBodyRejectsABodyThatEndsBeforeItsTerminator() {
        final AssertionError e = Assert.assertThrows(
                AssertionError.class,
                () -> QwpWireTestFixtures.readChunkedBody(stream("5\r\nhello\r\n"), HEADERS)
        );
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("ended before its terminating chunk"));
    }

    @Test
    public void testReadChunkedBodyRejectsAReplyThatIsNotChunked() {
        // 0x82 is the first byte of a server WebSocket BINARY frame: what a connection that switched
        // protocol instead of staying HTTP puts where the chunk size should be
        final AssertionError e = Assert.assertThrows(
                AssertionError.class,
                () -> QwpWireTestFixtures.readChunkedBody(new ByteArrayInputStream(new byte[]{(byte) 0x82, 0x01}), HEADERS)
        );
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("got byte 0x82"));
    }

    @Test
    public void testReadChunkedBodySkipsChunkExtensions() throws Exception {
        // chunk extensions are legal HTTP that no QuestDB endpoint emits today; parsing one as part
        // of the size would fail confusingly if one ever appeared
        Assert.assertEquals(
                "hello world",
                QwpWireTestFixtures.readChunkedBody(stream("5;a=b\r\nhello\r\n6;c\r\n world\r\n0;d=e\r\n\r\n"), HEADERS)
        );
    }

    private static InputStream stream(String wire) {
        return new ByteArrayInputStream(wire.getBytes(StandardCharsets.US_ASCII));
    }
}
