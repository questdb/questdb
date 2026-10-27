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

package io.questdb.test.cutlass.http;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

import static java.net.HttpURLConnection.HTTP_NOT_MODIFIED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;

/**
 * Client network facade that records the raw response bytes one byte at a time,
 * so tests can assert HTTP framing and connection reuse on the wire.
 */
final class RecordingHttpClientNetwork extends NetworkFacadeImpl {
    private final StringBuilder received = new StringBuilder();
    private int connectCount;

    @Override
    public int connectAddrInfo(int fd, long addrInfo) {
        connectCount++;
        return super.connectAddrInfo(fd, addrInfo);
    }

    @Override
    public int recvRaw(int fd, long buffer, int bufferLen) {
        // Do not let the client's header parser discard unexpected bytes after the terminator.
        final int count = super.recvRaw(fd, buffer, Math.min(bufferLen, 1));
        if (count > 0) {
            received.append((char) (Unsafe.getUnsafe().getByte(buffer) & 0xff));
        }
        return count;
    }

    void assertEmptyResponse(HttpClient.ResponseHeaders response, int status) {
        TestUtils.assertEquals(Integer.toString(status), response.getStatusCode().asAsciiCharSequence());
        final boolean isLengthForbidden = status == HTTP_NO_CONTENT || status == HTTP_NOT_MODIFIED;
        Assert.assertEquals("empty responses need status-appropriate framing", isLengthForbidden ? -1 : 0, response.getContentLength());
        Assert.assertNull(response.getHeader(new Utf8String("Transfer-Encoding")));
        Assert.assertNull(response.getHeader(new Utf8String("Content-Type")));
        Assert.assertFalse(response.isChunked());
        ClientHttpUtils.assertChunkedBody(response, "");
        assertStatusLine(status);
        final String wire = received.toString();
        Assert.assertEquals(wire, wire.length() - 4, wire.indexOf("\r\n\r\n"));
        Assert.assertEquals(wire, isLengthForbidden ? 0 : 1, wire.split("Content-Length:", -1).length - 1);
    }

    void assertSingleConnection() {
        Assert.assertEquals(1, connectCount);
    }

    void assertStatusLine(int status) {
        Assert.assertTrue(received.toString(), received.toString().startsWith("HTTP/1.1 " + status + " "));
    }

    void clearReceived() {
        received.setLength(0);
    }

    HttpClient newClient() {
        return HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration() {
            @Override
            public NetworkFacade getNetworkFacade() {
                return RecordingHttpClientNetwork.this;
            }

            @Override
            public int getTimeout() {
                return 10_000;
            }
        });
    }
}
