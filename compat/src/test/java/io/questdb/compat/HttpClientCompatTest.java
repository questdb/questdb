/*******************************************************************************
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

package io.questdb.compat;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.Callback;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class HttpClientCompatTest {
    private static final int DEFAULT_SERVER_PORT = 8080;

    @Test
    public void testRequestWithExplicitHostAndPort() throws Exception {
        final String expectedResponseA = "{\"server\": \"A\"}";
        final String expectedResponseB = "{\"server\": \"B\"}";

        int portA = DEFAULT_SERVER_PORT;
        int portB = portA + 1;
        Server serverA = startWebServer(portA, expectedResponseA);
        Server serverB = startWebServer(portB, "{\"server\": \"B\"}");

        try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
            HttpClient.Request request = client.newRequest("localhost", portA).GET().url("/test");
            HttpClient.ResponseHeaders respHeaders = request.send(); // send request to default host/port
            respHeaders.await();
            assertResponse(respHeaders, expectedResponseA);

            respHeaders = request.send("localhost", portB, DefaultHttpClientConfiguration.INSTANCE.getTimeout());
            respHeaders.await();
            assertResponse(respHeaders, expectedResponseB);
        } finally {
            serverA.stop();
            serverB.stop();
        }
    }

    @Test
    public void testSmoke() throws Exception {
        final String expectedResponse = "{\"foobar\": \"barbaz\"}";
        Server server = startWebServer();
        try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
            try (HttpClient.ResponseHeaders respHeaders = client.newRequest("localhost", DEFAULT_SERVER_PORT).GET().url("/test").send()) {
                respHeaders.await();
                assertResponse(respHeaders, expectedResponse);
            }
        } finally {
            server.stop();
        }
    }

    private static void assertResponse(HttpClient.ResponseHeaders respHeaders, String expectedResponseA) {
        Assert.assertEquals("200", Utf8s.toString(respHeaders.getStatusCode()));
        Assert.assertEquals(MimeTypes.Type.APPLICATION_JSON.toString(), Utf8s.toString(respHeaders.getContentType()));

        Assert.assertFalse(respHeaders.isChunked());
        Utf8StringSink sink = new Utf8StringSink();
        respHeaders.getResponse().copyTextTo(sink);
        Assert.assertEquals(expectedResponseA, sink.toString());
    }

    private static Server startWebServer() throws Exception {
        return startWebServer(DEFAULT_SERVER_PORT, "{\"foobar\": \"barbaz\"}");
    }

    private static Server startWebServer(int port, String response) throws Exception {
        Server server = new Server(port);
        server.setHandler(new TestHandler(HttpStatus.OK_200, response));
        server.start();
        return server;
    }

    static class TestHandler extends Handler.Abstract {
        private final String response;
        private final int statusCode;

        public TestHandler(int statusCode, String response) {
            this.response = response;
            this.statusCode = statusCode;
        }

        @Override
        public boolean handle(Request request, org.eclipse.jetty.server.Response response, Callback callback) throws Exception {
            String method = request.getMethod();
            if (HttpMethod.GET.is(method)) {
                response.setStatus(statusCode);
                response.getHeaders().put("Content-Type", MimeTypes.Type.APPLICATION_JSON.toString());

                try (
                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        OutputStreamWriter writer = new OutputStreamWriter(outputStream, UTF_8)
                ) {
                    writer.append(this.response);
                    writer.flush();

                    byte[] content = outputStream.toByteArray();
                    response.getHeaders().put("Content-Length", String.valueOf(content.length));
                    response.write(true, ByteBuffer.wrap(content), callback);
                    return true;
                }
            }
            callback.succeeded();
            return true;
        }
    }
}
