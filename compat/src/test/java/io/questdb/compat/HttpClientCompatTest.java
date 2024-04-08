/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import static java.nio.charset.StandardCharsets.UTF_8;

public class HttpClientCompatTest {
    private static final int SERVER_PORT = 8080;

    @Test
    public void testSmoke() throws Exception {
        final String expectedResponse = "{\"foobar\": \"barbaz\"}";
        Server server = startWebServer(HttpStatus.OK_200, expectedResponse);
        try (HttpClient client = HttpClientFactory.newPlainTextInstance()) {
            try (HttpClient.ResponseHeaders respHeaders = client.newRequest("localhost", SERVER_PORT).GET().url("/test").send()) {
                respHeaders.await();
                Assert.assertEquals("200", Utf8s.toString(respHeaders.getStatusCode()));
                Assert.assertEquals(MimeTypes.Type.APPLICATION_JSON.toString(), Utf8s.toString(respHeaders.getContentType()));

                Assert.assertFalse(respHeaders.isChunked());
                Response resp = respHeaders.getResponse();

                Utf8StringSink sink = new Utf8StringSink();

                Fragment fragment;
                while ((fragment = resp.recv()) != null) {
                    Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
                }

                Assert.assertEquals(expectedResponse, sink.toString());
            }
        } finally {
            server.stop();
        }
    }

    private static Server startWebServer(int statusCode, String response) throws Exception {
        Server server = new Server(SERVER_PORT);
        server.setHandler(new TestHandler(statusCode, response));
        server.start();
        return server;
    }

    static class TestHandler extends AbstractHandler {
        private final String response;
        private final int statusCode;

        public TestHandler(int statusCode, String response) {
            this.response = response;
            this.statusCode = statusCode;
        }

        @Override
        public void handle(
                String s,
                Request baseRequest,
                HttpServletRequest request,
                HttpServletResponse response
        ) throws IOException {
            baseRequest.setHandled(true);

            String method = baseRequest.getMethod();
            if (HttpMethod.GET.is(method)) {
                response.setStatus(statusCode);
                response.setContentType(MimeTypes.Type.APPLICATION_JSON.toString());
                try (
                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        OutputStreamWriter writer = new OutputStreamWriter(outputStream, UTF_8)
                ) {
                    writer.append(this.response);
                    writer.flush();

                    byte[] content = outputStream.toByteArray();
                    response.setContentLength(content.length);
                    try (OutputStream out = response.getOutputStream()) {
                        out.write(content);
                    }
                }
            }
        }
    }
}
