/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net.http;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class ChunkedResponseTest {

    @Test
    @Ignore
    public void testChunked() throws Exception {
        HttpServer server = new HttpServer(new HttpServerConfiguration(), new SimpleUrlMatcher() {{
            put("/chunked", new DummyHandler());
        }});
        server.start();
        try {
            HttpGet get = new HttpGet("http://localhost:9090/chunked");

            try (CloseableHttpClient client = HttpClients.createDefault()) {
                try (CloseableHttpResponse r = client.execute(get)) {
                    Assert.assertEquals(200, r.getStatusLine().getStatusCode());
                }
            }
        } finally {
            server.halt();
        }

    }

    private static class DummyHandler implements ContextHandler {

        @Override
        public void handle(IOContext context) throws IOException {
        }

        @Override
        public void resume(IOContext context) throws IOException {

        }
    }
}
