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

package io.questdb.test.cutlass.http.line;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class LineRawHttpFuzzTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testChunkedUpperCaseChunkSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
            )) {
                serverMain.start();

                Rnd rnd = TestUtils.generateRandom(LOG);

                int totalCount = 0;
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                    for (int r = 0; r < 3; r++) {
                        int count = 1 + rnd.nextInt(100);
                        HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                        request.POST()
                                .url("/write ")
                                .withChunkedContent();

                        String hexChunkLen = Integer.toHexString(line.length() * count);
                        if (rnd.nextBoolean()) {
                            hexChunkLen = hexChunkLen.toUpperCase();
                        } else {
                            hexChunkLen = hexChunkLen.toLowerCase();
                        }
                        request.putAscii(hexChunkLen).putEOL();

                        for (int i = 0; i < count; i++) {
                            request.putAscii(line);
                        }

                        request.putEOL().putAscii("0").putEOL().putEOL();
                        try (HttpClient.ResponseHeaders resp = request.send(5000)) {
                            resp.await();
                            totalCount += count;
                        }
                    }
                }

                serverMain.awaitTable("line");
                serverMain.assertSql("select count() from line", "count\n" +
                        totalCount + "\n");
            }
        });
    }

    @Test
    public void testValidRequestAfterInvalidWithKeepAlive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
            )) {
                serverMain.start();

                Rnd rnd = TestUtils.generateRandom(LOG);

                int totalCount = 0;
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                    for (int r = 0; r < 30; r++) {
                        if (r % 3 == 0) {
                            int count = 1 + rnd.nextInt(1000);
                            HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                            request.POST()
                                    .url("/not_found_chunked ")
                                    .withChunkedContent();
                            String hexChunkLen = Integer.toHexString(line.length() * count);
                            hexChunkLen = hexChunkLen.toUpperCase();
                            request.putAscii(hexChunkLen).putEOL();

                            for (int i = 0; i < count; i++) {
                                request.putAscii(line);
                            }
                            request.putEOL().putAscii("0").putEOL().putEOL();
                            try (HttpClient.ResponseHeaders resp = request.send(5000)) {
                                resp.await();
                            }
                        } else if (r % 3 == 1) {
                            // Good request
                            int count = 1 + rnd.nextInt(100);
                            HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                            request.POST()
                                    .url("/write ")
                                    .withChunkedContent();

                            String hexChunkLen = Integer.toHexString(line.length() * count);
                            hexChunkLen = hexChunkLen.toLowerCase();
                            request.putAscii(hexChunkLen).putEOL();

                            for (int i = 0; i < count; i++) {
                                request.putAscii(line);
                            }

                            request.putEOL().putAscii("0").putEOL().putEOL();
                            try (HttpClient.ResponseHeaders resp = request.send(5000)) {
                                resp.await();
                                totalCount += count;
                            }
                        } else {
                            // Good request
                            int count = 1 + rnd.nextInt(100);
                            HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                            request.POST()
                                    .url("/not_found ")
                                    .withContent();

                            for (int i = 0; i < count; i++) {
                                request.putAscii(line);
                            }

                            request.putEOL().putAscii("0").putEOL().putEOL();
                            try (HttpClient.ResponseHeaders resp = request.send(5000)) {
                                resp.await();
                            }
                        }
                    }
                }

                serverMain.awaitTable("line");
                serverMain.assertSql("select count() from line", "count\n" +
                        totalCount + "\n");
            }
        });
    }
}
