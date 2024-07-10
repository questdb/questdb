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

package io.questdb.test.cutlass.http;

import io.questdb.*;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.cutlass.http.client.Response;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.ErrorTag.*;

public class WarningsEndpointTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAllWarnings() throws Exception {
        testWarningsWithProps(25600, 1024L, 1024L, "[" +
                "{" +
                "\"tag\":\"" + UNSUPPORTED_FILE_SYSTEM.text() + "\"," +
                "\"warning\":\"Unsupported file system [dir=" + root + ", magic=0x6400]\"" +
                "},{" +
                "\"tag\":\"" + TOO_MANY_OPEN_FILES.text() + "\"," +
                "\"warning\":\"fs.file-max limit is too low [current=1024, recommended=1048576]\"" +
                "},{" +
                "\"tag\":\"" + OUT_OF_MMAP_AREAS.text() + "\"," +
                "\"warning\":\"vm.max_map_count limit is too low [current=1024, recommended=1048576]\"" +
                "}" +
                "]");
    }

    @Test
    public void testFileSystemWarning() throws Exception {
        testWarningsWithProps(25600, 1048576L, 1048576L, "[" +
                "{" +
                "\"tag\":\"" + UNSUPPORTED_FILE_SYSTEM.text() + "\"," +
                "\"warning\":\"Unsupported file system [dir=" + root + ", magic=0x6400]\"" +
                "}" +
                "]");
    }

    @Test
    public void testMaxMapCountWarning() throws Exception {
        testWarningsWithProps(-200, 4194304L, 65536L, "[" +
                "{" +
                "\"tag\":\"" + OUT_OF_MMAP_AREAS.text() + "\"," +
                "\"warning\":\"vm.max_map_count limit is too low [current=65536, recommended=1048576]\"" +
                "}" +
                "]");
    }

    @Test
    public void testMixedWarnings() throws Exception {
        testWarningsWithProps(-55, 10240L, 4096L, "[" +
                "{" +
                "\"tag\":\"" + TOO_MANY_OPEN_FILES.text() + "\"," +
                "\"warning\":\"fs.file-max limit is too low [current=10240, recommended=1048576]\"" +
                "},{" +
                "\"tag\":\"" + OUT_OF_MMAP_AREAS.text() + "\"," +
                "\"warning\":\"vm.max_map_count limit is too low [current=4096, recommended=1048576]\"" +
                "}" +
                "]");
    }

    @Test
    public void testNoWarnings() throws Exception {
        testWarningsWithProps(-256, 1048576L, 1048576L, "[]");
    }

    @Test
    public void testOpenFilesWarning() throws Exception {
        testWarningsWithProps(-100, 1024L, 1048576L, "[" +
                "{" +
                "\"tag\":\"" + TOO_MANY_OPEN_FILES.text() + "\"," +
                "\"warning\":\"fs.file-max limit is too low [current=1024, recommended=1048576]\"" +
                "}" +
                "]");
    }

    @Test
    public void testZeroLimits() throws Exception {
        testWarningsWithProps(-256, 0L, 0L, "[]");
    }

    private void assertWarningsRequest(HttpClient httpClient, String expectedHttpResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url("/warnings");
        try (HttpClient.ResponseHeaders responseHeaders = request.send()) {
            responseHeaders.await();

            TestUtils.assertEquals(String.valueOf(200), responseHeaders.getStatusCode());

            final Utf8StringSink sink = new Utf8StringSink();

            Fragment fragment;
            final Response response = responseHeaders.getResponse();
            while ((fragment = response.recv()) != null) {
                Utf8s.strCpy(fragment.lo(), fragment.hi(), sink);
            }

            TestUtils.assertEquals(expectedHttpResponse, sink.toString());
            sink.clear();
        }
    }

    private void testWarningsWithProps(int fsMagic, long openFilesLimit, long mapCountLimit, String expectedWarnings) throws Exception {
        final Bootstrap bootstrap = new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
                        return new PropServerConfiguration(
                                bootstrap.getRootDirectory(),
                                bootstrap.loadProperties(),
                                getEnv(),
                                bootstrap.getLog(),
                                bootstrap.getBuildInformation(),
                                new FilesFacadeImpl(),
                                bootstrap.getMicrosecondClock(),
                                (configuration, engine, freeOnExit) -> new FactoryProviderImpl(configuration)
                        ) {
                            @Override
                            public CairoConfiguration getCairoConfiguration() {
                                return new DefaultCairoConfiguration(bootstrap.getRootDirectory()) {
                                    @Override
                                    public @NotNull FilesFacade getFilesFacade() {
                                        return new FilesFacadeImpl() {
                                            @Override
                                            public long getFileLimit() {
                                                return openFilesLimit;
                                            }

                                            @Override
                                            public int getFileSystemStatus(LPSZ lpszName) {
                                                return fsMagic;
                                            }

                                            @Override
                                            public long getMapCountLimit() {
                                                return mapCountLimit;
                                            }
                                        };
                                    }
                                };
                            }
                        };
                    }
                },
                getServerMainArgs()
        );

        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, expectedWarnings);
                }
            }
        });
    }
}
