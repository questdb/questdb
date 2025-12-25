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

package io.questdb.test.cutlass.http;

import io.questdb.Bootstrap;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.FactoryProviderImpl;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

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
    public void testWarningsWithContextPath() throws Exception {
        final String contextPath = "/context";
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true");
                put(PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), contextPath);
            }})
            ) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();

                // clear warnings
                final String tag1 = "";
                final String warning1 = "";
                setWarning(engine, tag1, warning1);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, contextPath, "[]");
                }

                // add 1st warning
                final String tag2 = "UNSUPPORTED FILE SYSTEM";
                final String warning2 = "Unsupported file system [dir=/questdb/path/dbRoot, magic=0x6400A468]";
                setWarning(engine, tag2, warning2);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, contextPath, "[{\"tag\":\"" + tag2 + "\",\"warning\":\"" + warning2 + "\"}]");
                }

                // add 2nd warning
                final String tag3 = "OUT OF MMAP AREAS";
                final String warning3 = "vm.max_map_count limit is too low [current=4096, recommended=1048576]";
                setWarning(engine, tag3, warning3);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, contextPath, "[{\"tag\":\"" + tag2 + "\",\"warning\":\"" + warning2 + "\"}," +
                            "{\"tag\":\"" + tag3 + "\",\"warning\":\"" + warning3 + "\"}]");
                }

                // clear warnings
                setWarning(engine, tag1, warning1);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, contextPath, "[]");
                }

                // add a new warning
                final String tag4 = "TOO MANY OPEN FILES";
                final String warning4 = "fs.file-max limit is too low [current=1024, recommended=1048576]";
                setWarning(engine, tag4, warning4);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, contextPath, "[{\"tag\":\"" + tag4 + "\",\"warning\":\"" + warning4 + "\"}]");
                }
            }
        });
    }

    @Test
    public void testWarningsWithSimulation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true");
            }})
            ) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();

                // clear warnings
                final String tag1 = "";
                final String warning1 = "";
                setWarning(engine, tag1, warning1);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, "[]");
                }

                // add 1st warning
                final String tag2 = "UNSUPPORTED FILE SYSTEM";
                final String warning2 = "Unsupported file system [dir=/questdb/path/dbRoot, magic=0x6400A468]";
                setWarning(engine, tag2, warning2);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, "[{\"tag\":\"" + tag2 + "\",\"warning\":\"" + warning2 + "\"}]");
                }

                // add 2nd warning
                final String tag3 = "OUT OF MMAP AREAS";
                final String warning3 = "vm.max_map_count limit is too low [current=4096, recommended=1048576]";
                setWarning(engine, tag3, warning3);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, "[{\"tag\":\"" + tag2 + "\",\"warning\":\"" + warning2 + "\"}," +
                            "{\"tag\":\"" + tag3 + "\",\"warning\":\"" + warning3 + "\"}]");
                }

                // clear warnings
                setWarning(engine, tag1, warning1);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, "[]");
                }

                // add a new warning
                final String tag4 = "TOO MANY OPEN FILES";
                final String warning4 = "fs.file-max limit is too low [current=1024, recommended=1048576]";
                setWarning(engine, tag4, warning4);
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    assertWarningsRequest(httpClient, "[{\"tag\":\"" + tag4 + "\",\"warning\":\"" + warning4 + "\"}]");
                }
            }
        });
    }

    @Test
    public void testZeroLimits() throws Exception {
        testWarningsWithProps(-256, 0L, 0L, "[]");
    }

    private static void setWarning(CairoEngine engine, String tag, String warning) throws SqlException {
        try (
                SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
                RecordCursorFactory rcf = engine.select("select simulate_warnings('" + tag + "', '" + warning + "')", context);
                RecordCursor cursor = rcf.getCursor(context)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final boolean result = record.getBool(0);
                if (!result) {
                    Assert.fail();
                }
            }
        }
    }

    private void assertWarningsRequest(HttpClient httpClient, String expectedHttpResponse) {
        assertWarningsRequest(httpClient, "", expectedHttpResponse);
    }

    private void assertWarningsRequest(HttpClient httpClient, String httpContext, String expectedHttpResponse) {
        final HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
        request.GET().url(httpContext + "/warnings");
        TestUtils.assertResponse(request, 200, expectedHttpResponse);
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
