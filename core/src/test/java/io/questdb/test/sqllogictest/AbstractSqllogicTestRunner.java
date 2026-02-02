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

package io.questdb.test.sqllogictest;

import io.questdb.network.NetworkError;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.Sqllogictest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.questdb.PropertyKey.*;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public abstract class AbstractSqllogicTestRunner extends AbstractBootstrapTest {
    protected final boolean parallelReadParquet;
    private final String testFile;
    private short pgPort;
    private TestServerMain serverMain;

    public AbstractSqllogicTestRunner(String testFile) {
        this(testFile, true);
    }

    public AbstractSqllogicTestRunner(String testFile, boolean parallelReadParquet) {
        this.testFile = testFile;
        this.parallelReadParquet = parallelReadParquet;
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try (Path key = new Path(); Path value = new Path()) {
            key.of("DB_ROOT");
            value.of(root);
            Sqllogictest.setEnvVar(key.$().ptr(), value.$().ptr());

            key.of("TEST_RESOURCE_ROOT");
            value.of(getTestResourcePath());
            Sqllogictest.setEnvVar(key.$().ptr(), value.$().ptr());
        }
    }

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            Rnd rnd = TestUtils.generateRandom(null);
            while (true) {
                pgPort = (short) (10000 + rnd.nextInt(1000));
                String testResourcePath = getTestResourcePath();
                path.of(testResourcePath).concat("test");

                try {
                    serverMain = startWithEnvVariables(
                            PG_NET_BIND_TO.getEnvVarName(), "0.0.0.0:" + pgPort,
                            CAIRO_SQL_COPY_ROOT.getEnvVarName(), testResourcePath,
                            CONFIG_RELOAD_ENABLED.getEnvVarName(), "false",
                            HTTP_MIN_ENABLED.getEnvVarName(), "false",
                            HTTP_ENABLED.getEnvVarName(), "false",
                            LINE_TCP_ENABLED.getEnvVarName(), "false",
                            TELEMETRY_DISABLE_COMPLETELY.getEnvVarName(), "true",
                            CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED.getEnvVarName(), String.valueOf(parallelReadParquet)
                    );
                    serverMain.start();
                    break;
                } catch (NetworkError e) {
                    Misc.free(serverMain);
                    if (e.getMessage() == null || !Chars.contains(e.getMessage(), "could not bind socket")) {
                        throw e;
                    }
                }
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        serverMain = Misc.free(serverMain);
        pgPort = 0;
        super.tearDown();
    }

    @Test
    public void test() {
        try (Path path = new Path()) {
            String testResourcePath = getTestResourcePath();
            path.of(testResourcePath).concat("test").concat(testFile);
            Assert.assertTrue(Misc.getThreadLocalUtf8Sink().put(path).toString(), FilesFacadeImpl.INSTANCE.exists(path.$()));
            Sqllogictest.run(pgPort, path.$().ptr());
        }
    }

    private static String getTestResourcePath() {
        URL resource = TestUtils.class.getResource("/sqllogictest");
        assertNotNull("Someone accidentally deleted test resource /sqllogictest?", resource);
        try {
            return Paths.get(resource.toURI()).toFile().getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Could not determine resource path", e);
        }
    }

    protected static Collection<Object[]> files(String dirName) {
        String testResourcePath = getTestResourcePath();
        try (Path path = new Path()) {
            path.concat(testResourcePath).concat("test");
            int basePathLen = path.size();
            path.concat(dirName);
            List<Object[]> paths = new ArrayList<>();
            final StringSink sink = new StringSink();
            Files.walk(path, (pUtf8NameZ, type) -> {
                if (Files.notDots(pUtf8NameZ)) {
                    path.concat(pUtf8NameZ).$();
                    if (Utf8s.endsWithAscii(path, ".test") && !Utf8s.containsAscii(path, ".ignore.")) {
                        sink.clear();
                        sink.put(path, basePathLen, path.size());
                        paths.add(new Object[]{sink.toString()});
                    }
                }
            });
            return paths;
        }
    }
}
