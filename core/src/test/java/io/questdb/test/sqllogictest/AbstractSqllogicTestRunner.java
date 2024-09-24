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

package io.questdb.test.sqllogictest;

import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.Sqllogictest;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.questdb.PropertyKey.*;
import static io.questdb.std.Files.notDots;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public abstract class AbstractSqllogicTestRunner extends AbstractBootstrapTest {
    private static short pgPort;
    private static TestServerMain serverMain;
    private final String testFile;

    public AbstractSqllogicTestRunner(String testFile) {
        this.testFile = testFile;
    }

    public static void removeNonSystemTables(CharSequence dbRoot) {
        try (Path path = new Path()) {
            path.of(dbRoot);
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            path.slash();
            if (!removeNonSystemTables(path, false)) {
                StringSink dir = new StringSink();
                dir.put(path.$());
                Assert.fail("Test dir " + dir + " cleanup error: " + ff.errno());
            }
        }
    }

    public static boolean removeNonSystemTables(Path path, boolean haltOnFail) {
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        path.$();
        long pFind = ff.findFirst(path.$());
        if (pFind > 0L) {
            int len = path.size();
            boolean res;
            int type;
            long nameUtf8Ptr;
            try {
                do {
                    nameUtf8Ptr = ff.findName(pFind);
                    path.trimTo(len).concat(nameUtf8Ptr).$();
                    type = ff.findType(pFind);
                    if (type == Files.DT_FILE) {
                        if (!ff.removeQuiet(path.$()) && haltOnFail) {
                            return false;
                        }
                    } else if (notDots(nameUtf8Ptr)) {
                        if (path.size() - len < 4 || !Utf8s.equalsAscii("sys.", path, len, len + 4)) {
                            res = type == Files.DT_LNK ? ff.unlink(path.$()) == 0 : ff.rmdir(path, haltOnFail);
                            if (!res && haltOnFail) {
                                return false;
                            }
                        }
                    }
                }
                while (ff.findNext(pFind) > 0);
            } finally {
                ff.findClose(pFind);
                path.trimTo(len).$();
            }

            if (ff.isSoftLink(path.$())) {
                return ff.unlink(path.$()) == 0;
            }
            return true;
        }
        return false;
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

    @AfterClass
    public static void tearDownUpStatic() {
        serverMain = Misc.free(serverMain);
        AbstractBootstrapTest.tearDownStatic();
    }

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            if (serverMain == null) {
                pgPort = (short) (10000 + TestUtils.generateRandom(null).nextInt(1000));
                String testResourcePath = getTestResourcePath();
                path.of(testResourcePath).concat("test");

                serverMain = startWithEnvVariables(
                        PG_NET_BIND_TO.getEnvVarName(), "0.0.0.0:" + pgPort,
                        CAIRO_SQL_COPY_ROOT.getEnvVarName(), testResourcePath,
                        CONFIG_RELOAD_ENABLED.getEnvVarName(), "false",
                        HTTP_MIN_ENABLED.getEnvVarName(), "false",
                        HTTP_ENABLED.getEnvVarName(), "false",
                        LINE_TCP_ENABLED.getEnvVarName(), "false",
                        TELEMETRY_DISABLE_COMPLETELY.getEnvVarName(), "true"
                );
                serverMain.start();
            } else {
                serverMain.reset();
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        LOG.info().$("Finished test ").$(getClass().getSimpleName()).$('#').$(testName.getMethodName()).$();
        if (serverMain != null) {
            serverMain.reset();
            removeNonSystemTables(root + Files.SEPARATOR + "db");
        }
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

    private static int runRecursive(FilesFacade ff, String dirName, Path src, List<Object[]> paths) {
        int srcLen = src.size();
        int len = src.size();
        long p = ff.findFirst(src.$());
        String root = src.toString();

        if (p > 0) {
            try {
                int res;
                do {
                    long name = ff.findName(p);
                    if (notDots(name)) {
                        int type = ff.findType(p);
                        src.trimTo(len);
                        src.concat(name);
                        if (type == Files.DT_FILE) {
                            if (Utf8s.endsWithAscii(src, ".test") && !Utf8s.containsAscii(src, ".ignore.")) {
                                String path = src.toString();
                                paths.add(
                                        new Object[]{
                                                path.substring(root.length() - dirName.length())
                                        }
                                );
                            }
                        } else {
                            if ((res = runRecursive(ff, dirName, src, paths)) < 0) {
                                return res;
                            }
                        }
                        src.trimTo(srcLen);
                    }
                } while (ff.findNext(p) > 0);
            } finally {
                ff.findClose(p);
                src.trimTo(srcLen);
            }
        }

        return 0;
    }

    protected static Collection<Object[]> files(String dirName) {
        String testResourcePath = getTestResourcePath();
        try (Path path = new Path()) {
            path.concat(testResourcePath).concat("test").concat(dirName);
            List<Object[]> paths = new ArrayList<>();
            runRecursive(FilesFacadeImpl.INSTANCE, dirName, path, paths);
            return paths;
        }
    }
}
