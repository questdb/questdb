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

package io.questdb.test.sqllogictest.parquet;

import io.questdb.std.*;
import io.questdb.std.str.Utf8s;
import io.questdb.test.Sqllogictest;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.questdb.PropertyKey.CAIRO_SQL_COPY_ROOT;
import static io.questdb.PropertyKey.PG_NET_BIND_TO;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class ParquetTest extends AbstractBootstrapTest {
    private final String testFile;

    public ParquetTest(String testFile) {
        this.testFile = testFile;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> files() {
        String testResourcePath = getTestResourcePath();
        FilesFacade ff = FilesFacadeImpl.INSTANCE;

        try (Path path = new Path()) {
            path.concat(testResourcePath).concat("test").concat("parquet");
            List<Object[]> paths = new ArrayList<>();
            runRecursive(FilesFacadeImpl.INSTANCE, path, paths);
            return paths;
        }
    }

    public static String getTestResourcePath() {
        URL resource = TestUtils.class.getResource("/sqllogictest");
        assertNotNull("Someone accidentally deleted test resource /sqllogictest?", resource);
        try {
            return Paths.get(resource.toURI()).toFile().getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Could not determine resource path", e);
        }
    }

    @Test
    public void test() {
        short pgPort = 6465;

        try (Path path = new Path()) {
            String testResourcePath = getTestResourcePath();
            path.of(testResourcePath).concat("test").concat("parquet").concat(testFile);
            Assert.assertTrue(Misc.getThreadLocalUtf8Sink().put(path).toString(), FilesFacadeImpl.INSTANCE.exists(path.$()));

            try (final TestServerMain serverMain = startWithEnvVariables(
                    PG_NET_BIND_TO.getEnvVarName(), "0.0.0.0:" + pgPort,
                    CAIRO_SQL_COPY_ROOT.getEnvVarName(), testResourcePath
            )) {
                serverMain.start();
                Sqllogictest.run(pgPort, path.$().ptr());
            }
        }
    }

    private static int runRecursive(FilesFacade ff, Path src, List<Object[]> paths) {
        int srcLen = src.size();
        int len = src.size();
        long p = ff.findFirst(src.$());
        String root = src.toString();

        if (p > 0) {
            try {
                int res;
                do {
                    long name = ff.findName(p);
                    if (Files.notDots(name)) {
                        int type = ff.findType(p);
                        src.trimTo(len);
                        src.concat(name);
                        if (type == Files.DT_FILE) {
                            if (Utf8s.endsWithAscii(src, ".test") && !Utf8s.containsAscii(src, ".ignore.")) {
                                String path = src.toString();
                                paths.add(
                                        new Object[]{
                                                path.substring(root.length() + 1)
                                        }
                                );
                            }
                        } else {
                            if ((res = runRecursive(ff, src, paths)) < 0) {
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

//    @Test
//    public void parquet_10279() {
//        short pgPort = 6465;
//
//        try (Path path = new Path()) {
//            String testResourcePath = getTestResourcePath();
//            path.concat(testResourcePath).concat("test/parquet/parquet_10279.test");
//            Assert.assertTrue(Misc.getThreadLocalUtf8Sink().put(path).toString(), FilesFacadeImpl.INSTANCE.exists(path.$()));
//
//            try (final TestServerMain serverMain = startWithEnvVariables(
//                    PG_NET_BIND_TO.getEnvVarName(), "0.0.0.0:" + pgPort,
//                    CAIRO_SQL_COPY_ROOT.getEnvVarName(), testResourcePath
//            )) {
//                serverMain.start();
//                Sqllogictest.run(pgPort, path.$().ptr());
//            }
//        }
//    }
//
//    @Test
//    public void test_aws_files() {
//        short pgPort = 6465;
//
//        try (Path path = new Path()) {
//            String testResourcePath = getTestResourcePath();
//            path.concat(testResourcePath).concat("test/parquet/test_aws_files.test");
//            Assert.assertTrue(Misc.getThreadLocalUtf8Sink().put(path).toString(), FilesFacadeImpl.INSTANCE.exists(path.$()));
//
//            try (final TestServerMain serverMain = startWithEnvVariables(
//                    PG_NET_BIND_TO.getEnvVarName(), "0.0.0.0:" + pgPort,
//                    CAIRO_SQL_COPY_ROOT.getEnvVarName(), testResourcePath
//            )) {
//                serverMain.start();
//                Sqllogictest.run(pgPort, path.$().ptr());
//            }
//        }
//    }
//
//    @Test
//    public void test_parquet_scan() {
//        short pgPort = 6465;
//
//        try (Path path = new Path()) {
//            String testResourcePath = getTestResourcePath();
//            path.concat(testResourcePath).concat("test/parquet/test_parquet_scan.test");
//            Assert.assertTrue(Misc.getThreadLocalUtf8Sink().put(path).toString(), FilesFacadeImpl.INSTANCE.exists(path.$()));
//
//            try (final TestServerMain serverMain = startWithEnvVariables(
//                    PG_NET_BIND_TO.getEnvVarName(), "0.0.0.0:" + pgPort,
//                    CAIRO_SQL_COPY_ROOT.getEnvVarName(), testResourcePath
//            )) {
//                serverMain.start();
//                Sqllogictest.run(pgPort, path.$().ptr());
//            }
//        }
//    }
}
