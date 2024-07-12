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

import io.questdb.test.Sqllogictest;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static io.questdb.PropertyKey.CAIRO_SQL_COPY_ROOT;
import static io.questdb.PropertyKey.PG_NET_BIND_TO;
import static org.junit.Assert.assertNotNull;

public class ParquetTest extends AbstractBootstrapTest {
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
    public void parquet_10279() {
        short pgPort = 6465;

        try (Path path = new Path()) {
            String testResourcePath = getTestResourcePath();
            path.concat(testResourcePath).concat("test/parquet/parquet_10279.test");
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

    @Test
    public void test_aws_files() {
        short pgPort = 6465;

        try (Path path = new Path()) {
            String testResourcePath = getTestResourcePath();
            path.concat(testResourcePath).concat("test/parquet/test_aws_files.test");
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

    @Test
    public void test_parquet_scan() {
        short pgPort = 6465;

        try (Path path = new Path()) {
            String testResourcePath = getTestResourcePath();
            path.concat(testResourcePath).concat("test/parquet/test_parquet_scan.test");
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
}
