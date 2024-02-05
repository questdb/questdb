/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.Bootstrap;
import io.questdb.DefaultBootstrapConfiguration;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.influxdb.InfluxDB;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.wal.WalUtils.EVENT_INDEX_FILE_NAME;
import static io.questdb.compat.InfluxDBUtils.assertRequestErrorContains;
import static io.questdb.compat.InfluxDBUtils.assertRequestOk;
import static io.questdb.test.cutlass.http.line.LineHttpUtils.getHttpPort;

public class InfluxDBClientFailureTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAppendErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                private final AtomicInteger attempt = new AtomicInteger();

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d") && attempt.getAndIncrement() == 0) {
                        return -1;
                    }
                    return super.openRW(name, opts);
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, TestUtils.getServerMainArgs(root));

            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();

                final List<String> points = new ArrayList<>();
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i");

                    assertRequestErrorContains(influxDB, points, "m1,tag1=value1 f1=1i,x=12i",
                            "errors encountered on line(s):write error: m1, errno: ",
                            ",\"errorId\":",
                            ", error: could not open read-write"
                    );

                    // Retry is ok
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,x=12i");
                }
            }
        });
    }

    @Test
    public void testAppendExceptions() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                private final AtomicInteger attempt = new AtomicInteger();

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d") && attempt.getAndIncrement() == 0) {
                        throw new OutOfMemoryError();
                    }
                    return super.openRW(name, opts);
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, TestUtils.getServerMainArgs(root));

            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();

                final List<String> points = new ArrayList<>();
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i");

                    assertRequestErrorContains(influxDB, points, "m1,tag1=value1 f1=1i,x=12i",
                            "{\"code\":\"internal error\",\"message\":\"failed to parse line protocol:errors encountered on line(s):write error: m1, error: java.lang.OutOfMemoryError\"," +
                                    "\"line\":1,\"errorId\"",
                            ",\"errorId\":"
                    );

                    // Retry is ok
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,x=12i");
                }
            }
        });
    }

    @Test
    public void testCommitFailed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                private final AtomicInteger counter = new AtomicInteger(2);

                @Override
                public long append(int fd, long buf, int len) {
                    if (fd == this.fd && counter.decrementAndGet() == 0) {
                        throw CairoException.critical(24).put("test error");
                    }
                    return Files.append(fd, buf, len);
                }

                @Override
                public int openRW(LPSZ name, long opts) {
                    int fd = super.openRW(name, opts);
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + EVENT_INDEX_FILE_NAME)
                            && Utf8s.containsAscii(name, "failed_table")) {
                        this.fd = fd;
                    }
                    return fd;
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, TestUtils.getServerMainArgs(root));

            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();

                final List<String> points = new ArrayList<>();
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    points.add("first_table,ok=true allgood=true\n");
                    points.add("second_table,ok=true allgood=true\n");
                    assertRequestErrorContains(influxDB, points, "failed_table,tag1=value1 f1=1i,y=12i",
                            "{\"code\":\"internal error\",\"message\":\"commit error for table: failed_table, errno: 24, error: test error\",\"errorId\":"
                    );

                    // Retry is ok
                    assertRequestOk(influxDB, points, "failed_table,tag1=value1 f1=1i,y=12i,x=12i");
                }
            }
        });
    }

    @Test
    public void testDropTableWhileAppend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicReference<TestServerMain> server = new AtomicReference<>();
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                private final AtomicInteger attempt = new AtomicInteger();

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d") && attempt.getAndIncrement() == 0) {
                        server.get().compile("drop table m1");
                    }
                    return super.openRW(name, opts);
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, TestUtils.getServerMainArgs(root));

            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();
                server.set(serverMain);

                final List<String> points = new ArrayList<>();
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i");
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i,x=12i");
                }

                Assert.assertNull(serverMain.getEngine().getTableTokenIfExists("m1"));
            }
        });
    }

    @Test
    public void testDropTableWhileWrite() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicReference<TestServerMain> server = new AtomicReference<>();
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                private final AtomicInteger attempt = new AtomicInteger();

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d") && attempt.getAndIncrement() == 0) {
                        server.get().compile("drop table m1");
                    }
                    return super.openRW(name, opts);
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, TestUtils.getServerMainArgs(root));

            long timestamp = IntervalUtils.parseFloorPartialTimestamp("2023-11-27T18:53:24.834Z");
            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();
                server.set(serverMain);

                final List<String> points = new ArrayList<>();
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i");

                    points.add("ok_point m1=1i " + timestamp + "000\n");
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i,x=12i");
                }

                Assert.assertNull(serverMain.getEngine().getTableTokenIfExists("m1"));
                Assert.assertNotNull(serverMain.getEngine().getTableTokenIfExists("ok_point"));

                serverMain.waitWalTxnApplied("ok_point", 1);
                serverMain.assertSql("select * from ok_point", "m1\ttimestamp\n" +
                        "1\t2023-11-27T18:53:24.834000Z\n");
            }
        });
    }

    @Test
    public void testGzipNotSupported() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();

                final List<String> points = new ArrayList<>();
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
                    influxDB.enableGzip();
                    assertRequestErrorContains(influxDB, points, "m1,tag1=value1 f1=1i,x=12i",
                            "\"message\":\"gzip encoding is not supported\","
                    );

                    // Retry is ok
                    influxDB.disableGzip();
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,x=12i");
                }
            }
        });
    }

    @Test
    public void testTableColumnAddFailedDoesNotCommit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                private final AtomicInteger counter = new AtomicInteger(0);

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "z.d") && counter.getAndIncrement() == 0) {
                        throw new UnsupportedOperationException();
                    }
                    return super.openRW(name, opts);
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, TestUtils.getServerMainArgs(root));

            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();

                final List<String> points = new ArrayList<>();
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    points.add("good,tag1=value1 f1=1i");

                    // This will trigger commit and the commit will fail
                    points.add("drop,tag1=value1 f1=1i,y=12i");
                    assertRequestErrorContains(influxDB, points, "drop,tag1=value1 f1=1i,y=12i,z=45",
                            "{\"code\":\"internal error\",\"message\":\"failed to parse line protocol:errors encountered on line(s):write error: drop, error: java.lang.UnsupportedOperationException\",\"line\":3,\"errorId\":"
                    );

                    // Retry is ok
                    points.add("good,tag1=value1 f1=1i");
                    assertRequestOk(influxDB, points, "drop,tag1=value1 f1=1i,y=12i");
                }

                serverMain.waitWalTxnApplied("good", 1);
                serverMain.assertSql("select count() from good", "count\n" +
                        "1\n");
                serverMain.waitWalTxnApplied("drop");
                serverMain.assertSql("select count() from \"drop\"", "count\n" +
                        "1\n");
            }
        });
    }

    @Test
    public void testTableIsDroppedWhileColumnIsAdded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicReference<TestServerMain> server = new AtomicReference<>();
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                private final AtomicInteger attempt = new AtomicInteger();

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "good_y.d") && attempt.getAndIncrement() == 0) {
                        server.get().compile("drop table \"drop\"");
                    }
                    return super.openRW(name, opts);
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, TestUtils.getServerMainArgs(root));

            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();
                server.set(serverMain);

                final List<String> points = new ArrayList<>();
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    points.add("good,tag1=value1 f1=1i");
                    points.add("drop,tag1=value1 f1=1i,y=12i");
                    // This will trigger dropping table "drop"
                    points.add("good,tag1=value1 f1=1i,good_y=12i");
                    // This line append will fail withe error that the table is dropped
                    // but it shouldn't affect the transaction
                    points.add("drop,tag1=value1 f1=1i,y=12i,z=45");

                    points.add("good,tag1=value1 f1=1i,good_y=12i");
                    points.add("drop,tag1=value1 f1=1i,y=12i,z=45");
                    points.add("drop,tag1=value1 f1=1i,y=12i,z=45");

                    assertRequestOk(influxDB, points, "drop,tag1=value1 f1=1i,y=12i,z=45");

                    Assert.assertNull(serverMain.getEngine().getTableTokenIfExists("drop"));
                    Assert.assertNotNull(serverMain.getEngine().getTableTokenIfExists("good"));

                    serverMain.waitWalTxnApplied("good");
                    serverMain.assertSql("select count from good", "count\n" +
                            "3\n");

                    // This should re-create table "drop"
                    points.add("good,tag1=value1 f1=1i");
                    assertRequestOk(influxDB, points, "drop,tag1=value1 f1=1i,y=12i");
                }
            }
        });
    }

    @Test
    public void testUnsupportedPrecision() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                String line = "line,sym1=123 field1=123i 1234567890000000000\n";

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                    HttpClient.Request request = httpClient.newRequest("localhost", getHttpPort(serverMain));
                    try (
                            HttpClient.ResponseHeaders resp = request.POST()
                                    .url("/write?precision=ml ")
                                    .withContent()
                                    .putAscii(line)
                                    .putAscii(line)
                                    .send()
                    ) {
                        resp.await();
                        TestUtils.assertEquals("400", resp.getStatusCode());
                    }
                }
            }
        });
    }
}
