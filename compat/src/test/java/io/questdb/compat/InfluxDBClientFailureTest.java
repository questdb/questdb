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

import io.questdb.Bootstrap;
import io.questdb.DefaultBootstrapConfiguration;
import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.ServerMain;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import org.influxdb.InfluxDB;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.wal.WalUtils.EVENT_INDEX_FILE_NAME;

public class InfluxDBClientFailureTest extends AbstractTest {

    @Test
    public void testAppendErrors() {
        final FilesFacade filesFacade = new FilesFacadeImpl() {
            private final AtomicInteger attempt = new AtomicInteger();

            @Override
            public long openRW(LPSZ name, int opts) {
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
        }, Bootstrap.getServerMainArgs(root));

        try (final ServerMain serverMain = new ServerMain(bootstrap)) {
            serverMain.start();

            final List<String> points = new ArrayList<>();
            try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i");

                InfluxDBUtils.assertRequestErrorContains(influxDB, points, "m1,tag1=value1 f1=1i,x=12i",
                        "errors encountered on line(s):write error: m1, errno: ",
                        ",\"errorId\":",
                        ", error: could not open read-write"
                );

                // Retry is ok
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,x=12i");
            }
        }
    }

    @Test
    public void testAppendExceptions() {
        final FilesFacade filesFacade = new FilesFacadeImpl() {
            private final AtomicInteger attempt = new AtomicInteger();

            @Override
            public long openRW(LPSZ name, int opts) {
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
        }, Bootstrap.getServerMainArgs(root));

        try (final ServerMain serverMain = new ServerMain(bootstrap)) {
            serverMain.start();

            final List<String> points = new ArrayList<>();
            try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i");

                InfluxDBUtils.assertRequestErrorContains(influxDB, points, "m1,tag1=value1 f1=1i,x=12i",
                        "{\"code\":\"internal error\",\"message\":\"failed to parse line protocol:errors encountered on line(s):write error: m1, error: java.lang.OutOfMemoryError\"," +
                                "\"line\":1,\"errorId\"",
                        ",\"errorId\":"
                );

                // Retry is ok
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,x=12i");
            }
        }
    }

    @Test
    public void testCommitFailed() {
        final FilesFacade filesFacade = new FilesFacadeImpl() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, Files.SEPARATOR + EVENT_INDEX_FILE_NAME)
                        && Utf8s.containsAscii(name, "failed_table")
                        && (counter.getAndDecrement() > 0)) {
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
        }, Bootstrap.getServerMainArgs(root));

        try (final ServerMain serverMain = new ServerMain(bootstrap)) {
            serverMain.start();

            final List<String> points = new ArrayList<>();
            try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                points.add("first_table,ok=true allgood=true\n");
                points.add("second_table,ok=true allgood=true\n");

                InfluxDBUtils.assertRequestErrorContains(
                        influxDB,
                        points,
                        "failed_table,tag1=value1 f1=1i,y=12i",
                        // the expected error message is split into 2 to avoid asserting the exact errno.
                        // why? test artificially fails at `openRW()`. Upon failure QuestDB fetches errno(), but this will get
                        // whatever errno was set at the last actual failure. thus is not determistic, on Windows it occasionally
                        // gets 0. 
                        "{\"code\":\"internal error\",\"message\":\"failed to parse line protocol:errors encountered on line(s):write error: failed_table, errno: ", "error: could not open read-write"
                );

                // Retry is ok
                InfluxDBUtils.assertRequestOk(influxDB, points, "failed_table,tag1=value1 f1=1i,y=12i,x=12i");
            }
        }
    }

    @Test
    public void testDropTableWhileAppend() {
        AtomicReference<ServerMain> server = new AtomicReference<>();
        final FilesFacade filesFacade = new FilesFacadeImpl() {
            private final AtomicInteger attempt = new AtomicInteger();

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d") && attempt.getAndIncrement() == 0) {
                    try {
                        server.get().getEngine().execute("drop table m1");
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                }
                return super.openRW(name, opts);
            }
        };

        final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
            @Override
            public FilesFacade getFilesFacade() {
                return filesFacade;
            }
        }, Bootstrap.getServerMainArgs(root));

        try (final ServerMain serverMain = new ServerMain(bootstrap)) {
            serverMain.start();
            server.set(serverMain);

            final List<String> points = new ArrayList<>();
            try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i");
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i,x=12i");
            }

            Assert.assertNull(serverMain.getEngine().getTableTokenIfExists("m1"));
        }
    }

    @Test
    public void testDropTableWhileWrite() throws Exception {
        AtomicReference<ServerMain> server = new AtomicReference<>();
        final FilesFacade filesFacade = new FilesFacadeImpl() {
            private final AtomicInteger attempt = new AtomicInteger();

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d") && attempt.getAndIncrement() == 0) {
                    try {
                        server.get().getEngine().execute("drop table m1");
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                }
                return super.openRW(name, opts);
            }
        };

        final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
            @Override
            public FilesFacade getFilesFacade() {
                return filesFacade;
            }
        }, Bootstrap.getServerMainArgs(root));

        long timestamp = MicrosTimestampDriver.floor("2023-11-27T18:53:24.834Z");
        try (final ServerMain serverMain = new ServerMain(bootstrap)) {
            serverMain.start();
            server.set(serverMain);

            final List<String> points = new ArrayList<>();
            try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i");

                points.add("ok_point m1=1i " + timestamp + "000\n");
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i,x=12i");
            }

            Assert.assertNull(serverMain.getEngine().getTableTokenIfExists("m1"));
            Assert.assertNotNull(serverMain.getEngine().getTableTokenIfExists("ok_point"));

            serverMain.getEngine().awaitTxn("ok_point", 1, 2, TimeUnit.SECONDS);
            serverMain.getEngine().print("select * from ok_point", sink);
            Assert.assertTrue(Chars.equals(sink, "m1\ttimestamp\n" +
                    "1\t2023-11-27T18:53:24.834000Z\n"));
        }
    }

    @Test
    public void testGzipSupported() {
        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            final List<String> points = new ArrayList<>();
            try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
                influxDB.enableGzip();
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,x=12i");

                // Retry is ok
                influxDB.disableGzip();
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,x=12i");
            }
        }
    }

    @Test
    public void testGzipSupportedLotsOfData() {
        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();

            final List<String> points = new ArrayList<>();
            try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);
                influxDB.enableGzip();

                for (int i = 0; i < 1_000_000; i++) {
                    points.add("m1,tag1=value1 f1=1i,x=12i");
                }
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,x=12i");

                // Retry is ok
                influxDB.disableGzip();
                InfluxDBUtils.assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,x=12i");
            }
        }
    }

    @Test
    public void testTableColumnAddFailedDoesNotCommit() throws Exception {
        final FilesFacade filesFacade = new FilesFacadeImpl() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public long openRW(LPSZ name, int opts) {
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
        }, Bootstrap.getServerMainArgs(root));

        try (final ServerMain serverMain = new ServerMain(bootstrap)) {
            serverMain.start();

            final List<String> points = new ArrayList<>();
            try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                points.add("good,tag1=value1 f1=1i");

                // This will trigger commit and the commit will fail
                points.add("drop,tag1=value1 f1=1i,y=12i");
                InfluxDBUtils.assertRequestErrorContains(influxDB, points, "drop,tag1=value1 f1=1i,y=12i,z=45",
                        "{\"code\":\"internal error\",\"message\":\"failed to parse line protocol:errors encountered on line(s):write error: drop, error: java.lang.UnsupportedOperationException\",\"line\":3,\"errorId\":"
                );

                // Retry is ok
                points.add("good,tag1=value1 f1=1i");
                InfluxDBUtils.assertRequestOk(influxDB, points, "drop,tag1=value1 f1=1i,y=12i");
            }

            serverMain.awaitTxn("good", 1);
            assertSql(serverMain.getEngine(), "select count() from good", "count()\n" +
                    "1\n");
            serverMain.awaitTable("drop");
            assertSql(serverMain.getEngine(), "select count() from \"drop\"", "count()\n" +
                    "1\n");
        }
    }

    @Test
    public void testTableIsDroppedWhileColumnIsAdded() throws Exception {
        AtomicReference<ServerMain> server = new AtomicReference<>();
        final FilesFacade filesFacade = new FilesFacadeImpl() {
            private final AtomicInteger attempt = new AtomicInteger();

            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "good_y.d") && attempt.getAndIncrement() == 0) {
                    try {
                        server.get().getEngine().execute("drop table \"drop\"");
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                }
                return super.openRW(name, opts);
            }
        };

        final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
            @Override
            public FilesFacade getFilesFacade() {
                return filesFacade;
            }
        }, Bootstrap.getServerMainArgs(root));

        try (final ServerMain serverMain = new ServerMain(bootstrap)) {
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

                InfluxDBUtils.assertRequestOk(influxDB, points, "drop,tag1=value1 f1=1i,y=12i,z=45");

                Assert.assertNull(serverMain.getEngine().getTableTokenIfExists("drop"));
                Assert.assertNotNull(serverMain.getEngine().getTableTokenIfExists("good"));

                serverMain.awaitTable("good");
                assertSql(serverMain.getEngine(), "select count from good", "count\n" +
                        "3\n");

                // This should re-create table "drop"
                points.add("good,tag1=value1 f1=1i");
                InfluxDBUtils.assertRequestOk(influxDB, points, "drop,tag1=value1 f1=1i,y=12i");
            }
        }
    }

    @Test
    public void testUnsupportedPrecision() throws NumericException {
        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            String line = "line,sym1=123 field1=123i 1234567890000000000\n";

            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance(new DefaultHttpClientConfiguration())) {
                HttpClient.Request request = httpClient.newRequest("localhost", serverMain.getHttpServerPort());
                try (
                        HttpClient.ResponseHeaders resp = request.POST()
                                .url("/write?precision=ml ")
                                .withContent()
                                .putAscii(line)
                                .putAscii(line)
                                .send()
                ) {
                    resp.await();
                    Assert.assertEquals(400, Numbers.parseInt(resp.getStatusCode().asAsciiCharSequence()));
                }
            }
        }
    }
}
