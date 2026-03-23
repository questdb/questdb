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

package io.questdb.test.griffin.pt;

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.unchecked;

public class IngestEndpointTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testIngestJsonPayload() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM json_readings
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);
                // WAL table needs to be applied before transform is visible for lookup
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=json_readings");
                    request.withContent().put("{\"sensor\":\"temp_1\",\"value\":23.5}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                        String statusCode = Utf8s.toString(rsp.getStatusCode());
                        if (!"200".equals(statusCode)) {
                            throw new AssertionError("Expected 200 but got " + statusCode + ": " + sink);
                        }
                    }
                    TestUtils.assertContains(sink.toString(), "\"status\":\"ok\"");
                    TestUtils.assertContains(sink.toString(), "\"rows_inserted\":1");
                }

                // Allow WAL to apply
                Os.sleep(2000);

                try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    TestUtils.assertSql(
                            engine,
                            ctx,
                            "SELECT sensor, value FROM readings",
                            new StringSink(),
                            "sensor\tvalue\n" +
                                    "temp_1\t23.5\n"
                    );
                }
            }
        });
    }

    @Test
    public void testIngestMissingTransformParam() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest");
                    request.withContent().put("some body");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        TestUtils.assertEquals("400", Utf8s.toString(rsp.getStatusCode()));
                        rsp.getResponse().copyTextTo(sink);
                    }
                    TestUtils.assertContains(sink.toString(), "missing 'transform' query parameter");
                }
            }
        });
    }

    @Test
    public void testIngestNonExistentTransform() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=does_not_exist");
                    request.withContent().put("some body");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        TestUtils.assertEquals("400", Utf8s.toString(rsp.getStatusCode()));
                        rsp.getResponse().copyTextTo(sink);
                    }
                    TestUtils.assertContains(sink.toString(), "transform not found");
                }
            }
        });
    }

    @Test
    public void testIngestColumnsInDifferentOrder() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // SELECT produces columns in reverse order: value, sensor, ts
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM reordered
                        INTO readings
                        AS SELECT
                            json_extract(payload(), '$.value')::DOUBLE AS value,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            now() AS ts
                        """);
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=reordered");
                    request.withContent().put("{\"sensor\":\"temp_1\",\"value\":42.0}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                        String statusCode = Utf8s.toString(rsp.getStatusCode());
                        if (!"200".equals(statusCode)) {
                            throw new AssertionError("Expected 200 but got " + statusCode + ": " + sink);
                        }
                    }
                }

                Os.sleep(2000);

                try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    TestUtils.assertSql(
                            engine,
                            ctx,
                            "SELECT sensor, value FROM readings",
                            new StringSink(),
                            "sensor\tvalue\n" +
                                    "temp_1\t42.0\n"
                    );
                }
            }
        });
    }

    @Test
    public void testIngestFewerColumnsThanTable() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Table has 4 columns, transform only produces 3 (skips 'label')
                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE, label STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM partial
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=partial");
                    request.withContent().put("{\"sensor\":\"temp_1\",\"value\":99.9}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                        String statusCode = Utf8s.toString(rsp.getStatusCode());
                        if (!"200".equals(statusCode)) {
                            throw new AssertionError("Expected 200 but got " + statusCode + ": " + sink);
                        }
                    }
                }

                Os.sleep(2000);

                try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    TestUtils.assertSql(
                            engine,
                            ctx,
                            "SELECT sensor, value, label FROM readings",
                            new StringSink(),
                            "sensor\tvalue\tlabel\n" +
                                    "temp_1\t99.9\t\n"
                    );
                }
            }
        });
    }

    @Test
    public void testIngestColumnNotInTargetTable() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Transform SELECT produces 'extra' column which doesn't exist in target table
                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM bad_cols
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value,
                            json_extract(payload(), '$.extra')::STRING AS extra
                        """);
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=bad_cols");
                    request.withContent().put("{\"sensor\":\"temp_1\",\"value\":23.5,\"extra\":\"x\"}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                    }
                    TestUtils.assertContains(sink.toString(), "column not found in target table [column=extra]");
                }
            }
        });
    }

    @Test
    public void testIngestDlqNotConfigured() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM no_dlq
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);
                Os.sleep(2000);

                // Send invalid JSON - json_extract will return null, cast to DOUBLE produces NaN which is fine
                // Use a transform with extra columns to trigger an error
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM no_dlq_bad
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value,
                            json_extract(payload(), '$.extra')::STRING AS extra
                        """);
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=no_dlq_bad");
                    request.withContent().put("{\"sensor\":\"temp_1\",\"value\":23.5}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                    }
                    // Error returned, no DLQ (just verify no crash)
                    TestUtils.assertContains(sink.toString(), "column not found in target table");
                }
            }
        });
    }

    @Test
    public void testIngestDlqOnTransformError() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM dlq_test
                        INTO readings
                        DLQ dlq_errors PARTITION BY DAY
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value,
                            json_extract(payload(), '$.extra')::STRING AS extra
                        """);
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=dlq_test");
                    request.withContent().put("{\"sensor\":\"temp_1\",\"value\":23.5}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                    }
                    // HTTP error is still returned
                    TestUtils.assertContains(sink.toString(), "column not found in target table");
                }

                Os.sleep(2000);

                // Verify DLQ contains the error
                try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    TestUtils.assertSql(
                            engine,
                            ctx,
                            "SELECT transform_name, stage, error FROM dlq_errors",
                            new StringSink(),
                            "transform_name\tstage\terror\n" +
                                    "dlq_test\ttransform\tcolumn not found in target table [column=extra]\n"
                    );
                }
            }
        });
    }

    @Test
    public void testIngestDeclareOverride() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM override_test
                        INTO readings
                        AS DECLARE OVERRIDABLE @sensor_name := 'default_sensor'
                        SELECT
                            now() AS ts,
                            @sensor_name AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=override_test&sensor_name=custom_sensor");
                    request.withContent().put("{\"value\":42.0}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                        String statusCode = Utf8s.toString(rsp.getStatusCode());
                        if (!"200".equals(statusCode)) {
                            throw new AssertionError("Expected 200 but got " + statusCode + ": " + sink);
                        }
                    }
                }

                Os.sleep(2000);

                try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    TestUtils.assertSql(
                            engine,
                            ctx,
                            "SELECT sensor, value FROM readings",
                            new StringSink(),
                            "sensor\tvalue\n" +
                                    "custom_sensor\t42.0\n"
                    );
                }
            }
        });
    }

    @Test
    public void testIngestDeclareOverrideDefault() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM default_test
                        INTO readings
                        AS DECLARE OVERRIDABLE @sensor_name := 'default_sensor'
                        SELECT
                            now() AS ts,
                            @sensor_name AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);
                Os.sleep(2000);

                // No override - should use default value
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=default_test");
                    request.withContent().put("{\"value\":99.0}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                        String statusCode = Utf8s.toString(rsp.getStatusCode());
                        if (!"200".equals(statusCode)) {
                            throw new AssertionError("Expected 200 but got " + statusCode + ": " + sink);
                        }
                    }
                }

                Os.sleep(2000);

                try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    TestUtils.assertSql(
                            engine,
                            ctx,
                            "SELECT sensor, value FROM readings",
                            new StringSink(),
                            "sensor\tvalue\n" +
                                    "default_sensor\t99.0\n"
                    );
                }
            }
        });
    }

    @Test
    public void testIngestDeclareOverrideNonOverridable() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // @sensor_name is NOT OVERRIDABLE
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM not_overridable
                        INTO readings
                        AS DECLARE @sensor_name := 'fixed_sensor'
                        SELECT
                            now() AS ts,
                            @sensor_name AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=not_overridable&sensor_name=hacked");
                    request.withContent().put("{\"value\":1.0}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                    }
                    TestUtils.assertContains(sink.toString(), "variable is not overridable");
                }
            }
        });
    }

    @Test
    public void testIngestDeclareOverrideMultiple() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM multi_override
                        INTO readings
                        AS DECLARE OVERRIDABLE @sensor_name := 'default', OVERRIDABLE @multiplier := '1'
                        SELECT
                            now() AS ts,
                            @sensor_name AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE * @multiplier::DOUBLE AS value
                        """);
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=multi_override&sensor_name=temp_1&multiplier=10");
                    request.withContent().put("{\"value\":5.0}");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        rsp.getResponse().copyTextTo(sink);
                        String statusCode = Utf8s.toString(rsp.getStatusCode());
                        if (!"200".equals(statusCode)) {
                            throw new AssertionError("Expected 200 but got " + statusCode + ": " + sink);
                        }
                    }
                }

                Os.sleep(2000);

                try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    TestUtils.assertSql(
                            engine,
                            ctx,
                            "SELECT sensor, value FROM readings",
                            new StringSink(),
                            "sensor\tvalue\n" +
                                    "temp_1\t50.0\n"
                    );
                }
            }
        });
    }

    @Test
    public void testIngestMultipleRequests() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM json_readings2
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);
                Os.sleep(2000);

                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    // First request
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=json_readings2");
                    request.withContent().put("{\"sensor\":\"temp_1\",\"value\":10.0}");
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        TestUtils.assertEquals("200", Utf8s.toString(rsp.getStatusCode()));
                    }

                    // Second request
                    httpClient.disconnect();
                    request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=json_readings2");
                    request.withContent().put("{\"sensor\":\"temp_2\",\"value\":20.0}");
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        TestUtils.assertEquals("200", Utf8s.toString(rsp.getStatusCode()));
                    }
                }

                Os.sleep(2000);

                try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                    TestUtils.assertSql(
                            engine,
                            ctx,
                            "SELECT sensor, value FROM readings ORDER BY sensor",
                            new StringSink(),
                            "sensor\tvalue\n" +
                                    "temp_1\t10.0\n" +
                                    "temp_2\t20.0\n"
                    );
                }
            }
        });
    }

}
