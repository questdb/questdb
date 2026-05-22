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

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileWriter;
import java.io.PrintWriter;

import static io.questdb.test.tools.TestUtils.unchecked;

public class IngestEndpointTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testIngestColumnNotInTargetTable() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Transform SELECT produces 'extra' column which doesn't exist in target table
                // DDL validation rejects this at CREATE time
                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try {
                    engine.execute("""
                            CREATE PAYLOAD TRANSFORM bad_cols
                            INTO readings
                            AS SELECT
                                now() AS ts,
                                json_extract(payload(), '$.sensor')::STRING AS sensor,
                                json_extract(payload(), '$.value')::DOUBLE AS value,
                                json_extract(payload(), '$.extra')::STRING AS extra
                            """);
                    Assert.fail("expected SqlException");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "column not found in target table [column=extra");
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

                assertIngestSuccess(engine, "/ingest?transform=reordered", "{\"sensor\":\"temp_1\",\"value\":42.0}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "temp_1\t42.0\n"
                );
            }
        });
    }

    @Test
    public void testIngestCairoExceptionReturns500() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Create transform targeting table, then drop the table
                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM target_missing
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                // Wait for the transform to be visible, then drop the target table
                assertIngestSuccess(engine, "/ingest?transform=target_missing", "{\"sensor\":\"s1\",\"value\":1.0}");
                engine.execute("DROP TABLE readings");

                TestUtils.assertEventually(() -> {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                        request.POST().url("/ingest?transform=target_missing");
                        request.withContent().put("{\"sensor\":\"s1\",\"value\":1.0}");

                        Utf8StringSink sink = new Utf8StringSink();
                        try (HttpClient.ResponseHeaders rsp = request.send()) {
                            rsp.await();
                            TestUtils.assertEquals("500", Utf8s.toString(rsp.getStatusCode()));
                            rsp.getResponse().copyTextTo(sink);
                        }
                        TestUtils.assertContains(sink.toString(), "\"status\":\"error\"");
                    }
                });
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

                assertIngestSuccess(engine, "/ingest?transform=override_test&sensor_name=custom_sensor", "{\"value\":42.0}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "custom_sensor\t42.0\n"
                );
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

                // No override - should use default value
                assertIngestSuccess(engine, "/ingest?transform=default_test", "{\"value\":99.0}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "default_sensor\t99.0\n"
                );
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

                assertIngestSuccess(engine, "/ingest?transform=multi_override&sensor_name=temp_1&multiplier=10", "{\"value\":5.0}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "temp_1\t50.0\n"
                );
            }
        });
    }

    @Test
    public void testIngestDeclareOverrideInsideCte() throws Exception {
        // Regression: a DECLARE OVERRIDABLE inside a CTE body must still see the
        // caller-supplied URL override. Without parseWith / parseWithClauses /
        // parseAsSubQueryAndExpectClosingBrace threading the overrideDeclare flag,
        // the WITH-driven path silently fell back to the inline DECLARE default.
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM cte_override
                        INTO readings
                        AS WITH params AS (
                                DECLARE OVERRIDABLE @sensor_name := 'default_sensor'
                                SELECT @sensor_name AS sensor
                            )
                            SELECT
                                now() AS ts,
                                params.sensor AS sensor,
                                json_extract(payload(), '$.value')::DOUBLE AS value
                            FROM params
                        """);

                assertIngestSuccess(engine, "/ingest?transform=cte_override&sensor_name=cte_custom", "{\"value\":7.0}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "cte_custom\t7.0\n"
                );
            }
        });
    }

    @Test
    public void testIngestDeclareOverrideTopLevelWith() throws Exception {
        // Companion regression: when the transform body starts with WITH ... SELECT
        // (top-level CTE form, not just bare SELECT), the override flag must be
        // threaded through parseWith. Before the fix, the bare-SELECT entry point
        // got the flag but the WITH entry point dropped it.
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM top_with_override
                        INTO readings
                        AS DECLARE OVERRIDABLE @sensor_name := 'default_top'
                            WITH src AS (SELECT @sensor_name AS sensor)
                            SELECT now() AS ts, src.sensor AS sensor, json_extract(payload(), '$.value')::DOUBLE AS value
                            FROM src
                        """);

                assertIngestSuccess(engine, "/ingest?transform=top_with_override&sensor_name=top_custom", "{\"value\":11.0}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "top_custom\t11.0\n"
                );
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

                TestUtils.assertEventually(() -> {
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
                });
            }
        });
    }

    @Test
    public void testIngestDlqExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // Pre-create the DLQ table before creating the transform
                engine.execute("""
                        CREATE TABLE my_dlq (
                            ts TIMESTAMP,
                            transform_name SYMBOL,
                            payload VARCHAR,
                            query VARCHAR,
                            stage SYMBOL,
                            error VARCHAR
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL
                        """);
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM dlq_exists
                        INTO readings
                        DLQ my_dlq PARTITION BY DAY
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                // Verify transform works, then drop target table to trigger runtime error
                assertIngestSuccess(engine, "/ingest?transform=dlq_exists", "{\"sensor\":\"s1\",\"value\":1.0}");
                engine.execute("DROP TABLE readings");

                TestUtils.assertEventually(() -> {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                        request.POST().url("/ingest?transform=dlq_exists");
                        request.withContent().put("{\"sensor\":\"s2\",\"value\":2.0}");

                        Utf8StringSink sink = new Utf8StringSink();
                        try (HttpClient.ResponseHeaders rsp = request.send()) {
                            rsp.await();
                            TestUtils.assertEquals("500", Utf8s.toString(rsp.getStatusCode()));
                        }
                    }
                });

                // DLQ should receive error records even though it was pre-created
                assertSqlEventually(engine,
                        "SELECT transform_name, stage FROM my_dlq",
                        "transform_name\tstage\n" +
                                "dlq_exists\ttransform\n"
                );
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
                // Transform without DLQ
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM no_dlq
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                // Verify transform works, then drop target table to trigger runtime error
                assertIngestSuccess(engine, "/ingest?transform=no_dlq", "{\"sensor\":\"temp_1\",\"value\":23.5}");
                engine.execute("DROP TABLE readings");

                TestUtils.assertEventually(() -> {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                        request.POST().url("/ingest?transform=no_dlq");
                        request.withContent().put("{\"sensor\":\"temp_1\",\"value\":23.5}");

                        Utf8StringSink sink = new Utf8StringSink();
                        try (HttpClient.ResponseHeaders rsp = request.send()) {
                            rsp.await();
                            TestUtils.assertEquals("500", Utf8s.toString(rsp.getStatusCode()));
                            rsp.getResponse().copyTextTo(sink);
                        }
                        // Error returned, no DLQ (just verify no crash)
                        TestUtils.assertContains(sink.toString(), "\"status\":\"error\"");
                    }
                });
            }
        });
    }

    @Test
    public void testIngestEmptyPayload() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM empty_body
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                // An empty body must be rejected with HTTP 400 - silently inserting a NULL row
                // would mask client mistakes and is not the documented contract for /ingest.
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=empty_body");
                    request.withContent().put("");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        TestUtils.assertEquals("400", Utf8s.toString(rsp.getStatusCode()));
                        rsp.getResponse().copyTextTo(sink);
                    }
                    TestUtils.assertContains(sink.toString(), "request body is empty");
                }

                // Verify no row was inserted.
                assertSqlEventually(engine, "SELECT count() FROM readings", "count()\n0\n");
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
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                // Verify transform works, then drop target table to trigger runtime error
                assertIngestSuccess(engine, "/ingest?transform=dlq_test", "{\"sensor\":\"temp_1\",\"value\":23.5}");
                engine.execute("DROP TABLE readings");

                TestUtils.assertEventually(() -> {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                        request.POST().url("/ingest?transform=dlq_test");
                        request.withContent().put("{\"sensor\":\"temp_2\",\"value\":99.0}");

                        Utf8StringSink sink = new Utf8StringSink();
                        try (HttpClient.ResponseHeaders rsp = request.send()) {
                            rsp.await();
                            TestUtils.assertEquals("500", Utf8s.toString(rsp.getStatusCode()));
                        }
                    }
                });

                // Verify DLQ contains the error
                assertSqlEventually(engine,
                        "SELECT transform_name, stage FROM dlq_errors",
                        "transform_name\tstage\n" +
                                "dlq_test\ttransform\n"
                );
            }
        });
    }

    @Test
    public void testIngestDlqSkippedOnCompilationError() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // Transform with DLQ and a non-overridable variable
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM compile_err
                        INTO readings
                        DLQ dlq_compile PARTITION BY DAY
                        AS DECLARE @sensor_name := 'fixed'
                        SELECT
                            now() AS ts,
                            @sensor_name AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                // Ensure the transform is usable first
                assertIngestSuccess(engine, "/ingest?transform=compile_err", "{\"value\":1.0}");

                // Override a non-overridable variable - triggers compilation error, not runtime
                TestUtils.assertEventually(() -> {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                        request.POST().url("/ingest?transform=compile_err&sensor_name=hacked");
                        request.withContent().put("{\"value\":2.0}");

                        Utf8StringSink sink = new Utf8StringSink();
                        try (HttpClient.ResponseHeaders rsp = request.send()) {
                            rsp.await();
                            rsp.getResponse().copyTextTo(sink);
                        }
                        TestUtils.assertContains(sink.toString(), "variable is not overridable");
                    }
                });

                // DLQ must be empty - compilation errors are config problems, not payload problems
                assertSqlEventually(engine,
                        "SELECT count() AS cnt FROM dlq_compile",
                        "cnt\n" +
                                "0\n"
                );
            }
        });
    }

    @Test
    public void testIngestExcessivelyLargePayload() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM large_payload
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                // Build a large payload (~100KB)
                StringBuilder sb = new StringBuilder();
                sb.append("{\"sensor\":\"");
                for (int i = 0; i < 100_000; i++) {
                    sb.append('x');
                }
                sb.append("\",\"value\":1.0}");

                assertIngestSuccess(engine, "/ingest?transform=large_payload", sb.toString());

                assertSqlEventually(engine,
                        "SELECT length(sensor) AS len, value FROM readings",
                        "len\tvalue\n" +
                                "100000\t1.0\n"
                );
            }
        });
    }

    @Test
    public void testIngestInvalidUtf8Body() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM utf8_test
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            payload()::STRING AS sensor,
                            0.0 AS value
                        """);

                TestUtils.assertEventually(() -> {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                        request.POST().url("/ingest?transform=utf8_test");
                        // Invalid UTF-8: 0xFF is never valid in UTF-8
                        request.withContent().putAscii("valid start").put((byte) 0xFF).putAscii("end");

                        Utf8StringSink sink = new Utf8StringSink();
                        try (HttpClient.ResponseHeaders rsp = request.send()) {
                            rsp.await();
                            TestUtils.assertEquals("400", Utf8s.toString(rsp.getStatusCode()));
                            rsp.getResponse().copyTextTo(sink);
                        }
                        TestUtils.assertContains(sink.toString(), "invalid UTF-8 in request body");
                    }
                });
            }
        });
    }

    @Test
    public void testIngestRowsInsertedCount() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM count_test
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                TestUtils.assertEventually(() -> {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                        request.POST().url("/ingest?transform=count_test");
                        request.withContent().put("{\"sensor\":\"temp_1\",\"value\":42.0}");

                        Utf8StringSink sink = new Utf8StringSink();
                        try (HttpClient.ResponseHeaders rsp = request.send()) {
                            rsp.await();
                            TestUtils.assertEquals("200", Utf8s.toString(rsp.getStatusCode()));
                            rsp.getResponse().copyTextTo(sink);
                        }
                        TestUtils.assertContains(sink.toString(), "\"rows_inserted\":1");
                    }
                });

                // Multi-row: transform that produces 3 rows per request
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM count_multi
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            'sensor_' || x::STRING AS sensor,
                            x::DOUBLE AS value
                        FROM long_sequence(3)
                        """);

                TestUtils.assertEventually(() -> {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                        request.POST().url("/ingest?transform=count_multi");
                        request.withContent().put("ignored");

                        Utf8StringSink sink = new Utf8StringSink();
                        try (HttpClient.ResponseHeaders rsp = request.send()) {
                            rsp.await();
                            TestUtils.assertEquals("200", Utf8s.toString(rsp.getStatusCode()));
                            rsp.getResponse().copyTextTo(sink);
                        }
                        TestUtils.assertContains(sink.toString(), "\"rows_inserted\":3");
                    }
                });
            }
        });
    }

    @Test
    public void testIngestUnicodePayload() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM unicode_test
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                assertIngestSuccess(engine, "/ingest?transform=unicode_test",
                        "{\"sensor\":\"\u6e29\u5ea6\u30bb\u30f3\u30b5\u30fc\",\"value\":36.6}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "\u6e29\u5ea6\u30bb\u30f3\u30b5\u30fc\t36.6\n"
                );
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

                assertIngestSuccess(engine, "/ingest?transform=partial", "{\"sensor\":\"temp_1\",\"value\":99.9}");

                assertSqlEventually(engine,
                        "SELECT sensor, value, label FROM readings",
                        "sensor\tvalue\tlabel\n" +
                                "temp_1\t99.9\t\n"
                );
            }
        });
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

                assertIngestSuccess(engine, "/ingest?transform=json_readings", "{\"sensor\":\"temp_1\",\"value\":23.5}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "temp_1\t23.5\n"
                );
            }
        });
    }

    @Test
    public void testIngestMaxRequestSize() throws Exception {
        assertMemoryLeak(() -> {
            // Append a small max request size to the config file
            String confFile = root + Files.SEPARATOR + "conf" + Files.SEPARATOR + "server.conf";
            try (PrintWriter pw = new PrintWriter(new FileWriter(confFile, true))) {
                pw.println(PropertyKey.HTTP_INGEST_MAX_REQUEST_SIZE.getPropertyPath() + "=64");
            }

            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM size_test
                        INTO readings
                        AS SELECT
                            now() AS ts,
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                // Build a body larger than 64 bytes
                String largeBody = "{\"sensor\":\"this_sensor_name_is_long_enough_to_exceed_the_limit\",\"value\":1.0}";
                Assert.assertTrue("body must exceed limit", largeBody.length() > 64);

                TestUtils.assertEventually(() -> {
                    try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                        HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                        request.POST().url("/ingest?transform=size_test");
                        request.withContent().put(largeBody);

                        Utf8StringSink sink = new Utf8StringSink();
                        try (HttpClient.ResponseHeaders rsp = request.send()) {
                            rsp.await();
                            TestUtils.assertEquals("413", Utf8s.toString(rsp.getStatusCode()));
                            rsp.getResponse().copyTextTo(sink);
                        }
                        TestUtils.assertContains(sink.toString(), "request body exceeds maximum size");
                    }
                });
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

                assertIngestSuccess(engine, "/ingest?transform=json_readings2", "{\"sensor\":\"temp_1\",\"value\":10.0}");
                assertIngestSuccess(engine, "/ingest?transform=json_readings2", "{\"sensor\":\"temp_2\",\"value\":20.0}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings ORDER BY sensor",
                        "sensor\tvalue\n" +
                                "temp_1\t10.0\n" +
                                "temp_2\t20.0\n"
                );
            }
        });
    }

    @Test
    public void testIngestNonExistentTransformWithSpecialChars() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Seed a victim table that the injected SQL would destroy if the lookup
                // were vulnerable to SQL injection.
                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("INSERT INTO readings VALUES (now(), 'sentinel', 1.0)");
                TestUtils.drainWalQueue(engine);

                // Try SQL injection via transform name - single quotes and special chars.
                // The handler must reject the request with 400 and must NOT execute the
                // payload as SQL against the system table.
                try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                    HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                    request.POST().url("/ingest?transform=';DROP TABLE readings;--");
                    request.withContent().put("body");

                    Utf8StringSink sink = new Utf8StringSink();
                    try (HttpClient.ResponseHeaders rsp = request.send()) {
                        rsp.await();
                        TestUtils.assertEquals("400", Utf8s.toString(rsp.getStatusCode()));
                        rsp.getResponse().copyTextTo(sink);
                    }
                    TestUtils.assertContains(sink.toString(), "transform not found");
                }

                // The victim table must still exist and still hold its row.
                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "sentinel\t1.0\n"
                );
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
    public void testIngestTransformOmitsTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();

                // Table has designated timestamp, but transform SELECT omits it
                engine.execute("CREATE TABLE readings (ts TIMESTAMP, sensor STRING, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("""
                        CREATE PAYLOAD TRANSFORM no_ts
                        INTO readings
                        AS SELECT
                            json_extract(payload(), '$.sensor')::STRING AS sensor,
                            json_extract(payload(), '$.value')::DOUBLE AS value
                        """);

                // Should use copyUnordered since cursor has no timestamp column
                assertIngestSuccess(engine, "/ingest?transform=no_ts", "{\"sensor\":\"temp_1\",\"value\":77.0}");

                assertSqlEventually(engine,
                        "SELECT sensor, value FROM readings",
                        "sensor\tvalue\n" +
                                "temp_1\t77.0\n"
                );
            }
        });
    }

    /**
     * Sends a POST to the ingest endpoint and asserts HTTP 200.
     * Retries via assertEventually to wait for WAL apply of the transform definition.
     */
    private void assertIngestSuccess(CairoEngine engine, String url, String body) throws Exception {
        TestUtils.assertEventually(() -> {
            try (HttpClient httpClient = HttpClientFactory.newPlainTextInstance()) {
                HttpClient.Request request = httpClient.newRequest("localhost", HTTP_PORT);
                request.POST().url(url);
                request.withContent().put(body);

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
            }
        });
    }

    /**
     * Asserts SQL query results, retrying via assertEventually to wait for WAL apply.
     */
    private void assertSqlEventually(CairoEngine engine, String query, String expected) throws Exception {
        TestUtils.assertEventually(() -> {
            try (SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
                TestUtils.assertSql(
                        engine,
                        ctx,
                        query,
                        new StringSink(),
                        expected
                );
            }
        });
    }
}
