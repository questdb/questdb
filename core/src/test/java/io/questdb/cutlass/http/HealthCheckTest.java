/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.http;

import io.questdb.Metrics;
import io.questdb.network.NetworkFacadeImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

public class HealthCheckTest {

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    private static final String healthCheckRequest = "GET /status HTTP/1.1\r\n" +
            "Host: localhost:9003\r\n" +
            "User-Agent: Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36\r\n" +
            "Accept: */*\r\n" +
            "Accept-Encoding: gzip, deflate, br\r\n" +
            "\r\n";
    private static final String healthCheckOnRootRequest = "GET / HTTP/1.1\r\n" +
            "Host: localhost:9003\r\n" +
            "User-Agent: Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36\r\n" +
            "Accept: */*\r\n" +
            "Accept-Encoding: gzip, deflate, br\r\n" +
            "\r\n";

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testHealthyViaStatusPathWhenMetricsEnabled() throws Exception {
        testHealthy(Metrics.enabled(), healthCheckRequest);
    }

    @Test
    public void testHealthyViaStatusPathWhenMetricsDisabled() throws Exception {
        testHealthy(Metrics.disabled(), healthCheckRequest);
    }

    @Test
    public void testHealthyViaRootPathWhenMetricsEnabled() throws Exception {
        testHealthy(Metrics.enabled(), healthCheckOnRootRequest);
    }

    @Test
    public void testHealthyViaRootPathWhenMetricsDisabled() throws Exception {
        testHealthy(Metrics.disabled(), healthCheckOnRootRequest);
    }

    private void testHealthy(Metrics metrics, String request) throws Exception {
        new HttpHealthCheckTestBuilder()
                .withTempFolder(temp)
                .withMetrics(metrics)
                .run(engine -> {
                    String expectedResponse = "HTTP/1.1 200 OK\r\n" +
                            "Server: questDB/1.0\r\n" +
                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                            "Transfer-Encoding: chunked\r\n" +
                            "Content-Type: text/plain\r\n" +
                            "\r\n" +
                            "0f\r\n" +
                            "Status: Healthy\r\n" +
                            "00\r\n" +
                            "\r\n";

                    new SendAndReceiveRequestBuilder()
                            .withNetworkFacade(NetworkFacadeImpl.INSTANCE)
                            .withExpectDisconnect(false)
                            .withPrintOnly(false)
                            .withRequestCount(1)
                            .withPauseBetweenSendAndReceive(0)
                            .execute(request, expectedResponse);
                });
    }

    @Test
    public void testUnhealthyWhenMetricsEnabled() throws Exception {
        final String expectedResponse = "HTTP/1.1 500 Internal server error\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: text/plain\r\n" +
                "\r\n" +
                "25\r\n" +
                "Status: Unhealthy\n" +
                "Unhandled errors: 1\r\n" +
                "00\r\n" +
                "\r\n";
        testUnhealthy(Metrics.enabled(), expectedResponse);
    }

    @Test
    public void testUnhealthyWhenMetricsDisabled() throws Exception {
        // Unhandled error detection is based on metrics,
        // so we expect the health check to return HTTP 200.
        final String expectedResponse = "HTTP/1.1 200 OK\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: text/plain\r\n" +
                "\r\n" +
                "0f\r\n" +
                "Status: Healthy\r\n" +
                "00\r\n" +
                "\r\n";
        testUnhealthy(Metrics.disabled(), expectedResponse);
    }

    private void testUnhealthy(Metrics metrics, String expectedResponse) throws Exception {
        new HttpHealthCheckTestBuilder()
                .withTempFolder(temp)
                .withMetrics(metrics)
                .withInjectedUnhandledError()
                .run(engine -> {
                    new SendAndReceiveRequestBuilder()
                            .withNetworkFacade(NetworkFacadeImpl.INSTANCE)
                            .withExpectDisconnect(false)
                            .withPrintOnly(false)
                            .withRequestCount(1)
                            .withPauseBetweenSendAndReceive(0)
                            .execute(healthCheckRequest, expectedResponse);
                });
    }
}
