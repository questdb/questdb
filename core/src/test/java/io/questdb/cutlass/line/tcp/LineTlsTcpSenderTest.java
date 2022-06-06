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

package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.line.LineTcpSender;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class LineTlsTcpSenderTest extends AbstractLineTcpReceiverTest {

    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static String TOKEN = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    public static final DockerImageName HA_PROXY_IMAGE = DockerImageName.parse("haproxy:2.6.0");

    @ClassRule
    public static GenericContainer<?> haProxy = new GenericContainer<>(HA_PROXY_IMAGE)
            .withClasspathResourceMapping("/io/questdb/cutlass/line/tcp/haproxy.pem", "/usr/local/etc/haproxy/haproxy.pem", BindMode.READ_ONLY)
            .withClasspathResourceMapping("/io/questdb/cutlass/line/tcp/haproxy.cfg", "/usr/local/etc/haproxy/haproxy.cfg", BindMode.READ_ONLY)
            .withExposedPorts(8443)
            .withAccessToHost(true);

    @Test
    public void simpleTest() throws Exception {
        authKeyId = AUTH_KEY_ID1;
        runInContext(c -> {
            Testcontainers.exposeHostPorts(9002);

            try (LineTcpSender sender = LineTcpSender.builder()
                    .enableTls()
                    .address(haProxy.getHost())
                    .port(haProxy.getMappedPort(8443))
                    .enableAuth(AUTH_KEY_ID1).token(TOKEN)
                    .customTrustStore("classpath:/keystore/haproxy_ca.jks", "questdb".toCharArray())
                    .build()) {
                sender.metric("mytable").field("value", 42).$();
                sender.flush();
            }
            assertTableExistsEventually(engine, "mytable");
        });
    }
}
