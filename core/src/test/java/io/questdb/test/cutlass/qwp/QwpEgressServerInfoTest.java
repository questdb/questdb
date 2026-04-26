/*+*****************************************************************************
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

package io.questdb.test.cutlass.qwp;

import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end coverage of the v2 {@code SERVER_INFO} frame: boot an embedded
 * OSS server, open a {@link QwpQueryClient}, and verify the first frame
 * received carries the expected standalone role and cluster identity. Covers
 * the routing / failover contracts that don't need a multi-node deployment
 * (role matching, role-mismatch exception).
 */
public class QwpEgressServerInfoTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testNegotiatesVersion2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    Assert.assertEquals(
                            "server should advertise v2",
                            QwpConstants.VERSION_2,
                            client.getNegotiatedQwpVersion()
                    );
                    Assert.assertNotNull("v2 must surface SERVER_INFO", client.getServerInfo());
                }
            }
        });
    }

    @Test
    public void testOssServerReportsStandalone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                try (QwpQueryClient client = QwpQueryClient.newPlainText("127.0.0.1", HTTP_PORT)) {
                    client.connect();
                    QwpServerInfo info = client.getServerInfo();
                    Assert.assertNotNull(info);
                    Assert.assertEquals(
                            "OSS default must report STANDALONE role",
                            QwpEgressMsgKind.ROLE_STANDALONE,
                            info.getRole()
                    );
                    Assert.assertEquals("questdb", info.getClusterId());
                    // node_id and epoch are reserved defaults on OSS; spot-check shape rather than value
                    Assert.assertNotNull(info.getNodeId());
                    Assert.assertTrue("wall-clock hint should be positive", info.getServerWallNs() > 0);
                }
            }
        });
    }

    @Test
    public void testTargetAnyAcceptsStandalone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                String conf = "ws::addr=127.0.0.1:" + HTTP_PORT + ";target=any;";
                try (QwpQueryClient client = QwpQueryClient.fromConfig(conf)) {
                    client.connect();
                    Assert.assertTrue(client.isConnected());
                }
            }
        });
    }

    @Test
    public void testTargetPrimaryAcceptsStandalone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                // OSS standalone passes the primary filter so single-node deployments
                // don't get excluded by client configs that default to target=primary.
                String conf = "ws::addr=127.0.0.1:" + HTTP_PORT + ";target=primary;";
                try (QwpQueryClient client = QwpQueryClient.fromConfig(conf)) {
                    client.connect();
                    Assert.assertTrue(client.isConnected());
                    Assert.assertEquals(QwpEgressMsgKind.ROLE_STANDALONE, client.getServerInfo().getRole());
                }
            }
        });
    }

    @Test
    public void testTargetReplicaRejectsStandalone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                String conf = "ws::addr=127.0.0.1:" + HTTP_PORT + ";target=replica;";
                try (QwpQueryClient client = QwpQueryClient.fromConfig(conf)) {
                    try {
                        client.connect();
                        Assert.fail("expected QwpRoleMismatchException for target=replica on standalone server");
                    } catch (QwpRoleMismatchException expected) {
                        Assert.assertEquals("replica", expected.getTargetRole());
                        Assert.assertNotNull("last observed info should be attached", expected.getLastObserved());
                        Assert.assertEquals(
                                QwpEgressMsgKind.ROLE_STANDALONE,
                                expected.getLastObserved().getRole()
                        );
                    }
                    Assert.assertFalse(client.isConnected());
                }
            }
        });
    }
}
