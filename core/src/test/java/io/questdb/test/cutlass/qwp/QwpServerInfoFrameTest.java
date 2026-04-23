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

import io.questdb.client.cutlass.qwp.client.QwpDecodeException;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.client.QwpServerInfoDecoder;
import io.questdb.cutlass.qwp.codec.QwpEgressFrameWriter;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Round-trip unit test for {@link QwpEgressFrameWriter#writeServerInfo} paired
 * with {@link QwpServerInfoDecoder#decode}. Exercises the happy path plus the
 * truncation / bounds-check failure paths that a hostile or buggy server could
 * drive the client through.
 */
public class QwpServerInfoFrameTest {

    @Test
    public void testDefaultStandaloneRoundTrip() throws Exception {
        QwpServerInfo info = encodeAndDecode(
                io.questdb.cutlass.qwp.codec.QwpEgressMsgKind.ROLE_STANDALONE,
                0L,
                0,
                1_700_000_000_000_000_000L,
                "questdb",
                ""
        );
        Assert.assertEquals(QwpEgressMsgKind.ROLE_STANDALONE, info.getRole());
        Assert.assertEquals(0L, info.getEpoch());
        Assert.assertEquals(0, info.getCapabilities());
        Assert.assertEquals(1_700_000_000_000_000_000L, info.getServerWallNs());
        Assert.assertEquals("questdb", info.getClusterId());
        Assert.assertEquals("", info.getNodeId());
    }

    @Test
    public void testLongClusterAndNodeIds() throws Exception {
        String clusterId = repeat('c', 1024);
        String nodeId = repeat('n', 256);
        QwpServerInfo info = encodeAndDecode(
                io.questdb.cutlass.qwp.codec.QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP,
                99_999L,
                0xABCDEF01,
                0L,
                clusterId,
                nodeId
        );
        Assert.assertEquals(QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP, info.getRole());
        Assert.assertEquals(99_999L, info.getEpoch());
        Assert.assertEquals(0xABCDEF01, info.getCapabilities());
        Assert.assertEquals(clusterId, info.getClusterId());
        Assert.assertEquals(nodeId, info.getNodeId());
    }

    @Test
    public void testPrimaryRoleWithClusterId() throws Exception {
        QwpServerInfo info = encodeAndDecode(
                io.questdb.cutlass.qwp.codec.QwpEgressMsgKind.ROLE_PRIMARY,
                7L,
                0,
                123_456_789L,
                "prod-east",
                "node-a"
        );
        Assert.assertEquals(QwpEgressMsgKind.ROLE_PRIMARY, info.getRole());
        Assert.assertEquals(7L, info.getEpoch());
        Assert.assertEquals("prod-east", info.getClusterId());
        Assert.assertEquals("node-a", info.getNodeId());
        Assert.assertEquals("PRIMARY", QwpServerInfo.roleName(info.getRole()));
    }

    @Test(expected = QwpDecodeException.class)
    public void testRejectsFrameWithNonServerInfoMsgKind() throws Exception {
        int cap = 128;
        long buf = Unsafe.getUnsafe().allocateMemory(cap);
        try {
            long qwpStart = buf;
            long bodyStart = QwpEgressFrameWriter.writeMessageHeader(qwpStart, QwpConstants.VERSION_2, (byte) 0, 0, 0);
            // Write a RESULT_END body instead of SERVER_INFO; decoder must reject.
            long bodyEnd = QwpEgressFrameWriter.writeResultEnd(bodyStart, 1L, 0L, 0L);
            int qwpSize = (int) (bodyEnd - qwpStart);
            int payloadLen = qwpSize - QwpConstants.HEADER_SIZE;
            QwpEgressFrameWriter.patchPayloadLength(qwpStart, payloadLen);
            QwpServerInfoDecoder.decode(qwpStart, qwpSize);
        } finally {
            Unsafe.getUnsafe().freeMemory(buf);
        }
    }

    @Test(expected = QwpDecodeException.class)
    public void testRejectsTruncatedFrame() throws Exception {
        int cap = 128;
        long buf = Unsafe.getUnsafe().allocateMemory(cap);
        try {
            long qwpStart = buf;
            long bodyStart = QwpEgressFrameWriter.writeMessageHeader(qwpStart, QwpConstants.VERSION_2, (byte) 0, 0, 0);
            long bodyEnd = QwpEgressFrameWriter.writeServerInfo(
                    bodyStart, 64,
                    io.questdb.cutlass.qwp.codec.QwpEgressMsgKind.ROLE_PRIMARY,
                    1L, 0, 0L, "cluster", "node");
            int fullQwpSize = (int) (bodyEnd - qwpStart);
            QwpEgressFrameWriter.patchPayloadLength(qwpStart, fullQwpSize - QwpConstants.HEADER_SIZE);
            // Shave off the last 4 bytes so node_id length declares more than is present.
            QwpServerInfoDecoder.decode(qwpStart, fullQwpSize - 4);
        } finally {
            Unsafe.getUnsafe().freeMemory(buf);
        }
    }

    @Test
    public void testReplicaRoleWithEpoch() throws Exception {
        QwpServerInfo info = encodeAndDecode(
                io.questdb.cutlass.qwp.codec.QwpEgressMsgKind.ROLE_REPLICA,
                42L,
                0,
                0L,
                "prod-east",
                "replica-b"
        );
        Assert.assertEquals(QwpEgressMsgKind.ROLE_REPLICA, info.getRole());
        Assert.assertEquals(42L, info.getEpoch());
        Assert.assertEquals("REPLICA", QwpServerInfo.roleName(info.getRole()));
    }

    @Test
    public void testUnicodeStrings() throws Exception {
        String clusterId = "prod-east-中文";
        String nodeId = "node-éñ";
        QwpServerInfo info = encodeAndDecode(
                io.questdb.cutlass.qwp.codec.QwpEgressMsgKind.ROLE_PRIMARY,
                1L,
                0,
                0L,
                clusterId,
                nodeId
        );
        Assert.assertEquals(clusterId, info.getClusterId());
        Assert.assertEquals(nodeId, info.getNodeId());
    }

    private static QwpServerInfo encodeAndDecode(
            byte role,
            long epoch,
            int capabilities,
            long wallNs,
            String clusterId,
            String nodeId
    ) throws Exception {
        int cap = 128 + (clusterId.length() + nodeId.length()) * 4;
        long buf = Unsafe.getUnsafe().allocateMemory(cap);
        try {
            long qwpStart = buf;
            long bodyStart = QwpEgressFrameWriter.writeMessageHeader(
                    qwpStart, QwpConstants.VERSION_2, (byte) 0, 0, 0);
            int bodyCap = cap - QwpConstants.HEADER_SIZE;
            long bodyEnd = QwpEgressFrameWriter.writeServerInfo(
                    bodyStart, bodyCap, role, epoch, capabilities, wallNs, clusterId, nodeId);
            Assert.assertTrue("writeServerInfo returned -1 with cap=" + cap, bodyEnd > 0);
            int qwpSize = (int) (bodyEnd - qwpStart);
            int payloadLen = qwpSize - QwpConstants.HEADER_SIZE;
            QwpEgressFrameWriter.patchPayloadLength(qwpStart, payloadLen);
            return QwpServerInfoDecoder.decode(qwpStart, qwpSize);
        } finally {
            Unsafe.getUnsafe().freeMemory(buf);
        }
    }

    private static String repeat(char c, int n) {
        char[] buf = new char[n];
        java.util.Arrays.fill(buf, c);
        return new String(buf);
    }
}
