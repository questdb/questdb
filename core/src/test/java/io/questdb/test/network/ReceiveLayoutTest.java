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


package io.questdb.test.network;

import io.questdb.network.ReceiveLayout;
import io.questdb.network.SteeringCpuTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReceiveLayoutTest {
    // CPUs 0-7 on socket 0 map to group 0, CPUs 8-15 on socket 1 to group 1, CPUs 16-17 to none
    private static final int[] TABLE = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, SteeringCpuTable.NO_GROUP, SteeringCpuTable.NO_GROUP};
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    private SysfsFixture fixture;

    @Before
    public void setUp() {
        fixture = new SysfsFixture(temp.getRoot().toPath());
    }

    @Test
    public void testAllInterfacesSkipVirtualAndDown() {
        fixture.nic("eth0", 2).irq("eth0", 100, "1").irq("eth0", 101, "9");
        fixture.nic("eth1", 2).irq("eth1", 110, "1").irq("eth1", 111, "2").down("eth1");
        fixture.virtualNic("docker0").virtualNic("veth1a2b");
        final ReceiveLayout.Prediction prediction = predict(0);
        Assert.assertNull(prediction.getUnreadable());
        Assert.assertEquals("eth0 50.0%/50.0%", prediction.getSignature());
    }

    @Test
    public void testBindAddressInterface() {
        fixture.nic("eth0", 2).irq("eth0", 100, "1").irq("eth0", 101, "2");
        fixture.nic("eth1", 2).irq("eth1", 110, "1").irq("eth1", 111, "9");
        final ReceiveLayout layout = new ReceiveLayout(fixture.getProcRoot(), fixture.getSysRoot(), ipv4 -> ipv4 == 0x0A000005 ? "eth1" : null);
        final ReceiveLayout.Prediction prediction = ReceiveLayout.predict(layout.read(0x0A000005), TABLE, 2);
        Assert.assertEquals("eth1 50.0%/50.0%", prediction.getSignature());
        assertUnreadable(ReceiveLayout.predict(layout.read(0x0A000006), TABLE, 2), "no interface owns the bind address 10.0.0.6");
    }

    @Test
    public void testBindAddressOnInterfaceWithoutDevice() {
        fixture.virtualNic("bond0");
        final ReceiveLayout layout = new ReceiveLayout(fixture.getProcRoot(), fixture.getSysRoot(), _ -> "bond0");
        assertUnreadable(ReceiveLayout.predict(layout.read(0x0A000005), TABLE, 2), "interface bond0 of the bind address has no device behind it");
    }

    @Test
    public void testBusyPolling() {
        fixture.nic("eth0", 1).irq("eth0", 100, "1").busyPoll(50);
        assertUnreadable(predict(0), "busy polling is on");
    }

    @Test
    public void testEachNicJudgedOnItsOwn() {
        // the data NIC interrupts socket 1 only, an idle management NIC socket 0 only
        fixture.nic("eth0", 2).irq("eth0", 100, "9").irq("eth0", 101, "10");
        fixture.nic("eth1", 2).irq("eth1", 110, "1").irq("eth1", 111, "2");
        final ReceiveLayout.Prediction prediction = predict(0);
        Assert.assertEquals("eth0 0.0%/100.0%, eth1 100.0%/0.0%", prediction.getSignature());
        Assert.assertEquals(1.0, prediction.getMaxShare(), 1e-9);
    }

    @Test
    public void testEightAndEight() {
        fixture.nic("eth0", 16);
        for (int i = 0; i < 16; i++) {
            fixture.irq("eth0", 100 + i, Integer.toString(i));
        }
        final ReceiveLayout.Prediction prediction = predict(0);
        Assert.assertEquals("eth0 50.0%/50.0%", prediction.getSignature());
        Assert.assertEquals(0.5, prediction.getMaxShare(), 1e-9);
    }

    @Test
    public void testFifteenAndOne() {
        fixture.nic("eth0", 16);
        for (int i = 0; i < 16; i++) {
            fixture.irq("eth0", 100 + i, i == 0 ? "0" : "9");
        }
        Assert.assertEquals(0.9375, predict(0).getMaxShare(), 1e-9);
    }

    @Test
    public void testInterruptCpusInDifferentGroups() {
        fixture.nic("eth0", 1).irq("eth0", 100, "6-9");
        assertUnreadable(predict(0), "the CPUs 6-9 of interrupt 100 of eth0 map to different groups");
    }

    @Test
    public void testInterruptCpusInOneGroup() {
        // without an effective affinity, a list on one socket still maps to one group
        fixture.nic("eth0", 2).irq("eth0", 100, "0").irq("eth0", 101, "9")
                .removeEffectiveAffinity(101)
                .smpAffinity(101, "8-15");
        Assert.assertEquals("eth0 50.0%/50.0%", predict(0).getSignature());
    }

    @Test
    public void testManagementVectorCountsAsAQueue() {
        // ENA: 16 receive queues on socket 1 and a management vector on socket 0
        fixture.nic("eth0", 16).irq("eth0", 99, "0");
        for (int i = 0; i < 16; i++) {
            fixture.irq("eth0", 100 + i, "9");
        }
        Assert.assertEquals("eth0 5.9%/94.1%", predict(0).getSignature());
    }

    @Test
    public void testNoInterface() {
        fixture.virtualNic("lo");
        assertUnreadable(predict(0), "no interface that is up has a device behind it");
    }

    @Test
    public void testNoMsiVectors() {
        fixture.nic("eth0", 1);
        assertUnreadable(predict(0), "interface eth0 has no requested interrupt vectors");
    }

    @Test
    public void testOneSocket() {
        fixture.nic("eth0", 4);
        for (int i = 0; i < 4; i++) {
            fixture.irq("eth0", 100 + i, Integer.toString(8 + i));
        }
        Assert.assertEquals(1.0, predict(0).getMaxShare(), 1e-9);
    }

    @Test
    public void testOutOfRangeCpuSpreadsEvenly() {
        fixture.nic("eth0", 2).irq("eth0", 100, "16").irq("eth0", 101, "9");
        Assert.assertEquals("eth0 25.0%/75.0%", predict(0).getSignature());
    }

    @Test
    public void testRpsOnEveryQueue() {
        // RPS sends queue 0's packets to CPUs 0-3 and 8-11, and queue 1's to CPUs 8-15
        fixture.nic("eth0", 2).irq("eth0", 100, "0").irq("eth0", 101, "0")
                .rps("eth0", 0, "00000000,00000f0f")
                .rps("eth0", 1, "00000000,0000ff00");
        Assert.assertEquals("eth0 25.0%/75.0%", predict(0).getSignature());
    }

    @Test
    public void testRpsOnSomeQueues() {
        fixture.nic("eth0", 2).irq("eth0", 100, "0").irq("eth0", 101, "9").rps("eth0", 0, "ff");
        assertUnreadable(predict(0), "interface eth0 has RPS on 1 of its 2 receive queues");
    }

    @Test
    public void testSmpAffinityWithoutEffectiveAffinity() {
        fixture.nic("eth0", 2).irq("eth0", 100, "0").irq("eth0", 101, "1")
                .removeEffectiveAffinity(101)
                .smpAffinity(101, "9");
        Assert.assertEquals("eth0 50.0%/50.0%", predict(0).getSignature());
    }

    @Test
    public void testTenAndSix() {
        fixture.nic("eth0", 16);
        for (int i = 0; i < 16; i++) {
            fixture.irq("eth0", 100 + i, i < 10 ? "0" : "9");
        }
        final ReceiveLayout.Prediction prediction = predict(0);
        Assert.assertEquals("eth0 62.5%/37.5%", prediction.getSignature());
        Assert.assertEquals(0.625, prediction.getMaxShare(), 1e-9);
    }

    @Test
    public void testThreadedNapi() {
        fixture.nic("eth0", 1).irq("eth0", 100, "1").threaded("eth0");
        assertUnreadable(predict(0), "interface eth0 runs threaded NAPI");
    }

    @Test
    public void testTooManyVectors() {
        // mlx5 without dynamic MSI-X: a vector per CPU for 2 receive queues
        fixture.nic("eth0", 2);
        for (int i = 0; i < 7; i++) {
            fixture.irq("eth0", 100 + i, Integer.toString(i));
        }
        assertUnreadable(predict(0), "interface eth0 has 7 interrupt vectors for 2 receive queues");
    }

    @Test
    public void testUnrequestedVectorsSkipped() {
        fixture.nic("eth0", 2).irq("eth0", 100, "1").irq("eth0", 101, "9");
        for (int i = 0; i < 10; i++) {
            fixture.unrequestedVector("eth0", 200 + i);
        }
        Assert.assertEquals("eth0 50.0%/50.0%", predict(0).getSignature());
    }

    @Test
    public void testVirtioReadsTheParentFunction() {
        // virtio-net: separate receive and transmit vectors, plus a configuration vector
        fixture.virtioNic("ens4", 2)
                .irq("ens4", 30, "0")
                .irq("ens4", 31, "1")
                .irq("ens4", 32, "1")
                .irq("ens4", 33, "9")
                .irq("ens4", 34, "9");
        Assert.assertEquals("ens4 60.0%/40.0%", predict(0).getSignature());
    }

    private static void assertUnreadable(ReceiveLayout.Prediction prediction, String expected) {
        final String unreadable = prediction.getUnreadable();
        Assert.assertNotNull("expected unreadable, got " + prediction.getSignature(), unreadable);
        Assert.assertTrue(unreadable, unreadable.contains(expected));
    }

    private ReceiveLayout.Prediction predict(int bindIPv4) {
        final ReceiveLayout layout = new ReceiveLayout(fixture.getProcRoot(), fixture.getSysRoot(), _ -> null);
        return ReceiveLayout.predict(layout.read(bindIPv4), TABLE, 2);
    }
}
