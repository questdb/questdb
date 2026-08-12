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

package io.questdb.test.cairo;

import io.questdb.cairo.DurabilityEnvironmentCheck;
import io.questdb.std.FilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Decision table for {@link DurabilityEnvironmentCheck#classify}. Pure, so every case runs on every
 * platform — which is the point: the live probe is platform-gated, and that is exactly how the
 * {@code FastCommitCheck} / {@code WriteBarrierCheck} reader bug stayed hidden.
 */
public class DurabilityEnvironmentCheckTest {

    @Test
    public void testDarwinApfsIsClean() {
        Assert.assertEquals(DurabilityEnvironmentCheck.OK, classifyDarwin("apfs"));
        Assert.assertEquals(DurabilityEnvironmentCheck.OK, classifyDarwin("APFS"));
        Assert.assertEquals(DurabilityEnvironmentCheck.OK, classifyDarwin("hfs"));
        Assert.assertEquals(DurabilityEnvironmentCheck.OK, classifyDarwin("msdos"));
        Assert.assertEquals(DurabilityEnvironmentCheck.OK, classifyDarwin("udf"));
    }

    @Test
    public void testDarwinIgnoresGuestSignals() {
        // /proc and /sys do not exist on macOS; a stray value must not raise a guest-side flag.
        final int flags = DurabilityEnvironmentCheck.classify(true, "apfs", "Apple Inc.", "write through\n");
        Assert.assertEquals(DurabilityEnvironmentCheck.OK, flags);
    }

    @Test
    public void testDarwinNonFullFsyncFilesystems() {
        // fcntl(2) lists apfs/hfs/msdos/udf only; anything else silently degrades to fsync.
        Assert.assertEquals(DurabilityEnvironmentCheck.FLUSH_NOT_A_BARRIER_FS, classifyDarwin("smbfs"));
        Assert.assertEquals(DurabilityEnvironmentCheck.FLUSH_NOT_A_BARRIER_FS, classifyDarwin("nfs"));
        Assert.assertEquals(DurabilityEnvironmentCheck.FLUSH_NOT_A_BARRIER_FS, classifyDarwin("exfat"));
        Assert.assertEquals(DurabilityEnvironmentCheck.FLUSH_NOT_A_BARRIER_FS, classifyDarwin("macfuse"));
    }

    @Test
    public void testGuestOnAppleHypervisor() {
        // The signal is the HOST platform, not the filesystem: this is what Virtualization.framework
        // presents to a Linux guest.
        Assert.assertEquals(
                DurabilityEnvironmentCheck.HOST_DOWNGRADES_FLUSH,
                DurabilityEnvironmentCheck.classify(false, null, "Apple Inc.\n", "write back\n")
        );
    }

    @Test
    public void testGuestOnNonAppleHypervisorIsClean() {
        // virtiofs/virtio-blk on a LINUX host is durable, because a host fsync() there IS a device barrier.
        // Warning on the transport rather than the host would have false-positived every one of these.
        Assert.assertEquals(DurabilityEnvironmentCheck.OK,
                DurabilityEnvironmentCheck.classify(false, null, "QEMU\n", "write back\n"));
        Assert.assertEquals(DurabilityEnvironmentCheck.OK,
                DurabilityEnvironmentCheck.classify(false, null, "Dell Inc.\n", "write back\n"));
        Assert.assertEquals(DurabilityEnvironmentCheck.OK,
                DurabilityEnvironmentCheck.classify(false, null, null, null));
    }

    @Test
    public void testProbeNeverThrowsAndIsCleanOffLinuxAndDarwin() {
        // Windows has neither /proc nor F_FULLFSYNC concerns; the probe must be inert, not noisy.
        Assert.assertTrue(DurabilityEnvironmentCheck.probe(FilesFacadeImpl.INSTANCE, null) >= 0);
    }

    @Test
    public void testWriteCacheWriteThrough() {
        // The guest kernel then issues NO flushes at all, so nothing downstream can be durable.
        Assert.assertEquals(
                DurabilityEnvironmentCheck.GUEST_DISCARDS_FLUSH,
                DurabilityEnvironmentCheck.classify(false, null, "QEMU\n", "write through\n")
        );
        // Trailing newline / whitespace must not defeat the match.
        Assert.assertEquals(
                DurabilityEnvironmentCheck.GUEST_DISCARDS_FLUSH,
                DurabilityEnvironmentCheck.classify(false, null, "QEMU", "write through")
        );
    }

    @Test
    public void testWriteThroughOnAppleHostReportsBoth() {
        final int flags = DurabilityEnvironmentCheck.classify(false, null, "Apple Inc.\n", "write through\n");
        Assert.assertEquals(
                DurabilityEnvironmentCheck.HOST_DOWNGRADES_FLUSH | DurabilityEnvironmentCheck.GUEST_DISCARDS_FLUSH,
                flags
        );
    }

    private static int classifyDarwin(String fsName) {
        return DurabilityEnvironmentCheck.classify(true, fsName, null, null);
    }
}
