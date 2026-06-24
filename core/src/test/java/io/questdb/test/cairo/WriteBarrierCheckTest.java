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

package io.questdb.test.cairo;

import io.questdb.cairo.WriteBarrierCheck;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link WriteBarrierCheck#classify(CharSequence, CharSequence)}.
 *
 * All cases use injected /proc/mounts content so they are fully deterministic,
 * require no real filesystem access, and run on all platforms.
 */
public class WriteBarrierCheckTest {

    // -----------------------------------------------------------------------
    // Basic barrier-state detection
    // -----------------------------------------------------------------------

    @Test
    public void testExt4Nobarrier() {
        // ext4 mounted with nobarrier → BARRIERS_DISABLED
        String mounts =
                "sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0\n" +
                "/dev/sda1 / ext4 rw,relatime,errors=remount-ro 0 0\n" +
                "/dev/sdb1 /data ext4 rw,relatime,nobarrier,data=ordered 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_DISABLED,
                WriteBarrierCheck.classify(mounts, "/data/qdb"));
    }

    @Test
    public void testExt4BarrierEquals0() {
        // ext4 mounted with barrier=0 (older kernel syntax) → BARRIERS_DISABLED
        String mounts =
                "/dev/sda1 / ext4 rw,relatime,errors=remount-ro 0 0\n" +
                "/dev/sdb1 /data ext4 rw,relatime,barrier=0 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_DISABLED,
                WriteBarrierCheck.classify(mounts, "/data/qdb"));
    }

    @Test
    public void testExt4DefaultBarriers() {
        // ext4 with default options (no barrier token) → BARRIERS_PRESUMED_ENABLED
        String mounts =
                "/dev/sda1 / ext4 rw,relatime,errors=remount-ro 0 0\n" +
                "/dev/sdb1 /data ext4 rw,relatime,data=ordered 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_PRESUMED_ENABLED,
                WriteBarrierCheck.classify(mounts, "/data/qdb"));
    }

    @Test
    public void testXfsDefault() {
        // xfs default (barriers on, not advertised in options) → BARRIERS_PRESUMED_ENABLED
        String mounts =
                "/dev/sda1 / xfs rw,relatime,attr2,inode64,noquota 0 0\n" +
                "/dev/nvme0n1p1 /data xfs rw,relatime,attr2,inode64,noquota 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_PRESUMED_ENABLED,
                WriteBarrierCheck.classify(mounts, "/data/qdb"));
    }

    // -----------------------------------------------------------------------
    // Longest-prefix mount selection
    // -----------------------------------------------------------------------

    @Test
    public void testLongestPrefixWins_specificMountSafe() {
        // dbRoot=/data/qdb, root mount has nobarrier, /data mount is default (safe).
        // Longest prefix = /data → PRESUMED_ENABLED (the specific, safe mount wins).
        String mounts =
                "/dev/sda1 / ext4 rw,relatime,nobarrier 0 0\n" +
                "/dev/sdb1 /data ext4 rw,relatime,data=ordered 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_PRESUMED_ENABLED,
                WriteBarrierCheck.classify(mounts, "/data/qdb"));
    }

    @Test
    public void testLongestPrefixWins_specificMountNobarrier() {
        // dbRoot=/data/qdb, root mount is safe, /data mount has nobarrier.
        // Longest prefix = /data → BARRIERS_DISABLED.
        String mounts =
                "/dev/sda1 / ext4 rw,relatime,data=ordered 0 0\n" +
                "/dev/sdb1 /data ext4 rw,relatime,nobarrier 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_DISABLED,
                WriteBarrierCheck.classify(mounts, "/data/qdb"));
    }

    @Test
    public void testLongestPrefixWins_dbRootExactMatch() {
        // Mount exactly equal to dbRoot.
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                "/dev/sdb1 /data/qdb ext4 rw,relatime,nobarrier 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_DISABLED,
                WriteBarrierCheck.classify(mounts, "/data/qdb"));
    }

    // -----------------------------------------------------------------------
    // Path-boundary correctness
    // -----------------------------------------------------------------------

    @Test
    public void testPathBoundaryNoFalsePrefix() {
        // dbRoot=/database must NOT match /data mount — they share a common prefix string
        // but /data is not a path-component prefix of /database.
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                "/dev/sdb1 /data ext4 rw,relatime,nobarrier 0 0\n";
        // /database is not under /data, so we should fall back to / which has no nobarrier.
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_PRESUMED_ENABLED,
                WriteBarrierCheck.classify(mounts, "/database"));
    }

    @Test
    public void testRootMountCoversEverything() {
        // Only a root mount with nobarrier, dbRoot=/some/deep/path.
        String mounts =
                "/dev/sda1 / ext4 rw,relatime,nobarrier 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_DISABLED,
                WriteBarrierCheck.classify(mounts, "/some/deep/path"));
    }

    // -----------------------------------------------------------------------
    // No matching mount → UNKNOWN
    // -----------------------------------------------------------------------

    @Test
    public void testNoMatchingMount() {
        // Mounts do not include dbRoot's path at all, and there is no / mount.
        // This is theoretically impossible on a real Linux box but is a useful
        // defensive test for the classifier.
        String mounts =
                "/dev/sdb1 /other ext4 rw,relatime 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.UNKNOWN,
                WriteBarrierCheck.classify(mounts, "/data/qdb"));
    }

    @Test
    public void testEmptyMounts() {
        Assert.assertEquals(WriteBarrierCheck.UNKNOWN,
                WriteBarrierCheck.classify("", "/data/qdb"));
    }

    @Test
    public void testNullMounts() {
        Assert.assertEquals(WriteBarrierCheck.UNKNOWN,
                WriteBarrierCheck.classify(null, "/data/qdb"));
    }

    @Test
    public void testNullDbRoot() {
        Assert.assertEquals(WriteBarrierCheck.UNKNOWN,
                WriteBarrierCheck.classify("sysfs /sys sysfs rw 0 0\n", null));
    }

    // -----------------------------------------------------------------------
    // Comment / blank lines are ignored
    // -----------------------------------------------------------------------

    @Test
    public void testCommentLinesIgnored() {
        String mounts =
                "# This is a comment\n" +
                "\n" +
                "/dev/sda1 / ext4 rw,relatime,nobarrier 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_DISABLED,
                WriteBarrierCheck.classify(mounts, "/var/qdb"));
    }

    // -----------------------------------------------------------------------
    // Smoke test: classifyDbRoot on real /proc/mounts (Linux only)
    // Verifies the method does not throw regardless of what it finds.
    // -----------------------------------------------------------------------

    @Test
    public void testClassifyDbRootSmokeNoException() {
        // Should never throw. On non-Linux it returns UNKNOWN.
        int result = WriteBarrierCheck.classifyDbRoot(FilesFacadeImpl.INSTANCE, "/tmp");
        Assert.assertTrue(
                "result must be UNKNOWN, PRESUMED_ENABLED, or BARRIERS_DISABLED",
                result == WriteBarrierCheck.UNKNOWN
                        || result == WriteBarrierCheck.BARRIERS_PRESUMED_ENABLED
                        || result == WriteBarrierCheck.BARRIERS_DISABLED
        );
    }

    @Test
    public void testClassifyDbRootReturnsUnknownOnNonLinux() {
        if (!Os.isLinux()) {
            Assert.assertEquals(WriteBarrierCheck.UNKNOWN,
                    WriteBarrierCheck.classifyDbRoot(FilesFacadeImpl.INSTANCE, "/tmp"));
        }
        // On Linux: just verify no exception is thrown (covered by the smoke test above).
    }

    // -----------------------------------------------------------------------
    // Octal-escape decoding in mount points
    // -----------------------------------------------------------------------

    @Test
    public void testOctalEscapeInMountpoint() {
        // Mountpoint with a space encoded as \040 — /data\040qdb = "/data qdb"
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                "/dev/sdb1 /data\\040qdb ext4 rw,relatime,nobarrier 0 0\n";
        Assert.assertEquals(WriteBarrierCheck.BARRIERS_DISABLED,
                WriteBarrierCheck.classify(mounts, "/data qdb"));
    }
}
