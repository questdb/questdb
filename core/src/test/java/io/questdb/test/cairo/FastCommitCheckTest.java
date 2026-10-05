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

import io.questdb.cairo.CommitMode;
import io.questdb.cairo.FastCommitCheck;
import io.questdb.cairo.ProcFs;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link FastCommitCheck}.
 *
 * <p>The {@link FastCommitCheck#classify(CharSequence, CharSequence, CharSequence)} core is pure: all
 * cases inject the /proc/mounts text (to find + type the DB-root mount) plus the device's ext4
 * {@code options} pseudo-file content (to detect the {@code fast_commit} token), so they are fully
 * deterministic and require no real filesystem. The device-name mapping ({@link FastCommitCheck#resolveExt4DeviceName})
 * is exercised with a fake {@link FastCommitCheck.DeviceProbe}.
 */
public class FastCommitCheckTest {

    // A realistic ext4 options-file body WITHOUT the fast_commit token (shared journal / safe).
    private static final String OPTIONS_NO_FC =
            "rw\nbsddf\nnogrpid\nblock_validity\ndelalloc\njournal_checksum\nbarrier\nuser_xattr\nacl\n" +
                    "noquota\nerrors=remount-ro\ncommit=5\ndata=ordered\ninode_readahead_blks=32\n";
    // The same body WITH the fast_commit token the kernel emits when the feature is active.
    private static final String OPTIONS_WITH_FC = OPTIONS_NO_FC + "fast_commit\n";

    // -----------------------------------------------------------------------
    // classify(): ENABLED / NOT_DETECTED / UNKNOWN on injected signals
    // -----------------------------------------------------------------------

    @Test
    public void testExt4FastCommitEnabled() {
        String mounts =
                "sysfs /sys sysfs rw,nosuid 0 0\n" +
                        "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                        "/dev/sdb1 /data ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.FAST_COMMIT_ENABLED,
                FastCommitCheck.classify(mounts, "/data/qdb", OPTIONS_WITH_FC));
    }

    @Test
    public void testExt4FastCommitNotPresent() {
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                        "/dev/sdb1 /data ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.FAST_COMMIT_NOT_DETECTED,
                FastCommitCheck.classify(mounts, "/data/qdb", OPTIONS_NO_FC));
    }

    @Test
    public void testExt4ButOptionsUnreadableIsUnknown() {
        // ext4 mount found, but the device's options pseudo-file could not be read (null) -> UNKNOWN.
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                        "/dev/sdb1 /data ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.UNKNOWN,
                FastCommitCheck.classify(mounts, "/data/qdb", null));
    }

    @Test
    public void testGarbageOptionsContentIsNotDetected() {
        // Non-empty but garbage options content (no fast_commit token) -> NOT_DETECTED, never a false ENABLE.
        String mounts = "/dev/sdb1 /data ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.FAST_COMMIT_NOT_DETECTED,
                FastCommitCheck.classify(mounts, "/data/qdb", "\u0000\u0001 random ;;; not-an-option \n\n"));
    }

    // -----------------------------------------------------------------------
    // classify(): non-ext4 filesystems are out of scope -> UNKNOWN
    // -----------------------------------------------------------------------

    @Test
    public void testXfsIsUnknownEvenWithFcLikeOptions() {
        // xfs uses a shared journal; fast_commit is ext4-specific. Even if the (irrelevant) options blob
        // contained the token, an xfs mount must classify UNKNOWN (the optimization is safe on xfs).
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                        "/dev/nvme0n1p1 /data xfs rw,relatime,attr2,inode64 0 0\n";
        Assert.assertEquals(FastCommitCheck.UNKNOWN,
                FastCommitCheck.classify(mounts, "/data/qdb", OPTIONS_WITH_FC));
    }

    @Test
    public void testTmpfsIsUnknown() {
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                        "tmpfs /data tmpfs rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.UNKNOWN,
                FastCommitCheck.classify(mounts, "/data/qdb", OPTIONS_WITH_FC));
    }

    // -----------------------------------------------------------------------
    // Longest-prefix mount selection (mirrors WriteBarrierCheckTest cases)
    // -----------------------------------------------------------------------

    @Test
    public void testLongestPrefixWins_specificMountEnabled() {
        // dbRoot=/data/qdb: root mount ext4 (no fc), /data mount ext4 WITH fc. The longer /data wins.
        // Both ext4 here; the injected options correspond to the WINNING (/data) device.
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                        "/dev/sdb1 /data ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.FAST_COMMIT_ENABLED,
                FastCommitCheck.classify(mounts, "/data/qdb", OPTIONS_WITH_FC));
    }

    @Test
    public void testLongestPrefixWins_dbRootExactMatch() {
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                        "/dev/sdb1 /data/qdb ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.FAST_COMMIT_ENABLED,
                FastCommitCheck.classify(mounts, "/data/qdb", OPTIONS_WITH_FC));
    }

    @Test
    public void testPathBoundaryNoFalsePrefix() {
        // dbRoot=/database must NOT match the /data mount; it falls back to the root ext4 mount.
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                        "/dev/sdb1 /data ext4 rw,relatime 0 0\n";
        // The matched mount is "/" (ext4); options here describe "/" and have NO fast_commit -> NOT_DETECTED.
        Assert.assertEquals(FastCommitCheck.FAST_COMMIT_NOT_DETECTED,
                FastCommitCheck.classify(mounts, "/database", OPTIONS_NO_FC));
    }

    @Test
    public void testRootMountCoversEverything() {
        String mounts = "/dev/sda1 / ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.FAST_COMMIT_ENABLED,
                FastCommitCheck.classify(mounts, "/some/deep/path", OPTIONS_WITH_FC));
    }

    @Test
    public void testOctalEscapeInMountpoint() {
        String mounts =
                "/dev/sda1 / ext4 rw,relatime 0 0\n" +
                        "/dev/sdb1 /data\\040qdb ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.FAST_COMMIT_ENABLED,
                FastCommitCheck.classify(mounts, "/data qdb", OPTIONS_WITH_FC));
    }

    // -----------------------------------------------------------------------
    // No matching mount / null inputs -> UNKNOWN
    // -----------------------------------------------------------------------

    @Test
    public void testNoMatchingMount() {
        String mounts = "/dev/sdb1 /other ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.UNKNOWN,
                FastCommitCheck.classify(mounts, "/data/qdb", OPTIONS_WITH_FC));
    }

    @Test
    public void testEmptyMounts() {
        Assert.assertEquals(FastCommitCheck.UNKNOWN,
                FastCommitCheck.classify("", "/data/qdb", OPTIONS_WITH_FC));
    }

    @Test
    public void testNullMounts() {
        Assert.assertEquals(FastCommitCheck.UNKNOWN,
                FastCommitCheck.classify(null, "/data/qdb", OPTIONS_WITH_FC));
    }

    @Test
    public void testNullDbRoot() {
        Assert.assertEquals(FastCommitCheck.UNKNOWN,
                FastCommitCheck.classify("/dev/sda1 / ext4 rw 0 0\n", null, OPTIONS_WITH_FC));
    }

    @Test
    public void testCommentAndBlankLinesIgnored() {
        String mounts =
                "# a comment\n" +
                        "\n" +
                        "/dev/sda1 / ext4 rw,relatime 0 0\n";
        Assert.assertEquals(FastCommitCheck.FAST_COMMIT_ENABLED,
                FastCommitCheck.classify(mounts, "/var/qdb", OPTIONS_WITH_FC));
    }

    // -----------------------------------------------------------------------
    // hasFastCommitToken: exact-token matching, no false positives
    // -----------------------------------------------------------------------

    @Test
    public void testHasFastCommitTokenForms() {
        Assert.assertTrue(FastCommitCheck.hasFastCommitToken("fast_commit"));
        Assert.assertTrue(FastCommitCheck.hasFastCommitToken("rw\nbarrier\nfast_commit\ndata=ordered"));
        Assert.assertTrue(FastCommitCheck.hasFastCommitToken("rw,barrier,fast_commit,data=ordered"));
        // tolerate a "=value" suffix, just in case a kernel renders it that way
        Assert.assertTrue(FastCommitCheck.hasFastCommitToken("fast_commit=1"));

        Assert.assertFalse(FastCommitCheck.hasFastCommitToken("rw\nbarrier\ndata=ordered"));
        Assert.assertFalse(FastCommitCheck.hasFastCommitToken(""));
        // must not partial-match a different token that merely contains the string
        Assert.assertFalse(FastCommitCheck.hasFastCommitToken("no_fast_commit_debug"));
        Assert.assertFalse(FastCommitCheck.hasFastCommitToken("fast_commitment"));
    }

    // -----------------------------------------------------------------------
    // resolveExt4DeviceName: plain device + device-mapper (LVM) symlink deref
    // -----------------------------------------------------------------------

    @Test
    public void testResolvePlainDevice() {
        // /dev/sda1 -> proc entry "sda1" exists directly, no symlink deref needed.
        FakeDeviceProbe probe = new FakeDeviceProbe();
        probe.procEntries.add("sda1");
        Assert.assertEquals("sda1", FastCommitCheck.resolveExt4DeviceName("/dev/sda1", probe));
    }

    @Test
    public void testResolveDeviceMapperSymlink() {
        // /dev/mapper/vg-root has no "vg-root" proc entry; it is a symlink to ../dm-0, whose basename
        // "dm-0" DOES have a proc entry. This is the LVM/device-mapper case observed on real hosts.
        FakeDeviceProbe probe = new FakeDeviceProbe();
        probe.procEntries.add("dm-0");
        probe.symlinks.put("/dev/mapper/vg-root", "/dev/dm-0");
        Assert.assertEquals("dm-0", FastCommitCheck.resolveExt4DeviceName("/dev/mapper/vg-root", probe));
    }

    @Test
    public void testResolveUnresolvableDeviceIsNull() {
        // No direct proc entry and no symlink -> null (caller maps this to UNKNOWN).
        FakeDeviceProbe probe = new FakeDeviceProbe();
        Assert.assertNull(FastCommitCheck.resolveExt4DeviceName("/dev/nope9", probe));
    }

    @Test
    public void testResolveNullOrEmptyDeviceIsNull() {
        FakeDeviceProbe probe = new FakeDeviceProbe();
        Assert.assertNull(FastCommitCheck.resolveExt4DeviceName(null, probe));
        Assert.assertNull(FastCommitCheck.resolveExt4DeviceName("", probe));
    }

    // -----------------------------------------------------------------------
    // Live classifyDbRoot smoke: must never throw; result is one of the tri-state values.
    // -----------------------------------------------------------------------

    @Test
    public void testClassifyDbRootSmokeNoException() {
        int result = FastCommitCheck.classifyDbRoot(FilesFacadeImpl.INSTANCE, "/tmp");
        Assert.assertTrue(
                "result must be UNKNOWN, NOT_DETECTED, or ENABLED",
                result == FastCommitCheck.UNKNOWN
                        || result == FastCommitCheck.FAST_COMMIT_NOT_DETECTED
                        || result == FastCommitCheck.FAST_COMMIT_ENABLED);
    }

    @Test
    public void testClassifyDbRootReturnsUnknownOnNonLinux() {
        if (!Os.isLinux()) {
            Assert.assertEquals(FastCommitCheck.UNKNOWN,
                    FastCommitCheck.classifyDbRoot(FilesFacadeImpl.INSTANCE, "/tmp"));
        }
        // On Linux: covered by the smoke test (no exception, valid tri-state).
    }

    @Test
    public void testClassifyDbRootNullIsUnknown() {
        Assert.assertEquals(FastCommitCheck.UNKNOWN,
                FastCommitCheck.classifyDbRoot(FilesFacadeImpl.INSTANCE, null));
    }

    private static final class FakeDeviceProbe implements FastCommitCheck.DeviceProbe {
        final Set<String> procEntries = new HashSet<>();
        final Map<String, String> symlinks = new HashMap<>();

        @Override
        public boolean procEntryExists(String devName) {
            return procEntries.contains(devName);
        }

        @Override
        public String resolveSymlink(String devicePath) {
            return symlinks.get(devicePath);
        }
    }

    /**
     * Pseudo-files under /proc and /sys do not report a usable size, so the reader must size its read from
     * its own cap rather than from {@code ff.length(fd)}. Measured on Linux 6.8: {@code /proc/mounts} and
     * {@code /proc/fs/ext4/<dev>/options} both fstat as 0 while holding 2007 and 305 readable bytes
     * respectively; sysfs files fstat as 4096 while holding a handful.
     * <p>
     * Sizing from fstat therefore read ZERO bytes and produced an empty string, which
     * {@code findLongestPrefixMount} reads as "no mounts" -- so {@code classifyDbRoot} returned UNKNOWN,
     * the caller treated fast_commit as absent, and batched syncfs stayed ENABLED on exactly the
     * configuration this class exists to rule out. It survived because every existing test here drives the
     * pure {@code classify}, and {@code classifyDbRoot} early-returns off Linux, so nothing exercised the
     * reader on any platform.
     */
    @Test
    public void testReadSmallFileIgnoresPseudoFileStatSize() {
        final String content = "/dev/vda1 / ext4 rw,relatime 0 0\n";
        final ProcFsFacade ff = new ProcFsFacade(content);
        final String read = ProcFs.read(ff, "/proc/mounts", 256 * 1024);
        Assert.assertEquals("a pseudo-file reporting fstat size 0 must still be read in full", content, read);
    }

    @Test
    public void testReadSmallFileHandlesSysfsPageSizedStat() {
        // sysfs reports a page size regardless of content length; the short read must not be rejected.
        final ProcFsFacade ff = new ProcFsFacade("Apple Inc.\n");
        ff.statSize = 4096;
        Assert.assertEquals("Apple Inc.\n", ProcFs.read(ff, "/sys/class/dmi/id/sys_vendor", 4096));
    }

    // -----------------------------------------------------------------------
    // logAdvisory(): the commit-mode gate and the message. A correct classify()
    // is worthless if the advisory reaches the wrong operators.
    // -----------------------------------------------------------------------

    @Test
    public void testAdvisoryAppliesToIsTheGateBothCallSitesTake() {
        // checkAndReport takes this predicate for its cheap early-out and logAdvisory takes it for its own
        // gate, so narrowing it narrows both; Bootstrap now holds no copy to narrow independently. The
        // duplicated SYNC-only gate is what shipped the defect this suite exists to pin.
        Assert.assertTrue(
                "adaptive runs the durable-epoch cadence that drives the batched flush",
                FastCommitCheck.advisoryAppliesTo(CommitMode.ADAPTIVE)
        );
        Assert.assertTrue(
                "sync reaches the batched flush through the adaptive-exit reconciliation",
                FastCommitCheck.advisoryAppliesTo(CommitMode.SYNC)
        );
        // NOSYNC and ASYNC reach the batched flush through that same exit reconciliation, but run no
        // durable-epoch cadence; and NOSYNC is CommitMode.DEFAULT, so warning it would fire on every
        // default install. Silence here is a deliberate signal-to-noise choice, not unreachability.
        Assert.assertFalse(
                "nosync is CommitMode.DEFAULT; the advisory is deliberately silent for it",
                FastCommitCheck.advisoryAppliesTo(CommitMode.NOSYNC)
        );
        Assert.assertFalse(
                "async is deliberately not warned",
                FastCommitCheck.advisoryAppliesTo(CommitMode.ASYNC)
        );
        // Not configurable modes: UNSET is the _meta enrolment sentinel, UNKNOWN a rejected token.
        Assert.assertFalse(FastCommitCheck.advisoryAppliesTo(CommitMode.UNSET));
        Assert.assertFalse(FastCommitCheck.advisoryAppliesTo(CommitMode.UNKNOWN));
    }

    @Test
    public void testAdvisoryLoggedForAdaptive() {
        // ADAPTIVE is the mode that actually runs the batched flush: syncColumnsBatchedSync() is reached
        // only from TableWriter.fsyncMaterializedState, whose cadence call sites are all gated on
        // getEffectiveCommitMode() == ADAPTIVE. So an adaptive operator is the one who loses the
        // optimization when fast_commit disables it, and the one the advisory exists for.
        final DurabilityEnvironmentCheckTest.RecordingLog log = new DurabilityEnvironmentCheckTest.RecordingLog();
        Assert.assertTrue(
                "adaptive is the mode that loses the batched flush and must be told",
                FastCommitCheck.logAdvisory(log, FastCommitCheck.FAST_COMMIT_ENABLED, CommitMode.ADAPTIVE, "/db")
        );
        Assert.assertEquals("expected advisory, not error", "[advisoryW]", log.levels.toString());
    }

    @Test
    public void testAdvisoryLoggedForSyncBecauseOfTheAdaptiveExitPath() {
        // SYNC must stay in the gate: a table left enrolled ADAPTIVE and reopened under
        // cairo.commit.mode=sync reconciles out of adaptive and runs one batched flush
        // (see FastCommitCheck.advisoryAppliesTo). Narrowing this gate to ADAPTIVE would
        // silently drop that operator's signal.
        final DurabilityEnvironmentCheckTest.RecordingLog log = new DurabilityEnvironmentCheckTest.RecordingLog();
        Assert.assertTrue(
                "the adaptive-exit reconciliation reaches the batched flush under sync",
                FastCommitCheck.logAdvisory(log, FastCommitCheck.FAST_COMMIT_ENABLED, CommitMode.SYNC, "/db")
        );
        Assert.assertEquals("expected advisory, not error", "[advisoryW]", log.levels.toString());
    }

    @Test
    public void testAdvisoryNamesTheAdaptiveEpochAndTheDbRoot() {
        final DurabilityEnvironmentCheckTest.RecordingLog log = new DurabilityEnvironmentCheckTest.RecordingLog();
        Assert.assertTrue(FastCommitCheck.logAdvisory(
                log, FastCommitCheck.FAST_COMMIT_ENABLED, CommitMode.ADAPTIVE, "/var/lib/questdb/db"));
        final String text = log.text();
        assertContains(text, "fast_commit");
        // What was disabled belongs to the ADAPTIVE DURABLE EPOCH, not to ordinary sync commits. A
        // rewording that drops this reintroduces the misattribution the old SYNC-only gate encoded.
        assertContains(text, "adaptive durable epoch");
        assertContains(text, "DISABLED");
        // Which root is affected is the one thing the operator cannot infer.
        assertContains(text, "[dbRoot=/var/lib/questdb/db]");
    }

    @Test
    public void testAdvisoryNotLoggedForModesWithNoEpochCadence() {
        // These modes can still touch the batched flush once per table, through the same adaptive-exit
        // reconciliation that keeps SYNC in the gate. They are excluded because they run no durable-epoch
        // cadence and NOSYNC is CommitMode.DEFAULT, so the advisory would be noise on every default install.
        for (int mode : new int[]{CommitMode.NOSYNC, CommitMode.ASYNC}) {
            final DurabilityEnvironmentCheckTest.RecordingLog log = new DurabilityEnvironmentCheckTest.RecordingLog();
            Assert.assertFalse(
                    "commit mode " + CommitMode.toString(mode) + " runs no durable-epoch cadence and is deliberately not warned",
                    FastCommitCheck.logAdvisory(log, FastCommitCheck.FAST_COMMIT_ENABLED, mode, "/db")
            );
            Assert.assertEquals("", log.text());
        }
    }

    @Test
    public void testAdvisoryNotLoggedWhenFastCommitIsNotEnabled() {
        for (int mode : new int[]{CommitMode.SYNC, CommitMode.ADAPTIVE}) {
            for (int result : new int[]{FastCommitCheck.UNKNOWN, FastCommitCheck.FAST_COMMIT_NOT_DETECTED}) {
                final DurabilityEnvironmentCheckTest.RecordingLog log = new DurabilityEnvironmentCheckTest.RecordingLog();
                Assert.assertFalse(
                        "nothing was disabled, so there is nothing to warn about",
                        FastCommitCheck.logAdvisory(log, result, mode, "/db")
                );
                Assert.assertEquals("", log.text());
            }
        }
    }

    @Test
    public void testCheckAndReportDoesNotReadProcForModesThatNeverReport() {
        // The early-out is what keeps /proc out of a NOSYNC or ASYNC start, so it is asserted by COUNTING
        // opens rather than by reading the log: a gate applied after classifyDbRoot would still log nothing
        // and would still look correct from the log alone, while having paid for the reads.
        for (int mode : new int[]{CommitMode.NOSYNC, CommitMode.ASYNC}) {
            final ProcFsFacade ff = fastCommitProcFs();
            final DurabilityEnvironmentCheckTest.RecordingLog log = new DurabilityEnvironmentCheckTest.RecordingLog();
            Assert.assertFalse(
                    "commit mode " + CommitMode.toString(mode) + " is outside the advisory's gate",
                    FastCommitCheck.checkAndReport(log, ff, "/data/qdb", mode)
            );
            Assert.assertEquals(
                    "the gate must run BEFORE classifyDbRoot touches /proc",
                    0, ff.openCount
            );
            Assert.assertEquals("", log.text());
        }
    }

    @Test
    public void testCheckAndReportReadsProcAndLogsForReportingModes() {
        // classifyDbRoot early-returns UNKNOWN off Linux, so only there can an injected facade observe the
        // /proc reads at all. The skip-/proc half above needs no such gate and runs everywhere.
        Assume.assumeTrue("classifyDbRoot reads /proc on Linux only", Os.isLinux());
        for (int mode : new int[]{CommitMode.SYNC, CommitMode.ADAPTIVE}) {
            final ProcFsFacade ff = fastCommitProcFs();
            final DurabilityEnvironmentCheckTest.RecordingLog log = new DurabilityEnvironmentCheckTest.RecordingLog();
            Assert.assertTrue(
                    "commit mode " + CommitMode.toString(mode) + " must be told the batched flush was disabled",
                    FastCommitCheck.checkAndReport(log, ff, "/data/qdb", mode)
            );
            Assert.assertTrue("classifyDbRoot must have read /proc", ff.openCount > 0);
            Assert.assertEquals("expected advisory, not error", "[advisoryW]", log.levels.toString());
            assertContains(log.text(), "fast_commit");
        }
    }

    private static void assertContains(String haystack, String needle) {
        if (!haystack.contains(needle)) {
            Assert.fail("expected to find '" + needle + "' in: " + haystack);
        }
    }

    /**
     * A fake {@code /proc} describing an ext4 db root mount whose device HAS fast_commit, so a reporting
     * commit mode reaches the advisory and a non-reporting one has something it could have read.
     */
    private static ProcFsFacade fastCommitProcFs() {
        final ProcFsFacade ff = new ProcFsFacade();
        ff.files.put("/proc/mounts", "/dev/sda1 / ext4 rw,relatime 0 0\n/dev/sdb1 /data ext4 rw,relatime 0 0\n");
        ff.procEntries.add("/proc/fs/ext4/sdb1");
        ff.files.put("/proc/fs/ext4/sdb1/options", OPTIONS_WITH_FC);
        return ff;
    }

    /**
     * Minimal FilesFacade that reproduces pseudo-file semantics: {@code length()} lies, {@code read()} tells
     * the truth. It serves either one content for every path (the single-argument constructor) or a fake
     * {@code /proc} tree ({@link #files} plus {@link #procEntries}), and counts opens so a test can assert
     * that a commit mode which never reports never read {@code /proc} in the first place.
     */
    private static final class ProcFsFacade extends FilesFacadeImpl {
        final Map<String, String> files = new HashMap<>();
        final Set<String> procEntries = new HashSet<>();
        int openCount;
        long statSize;
        private final byte[] defaultBytes;
        private byte[] pending;

        ProcFsFacade() {
            this(null);
        }

        ProcFsFacade(String content) {
            this.defaultBytes = content == null ? null : content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            this.statSize = 0; // procfs
        }

        @Override
        public boolean close(long fd) {
            pending = null;
            return true;
        }

        @Override
        public boolean exists(io.questdb.std.str.LPSZ path) {
            return procEntries.contains(pathOf(path));
        }

        @Override
        public long length(long fd) {
            return statSize;
        }

        @Override
        public long openRONoCache(io.questdb.std.str.LPSZ name) {
            openCount++;
            // NOT name.toString(): an LPSZ renders as its identity hash, so string comparison silently
            // never matches and the whole fake goes quiet.
            final String content = files.get(pathOf(name));
            pending = content != null ? content.getBytes(java.nio.charset.StandardCharsets.UTF_8) : defaultBytes;
            return pending == null ? -1 : 4242; // any non-negative sentinel; this facade never touches a real fd
        }

        @Override
        public long read(long fd, long address, long len, long offset) {
            if (pending == null || offset >= pending.length) {
                return 0;
            }
            final long n = Math.min(len, pending.length - offset);
            for (long i = 0; i < n; i++) {
                io.questdb.std.Unsafe.getUnsafe().putByte(address + i, pending[(int) (offset + i)]);
            }
            return n;
        }

        private static String pathOf(io.questdb.std.str.LPSZ name) {
            final int n = name.size();
            final StringBuilder sb = new StringBuilder(n);
            for (int i = 0; i < n; i++) {
                sb.append((char) (name.byteAt(i) & 0xFF));
            }
            return sb.toString();
        }
    }
}
