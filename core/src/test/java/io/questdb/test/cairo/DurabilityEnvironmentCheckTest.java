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

import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurabilityEnvironmentCheck;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.StringSink;
import io.questdb.test.lifecycle.fakes.CapturingLog;
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

    // ---------------------------------------------------------------------
    // The advisories themselves. classify() being right is worthless if the
    // message never fires, fires at the wrong level, or fires for a mode that
    // made no durability promise.
    // ---------------------------------------------------------------------

    @Test
    public void testAdvisoryNotLoggedForModesThatPromiseNothing() {
        for (int mode : new int[]{CommitMode.NOSYNC, CommitMode.ASYNC}) {
            final RecordingLog log = new RecordingLog();
            Assert.assertFalse(
                    "commit mode " + CommitMode.toString(mode) + " promises no durability",
                    DurabilityEnvironmentCheck.logAdvisories(
                            log,
                            DurabilityEnvironmentCheck.GUEST_DISCARDS_FLUSH | DurabilityEnvironmentCheck.HOST_DOWNGRADES_FLUSH,
                            mode, "/db", null)
            );
            Assert.assertEquals("", log.text());
        }
    }

    @Test
    public void testGuestDiscardsFlushIsLoggedAtErrorWithTheUndoCommand() {
        final RecordingLog log = new RecordingLog();
        Assert.assertTrue(DurabilityEnvironmentCheck.logAdvisories(
                log, DurabilityEnvironmentCheck.GUEST_DISCARDS_FLUSH, CommitMode.ADAPTIVE, "/db", null));
        // The one an operator can actually undo, so it must be an ERROR and must carry the undo command.
        Assert.assertEquals("expected error level, got " + log.levels, "[errorW]", log.levels.toString());
        assertContains(log.text(), "write_cache=write through");
        assertContains(log.text(), "echo 'write back'");
        assertContains(log.text(), "adaptive");
    }

    @Test
    public void testHostDowngradesFlushIsAdvisoryAndOffersNoFix() {
        final RecordingLog log = new RecordingLog();
        Assert.assertTrue(DurabilityEnvironmentCheck.logAdvisories(
                log, DurabilityEnvironmentCheck.HOST_DOWNGRADES_FLUSH, CommitMode.ADAPTIVE, "/db", null));
        assertContains(log.text(), "Apple Virtualization");
        assertContains(log.text(), "NOT host power loss");
        // Must not imply a setting exists -- none does.
        assertContains(log.text(), "No guest-side or container-side setting changes this");
        // Nothing the operator can fix must not be shouted about.
        Assert.assertEquals("expected advisory, not error", "[advisoryW]", log.levels.toString());
    }

    @Test
    public void testNotABarrierFsAdvisoryNamesTheFilesystem() {
        final RecordingLog log = new RecordingLog();
        Assert.assertTrue(DurabilityEnvironmentCheck.logAdvisories(
                log, DurabilityEnvironmentCheck.FLUSH_NOT_A_BARRIER_FS, CommitMode.SYNC, "/db", "smbfs"));
        assertContains(log.text(), "F_FULLFSYNC");
        assertContains(log.text(), "smbfs");
        assertContains(log.text(), "sync");
        Assert.assertEquals("expected advisory, not error", "[advisoryW]", log.levels.toString());
    }

    @Test
    public void testNoFlagsLogsNothing() {
        final RecordingLog log = new RecordingLog();
        Assert.assertFalse(DurabilityEnvironmentCheck.logAdvisories(
                log, DurabilityEnvironmentCheck.OK, CommitMode.ADAPTIVE, "/db", "apfs"));
        Assert.assertEquals("", log.text());
        Assert.assertTrue(log.levels.isEmpty());
    }

    // ---------------------------------------------------------------------
    // The live guest probe: /sys reads plus the virtio device scan. Driven
    // through an injected FilesFacade so it runs off Linux too -- the whole
    // point, since a platform-gated probe is what hid the reader bug.
    // ---------------------------------------------------------------------

    @Test
    public void testProbeGuestFindsWriteThroughOnASecondDevice() {
        // The scan must not stop at the first device: the tweak is applied by hand, often to the data disk.
        final SysFsFacade ff = new SysFsFacade("QEMU\n");
        ff.devices.put("vda", "write back\n");
        ff.devices.put("vdb", "write through\n");
        Assert.assertEquals(DurabilityEnvironmentCheck.GUEST_DISCARDS_FLUSH,
                DurabilityEnvironmentCheck.probeGuest(ff));
    }

    @Test
    public void testProbeGuestOnAppleHostWithHealthyWriteCache() {
        final SysFsFacade ff = new SysFsFacade("Apple Inc.\n");
        ff.devices.put("vda", "write back\n");
        Assert.assertEquals(DurabilityEnvironmentCheck.HOST_DOWNGRADES_FLUSH,
                DurabilityEnvironmentCheck.probeGuest(ff));
    }

    @Test
    public void testProbeGuestIgnoresNonVirtioDevices() {
        // A physical host's sda saying "write through" is a legitimate no-volatile-cache device.
        final SysFsFacade ff = new SysFsFacade("Dell Inc.\n");
        ff.devices.put("sda", "write through\n");
        Assert.assertEquals(DurabilityEnvironmentCheck.OK, DurabilityEnvironmentCheck.probeGuest(ff));
    }

    @Test
    public void testProbeGuestWithNothingReadableIsClean() {
        Assert.assertEquals(DurabilityEnvironmentCheck.OK,
                DurabilityEnvironmentCheck.probeGuest(new SysFsFacade(null)));
    }

    /**
     * Serves a fake {@code /sys} tree: the DMI vendor file, and a {@code /sys/block} listing whose entries
     * each expose {@code queue/write_cache}. Lets the guest probe -- including the device scan -- run on any
     * platform.
     */
    private static final class SysFsFacade extends FilesFacadeImpl {
        final java.util.LinkedHashMap<String, String> devices = new java.util.LinkedHashMap<>();
        private final String dmiSysVendor;
        private java.util.Iterator<String> listing;
        private String current;
        private byte[] pending;

        SysFsFacade(String dmiSysVendor) {
            this.dmiSysVendor = dmiSysVendor;
        }

        @Override
        public boolean close(long fd) {
            pending = null;
            return true;
        }

        @Override
        public long findClose(long findPtr) {
            listing = null;
            return 0;
        }

        @Override
        public long findFirst(io.questdb.std.str.LPSZ path) {
            if (!io.questdb.std.str.Utf8s.equalsAscii("/sys/block", path)) {
                return 0;
            }
            listing = new java.util.ArrayList<>(devices.keySet()).iterator();
            return advance() ? 1 : 0;
        }

        @Override
        public long findName(long findPtr) {
            // The probe copies from this pointer, so hand back a NUL-terminated native copy.
            return nativeZ(current);
        }

        @Override
        public int findNext(long findPtr) {
            return advance() ? 1 : 0;
        }

        @Override
        public int findType(long findPtr) {
            return io.questdb.std.Files.DT_DIR;
        }

        @Override
        public long openRONoCache(io.questdb.std.str.LPSZ name) {
            // NOT name.toString(): an LPSZ renders as its identity hash, so string comparison silently
            // never matches and the whole fake goes quiet.
            if (io.questdb.std.str.Utf8s.equalsAscii("/sys/class/dmi/id/sys_vendor", name)) {
                pending = bytesOrNull(dmiSysVendor);
                return pending == null ? -1 : 7;
            }
            for (java.util.Map.Entry<String, String> e : devices.entrySet()) {
                if (io.questdb.std.str.Utf8s.equalsAscii("/sys/block/" + e.getKey() + "/queue/write_cache", name)) {
                    pending = bytesOrNull(e.getValue());
                    return pending == null ? -1 : 7;
                }
            }
            return -1;
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

        private boolean advance() {
            if (listing != null && listing.hasNext()) {
                current = listing.next();
                return true;
            }
            current = null;
            return false;
        }

        private static byte[] bytesOrNull(String s) {
            return s == null ? null : s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        private static long nativeZ(String s) {
            final byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            final long mem = io.questdb.std.Unsafe.malloc(b.length + 1, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < b.length; i++) {
                io.questdb.std.Unsafe.getUnsafe().putByte(mem + i, b[i]);
            }
            io.questdb.std.Unsafe.getUnsafe().putByte(mem + b.length, (byte) 0);
            NATIVE_NAMES.add(new long[]{mem, b.length + 1});
            return mem;
        }
    }

    // Freed after each test so the memory-leak accounting stays clean.
    private static final java.util.List<long[]> NATIVE_NAMES = new java.util.ArrayList<>();

    @org.junit.After
    public void freeNativeNames() {
        for (long[] p : NATIVE_NAMES) {
            io.questdb.std.Unsafe.free(p[0], p[1], io.questdb.std.MemoryTag.NATIVE_DEFAULT);
        }
        NATIVE_NAMES.clear();
    }

    private static void assertContains(String haystack, String needle) {
        if (!haystack.contains(needle)) {
            Assert.fail("expected to find '" + needle + "' in: " + haystack);
        }
    }

    /**
     * Deterministic in-memory {@link Log} recording both the rendered text and WHICH LEVEL was used.
     * <p>
     * Not {@code LogCapture}: that intercepts the console writer and needs the async log worker running, so
     * in a plain JUnit class every assertion either times out or passes vacuously because the queue was
     * never drained. Level matters as much as text here -- only the operator-fixable condition may be an
     * error, and asserting that is the point.
     */
    private static final class RecordingLog implements Log {
        final java.util.List<String> levels = new java.util.ArrayList<>();
        private final CapturingLog.CapturingRecord record;
        private final StringSink sink = new StringSink();

        RecordingLog() {
            this.record = new CapturingLog.CapturingRecord(sink);
        }

        String text() {
            return sink.toString();
        }

        @Override
        public LogRecord advisory() {
            return at("advisory");
        }

        @Override
        public LogRecord advisoryW() {
            return at("advisoryW");
        }

        @Override
        public LogRecord critical() {
            return at("critical");
        }

        @Override
        public LogRecord debug() {
            return at("debug");
        }

        @Override
        public LogRecord debugW() {
            return at("debugW");
        }

        @Override
        public LogRecord error() {
            return at("error");
        }

        @Override
        public LogRecord errorW() {
            return at("errorW");
        }

        @Override
        public LogRecord info() {
            return at("info");
        }

        @Override
        public LogRecord infoW() {
            return at("infoW");
        }

        @Override
        public LogRecord xDebugW() {
            return at("xDebugW");
        }

        @Override
        public LogRecord xInfoW() {
            return at("xInfoW");
        }

        @Override
        public LogRecord xadvisory() {
            return at("xadvisory");
        }

        @Override
        public LogRecord xcritical() {
            return at("xcritical");
        }

        @Override
        public LogRecord xdebug() {
            return at("xdebug");
        }

        @Override
        public LogRecord xerror() {
            return at("xerror");
        }

        @Override
        public LogRecord xinfo() {
            return at("xinfo");
        }

        private LogRecord at(String level) {
            levels.add(level);
            return record;
        }
    }
}
