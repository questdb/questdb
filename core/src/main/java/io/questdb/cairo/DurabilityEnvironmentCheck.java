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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Detects environments where a commit mode that promises power-loss durability ({@link CommitMode#SYNC},
 * {@link CommitMode#ADAPTIVE}) cannot deliver it, so startup can say so.
 *
 * <p>It covers what {@link WriteBarrierCheck} (a {@code nobarrier} mount) misses:
 *
 * <ol>
 *   <li>{@link #FLUSH_NOT_A_BARRIER_FS}, macOS only. {@code fcntl(F_FULLFSYNC)} is documented for HFS,
 *       MS-DOS (FAT), UDF and APFS only, so on an SMB, NFS, FUSE or exFAT database root the call fails with
 *       ENOTSUP and the engine falls back to {@code fsync}, which on Darwin does not flush the drive's write
 *       cache.</li>
 *   <li>{@link #HOST_DOWNGRADES_FLUSH}: a Linux guest on an Apple hypervisor. The guest issues
 *       {@code REQ_OP_FLUSH} correctly, but the host implements it as {@code fsync()} on the disk image, not
 *       {@code F_FULLFSYNC} (Lima/Colima's vz driver uses {@code VZDiskImageSynchronizationModeFsync}, which
 *       its framework header says does not move data out of the disk's cache). The user cannot configure
 *       this away, so the message suggests no fix.</li>
 *   <li>{@link #GUEST_DISCARDS_FLUSH}: a virtio block device that reports {@code write through}, so the
 *       guest kernel issues no flushes at all. A user sets this by hand
 *       ({@code echo 'write through' > /sys/block/vda/queue/write_cache}, a tip for slow VM disks), and it
 *       can be undone, so it is logged as an error.</li>
 * </ol>
 *
 * <p>The signal is the host platform, read from DMI, not the filesystem type: virtiofs on a Linux host is
 * durable, because a host {@code fsync()} there is a device barrier.
 *
 * <p>Blind spot: QEMU on macOS reports {@code sys_vendor=QEMU}, which does not reveal the host, so that
 * configuration reports {@link #OK}. The absence of a warning does not prove durability.
 *
 * <p>Best-effort: any detection failure yields {@link #OK} rather than a false alarm, and {@link #classify}
 * reads no files, so tests drive the decision table directly.
 */
public final class DurabilityEnvironmentCheck {

    /**
     * A virtio block device reports {@code write through}: the guest issues no flushes at all.
     */
    public static final int GUEST_DISCARDS_FLUSH = 1 << 2;
    /**
     * Linux guest under an Apple hypervisor: the macOS host downgrades the guest's flush to fsync.
     */
    public static final int HOST_DOWNGRADES_FLUSH = 1 << 1;
    /**
     * Nothing detected. Also returned when detection is impossible (see the blind spot in the class javadoc).
     */
    public static final int OK = 0;
    /**
     * macOS db root on a filesystem where {@code F_FULLFSYNC} is not implemented.
     */
    public static final int FLUSH_NOT_A_BARRIER_FS = 1;

    // macOS filesystems on which fcntl(2) documents F_FULLFSYNC as implemented.
    private static final String[] DARWIN_FULLFSYNC_FS = {"apfs", "hfs", "msdos", "udf"};
    private static final String PROC_DMI_SYS_VENDOR = "/sys/class/dmi/id/sys_vendor";
    private static final int SMALL_FILE_MAX_BYTES = 4096;
    private static final String SYS_BLOCK = "/sys/block";

    private DurabilityEnvironmentCheck() {
    }

    /**
     * The commit-mode gate of the durability advisories. {@link #logAdvisories} applies it to decide whether
     * to emit, and {@link #checkAndReport} applies it before {@link #probe} runs, so the two sites cannot drift
     * apart. SYNC and ADAPTIVE promise power-loss durability, which every condition this class detects
     * contradicts; NOSYNC and ASYNC promise none.
     *
     * @param commitMode the configured commit mode, a {@link CommitMode} constant
     * @return {@code true} if the advisories apply to this commit mode
     */
    public static boolean advisoryAppliesTo(int commitMode) {
        return commitMode == CommitMode.SYNC || commitMode == CommitMode.ADAPTIVE;
    }

    /**
     * The whole startup step: {@link #advisoryAppliesTo}, then the macOS filesystem name the decision needs,
     * then {@link #probe} and {@link #logAdvisories}. The caller keeps no commit-mode predicate of its own, so
     * it cannot drift from the advisories' gate, and a mode that never reports pays for no {@code statfs}.
     *
     * @param log        destination log
     * @param ff         files facade
     * @param path       scratch path buffer, overwritten by the macOS filesystem-name resolution
     * @param dbRoot     absolute path of the database root directory
     * @param commitMode the configured commit mode, a {@link CommitMode} constant
     * @return true if anything was logged
     */
    public static boolean checkAndReport(Log log, FilesFacade ff, Path path, CharSequence dbRoot, int commitMode) {
        if (!advisoryAppliesTo(commitMode)) {
            return false;
        }
        // On macOS the decision needs the db root's filesystem name, which getFileSystemStatus writes into
        // the path buffer (the same call Bootstrap.verifyFileSystem uses).
        String fsName = null;
        if (Os.isOSX()) {
            path.of(dbRoot);
            if (Files.exists(path.$())) {
                // A zero return means statfs failed and left the db root in the buffer, which must not be
                // reported as a filesystem name.
                if (Files.getFileSystemStatus(path.$()) != 0) {
                    path.seekZ();
                    fsName = path.toString();
                }
            }
        }
        final int flags = probe(ff, fsName);
        return logAdvisories(log, flags, commitMode, dbRoot, fsName);
    }

    /**
     * Pure decision table. Any argument may be {@code null} / empty, meaning "could not be read", which
     * never contributes a flag.
     *
     * @param darwin           true when running natively on macOS
     * @param dbRootFsName     db root filesystem name as reported by {@code statfs} (macOS only, e.g. "apfs")
     * @param dmiSysVendor     content of {@code /sys/class/dmi/id/sys_vendor} (Linux only)
     * @param virtioWriteCache content of a virtio device's {@code queue/write_cache} (Linux only)
     * @return zero or more of the flags in this class, OR-ed
     */
    public static int classify(
            boolean darwin,
            CharSequence dbRootFsName,
            CharSequence dmiSysVendor,
            CharSequence virtioWriteCache
    ) {
        int flags = OK;
        if (darwin) {
            if (dbRootFsName != null && dbRootFsName.length() > 0 && !isDarwinFullFsyncFs(dbRootFsName)) {
                flags |= FLUSH_NOT_A_BARRIER_FS;
            }
            // The remaining two signals are guest-side and read from /proc + /sys, which do not exist here.
            return flags;
        }
        // "Apple Inc." is what Virtualization.framework presents to a Linux guest (product_name is
        // "Apple Virtualization Generic Platform"); it is the only vendor string that implies a macOS host.
        if (dmiSysVendor != null && Chars.contains(dmiSysVendor, "Apple")) {
            flags |= HOST_DOWNGRADES_FLUSH;
        }
        if (virtioWriteCache != null && Chars.startsWith(trim(virtioWriteCache), "write through")) {
            flags |= GUEST_DISCARDS_FLUSH;
        }
        return flags;
    }

    /**
     * Live check. Reads the platform interfaces and classifies. Never throws.
     *
     * @param ff           files facade
     * @param dbRootFsName db root filesystem name already resolved by the caller (macOS), else {@code null}
     * @return the classification flags
     */
    public static int probe(FilesFacade ff, CharSequence dbRootFsName) {
        try {
            if (Os.isOSX()) {
                return classify(true, dbRootFsName, null, null);
            }
            if (!Os.isLinux()) {
                return OK;
            }
            return probeGuest(ff);
        } catch (Throwable t) {
            // A durability advisory must never be the thing that stops the database booting.
            return OK;
        }
    }

    /**
     * The Linux half of {@link #probe}, with no platform gate, so tests can drive the {@code /sys} reads and
     * the device scan on any platform through an injected {@link FilesFacade}. The {@code Os} check stays in
     * the caller.
     */
    public static int probeGuest(FilesFacade ff) {
        return classify(
                false,
                null,
                ProcFs.read(ff, PROC_DMI_SYS_VENDOR, SMALL_FILE_MAX_BYTES),
                readWorstVirtioWriteCache(ff)
        );
    }

    /**
     * Emits the advisories for {@code flags}. It is separate from {@link #probe} so tests can assert the messages
     * and their log levels on any platform. Only {@link #GUEST_DISCARDS_FLUSH} can be undone by an operator,
     * so it alone is an error; the other two are notices about environments that cannot be fixed.
     *
     * @return true if anything was logged
     */
    public static boolean logAdvisories(
            Log log,
            int flags,
            int commitMode,
            CharSequence dbRoot,
            CharSequence fsName
    ) {
        // NOSYNC/ASYNC make no power-loss promise, so there is nothing to contradict.
        if (!advisoryAppliesTo(commitMode)) {
            return false;
        }
        final String mode = CommitMode.toString(commitMode);
        boolean logged = false;
        if ((flags & GUEST_DISCARDS_FLUSH) != 0) {
            log.errorW().$("WARNING: a virtio block device reports write_cache=write through")
                    .$(": the guest kernel treats the device as having NO volatile cache and issues NO flushes")
                    .$(" -- commit mode ").$(mode).$(" cannot make anything durable;")
                    .$(" undo with: echo 'write back' > /sys/block/<dev>/queue/write_cache")
                    .$(" [dbRoot=").$(dbRoot).$(']').$();
            logged = true;
        }
        if ((flags & HOST_DOWNGRADES_FLUSH) != 0) {
            log.advisoryW().$("NOTE: running under Apple Virtualization, so the macOS host implements this")
                    .$(" guest's device flush as fsync() rather than F_FULLFSYNC")
                    .$(" -- commit mode ").$(mode).$(" survives process failure but NOT host power loss.")
                    .$(" No guest-side or container-side setting changes this")
                    .$(" [dbRoot=").$(dbRoot).$(']').$();
            logged = true;
        }
        if ((flags & FLUSH_NOT_A_BARRIER_FS) != 0) {
            log.advisoryW().$("NOTE: db root filesystem does not implement F_FULLFSYNC")
                    .$(" (macOS implements it on apfs/hfs/msdos/udf only)")
                    .$(" -- flushes degrade to fsync(), which does not flush the drive cache, so commit mode ")
                    .$(mode).$(" does NOT survive power loss; relocate the db root to an APFS volume")
                    .$(" [dbRoot=").$(dbRoot).$(", fs=").$(fsName).$(']').$();
            logged = true;
        }
        return logged;
    }

    private static boolean isDarwinFullFsyncFs(CharSequence fsName) {
        for (int i = 0; i < DARWIN_FULLFSYNC_FS.length; i++) {
            if (Chars.equalsIgnoreCase(fsName, DARWIN_FULLFSYNC_FS[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code "write through"} if any virtio block device reports it, else the last value read.
     * <p>
     * It scans every {@code vd*} device rather than resolving the db root's own: the setting is applied by
     * hand per device, and every such device belongs to this VM. Over-reporting is acceptable;
     * under-reporting is not, because then no flush reaches the host.
     */
    private static String readWorstVirtioWriteCache(FilesFacade ff) {
        String last = null;
        final Utf8StringSink nameSink = new Utf8StringSink();
        // Build the sysfs path with StringSink, not Path.concat: concat inserts Files.SEPARATOR, which is '\'
        // on Windows, while these literal sysfs paths always use '/', whatever platform a test runs them on.
        final StringSink pathSink = new StringSink();
        try (Path path = new Path()) {
            path.of(SYS_BLOCK);
            final long findPtr = ff.findFirst(path.$());
            if (findPtr <= 0) {
                return null;
            }
            try {
                do {
                    final long namePtr = ff.findName(findPtr);
                    if (!Files.notDots(namePtr)) {
                        continue;
                    }
                    nameSink.clear();
                    Utf8s.utf8ZCopy(namePtr, nameSink);
                    if (!Utf8s.startsWithAscii(nameSink, "vd")) {
                        continue;
                    }
                    pathSink.clear();
                    pathSink.put(SYS_BLOCK).put('/').put(nameSink).put("/queue/write_cache");
                    final String value = ProcFs.read(ff, pathSink.toString(), SMALL_FILE_MAX_BYTES);
                    if (value != null) {
                        last = value;
                        if (Chars.startsWith(trim(value), "write through")) {
                            return value;
                        }
                    }
                } while (ff.findNext(findPtr) > 0);
            } finally {
                ff.findClose(findPtr);
            }
        }
        return last;
    }

    private static CharSequence trim(CharSequence cs) {
        int hi = cs.length();
        while (hi > 0 && (cs.charAt(hi - 1) == '\n' || cs.charAt(hi - 1) == ' ' || cs.charAt(hi - 1) == '\r')) {
            hi--;
        }
        return cs.subSequence(0, hi);
    }
}
