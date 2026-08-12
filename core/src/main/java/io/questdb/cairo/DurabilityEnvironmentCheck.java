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
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Detects environments where a durability-promising commit mode ({@link CommitMode#SYNC} /
 * {@link CommitMode#ADAPTIVE}) cannot actually deliver power-loss durability, so startup can say so.
 *
 * <p>Complements {@link WriteBarrierCheck} (mount mounted {@code nobarrier}) and {@link FastCommitCheck}
 * (ext4 per-inode journaling) with the cases those two miss:
 *
 * <ol>
 *   <li><b>{@link #FLUSH_NOT_A_BARRIER_FS}</b> — macOS only. {@code fcntl(F_FULLFSYNC)} is documented as
 *       implemented on "HFS, MS-DOS (FAT), Universal Disk Format (UDF) and APFS" and nowhere else, so on an
 *       SMB/NFS/FUSE/exFAT db root the call fails with ENOTSUP and the engine falls back to plain
 *       {@code fsync} — which on Darwin does NOT flush the drive's write cache. The degradation is silent.</li>
 *   <li><b>{@link #HOST_DOWNGRADES_FLUSH}</b> — Linux guest on an Apple hypervisor. The guest does
 *       everything right (ext4 emits {@code REQ_OP_FLUSH}, virtio-blk carries it), but the host implements
 *       that flush as {@code fsync()} on the disk image rather than {@code F_FULLFSYNC}: Lima/Colima's vz
 *       driver hard-codes {@code VZDiskImageSynchronizationModeFsync}, whose own framework header says it
 *       "does not ensure the data is moved from the disk's internal cache to permanent storage". Nothing the
 *       user can configure changes this today, so the message must not suggest a fix.</li>
 *   <li><b>{@link #GUEST_DISCARDS_FLUSH}</b> — a virtio block device reporting {@code write through}. The
 *       guest kernel then believes the device has no volatile cache and stops issuing flushes ENTIRELY. This
 *       is user-caused: it is the {@code echo 'write through' > /sys/block/vda/queue/write_cache} tweak
 *       circulated as a fix for slow VM disks. Unlike the other two it IS undoable, so it is the one worth
 *       shouting about.</li>
 * </ol>
 *
 * <p><b>Why the signal is the hypervisor and not the filesystem type.</b> An earlier design warned on
 * virtiofs, which would have been wrong: virtiofs on a LINUX host is perfectly durable, because a host
 * {@code fsync()} there really is a device barrier. The hazard is the host platform's fsync semantics, not
 * the transport, so the probe reads DMI to ask "is my host macOS" instead.
 *
 * <p><b>Known blind spot:</b> QEMU-on-macOS reports {@code sys_vendor=QEMU}, which does not reveal the host
 * OS, so that configuration is undetectable from inside the guest and reports {@link #OK}. Absence of a
 * warning is therefore not proof of durability.
 *
 * <p>Best-effort throughout: any probe failure yields {@link #OK} rather than a false alarm, and the pure
 * {@link #classify} is IO-free so the decision table is unit-testable without a real {@code /proc}.
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
     * Nothing detected. Also returned when detection is impossible — see the class blind-spot note.
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
     * Pure decision table. Any argument may be {@code null} / empty, meaning "could not be read", which
     * never contributes a flag.
     *
     * @param darwin        true when running natively on macOS
     * @param dbRootFsName  db root filesystem name as reported by {@code statfs} (macOS only, e.g. "apfs")
     * @param dmiSysVendor  content of {@code /sys/class/dmi/id/sys_vendor} (Linux only)
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
     * Live probe. Reads the platform interfaces and classifies. Never throws.
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
     * The Linux half of {@link #probe}, with NO platform gate, so the /sys reads and the device scan can be
     * driven on any platform through an injected {@link FilesFacade}. Keeping the {@code Os} check in the
     * caller is deliberate: a probe that gates internally can only be tested on the platform it targets,
     * which is exactly how the {@code FastCommitCheck} reader bug survived.
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
     * Emit the advisories for {@code flags}. Separated from the probe so the messages and their log LEVELS
     * are assertable without a matching platform: only {@link #GUEST_DISCARDS_FLUSH} is something an
     * operator can undo, so it alone is an error; the other two are notices about environments that cannot
     * currently be fixed.
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
        if (commitMode != CommitMode.SYNC && commitMode != CommitMode.ADAPTIVE) {
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
     * Return {@code "write through"} if ANY virtio block device reports it, else the last value read.
     * <p>
     * Deliberately scans rather than resolving the db root's own device: the tweak that causes this is
     * applied by hand per device, and every {@code vd*} device belongs to this VM, so one of them being set
     * is the signal worth reporting. Over-reporting here is acceptable — under-reporting is not, because the
     * consequence is that no flush reaches the host at all.
     */
    private static String readWorstVirtioWriteCache(FilesFacade ff) {
        String last = null;
        final Utf8StringSink nameSink = new Utf8StringSink();
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
                    path.of(SYS_BLOCK).concat(nameSink).concat("queue").concat("write_cache");
                    final String value = ProcFs.read(ff, path.toString(), SMALL_FILE_MAX_BYTES);
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
