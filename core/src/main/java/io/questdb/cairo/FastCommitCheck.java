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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Detects whether the ext4 filesystem backing the QuestDB database root has the
 * {@code fast_commit} feature enabled.
 *
 * <p>Why this matters: the Linux SYNC-mode <em>batched</em> column-flush optimization
 * (see {@code TableWriter.syncColumns}) makes a within-page commit durable with only
 * {@code msync(MS_ASYNC)} + {@code sync_file_range(WAIT_AFTER)} per column (the column
 * bytes reach the device cache) and a single {@code fdatasync} of the {@code _cv} file as
 * the one device-cache flush. That is durable ONLY when the {@code _cv} fsync also
 * <em>journals</em> the columns' extent conversions, which is the case under the default
 * shared journal of ext4-jbd2 (and xfs). Under ext4 {@code fast_commit} the journal is
 * <em>per-inode</em>: a fsync of {@code _cv} journals only {@code _cv}'s inode, so the
 * within-page column bytes are never journaled and revert on power loss. This dependency
 * is proven by {@code BatchedFlushSharedJournalDependencyTest}. When fast_commit is
 * detected we therefore DISABLE the batched path and fall back to the proven per-file
 * {@code msync(MS_SYNC)} baseline (slower, but durable everywhere).
 *
 * <h3>The detectable signal (and its reliability)</h3>
 * {@code fast_commit} is an ext4 <em>superblock</em> incompat feature, NOT a
 * {@code /proc/mounts} mount option, so the {@link WriteBarrierCheck} approach of parsing
 * mount options does not directly find it (and {@code dumpe2fs}/raw superblock reads need
 * root). The reliable <em>unprivileged</em> signal for a MOUNTED ext4 filesystem is the
 * world-readable {@code /proc/fs/ext4/<dev>/options} pseudo-file: the kernel's
 * {@code ext4_show_options()} emits a literal {@code fast_commit} token there exactly when
 * the feature is active on that mount. (Note: the sibling {@code /proc/fs/ext4/<dev>/fc_info}
 * file is NOT a reliable signal — modern kernels register it unconditionally, present even
 * when fast_commit is OFF.)
 *
 * <p>Mapping DB-root to that pseudo-file:
 * <ol>
 *   <li>find the device of the longest-prefix mount of the DB root in {@code /proc/mounts}
 *       (same parsing as {@link WriteBarrierCheck}, but returning the <em>device</em> field);</li>
 *   <li>derive the kernel device name that ext4 registers under: the basename of the device,
 *       except that LVM/device-mapper aliases ({@code /dev/mapper/vg-lv}) are symlinks to
 *       {@code ../dm-N}, so if {@code /proc/fs/ext4/<basename>} is absent we dereference the
 *       device symlink and use the basename of its target (e.g. {@code dm-0});</li>
 *   <li>read {@code /proc/fs/ext4/<dev>/options} and look for the {@code fast_commit} token.</li>
 * </ol>
 *
 * <p><b>Reliability caveat:</b> this is best-effort and will frequently be {@link #UNKNOWN}
 * — on non-Linux, on non-ext4 (xfs/zfs/tmpfs/overlay), inside containers without
 * {@code /proc/fs/ext4} visibility, or when the device name cannot be resolved to a proc
 * entry. UNKNOWN is treated as "not detected": the batched optimization stays ON (the
 * operator override property is the reliable safety valve when detection cannot tell). The
 * fallback this drives is the EXISTING proven per-file {@code msync(MS_SYNC)} path, so an
 * over-eager positive only loses the optimization (safe), never durability.
 *
 * <p>This class is Linux-only. On all other platforms {@link #classifyDbRoot} returns
 * {@link #UNKNOWN} and never throws. The core parsing ({@link #classify}) is pure / IO-free
 * so it is exercised in unit tests with injected content.
 */
public final class FastCommitCheck {

    /** ext4 fast_commit (per-inode journaling) is enabled on the DB-root mount. */
    public static final int FAST_COMMIT_ENABLED = 2;
    /** The DB-root mount was found and fast_commit is NOT present (shared journal / safe). */
    public static final int FAST_COMMIT_NOT_DETECTED = 1;
    /** Could not determine (non-Linux, non-ext4, container, unresolvable device, IO error). */
    public static final int UNKNOWN = 0;

    private static final Log LOG = LogFactory.getLog(FastCommitCheck.class);
    private static final String PROC_FS_EXT4 = "/proc/fs/ext4/";
    private static final String PROC_MOUNTS = "/proc/mounts";
    // /proc/mounts can be large in containers with many bind-mounts; 256 KiB is generous.
    private static final int PROC_MOUNTS_MAX_BYTES = 256 * 1024;
    // The ext4 per-device "options" pseudo-file is small; cap generously.
    private static final int PROC_OPTIONS_MAX_BYTES = 64 * 1024;

    private FastCommitCheck() {
    }

    /**
     * Pure classifier. Decides, from already-read content, whether the mount covering
     * {@code dbRootAbsPath} is an ext4 filesystem with {@code fast_commit} enabled.
     *
     * <p>This takes the two pieces of content the live reader would have fetched, so it is
     * fully unit-testable with no real {@code /proc} access:
     * <ul>
     *   <li>{@code procMountsContent} — the text of {@code /proc/mounts}, used only to find the
     *       longest-prefix mount of {@code dbRootAbsPath} and confirm it is {@code ext4};</li>
     *   <li>{@code ext4OptionsContent} — the text of that device's
     *       {@code /proc/fs/ext4/<dev>/options} pseudo-file (or {@code null} if it could not be
     *       read / does not exist). The presence of the {@code fast_commit} token decides
     *       ENABLED vs NOT_DETECTED.</li>
     * </ul>
     *
     * <p>Decision table:
     * <ul>
     *   <li>no matching mount, or matching mount is not ext4 → {@link #UNKNOWN}
     *       (the within-page batched-flush concern is ext4-fast_commit-specific);</li>
     *   <li>ext4 mount but {@code ext4OptionsContent == null} (proc entry unreadable) →
     *       {@link #UNKNOWN};</li>
     *   <li>ext4 mount and options contain {@code fast_commit} → {@link #FAST_COMMIT_ENABLED};</li>
     *   <li>ext4 mount and options present without the token → {@link #FAST_COMMIT_NOT_DETECTED}.</li>
     * </ul>
     *
     * @param procMountsContent  full text of {@code /proc/mounts}
     * @param dbRootAbsPath      absolute path of the database root directory
     * @param ext4OptionsContent text of the matched device's ext4 {@code options} file, or {@code null}
     * @return one of {@link #FAST_COMMIT_ENABLED}, {@link #FAST_COMMIT_NOT_DETECTED}, {@link #UNKNOWN}
     */
    public static int classify(CharSequence procMountsContent, CharSequence dbRootAbsPath, CharSequence ext4OptionsContent) {
        if (procMountsContent == null || dbRootAbsPath == null) {
            return UNKNOWN;
        }
        final MountInfo mount = findLongestPrefixMount(procMountsContent, dbRootAbsPath);
        if (mount == null || mount.fsType == null) {
            return UNKNOWN;
        }
        if (!mount.fsType.equals("ext4")) {
            // fast_commit / per-inode journaling is an ext4-specific concern; for any other
            // filesystem the shared-journal assumption is either guaranteed (xfs) or N/A.
            return UNKNOWN;
        }
        if (ext4OptionsContent == null) {
            // ext4, but we couldn't read its options pseudo-file (container, race, perms).
            return UNKNOWN;
        }
        return hasFastCommitToken(ext4OptionsContent) ? FAST_COMMIT_ENABLED : FAST_COMMIT_NOT_DETECTED;
    }

    /**
     * Live check: read the real {@code /proc} interfaces and classify the mount covering
     * {@code dbRoot}.
     *
     * <p>No-op (returns {@link #UNKNOWN}) on non-Linux. Any IO / parsing failure is swallowed
     * and {@link #UNKNOWN} returned, so this call can never throw or break startup.
     *
     * @param ff     FilesFacade used for native file IO
     * @param dbRoot absolute path of the database root directory
     * @return one of {@link #FAST_COMMIT_ENABLED}, {@link #FAST_COMMIT_NOT_DETECTED}, {@link #UNKNOWN}
     */
    public static int classifyDbRoot(FilesFacade ff, CharSequence dbRoot) {
        if (!Os.isLinux() || dbRoot == null) {
            return UNKNOWN;
        }
        try {
            final String mounts = readSmallFile(ff, PROC_MOUNTS, PROC_MOUNTS_MAX_BYTES);
            if (mounts == null) {
                return UNKNOWN;
            }
            final MountInfo mount = findLongestPrefixMount(mounts, dbRoot);
            if (mount == null || mount.fsType == null || !mount.fsType.equals("ext4") || mount.device == null) {
                return UNKNOWN;
            }
            // Resolve the device path to the kernel name ext4 registers under, then read options.
            final String options = readExt4Options(ff, mount.device);
            return classify(mounts, dbRoot, options);
        } catch (Throwable t) {
            // Never break startup due to fast_commit detection failure.
            LOG.debug().$("fast_commit check failed [reason=").$(t.getMessage()).$(']').$();
            return UNKNOWN;
        }
    }

    // -----------------------------------------------------------------------
    // Package-private helpers (used by tests for white-box assertions)
    // -----------------------------------------------------------------------

    /**
     * Derive the ext4 proc/sysfs device basename for a {@code /proc/mounts} device field.
     * Plain block devices map by basename ({@code /dev/sda1} → {@code sda1}). LVM/device-mapper
     * aliases ({@code /dev/mapper/vg-lv}) are symlinks to {@code dm-N}, so when the direct
     * basename has no {@code /proc/fs/ext4} entry we fall back to the symlink target's basename.
     *
     * <p>Pure: the "does this proc entry exist?" check and the "resolve this symlink" step are
     * injected via {@code probe}, so the mapping is unit-testable without a real device tree.
     *
     * @param device the {@code /proc/mounts} device field (e.g. {@code /dev/sda1}, {@code /dev/mapper/vg-lv})
     * @param probe  resolves device names to proc-entry existence / symlink targets
     * @return the device basename whose {@code /proc/fs/ext4/<name>} entry exists, or {@code null}
     */
    public static String resolveExt4DeviceName(String device, DeviceProbe probe) {
        if (device == null || device.isEmpty()) {
            return null;
        }
        final String base = basename(device);
        if (base.isEmpty()) {
            return null;
        }
        if (probe.procEntryExists(base)) {
            return base;
        }
        // Device-mapper / LVM alias: dereference the symlink (e.g. /dev/mapper/vg-lv -> ../dm-0).
        final String target = probe.resolveSymlink(device);
        if (target != null) {
            final String targetBase = basename(target);
            if (!targetBase.isEmpty() && probe.procEntryExists(targetBase)) {
                return targetBase;
            }
        }
        return null;
    }

    /** True if the comma/newline/whitespace-delimited ext4 options text contains a {@code fast_commit} token. */
    public static boolean hasFastCommitToken(CharSequence options) {
        // The /proc/fs/ext4/<dev>/options file lists one option token per line; mount-option
        // strings elsewhere are comma-delimited. Accept either: split on newline AND comma and
        // match the exact token "fast_commit" (so "fast_commit_xyz" would not false-match, though
        // no such token exists). Tokens may carry a "=value" suffix which we strip before matching.
        final int len = options.length();
        int start = 0;
        while (start <= len) {
            int end = start;
            while (end < len && !isTokenDelimiter(options.charAt(end))) {
                end++;
            }
            if (end > start) {
                // strip an optional "=value" suffix
                int eq = start;
                while (eq < end && options.charAt(eq) != '=') {
                    eq++;
                }
                if (regionEqualsAscii(options, start, eq, "fast_commit")) {
                    return true;
                }
            }
            start = end + 1;
        }
        return false;
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private static String basename(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1);
    }

    private static String decodeOctalEscapes(String s) {
        if (s.indexOf('\\') == -1) {
            return s; // Fast path: no escapes at all.
        }
        StringBuilder sb = new StringBuilder(s.length());
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\\' && i + 3 < s.length()) {
                char d1 = s.charAt(i + 1);
                char d2 = s.charAt(i + 2);
                char d3 = s.charAt(i + 3);
                if (isOctalDigit(d1) && isOctalDigit(d2) && isOctalDigit(d3)) {
                    int val = (d1 - '0') * 64 + (d2 - '0') * 8 + (d3 - '0');
                    sb.append((char) val);
                    i += 4;
                    continue;
                }
            }
            sb.append(c);
            i++;
        }
        return sb.toString();
    }

    /**
     * Parse {@code /proc/mounts} and return the device + fstype of the longest path-component
     * prefix mount of {@code dbRootAbsPath}. Mirrors {@link WriteBarrierCheck#classify}'s
     * mount-selection (same longest-prefix rule, same {@code \040} octal decode), but exposes the
     * device + fstype fields instead of the options field.
     */
    private static MountInfo findLongestPrefixMount(CharSequence procMountsContent, CharSequence dbRootAbsPath) {
        String dbRoot = dbRootAbsPath.toString();
        if (dbRoot.length() > 1 && dbRoot.charAt(dbRoot.length() - 1) == '/') {
            dbRoot = dbRoot.substring(0, dbRoot.length() - 1);
        }

        int bestMatchLen = -1;
        String bestDevice = null;
        String bestFsType = null;

        final int len = procMountsContent.length();
        int lineStart = 0;
        while (lineStart < len) {
            int lineEnd = lineStart;
            while (lineEnd < len && procMountsContent.charAt(lineEnd) != '\n') {
                lineEnd++;
            }
            final String[] fields = splitLine(procMountsContent, lineStart, lineEnd);
            // Fields: 0 device, 1 mountpoint, 2 fstype, 3 options
            if (fields != null) {
                String mountpoint = decodeOctalEscapes(fields[1]);
                if (mountpoint.length() > 1 && mountpoint.charAt(mountpoint.length() - 1) == '/') {
                    mountpoint = mountpoint.substring(0, mountpoint.length() - 1);
                }
                if (isPathPrefix(mountpoint, dbRoot)) {
                    final int matchLen = mountpoint.length();
                    if (matchLen > bestMatchLen) {
                        bestMatchLen = matchLen;
                        bestDevice = decodeOctalEscapes(fields[0]);
                        bestFsType = fields[2];
                    }
                }
            }
            lineStart = lineEnd + 1;
        }

        if (bestMatchLen < 0) {
            return null;
        }
        return new MountInfo(bestDevice, bestFsType);
    }

    private static boolean isOctalDigit(char c) {
        return c >= '0' && c <= '7';
    }

    // Reuses WriteBarrierCheck's path-component-prefix semantics (mountpoint == dbRoot, or dbRoot
    // starts with mountpoint + "/", or mountpoint == "/").
    private static boolean isPathPrefix(String mountpoint, String dbRoot) {
        if (mountpoint.equals(dbRoot)) {
            return true;
        }
        if (dbRoot.startsWith(mountpoint + "/")) {
            return true;
        }
        return mountpoint.equals("/");
    }

    private static boolean isTokenDelimiter(char c) {
        return c == ',' || c == '\n' || c == '\r' || c == ' ' || c == '\t';
    }

    private static boolean isWhitespace(char c) {
        return c == ' ' || c == '\t';
    }

    /**
     * Read the ext4 {@code options} pseudo-file for {@code device}, resolving the device path to
     * the kernel name ext4 registers under (handling device-mapper symlinks). Returns {@code null}
     * if the device cannot be resolved or the file cannot be read.
     */
    private static String readExt4Options(FilesFacade ff, String device) {
        final LiveDeviceProbe probe = new LiveDeviceProbe(ff);
        final String devName = resolveExt4DeviceName(device, probe);
        if (devName == null) {
            return null;
        }
        return readSmallFile(ff, PROC_FS_EXT4 + devName + "/options", PROC_OPTIONS_MAX_BYTES);
    }

    /**
     * Read the entire content of a small text file into a Java {@link String} via native memory.
     * Returns {@code null} if the file cannot be opened, has unknown length, or exceeds {@code maxBytes}.
     * (Mirrors the equivalent reader in {@link WriteBarrierCheck}.)
     */
    private static String readSmallFile(FilesFacade ff, String path, int maxBytes) {
        long fd = -1;
        long mem = 0;
        long allocSize = 0;
        try (Path p = new Path()) {
            p.of(path);
            fd = ff.openRONoCache(p.$());
            if (fd < 0) {
                return null;
            }
            final long size = ff.length(fd);
            if (size < 0 || size > maxBytes) {
                return null;
            }
            allocSize = size + 1;
            mem = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
            final long bytesRead = ff.read(fd, mem, size, 0);
            if (bytesRead != size) {
                return null;
            }
            Unsafe.getUnsafe().putByte(mem + size, (byte) 0);
            final Utf8StringSink sink = new Utf8StringSink();
            Utf8s.strCpy(mem, mem + size, sink);
            return sink.toString();
        } finally {
            if (fd >= 0) {
                ff.close(fd);
            }
            if (mem != 0) {
                Unsafe.free(mem, allocSize, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static boolean regionEqualsAscii(CharSequence s, int from, int to, String ascii) {
        if (to - from != ascii.length()) {
            return false;
        }
        for (int i = 0; i < ascii.length(); i++) {
            if (s.charAt(from + i) != ascii.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Split a single {@code /proc/mounts} line ({@code [lineStart, lineEnd)}) by whitespace into
     * at most 4 fields. Returns {@code null} for blank / comment lines or lines with fewer than 4
     * fields. (Mirrors {@link WriteBarrierCheck}.)
     */
    private static String[] splitLine(CharSequence text, int lineStart, int lineEnd) {
        int pos = lineStart;
        while (pos < lineEnd && isWhitespace(text.charAt(pos))) {
            pos++;
        }
        if (pos >= lineEnd || text.charAt(pos) == '#') {
            return null;
        }
        final String[] fields = new String[4];
        int fieldIdx = 0;
        while (pos < lineEnd && fieldIdx < 4) {
            final int start = pos;
            while (pos < lineEnd && !isWhitespace(text.charAt(pos))) {
                pos++;
            }
            fields[fieldIdx++] = text.subSequence(start, pos).toString();
            while (pos < lineEnd && isWhitespace(text.charAt(pos))) {
                pos++;
            }
        }
        if (fieldIdx < 4) {
            return null;
        }
        return fields;
    }

    /**
     * Abstraction over the device tree used by {@link #resolveExt4DeviceName} so the basename /
     * symlink-deref mapping is unit-testable without a real {@code /dev} or {@code /proc/fs/ext4}.
     */
    public interface DeviceProbe {
        /** True if {@code /proc/fs/ext4/<devName>} exists. */
        boolean procEntryExists(String devName);

        /** Resolve {@code devicePath} as a symlink, returning its (possibly relative-resolved) target, or {@code null}. */
        String resolveSymlink(String devicePath);
    }

    private static final class LiveDeviceProbe implements DeviceProbe {
        private final FilesFacade ff;

        private LiveDeviceProbe(FilesFacade ff) {
            this.ff = ff;
        }

        @Override
        public boolean procEntryExists(String devName) {
            try (Path p = new Path()) {
                p.of(PROC_FS_EXT4).concat(devName);
                return ff.exists(p.$());
            }
        }

        @Override
        public String resolveSymlink(String devicePath) {
            try (Path src = new Path(); Path target = new Path()) {
                src.of(devicePath);
                if (!ff.readLink(src, target)) {
                    return null;
                }
                return Utf8s.toString(target);
            } catch (Throwable t) {
                return null;
            }
        }
    }

    private static final class MountInfo {
        private final String device;
        private final String fsType;

        private MountInfo(String device, String fsType) {
            this.device = device;
            this.fsType = fsType;
        }
    }
}
