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
 * Detects whether the ext4 filesystem holding the database root has the {@code fast_commit} feature
 * enabled.
 *
 * <p>The batched column flush of the adaptive save point (see {@code TableWriter.syncColumnsBatchedSync},
 * gated by {@link CairoConfiguration#isAdaptiveEpochColumnSyncBatched()}) writes each column back with
 * {@code msync(MS_ASYNC)} and {@code sync_file_range(WAIT_AFTER)}, with no per-file device flush, and relies
 * on one later flush to also journal the columns' extent conversions, as the shared journal of ext4 (jbd2)
 * and xfs does. Under ext4 {@code fast_commit} the journal is per inode, so that assumption does not hold.
 * When this check finds {@code fast_commit}, the configuration turns the batched flush off, and the save
 * point falls back to per-file {@code msync(MS_SYNC)} plus one filesystem-wide {@code syncfs}. Ordinary SYNC
 * and ASYNC commits flush per file and do not use the batched flush.
 *
 * <p>{@code fast_commit} is a superblock feature, not a mount option, so {@code /proc/mounts} does not show
 * it. The unprivileged signal for a mounted ext4 filesystem is {@code /proc/fs/ext4/<dev>/options}, where
 * {@code ext4_show_options()} lists a {@code fast_commit} token exactly when the feature is active. The
 * sibling {@code fc_info} file is not a signal: kernels create it whether or not the feature is on. To find
 * the options file, the check takes the device of the longest-prefix mount of the database root in
 * {@code /proc/mounts} and uses its basename or, for a device-mapper alias such as
 * {@code /dev/mapper/vg-lv}, the basename of the symlink target ({@code dm-0}).
 *
 * <p>The result is often {@link #UNKNOWN}: off Linux, off ext4, in a container without
 * {@code /proc/fs/ext4}, or when the device cannot be resolved. {@link #UNKNOWN} keeps the batched flush on;
 * the operator property overrides it when detection cannot tell. A false positive only costs the
 * optimization, never durability. {@link #classify} reads no files, so tests drive it with injected content.
 */
public final class FastCommitCheck {

    /**
     * ext4 fast_commit (per-inode journaling) is enabled on the DB-root mount.
     */
    public static final int FAST_COMMIT_ENABLED = 2;
    /**
     * The DB-root mount was found and fast_commit is not present (shared journal).
     */
    public static final int FAST_COMMIT_NOT_DETECTED = 1;
    /**
     * Could not determine (non-Linux, non-ext4, container, unresolvable device, IO error).
     */
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
     * The commit-mode gate of the startup advisory. {@link #logAdvisory} applies it to decide whether to
     * emit, and {@link #checkAndReport} applies it before {@link #classifyDbRoot} reads {@code /proc}, so
     * the two sites cannot drift apart.
     * <p>
     * ADAPTIVE reaches the batched flush through its save points; SYNC reaches it once, when a start makes
     * the tables that still have a save point durable before removing it ({@code RecoveryCoordinator.recover}).
     * NOSYNC and ASYNC reach it the same one-off way but stay silent: NOSYNC is {@link CommitMode#DEFAULT},
     * and a warning would fire on nearly every install, almost none of which ever enrolled a table.
     *
     * @param commitMode the configured commit mode, a {@link CommitMode} constant
     * @return {@code true} if the advisory applies to this commit mode
     */
    public static boolean advisoryAppliesTo(int commitMode) {
        return commitMode == CommitMode.SYNC || commitMode == CommitMode.ADAPTIVE;
    }

    /**
     * The whole startup step: {@link #advisoryAppliesTo}, then {@link #classifyDbRoot}, then
     * {@link #logAdvisory}. The caller keeps no commit-mode predicate of its own, so it cannot drift from the
     * advisory's gate, and modes that never report do not pay for the {@code /proc} reads.
     *
     * @param log        destination log
     * @param ff         FilesFacade used for native file IO
     * @param dbRoot     absolute path of the database root directory
     * @param commitMode the configured commit mode, a {@link CommitMode} constant
     * @return {@code true} if the advisory was logged
     */
    public static boolean checkAndReport(Log log, FilesFacade ff, CharSequence dbRoot, int commitMode) {
        if (!advisoryAppliesTo(commitMode)) {
            return false;
        }
        final int result = classifyDbRoot(ff, dbRoot);
        return logAdvisory(log, result, commitMode, dbRoot);
    }

    /**
     * Decides, from content already read, whether the mount covering {@code dbRootAbsPath} is ext4 with
     * {@code fast_commit} enabled. Reads no files.
     * <ul>
     *   <li>no matching mount, or a mount that is not ext4: {@link #UNKNOWN};</li>
     *   <li>ext4, but {@code ext4OptionsContent} is {@code null}: {@link #UNKNOWN};</li>
     *   <li>ext4, and the options hold a {@code fast_commit} token: {@link #FAST_COMMIT_ENABLED};</li>
     *   <li>ext4, and the options hold no such token: {@link #FAST_COMMIT_NOT_DETECTED}.</li>
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
            // fast_commit is an ext4 feature.
            return UNKNOWN;
        }
        if (ext4OptionsContent == null) {
            // ext4, but its options file could not be read (container, race, permissions).
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
            final String mounts = ProcFs.read(ff, PROC_MOUNTS, PROC_MOUNTS_MAX_BYTES);
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
            // A detection failure must not break startup.
            LOG.debug().$("fast_commit check failed [reason=").$(t.getMessage()).$(']').$();
            return UNKNOWN;
        }
    }

    /**
     * Emit the startup advisory when the DB root's ext4 {@code fast_commit} has disabled the batched
     * column flush. {@link #advisoryAppliesTo} decides which operators hear it.
     *
     * @param log        destination log
     * @param result     a {@link #classifyDbRoot} / {@link #classify} result
     * @param commitMode the configured commit mode, a {@link CommitMode} constant
     * @param dbRoot     absolute path of the database root directory
     * @return {@code true} if the advisory was logged
     */
    public static boolean logAdvisory(Log log, int result, int commitMode, CharSequence dbRoot) {
        if (!advisoryAppliesTo(commitMode)) {
            return false;
        }
        if (result != FAST_COMMIT_ENABLED) {
            return false;
        }
        log.advisoryW().$("WARNING: db root filesystem has ext4 fast_commit enabled")
                .$(": under per-inode journaling the batched column flush's within-page durability is NOT guaranteed")
                .$(" -- the batched flush reserved for the adaptive durable epoch has been DISABLED and that epoch falls back to per-file msync(MS_SYNC) plus one filesystem-wide syncfs")
                .$(" (slower, but durable everywhere; ordinary cairo.commit.mode=sync/async commits already flush per file and are unaffected)")
                .$(" [dbRoot=").$(dbRoot).$(']').$();
        return true;
    }


    /**
     * Derives the {@code /proc/fs/ext4} device name for a {@code /proc/mounts} device field. A block device
     * maps by basename ({@code /dev/sda1} to {@code sda1}). A device-mapper alias ({@code /dev/mapper/vg-lv})
     * is a symlink to {@code dm-N}, so when the basename has no {@code /proc/fs/ext4} entry, the symlink
     * target's basename is used. {@code probe} answers both questions, so tests need no real device tree.
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

    /**
     * True if the comma/newline/whitespace-delimited ext4 options text contains a {@code fast_commit} token.
     */
    public static boolean hasFastCommitToken(CharSequence options) {
        // /proc/fs/ext4/<dev>/options lists one token per line, while mount-option strings are
        // comma-delimited. Split on both and match the exact token "fast_commit" after stripping any
        // "=value" suffix.
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
        return ProcFs.read(ff, PROC_FS_EXT4 + devName + "/options", PROC_OPTIONS_MAX_BYTES);
    }

    /**
     * True if {@code s} holds exactly {@code ascii} in {@code [from, to)}.
     */
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
        /**
         * True if {@code /proc/fs/ext4/<devName>} exists.
         */
        boolean procEntryExists(String devName);

        /**
         * Resolve {@code devicePath} as a symlink, returning its (possibly relative-resolved) target, or {@code null}.
         */
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
