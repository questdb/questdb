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
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Detects whether the filesystem backing the QuestDB database root has write-barriers
 * disabled (mounted with {@code nobarrier} or {@code barrier=0}).
 *
 * <p>With {@code cairo.commit.mode=sync}, QuestDB relies on {@code fsync}/{@code fdatasync}
 * issuing a device cache flush to guarantee power-loss durability. On a filesystem mounted
 * without write barriers the kernel suppresses that flush, so committed data can silently
 * be lost on power failure even though fsync returns success.
 *
 * <p>This class is Linux-only (it reads {@code /proc/mounts}).  On all other platforms
 * {@link #classifyDbRoot} returns {@link #UNKNOWN} and no warning is emitted.
 *
 * <p>The core parsing logic ({@link #classify}) is deliberately pure / IO-free so it can
 * be exercised in unit tests with injected content, without access to the real
 * {@code /proc/mounts}.
 */
public final class WriteBarrierCheck {

    /** Write barriers are explicitly disabled (nobarrier / barrier=0). */
    public static final int BARRIERS_DISABLED = 2;

    /** Write barriers appear to be enabled (no disabling token found in mount options). */
    public static final int BARRIERS_PRESUMED_ENABLED = 1;

    /** Could not determine barrier status (non-Linux, /proc/mounts unreadable, no matching mount). */
    public static final int UNKNOWN = 0;

    private static final String PROC_MOUNTS = "/proc/mounts";
    // /proc/mounts can be large in containers with many bind-mounts; 256 KiB is generous.
    private static final int PROC_MOUNTS_MAX_BYTES = 256 * 1024;
    private static final Log LOG = LogFactory.getLog(WriteBarrierCheck.class);

    private WriteBarrierCheck() {
    }

    /**
     * Pure classifier: parse the content of {@code /proc/mounts} and decide whether the
     * mount covering {@code dbRootAbsPath} has write barriers disabled.
     *
     * <p>Format of each line (space-separated, 6 fields):
     * <pre>
     *   device  mountpoint  fstype  options  dump  pass
     * </pre>
     * The kernel uses {@code \040} (octal) to encode spaces in the mountpoint field; this
     * implementation decodes that escape before comparing path prefixes.
     *
     * <p>Matching rule: pick the mount whose mountpoint is the LONGEST path-component
     * prefix of {@code dbRootAbsPath}. A path-component prefix means the mountpoint is
     * either equal to {@code dbRootAbsPath} OR {@code dbRootAbsPath} starts with
     * {@code mountpoint + "/"}, ensuring that {@code /data} does not match {@code /database}.
     *
     * <p>Barrier-disabled tokens (comma-delimited option tokens):
     * <ul>
     *   <li>{@code nobarrier}
     *   <li>{@code barrier=0}
     * </ul>
     * ext4/xfs with barriers enabled do NOT emit {@code barrier=1}; absence of a disabling
     * token is therefore taken as barriers-presumed-enabled.
     *
     * @param procMountsContent full text of {@code /proc/mounts}
     * @param dbRootAbsPath     absolute path of the database root directory
     * @return {@link #BARRIERS_DISABLED}, {@link #BARRIERS_PRESUMED_ENABLED}, or {@link #UNKNOWN}
     */
    public static int classify(CharSequence procMountsContent, CharSequence dbRootAbsPath) {
        if (procMountsContent == null || dbRootAbsPath == null) {
            return UNKNOWN;
        }

        String dbRoot = dbRootAbsPath.toString();
        // Normalise: strip trailing slash unless it is the root "/" itself.
        if (dbRoot.length() > 1 && dbRoot.charAt(dbRoot.length() - 1) == '/') {
            dbRoot = dbRoot.substring(0, dbRoot.length() - 1);
        }

        // Track the best (longest-matching) mount we find.
        int bestMatchLen = -1;
        String bestOptions = null;

        int len = procMountsContent.length();
        int lineStart = 0;

        while (lineStart < len) {
            // Find end of this line.
            int lineEnd = lineStart;
            while (lineEnd < len && procMountsContent.charAt(lineEnd) != '\n') {
                lineEnd++;
            }

            // Parse the space-delimited fields.
            // Field 0: device, Field 1: mountpoint, Field 2: fstype, Field 3: options
            String[] fields = splitLine(procMountsContent, lineStart, lineEnd);
            if (fields != null && fields.length >= 4) {
                // Decode \040 escapes in mountpoint (the kernel uses octal escapes for spaces).
                String mountpoint = decodeOctalEscapes(fields[1]);
                // Normalise trailing slash.
                if (mountpoint.length() > 1 && mountpoint.charAt(mountpoint.length() - 1) == '/') {
                    mountpoint = mountpoint.substring(0, mountpoint.length() - 1);
                }

                // Check if mountpoint is a path-component prefix of dbRoot.
                if (isPathPrefix(mountpoint, dbRoot)) {
                    int matchLen = mountpoint.length();
                    if (matchLen > bestMatchLen) {
                        bestMatchLen = matchLen;
                        bestOptions = fields[3];
                    }
                }
            }

            lineStart = lineEnd + 1;
        }

        if (bestOptions == null) {
            return UNKNOWN;
        }

        return hasBarriersDisabled(bestOptions) ? BARRIERS_DISABLED : BARRIERS_PRESUMED_ENABLED;
    }

    /**
     * Live check: read {@code /proc/mounts} from the real filesystem (via {@link FilesFacade})
     * and classify the mount covering {@code dbRoot}.
     *
     * <p>This method is a no-op (returns {@link #UNKNOWN}) on non-Linux systems.
     * Any I/O or parsing failure is swallowed and {@link #UNKNOWN} is returned so this
     * call can never break startup.
     *
     * @param ff     FilesFacade used for native file I/O
     * @param dbRoot absolute path of the database root directory
     * @return {@link #BARRIERS_DISABLED}, {@link #BARRIERS_PRESUMED_ENABLED}, or {@link #UNKNOWN}
     */
    public static int classifyDbRoot(FilesFacade ff, CharSequence dbRoot) {
        if (!Os.isLinux()) {
            return UNKNOWN;
        }
        try {
            String content = readSmallFile(ff, PROC_MOUNTS, PROC_MOUNTS_MAX_BYTES);
            if (content == null) {
                return UNKNOWN;
            }
            return classify(content, dbRoot);
        } catch (Throwable t) {
            // Never break startup due to barrier detection failure.
            LOG.debug().$("write-barrier check failed [reason=").$(t.getMessage()).$(']').$();
            return UNKNOWN;
        }
    }

    // -----------------------------------------------------------------------
    // Package-private helpers (used by tests for white-box assertions)
    // -----------------------------------------------------------------------

    static boolean hasBarriersDisabled(String options) {
        // Options are comma-separated tokens.  We look for exact token matches to avoid
        // false positives (e.g., "nobarriers" should not match "nobarrier", though in
        // practice no such option exists).
        int start = 0;
        int len = options.length();
        while (start < len) {
            int comma = options.indexOf(',', start);
            int end = (comma == -1) ? len : comma;
            String token = options.substring(start, end);
            if (token.equals("nobarrier") || token.equals("barrier=0")) {
                return true;
            }
            start = end + 1;
        }
        return false;
    }

    static boolean isPathPrefix(String mountpoint, String dbRoot) {
        if (mountpoint.equals(dbRoot)) {
            return true;
        }
        // mountpoint must be a strict path-component prefix: dbRoot starts with mountpoint + "/"
        if (dbRoot.startsWith(mountpoint + "/")) {
            return true;
        }
        // Special case: mountpoint == "/" covers everything.
        return mountpoint.equals("/");
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Decode {@code \040}-style kernel octal escape sequences in a mount path string.
     * Only the 4-character sequence {@code \NNN} is decoded (backslash + 3 octal digits),
     * which is the only form the Linux kernel emits in {@code /proc/mounts}.
     */
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

    private static boolean isOctalDigit(char c) {
        return c >= '0' && c <= '7';
    }

    /**
     * Read the entire content of a small text file (e.g. {@code /proc/mounts}) into a
     * Java {@link String} using native memory.  Returns {@code null} if the file cannot
     * be opened or is larger than {@code maxBytes}.
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
            long size = ff.length(fd);
            if (size < 0 || size > maxBytes) {
                return null;
            }
            allocSize = size + 1;
            mem = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
            long bytesRead = ff.read(fd, mem, size, 0);
            if (bytesRead != size) {
                return null;
            }
            // Null-terminate for safety, though we use explicit length below.
            Unsafe.getUnsafe().putByte(mem + size, (byte) 0);

            // Copy native memory to a Java String via Utf8s.
            Utf8StringSink sink = new Utf8StringSink();
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

    /**
     * Split a single line (from {@code lineStart} inclusive to {@code lineEnd} exclusive)
     * by whitespace into at most 4 fields.  Returns {@code null} for blank / comment lines.
     */
    private static String[] splitLine(CharSequence text, int lineStart, int lineEnd) {
        // Skip leading whitespace.
        int pos = lineStart;
        while (pos < lineEnd && isWhitespace(text.charAt(pos))) {
            pos++;
        }
        if (pos >= lineEnd || text.charAt(pos) == '#') {
            return null; // blank or comment line
        }

        String[] fields = new String[4];
        int fieldIdx = 0;
        while (pos < lineEnd && fieldIdx < 4) {
            int start = pos;
            while (pos < lineEnd && !isWhitespace(text.charAt(pos))) {
                pos++;
            }
            fields[fieldIdx++] = text.subSequence(start, pos).toString();
            while (pos < lineEnd && isWhitespace(text.charAt(pos))) {
                pos++;
            }
        }
        if (fieldIdx < 4) {
            return null; // not enough fields
        }
        return fields;
    }

    private static boolean isWhitespace(char c) {
        return c == ' ' || c == '\t';
    }
}
