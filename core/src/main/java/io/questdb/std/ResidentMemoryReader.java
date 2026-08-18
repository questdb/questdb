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

package io.questdb.std;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Reads this process's actual resident memory (what the kernel OOM killer and
 * container orchestrators enforce against), as opposed to {@link Unsafe#getRssMemUsed()}
 * which is QuestDB's own <i>accounted</i> native-allocation counter and misses
 * JVM heap and JVM-internal native memory (metaspace, code cache, thread
 * stacks, GC) entirely.
 * <p>
 * Prefers cgroup v2's {@code memory.current} for this process's own cgroup —
 * the same figure a cgroup memory limit (e.g. a container's {@code --memory})
 * is compared against — falling back to {@link Os#getRss()} (backed by
 * {@code /proc/self/statm} field 2 on Linux, Mach task info on macOS) when no
 * cgroup v2 hierarchy applies to this process.
 * <p>
 * This does I/O (a file read) and must therefore never be called from a hot
 * path such as {@code Unsafe.checkAllocLimit}. It is meant to be polled
 * periodically (see {@code MemoryUsageLogJob}) with the result cached in a
 * volatile field via {@link Unsafe#updateResidentSample(long)}.
 * <p>
 * Every entry point degrades to {@link #UNKNOWN_RESIDENT_BYTES} rather than
 * throwing: this runs on every platform QuestDB supports, including ones
 * with no {@code /proc} or cgroup filesystem at all (e.g. Windows).
 */
public final class ResidentMemoryReader {
    /**
     * Sentinel returned when residency could not be determined by any source.
     */
    public static final long UNKNOWN_RESIDENT_BYTES = -1L;
    private static final String DEFAULT_CGROUP_FS_ROOT = "/sys/fs/cgroup";
    private static final String DEFAULT_CGROUP_SELF_PATH = "/proc/self/cgroup";

    private ResidentMemoryReader() {
    }

    /**
     * Reads current process resident memory in bytes, using the default,
     * real filesystem paths. Never throws.
     */
    public static long readResidentBytes() {
        final long cgroupBytes = readCgroupV2MemoryCurrent(DEFAULT_CGROUP_SELF_PATH, DEFAULT_CGROUP_FS_ROOT);
        if (cgroupBytes >= 0) {
            return cgroupBytes;
        }
        return readOsRssFallback();
    }

    /**
     * Reads {@code memory.current} from the cgroup v2 hierarchy for this
     * process: resolves the process's cgroup path from {@code cgroupSelfPath}
     * (normally {@code /proc/self/cgroup}) and looks for
     * {@code <cgroupFsRoot>/<cgroup-path>/memory.current}.
     * <p>
     * Public and parameterised (rather than hardcoded to the real filesystem)
     * so tests can point it at fixture files instead of {@code /proc} and
     * {@code /sys/fs/cgroup}; production code should normally call the
     * no-arg {@link #readResidentBytes()}. Never throws — any I/O failure,
     * missing file, or unparsable content degrades to
     * {@link #UNKNOWN_RESIDENT_BYTES}.
     */
    public static long readCgroupV2MemoryCurrent(String cgroupSelfPath, String cgroupFsRoot) {
        try {
            final String cgroupRelativePath = findCgroupV2Path(cgroupSelfPath);
            if (cgroupRelativePath == null) {
                return UNKNOWN_RESIDENT_BYTES;
            }
            // cgroupRelativePath is the raw path from /proc/self/cgroup, e.g.
            // "/user.slice/foo.scope" - already rooted with a leading '/'.
            final Path memCurrent = Paths.get(cgroupFsRoot + cgroupRelativePath, "memory.current");
            if (!Files.isReadable(memCurrent)) {
                return UNKNOWN_RESIDENT_BYTES;
            }
            final String content = new String(Files.readAllBytes(memCurrent), StandardCharsets.US_ASCII).trim();
            return Long.parseLong(content);
        } catch (Throwable t) {
            return UNKNOWN_RESIDENT_BYTES;
        }
    }

    /**
     * Falls back to {@link Os#getRss()} (process RSS, not cgroup-aware).
     * Public for the same testability reason as {@link #readCgroupV2MemoryCurrent}.
     * Never throws; a non-positive result (unsupported platform, read
     * failure) is treated as unknown.
     */
    public static long readOsRssFallback() {
        try {
            final long rss = Os.getRss();
            return rss > 0 ? rss : UNKNOWN_RESIDENT_BYTES;
        } catch (Throwable t) {
            return UNKNOWN_RESIDENT_BYTES;
        }
    }

    /**
     * Parses a cgroup v2 unified-hierarchy line out of {@code /proc/self/cgroup}
     * content. Such a file has one line per hierarchy, each of the form
     * {@code hierarchy-id:controller-list:cgroup-path}; the cgroup v2 unified
     * hierarchy line is distinguished by an empty controller-list field, e.g.
     * {@code 0::/user.slice/foo.scope}. cgroup v1 lines (non-empty controller
     * list, e.g. {@code 5:memory:/foo}) are skipped.
     *
     * @return the cgroup-relative path (starting with '/'), or {@code null} if
     * the file is unreadable or has no v2 unified-hierarchy line.
     */
    private static String findCgroupV2Path(String cgroupSelfPath) {
        final Path path = Paths.get(cgroupSelfPath);
        if (!Files.isReadable(path)) {
            return null;
        }
        try {
            for (String line : Files.readAllLines(path, StandardCharsets.US_ASCII)) {
                final int firstColon = line.indexOf(':');
                if (firstColon < 0) {
                    continue;
                }
                final int secondColon = line.indexOf(':', firstColon + 1);
                if (secondColon < 0) {
                    continue;
                }
                // Empty controller-list field between the two colons marks the
                // cgroup v2 unified-hierarchy line.
                if (secondColon == firstColon + 1) {
                    return line.substring(secondColon + 1);
                }
            }
        } catch (IOException e) {
            return null;
        }
        return null;
    }
}
