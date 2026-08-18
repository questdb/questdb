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

package io.questdb.test.std;

import io.questdb.std.ResidentMemoryReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class ResidentMemoryReaderTest {

    @Test
    public void testReadCgroupV2MemoryCurrentIgnoresNonUnifiedHierarchyLines() throws IOException {
        // A cgroup v1 line has a non-empty controller-list field between the
        // colons (e.g. "5:memory:/foo") and must be skipped rather than
        // misparsed as the v2 unified-hierarchy line.
        final Path cgroupFsRoot = Files.createTempDirectory("resident-memory-reader-test-fsroot");
        final Path cgroupDir = cgroupFsRoot.resolve("v2path");
        Files.createDirectories(cgroupDir);
        Files.write(cgroupDir.resolve("memory.current"), "42".getBytes(StandardCharsets.US_ASCII));

        final Path cgroupSelf = Files.createTempFile("resident-memory-reader-test-cgroup", ".txt");
        Files.write(cgroupSelf, "5:memory:/v1path\n0::/v2path\n".getBytes(StandardCharsets.US_ASCII));

        try {
            final long v = ResidentMemoryReader.readCgroupV2MemoryCurrent(cgroupSelf.toString(), cgroupFsRoot.toString());
            Assert.assertEquals(42L, v);
        } finally {
            deleteQuietly(cgroupSelf);
            deleteQuietly(cgroupDir.resolve("memory.current"));
            deleteQuietly(cgroupDir);
            deleteQuietly(cgroupFsRoot);
        }
    }

    @Test
    public void testReadCgroupV2MemoryCurrentParsesRealFixture() throws IOException {
        final Path cgroupFsRoot = Files.createTempDirectory("resident-memory-reader-test-fsroot");
        final Path cgroupDir = cgroupFsRoot.resolve("my.slice/my.scope");
        Files.createDirectories(cgroupDir);
        Files.write(cgroupDir.resolve("memory.current"), "123456789".getBytes(StandardCharsets.US_ASCII));

        final Path cgroupSelf = Files.createTempFile("resident-memory-reader-test-cgroup", ".txt");
        Files.write(cgroupSelf, "0::/my.slice/my.scope\n".getBytes(StandardCharsets.US_ASCII));

        try {
            final long v = ResidentMemoryReader.readCgroupV2MemoryCurrent(cgroupSelf.toString(), cgroupFsRoot.toString());
            Assert.assertEquals(123456789L, v);
        } finally {
            deleteQuietly(cgroupSelf);
            deleteQuietly(cgroupDir.resolve("memory.current"));
            deleteQuietly(cgroupDir);
            deleteQuietly(cgroupFsRoot.resolve("my.slice"));
            deleteQuietly(cgroupFsRoot);
        }
    }

    @Test
    public void testReadCgroupV2MemoryCurrentReturnsUnknownForGarbageContent() throws IOException {
        final Path cgroupFsRoot = Files.createTempDirectory("resident-memory-reader-test-fsroot");
        final Path cgroupDir = cgroupFsRoot.resolve("v2path");
        Files.createDirectories(cgroupDir);
        Files.write(cgroupDir.resolve("memory.current"), "max".getBytes(StandardCharsets.US_ASCII));

        final Path cgroupSelf = Files.createTempFile("resident-memory-reader-test-cgroup", ".txt");
        Files.write(cgroupSelf, "0::/v2path\n".getBytes(StandardCharsets.US_ASCII));

        try {
            final long v = ResidentMemoryReader.readCgroupV2MemoryCurrent(cgroupSelf.toString(), cgroupFsRoot.toString());
            Assert.assertEquals(ResidentMemoryReader.UNKNOWN_RESIDENT_BYTES, v);
        } finally {
            deleteQuietly(cgroupSelf);
            deleteQuietly(cgroupDir.resolve("memory.current"));
            deleteQuietly(cgroupDir);
            deleteQuietly(cgroupFsRoot);
        }
    }

    @Test
    public void testReadCgroupV2MemoryCurrentReturnsUnknownForNonexistentCgroupSelfPath() {
        // The core "degrade gracefully, never throw" requirement: point the reader
        // at a /proc/self/cgroup that does not exist at all.
        final long v = ResidentMemoryReader.readCgroupV2MemoryCurrent(
                "/nonexistent/path/does/not/exist/proc-self-cgroup",
                "/sys/fs/cgroup"
        );
        Assert.assertEquals(ResidentMemoryReader.UNKNOWN_RESIDENT_BYTES, v);
    }

    @Test
    public void testReadCgroupV2MemoryCurrentReturnsUnknownWhenMemoryCurrentFileMissing() throws IOException {
        // A believable /proc/self/cgroup fixture, but the cgroup fs root doesn't
        // actually have a memory.current under the resolved path.
        final Path cgroupSelf = Files.createTempFile("resident-memory-reader-test-cgroup", ".txt");
        Files.write(cgroupSelf, "0::/some/cgroup/path\n".getBytes(StandardCharsets.US_ASCII));
        try {
            final long v = ResidentMemoryReader.readCgroupV2MemoryCurrent(
                    cgroupSelf.toString(),
                    "/nonexistent/cgroup/fs/root"
            );
            Assert.assertEquals(ResidentMemoryReader.UNKNOWN_RESIDENT_BYTES, v);
        } finally {
            deleteQuietly(cgroupSelf);
        }
    }

    @Test
    public void testReadOsRssFallbackReturnsPlausiblePositiveValue() {
        final long v = ResidentMemoryReader.readOsRssFallback();
        Assert.assertTrue("expected a plausible positive residency, got " + v, v > 0);
        assertPlausibleProcessResidency(v);
    }

    @Test
    public void testReadResidentBytesReturnsPlausiblePositiveValue() {
        // End-to-end against the real filesystem: on this machine (Linux, cgroup v2
        // present - confirmed manually against /proc/self/cgroup and
        // /sys/fs/cgroup/.../memory.current) this exercises the cgroup path; on a
        // platform without cgroup v2 it exercises the Os.getRss() fallback. Either
        // way the result must be a plausible positive residency.
        final long bytes = ResidentMemoryReader.readResidentBytes();
        Assert.assertTrue("expected a plausible positive residency, got " + bytes, bytes > 0);
        assertPlausibleProcessResidency(bytes);
    }

    // A live JVM test process is comfortably above 1 MiB and (barring an absurd
    // test host) below 64 GiB; catches gross unit errors (e.g. KiB vs bytes,
    // pages vs bytes) without pinning to an exact, environment-dependent figure.
    private static void assertPlausibleProcessResidency(long bytes) {
        Assert.assertTrue("residency too small to be plausible: " + bytes, bytes > 1024 * 1024);
        Assert.assertTrue("residency too large to be plausible: " + bytes, bytes < 64L * 1024 * 1024 * 1024);
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignore) {
            // best-effort cleanup
        }
    }
}
