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

import io.questdb.BuildInformation;
import io.questdb.BuildInformationHolder;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.LastRunVersion;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

/**
 * Every engine start that may write to the database directory records the release it runs in the
 * {@code .version} file: 16 bytes, the QuestDB version and the QuestDB Enterprise version, each a 64-bit
 * number with a byte per number of the version, most significant first. See {@link LastRunVersion}.
 */
public class LastRunVersionTest extends AbstractBootstrapTest {
    private static final byte[] NO_VERSION_BYTES = {0, 0, 0, 0, 0, 0, 0, 0};

    @Test
    public void testDevelopmentBuildRecordsNoVersion() throws Exception {
        assertMemoryLeak(() -> {
            final String dbRoot = newDbRoot();
            final WriteCountingFacade ff = new WriteCountingFacade();
            // A build that does not run from its jar has no version.
            startEngine(new VersionConfiguration(dbRoot, "[DEVELOPMENT]", ff));
            assertFile(dbRoot, NO_VERSION_BYTES, NO_VERSION_BYTES);
            Assert.assertEquals(1, ff.writeCount);

            startEngine(new VersionConfiguration(dbRoot, "[DEVELOPMENT]", ff));
            assertFile(dbRoot, NO_VERSION_BYTES, NO_VERSION_BYTES);
            Assert.assertEquals("a restart of the same build must not write the file", 1, ff.writeCount);
        });
    }

    @Test
    public void testEncode() {
        // One byte per number, the major version in the second most significant byte.
        Assert.assertEquals(0x000A_0201_0000_0000L, LastRunVersion.encode("10.2.1"));
        Assert.assertEquals(0x0004_0101_0000_0000L, LastRunVersion.encode("4.1.1"));
        Assert.assertEquals(0x0004_0101_0100_0000L, LastRunVersion.encode("4.1.1.1"));
        Assert.assertEquals(0x0004_0100_0000_0000L, LastRunVersion.encode("4.1"));
        Assert.assertEquals(0x0004_0000_0000_0000L, LastRunVersion.encode("4"));
        Assert.assertEquals(0x0004_0101_0000_0000L, LastRunVersion.encode("04.01.001"));
        Assert.assertEquals(0x0001_0203_0405_0600L, LastRunVersion.encode("1.2.3.4.5.6"));
        Assert.assertEquals(0x00FF_FFFF_FFFF_FF00L, LastRunVersion.encode("255.255.255.255.255.255"));
        Assert.assertEquals(0x0000_0000_0100_0000L, LastRunVersion.encode("0.0.0.1"));

        // A qualifier: half a step above the release before, which is half the unit of the last number below
        // the release itself. 4.1.1-SNAPSHOT is 4.1.0 and a half: 4.1.0.128.
        Assert.assertEquals(0x0004_0100_8000_0000L, LastRunVersion.encode("4.1.1-SNAPSHOT"));
        Assert.assertEquals(0x0004_0100_8000_0000L, LastRunVersion.encode("4.1.1-Snapshot"));
        Assert.assertEquals(0x0004_0100_8000_0000L, LastRunVersion.encode("4.1.1-rc1"));
        Assert.assertEquals(0x0004_0100_8000_0000L, LastRunVersion.encode("4.1.1-x"));
        Assert.assertEquals(0x0004_01FF_8000_0000L, LastRunVersion.encode("4.2.0-SNAPSHOT"));
        Assert.assertEquals(0x0003_FFFF_8000_0000L, LastRunVersion.encode("4.0.0-SNAPSHOT"));
        Assert.assertEquals(0x0004_0101_0080_0000L, LastRunVersion.encode("4.1.1.1-SNAPSHOT"));
        Assert.assertEquals(0x0004_0080_0000_0000L, LastRunVersion.encode("4.1-SNAPSHOT"));
        Assert.assertEquals(0x0003_8000_0000_0000L, LastRunVersion.encode("4-SNAPSHOT"));
        Assert.assertEquals(0x0001_0203_0405_0580L, LastRunVersion.encode("1.2.3.4.5.6-SNAPSHOT"));
        Assert.assertEquals(0x0000_0000_8000_0000L, LastRunVersion.encode("0.0.1-SNAPSHOT"));
        Assert.assertEquals(0x00FF_FFFE_8000_0000L, LastRunVersion.encode("255.255.255-SNAPSHOT"));

        // No version: nothing, the zero version, and no release before the first one.
        Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.encode(null));
        Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.encode(""));
        Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.encode("0"));
        Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.encode("0.0.0"));
        Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.encode("0.0.0-SNAPSHOT"));
        Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.encode("0-SNAPSHOT"));

        // What the number cannot hold.
        for (String version : new String[]{
                "[DEVELOPMENT]", "unknown", "v4.1.1", " 4.1.1", "4.1.1 ", "4.1.", ".4.1", "4..1", ".", "-", "4.1.1-",
                "-SNAPSHOT", "4.1.1+build", "4,1,1", "4.1.x", "4.1.-1", "4.1.1.SNAPSHOT", "256.0.0", "1.256.0", "1.0.300",
                "1.0.99999999999999999999", "1.2.3.4.5.6.7", "1.2.3.4.5.6.7-SNAPSHOT", "1.2.3.4.5.6.", "\u0664.1.1",
                "4.1.1\n", "4.1\u00001"
        }) {
            Assert.assertEquals(version, LastRunVersion.NO_VERSION, LastRunVersion.encode(version));
        }
    }

    @Test
    public void testEncodeOrdersVersions() {
        final String[] ascending = {
                "0.0.1-SNAPSHOT", "0.0.1", "0.1.0", "3.255.255", "4.0.0-SNAPSHOT", "4.0.0", "4.1.0-SNAPSHOT", "4.1.0",
                "4.1.0.1", "4.1.0.127", "4.1.1-SNAPSHOT", "4.1.1", "4.1.1.1-SNAPSHOT", "4.1.1.1", "4.1.2", "4.2.0-SNAPSHOT",
                "4.2.0", "9.3.12", "10.0.0-SNAPSHOT", "10.0.0", "10.2.1", "255.255.255"
        };
        for (int i = 1; i < ascending.length; i++) {
            final long prev = LastRunVersion.encode(ascending[i - 1]);
            final long next = LastRunVersion.encode(ascending[i]);
            Assert.assertTrue(ascending[i - 1], prev > LastRunVersion.NO_VERSION);
            Assert.assertTrue(ascending[i - 1] + " must be below " + ascending[i], prev < next);
        }
    }

    @Test
    public void testEngineStartFailsWhenFileCannotBeWritten() throws Exception {
        assertMemoryLeak(() -> {
            final String dbRoot = newDbRoot();
            startEngine(new VersionConfiguration(dbRoot, "10.2.1"));
            final byte[] oss1021 = {0, 10, 2, 1, 0, 0, 0, 0};
            assertFile(dbRoot, oss1021, NO_VERSION_BYTES);

            // Another release cannot record itself: the start fails, as on any other failed write, and the
            // file still names the release before.
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, int opts) {
                    if (Utf8s.endsWithAscii(name, LastRunVersion.FILE_NAME)) {
                        return -1;
                    }
                    return super.openRW(name, opts);
                }
            };
            try {
                startEngine(new VersionConfiguration(dbRoot, "10.3.0", ff));
                Assert.fail("the engine must not start when it cannot record its version");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
                TestUtils.assertContains(e.getFlyweightMessage(), LastRunVersion.FILE_NAME);
            }
            assertFile(dbRoot, oss1021, NO_VERSION_BYTES);

            // The same release starts once the fault is gone.
            startEngine(new VersionConfiguration(dbRoot, "10.3.0"));
            assertFile(dbRoot, new byte[]{0, 10, 3, 0, 0, 0, 0, 0}, NO_VERSION_BYTES);
        });
    }

    @Test
    public void testEnterpriseEngineRecordsBothVersions() throws Exception {
        assertMemoryLeak(() -> {
            final String dbRoot = newDbRoot();
            // As Enterprise does: its build information names the Enterprise release.
            final CairoConfiguration configuration = new VersionConfiguration(dbRoot, "4.2.1");
            try (CairoEngine ignored = new CairoEngine(configuration) {
                @Override
                protected CharSequence getEnterpriseVersion() {
                    return getConfiguration().getBuildInformation().getSwVersion();
                }

                @Override
                protected CharSequence getOssVersion() {
                    return "10.2.1-SNAPSHOT";
                }
            }) {
                assertFile(dbRoot, new byte[]{0, 10, 2, 0, (byte) 0x80, 0, 0, 0}, new byte[]{0, 4, 2, 1, 0, 0, 0, 0});
            }
            Assert.assertEquals(LastRunVersion.encode("10.2.1-SNAPSHOT"), LastRunVersion.readOssVersion(configuration));
            Assert.assertEquals(LastRunVersion.encode("4.2.1"), LastRunVersion.readEntVersion(configuration));

            // OSS on the directory of Enterprise: the Enterprise version goes.
            startEngine(new VersionConfiguration(dbRoot, "10.2.1"));
            assertFile(dbRoot, new byte[]{0, 10, 2, 1, 0, 0, 0, 0}, NO_VERSION_BYTES);
            Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.readEntVersion(configuration));
        });
    }

    @Test
    public void testFreshDatabase() throws Exception {
        assertMemoryLeak(() -> {
            final String dbRoot = newDbRoot();
            final CairoConfiguration configuration = new VersionConfiguration(dbRoot, "10.2.1");
            Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.readOssVersion(configuration));
            Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.readEntVersion(configuration));

            startEngine(configuration);
            assertFile(dbRoot, new byte[]{0, 10, 2, 1, 0, 0, 0, 0}, NO_VERSION_BYTES);
            Assert.assertEquals(0x000A_0201_0000_0000L, LastRunVersion.readOssVersion(configuration));
            Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.readEntVersion(configuration));
        });
    }

    @Test
    public void testMalformedFileIsReplaced() throws Exception {
        assertMemoryLeak(() -> {
            // Too short, too long, and long enough to be anything: none is a version file, whatever its first
            // 16 bytes are.
            for (int size : new int[]{0, 1, 8, 15, 17, 32, 4096}) {
                final String dbRoot = newDbRoot();
                final CairoConfiguration configuration = new VersionConfiguration(dbRoot, "10.2.1");
                final byte[] garbage = new byte[size];
                for (int i = 0; i < size; i++) {
                    // reads as version 9.9.9 when taken for one
                    garbage[i] = (byte) (i % 8 == 0 ? 0 : 9);
                }
                Files.write(new File(dbRoot, LastRunVersion.FILE_NAME).toPath(), garbage);
                Assert.assertEquals("size " + size, LastRunVersion.NO_VERSION, LastRunVersion.readOssVersion(configuration));
                Assert.assertEquals("size " + size, LastRunVersion.NO_VERSION, LastRunVersion.readEntVersion(configuration));

                startEngine(configuration);
                assertFile(dbRoot, new byte[]{0, 10, 2, 1, 0, 0, 0, 0}, NO_VERSION_BYTES);
            }
        });
    }

    @Test
    public void testReadOnlyInstanceWritesNothing() throws Exception {
        assertMemoryLeak(() -> {
            final String dbRoot = newDbRoot();
            startEngine(new VersionConfiguration(dbRoot, "10.2.1"));
            final byte[] oss1021 = {0, 10, 2, 1, 0, 0, 0, 0};
            assertFile(dbRoot, oss1021, NO_VERSION_BYTES);

            final WriteCountingFacade ff = new WriteCountingFacade();
            startEngine(new VersionConfiguration(dbRoot, "10.3.0", ff) {
                @Override
                public boolean isReadOnlyInstance() {
                    return true;
                }
            });
            assertFile(dbRoot, oss1021, NO_VERSION_BYTES);
            Assert.assertEquals(0, ff.writeCount);
        });
    }

    @Test
    public void testRestartOfSameVersionWritesNothing() throws Exception {
        assertMemoryLeak(() -> {
            final String dbRoot = newDbRoot();
            final WriteCountingFacade ff = new WriteCountingFacade();
            startEngine(new VersionConfiguration(dbRoot, "10.2.1-SNAPSHOT", ff));
            final byte[] expected = {0, 10, 2, 0, (byte) 0x80, 0, 0, 0};
            assertFile(dbRoot, expected, NO_VERSION_BYTES);
            Assert.assertEquals(1, ff.writeCount);

            for (int i = 0; i < 3; i++) {
                startEngine(new VersionConfiguration(dbRoot, "10.2.1-SNAPSHOT", ff));
                assertFile(dbRoot, expected, NO_VERSION_BYTES);
            }
            Assert.assertEquals("a restart of the same release must not write the file", 1, ff.writeCount);
        });
    }

    @Test
    public void testSaveFailures() throws Exception {
        assertMemoryLeak(() -> {
            final String dbRoot = newDbRoot();
            final byte[] oss1021 = {0, 10, 2, 1, 0, 0, 0, 0};
            final byte[] ent421 = {0, 4, 2, 1, 0, 0, 0, 0};
            LastRunVersion.save(new VersionConfiguration(dbRoot, "unused"), "10.2.1", "4.2.1");
            assertFile(dbRoot, oss1021, ent421);

            // A short write.
            try {
                LastRunVersion.save(new VersionConfiguration(dbRoot, "unused", new TestFilesFacadeImpl() {
                    @Override
                    public long write(long fd, long address, long len, long offset) {
                        return len - 1;
                    }
                }), "10.3.0", "4.3.0");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not write version file");
            }
            assertFile(dbRoot, oss1021, ent421);

            // A file that is too long and cannot be cut: the 16 bytes are in, the tail stays, and the file
            // reads as no version file until a start replaces it.
            final File file = new File(dbRoot, LastRunVersion.FILE_NAME);
            Files.write(file.toPath(), new byte[40]);
            try {
                LastRunVersion.save(new VersionConfiguration(dbRoot, "unused", new TestFilesFacadeImpl() {
                    @Override
                    public boolean truncate(long fd, long size) {
                        return false;
                    }
                }), "10.3.0", "4.3.0");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not truncate version file");
            }
            Assert.assertEquals(40, file.length());
            final CairoConfiguration configuration = new VersionConfiguration(dbRoot, "unused");
            Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.readOssVersion(configuration));
            Assert.assertEquals(LastRunVersion.NO_VERSION, LastRunVersion.readEntVersion(configuration));

            // A failed flush.
            try {
                LastRunVersion.save(new VersionConfiguration(dbRoot, "unused", new TestFilesFacadeImpl() {
                    @Override
                    public void fsync(long fd) {
                        throw CairoException.critical(5).put("could not fsync [fd=").put(fd).put(']');
                    }
                }), "10.3.0", "4.3.0");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not fsync");
            }
            // written and cut before the flush failed
            assertFile(dbRoot, new byte[]{0, 10, 3, 0, 0, 0, 0, 0}, new byte[]{0, 4, 3, 0, 0, 0, 0, 0});

            // A file that cannot be read is replaced, as a malformed one is.
            LastRunVersion.save(new VersionConfiguration(dbRoot, "unused", new TestFilesFacadeImpl() {
                @Override
                public long read(long fd, long buf, long len, long offset) {
                    return -1;
                }
            }), "10.4.0", "4.4.0");
            assertFile(dbRoot, new byte[]{0, 10, 4, 0, 0, 0, 0, 0}, new byte[]{0, 4, 4, 0, 0, 0, 0, 0});
        });
    }

    @Test
    public void testToSink() {
        assertToSink("10.2.1", "10.2.1");
        assertToSink("4.1.1.1", "4.1.1.1");
        assertToSink("4.1.0", "4.1");
        assertToSink("4.0.0", "4");
        assertToSink("1.2.3.4.5.6", "1.2.3.4.5.6");
        assertToSink("255.255.255", "255.255.255");
        // 4.1.0 and a half
        assertToSink("4.1.0.128", "4.1.1-SNAPSHOT");
        assertToSink("4.1.255.128", "4.2.0-SNAPSHOT");
        assertToSink("1.2.3.4.5.5.128", "1.2.3.4.5.6-SNAPSHOT");
        assertToSink("0.0.0", null);
    }

    @Test
    public void testUpgradeAndDowngradeAreRecorded() throws Exception {
        assertMemoryLeak(() -> {
            final String dbRoot = newDbRoot();
            final WriteCountingFacade ff = new WriteCountingFacade();
            startEngine(new VersionConfiguration(dbRoot, "10.2.1", ff));
            assertFile(dbRoot, new byte[]{0, 10, 2, 1, 0, 0, 0, 0}, NO_VERSION_BYTES);

            // up, to a build on the way to 10.3.0: 10.2.255 and a half
            startEngine(new VersionConfiguration(dbRoot, "10.3.0-SNAPSHOT", ff));
            assertFile(dbRoot, new byte[]{0, 10, 2, (byte) 0xFF, (byte) 0x80, 0, 0, 0}, NO_VERSION_BYTES);

            // up, to the release
            startEngine(new VersionConfiguration(dbRoot, "10.3.0", ff));
            assertFile(dbRoot, new byte[]{0, 10, 3, 0, 0, 0, 0, 0}, NO_VERSION_BYTES);

            // down: the file names the release that ran last, not the highest one
            final CairoConfiguration configuration = new VersionConfiguration(dbRoot, "10.2.1", ff);
            Assert.assertTrue(LastRunVersion.readOssVersion(configuration) > LastRunVersion.encode("10.2.1"));
            startEngine(configuration);
            assertFile(dbRoot, new byte[]{0, 10, 2, 1, 0, 0, 0, 0}, NO_VERSION_BYTES);
            Assert.assertEquals(4, ff.writeCount);
        });
    }

    private static void assertFile(String dbRoot, byte[] expectedOss, byte[] expectedEnt) throws Exception {
        final byte[] expected = new byte[16];
        System.arraycopy(expectedOss, 0, expected, 0, 8);
        System.arraycopy(expectedEnt, 0, expected, 8, 8);
        Assert.assertArrayEquals(expected, Files.readAllBytes(new File(dbRoot, LastRunVersion.FILE_NAME).toPath()));
    }

    private static void assertToSink(String expected, String version) {
        final StringSink sink = new StringSink();
        Assert.assertSame(sink, LastRunVersion.toSink(LastRunVersion.encode(version), sink));
        TestUtils.assertEquals(expected, sink);
    }

    private static String newDbRoot() throws Exception {
        final File dbRoot = new File(temp.newFolder(), "db");
        Assert.assertTrue(dbRoot.mkdirs());
        return dbRoot.getAbsolutePath();
    }

    private static void startEngine(CairoConfiguration configuration) {
        //noinspection EmptyTryBlock
        try (CairoEngine ignored = new CairoEngine(configuration)) {
        }
    }

    private static class VersionConfiguration extends DefaultTestCairoConfiguration {
        private final BuildInformation buildInformation;
        private final FilesFacade ff;

        private VersionConfiguration(String dbRoot, String version) {
            this(dbRoot, version, TestFilesFacadeImpl.INSTANCE);
        }

        private VersionConfiguration(String dbRoot, String version, FilesFacade ff) {
            super(dbRoot);
            this.buildInformation = new BuildInformationHolder(version, "unknown", "unknown", "QuestDB");
            this.ff = ff;
        }

        @Override
        public @NotNull BuildInformation getBuildInformation() {
            return buildInformation;
        }

        @Override
        public @NotNull FilesFacade getFilesFacade() {
            // The constructor of the base class asks before the field is set.
            return ff != null ? ff : TestFilesFacadeImpl.INSTANCE;
        }
    }

    // Counts the writes to the version file.
    private static class WriteCountingFacade extends TestFilesFacadeImpl {
        private long versionFd = -1;
        private int writeCount;

        @Override
        public boolean close(long fd) {
            if (fd == versionFd) {
                versionFd = -1;
            }
            return super.close(fd);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.endsWithAscii(name, LastRunVersion.FILE_NAME)) {
                versionFd = fd;
            }
            return fd;
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            if (fd == versionFd) {
                writeCount++;
            }
            return super.write(fd, address, len, offset);
        }
    }
}
