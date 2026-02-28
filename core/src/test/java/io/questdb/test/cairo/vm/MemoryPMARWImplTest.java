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

package io.questdb.test.cairo.vm;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.MemoryPMARWImpl;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MemoryPMARWImplTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testTruncateIgnoresUserMappedFileErrorOnRestrictedFs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TruncateFailingFilesFacade ff = new TruncateFailingFilesFacade(Files.WINDOWS_ERROR_USER_MAPPED_FILE);
            try (
                    Path path = new Path().of(temp.newFile().getAbsolutePath());
                    MemoryPMARWImpl mem = new MemoryPMARWImpl()
            ) {
                mem.of(ff, path.$(), Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                mem.jumpTo(Files.PAGE_SIZE * 2L);
                mem.putLong(42);

                mem.truncate();

                Assert.assertTrue(ff.wasZeroTruncateAttempted());
                Assert.assertEquals(0, mem.getAppendOffset());

                mem.putLong(11);
                Assert.assertEquals(Long.BYTES, mem.getAppendOffset());
            }
        });
    }

    @Test
    public void testTruncateThrowsOnUnexpectedRestrictedFsError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final TruncateFailingFilesFacade ff = new TruncateFailingFilesFacade(CairoException.ERRNO_ACCESS_DENIED_WIN);
            try (
                    Path path = new Path().of(temp.newFile().getAbsolutePath());
                    MemoryPMARWImpl mem = new MemoryPMARWImpl()
            ) {
                mem.of(ff, path.$(), Files.PAGE_SIZE, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                mem.putLong(1);

                try {
                    mem.truncate();
                    Assert.fail("truncate should fail for non-mapped-file errors");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Cannot truncate fd=");
                }

                Assert.assertTrue(ff.wasZeroTruncateAttempted());
            }
        });
    }

    private static class TruncateFailingFilesFacade extends TestFilesFacadeImpl {
        private final int truncateErrno;
        private int errno;
        private long trackedFd = -1;
        private boolean zeroTruncateAttempted;

        private TruncateFailingFilesFacade(int truncateErrno) {
            this.truncateErrno = truncateErrno;
        }

        @Override
        public int errno() {
            return errno != 0 ? errno : super.errno();
        }

        @Override
        public boolean isRestrictedFileSystem() {
            return true;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            trackedFd = super.openRW(name, opts);
            return trackedFd;
        }

        @Override
        public boolean truncate(long fd, long size) {
            if (fd == trackedFd && size == 0) {
                errno = truncateErrno;
                zeroTruncateAttempted = true;
                return false;
            }
            errno = 0;
            return super.truncate(fd, size);
        }

        private boolean wasZeroTruncateAttempted() {
            return zeroTruncateAttempted;
        }
    }
}
