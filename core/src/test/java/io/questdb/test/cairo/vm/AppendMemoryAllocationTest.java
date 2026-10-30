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

package io.questdb.test.cairo.vm;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.MemoryPMARImpl;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Append memories grow their file by allocating disk space. The first growth
 * after a file is opened backs the whole file from offset 0, so holes a sparse
 * file had below its length are reserved before the memory can write into them
 * (on a full disk an mmap write into a hole raises SIGBUS). Later growths back
 * only the new range, which the memory's own earlier calls did not cover:
 * re-allocating from 0 every time costs time in proportion to the whole file.
 */
public class AppendMemoryAllocationTest extends AbstractCairoTest {
    private static final long PAGE = Files.PAGE_SIZE;

    @Test
    public void testCmarwFirstGrowthBacksWholeFileThenOnlyTheGrowth() throws Exception {
        assertMemoryLeak(() -> {
            final LongList calls = new LongList();
            final FilesFacade ff = recordingFacade(calls);
            try (Path path = sparseFile("cmarw.d", ff)) {
                try (MemoryCMARWImpl mem = new MemoryCMARWImpl(ff, path.$(), PAGE, 4 * PAGE, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    Assert.assertEquals("the length covers the open: nothing to allocate", 0, calls.size());
                    mem.jumpTo(4 * PAGE);
                    mem.putLong(1);
                    mem.jumpTo(5 * PAGE);
                    mem.putLong(2);
                }
            }
            assertAllocations(calls);
        });
    }

    @Test
    public void testPmarFirstGrowthBacksWholeFileThenOnlyTheGrowth() throws Exception {
        assertMemoryLeak(() -> {
            final LongList calls = new LongList();
            final FilesFacade ff = recordingFacade(calls);
            try (Path path = sparseFile("pmar.d", ff)) {
                try (MemoryPMARImpl mem = new MemoryPMARImpl(ff, path.$(), PAGE, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)) {
                    mem.jumpTo(4 * PAGE);
                    mem.putLong(1);
                    mem.jumpTo(5 * PAGE);
                    mem.putLong(2);
                }
            }
            assertAllocations(calls);
        });
    }

    private static void assertAllocations(LongList calls) {
        Assert.assertTrue("expected two growths, got " + calls, calls.size() >= 4);
        Assert.assertEquals("first growth after open backs the file from 0: " + calls, 0, calls.getQuick(0));
        Assert.assertEquals("second growth backs only past the first: " + calls, calls.getQuick(1), calls.getQuick(2));
    }

    private static FilesFacade recordingFacade(LongList calls) {
        return new FilesFacadeImpl() {
            @Override
            public boolean allocate(long fd, long size) {
                calls.add(0);
                calls.add(size);
                return super.allocate(fd, size);
            }

            @Override
            public boolean allocate(long fd, long allocatedSize, long size) {
                calls.add(allocatedSize);
                calls.add(size);
                return super.allocate(fd, allocatedSize, size);
            }
        };
    }

    private static Path sparseFile(String name, FilesFacade ff) {
        Path path = new Path().of(root).concat(name);
        long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
        Assert.assertTrue(fd > -1);
        try {
            // Grown by truncate: four pages of length, none of them allocated.
            Assert.assertTrue(ff.truncate(fd, 4 * PAGE));
        } finally {
            ff.close(fd);
        }
        return path;
    }
}
