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

package io.questdb.test.cairo.file;

import io.questdb.cairo.CommitMode;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileUtils;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Data before pointer for the block-file A/B region scheme. {@link BlockFileWriter#commit()} writes the new
 * region into the inactive area, then bumps the version word at offset 0 to select it. The two sit on
 * different pages, so without a barrier a crash can leave the version word durable and its region not, and
 * {@link io.questdb.cairo.file.BlockFileReader} then refuses the file, losing a view or mat-view definition.
 * The test asserts ordering, since a whole-file crash snapshot cannot express "page 0 durable, tail not": the
 * sync that makes the region durable happens while the version word still names the previous region.
 */
public class BlockFilePublishOrderTest extends AbstractCairoTest {

    private static final int MSG_TYPE = 1;

    @Test
    public void testRegionIsDurableBeforeTheVersionWordNamesIt() throws Exception {
        final VersionAtSyncFacade ff = new VersionAtSyncFacade();
        assertMemoryLeak(ff, () -> {
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat("defn_order.d");

                // Region 1, so there is a previous version for the second commit to supersede.
                try (BlockFileWriter writer = new BlockFileWriter(ff, CommitMode.SYNC)) {
                    writer.of(path.$());
                    final AppendableBlock block = writer.append();
                    block.putLong(1L);
                    block.commit(MSG_TYPE);
                    writer.commit();
                }

                // Region 2. Watch what the version word says at every sync this commit performs.
                ff.watch(path.toString());
                try (BlockFileWriter writer = new BlockFileWriter(ff, CommitMode.SYNC)) {
                    writer.of(path.$());
                    final AppendableBlock block = writer.append();
                    for (int i = 0; i < 512; i++) {
                        block.putLong(i);
                    }
                    block.commit(MSG_TYPE);
                    final long versionBefore = ff.readVersion();
                    writer.commit();
                    ff.unwatch();

                    Assert.assertFalse("commit() performed no sync at all under CommitMode.SYNC, so the "
                            + "test is not exercising the path under test", ff.versionsAtSync.isEmpty());
                    Assert.assertEquals(
                            "DATA BEFORE POINTER violated: every sync in commit() ran with the version word "
                                    + "ALREADY advanced to " + ff.versionsAtSync.get(0) + ", so nothing made "
                                    + "the new region durable before the header started naming it. The "
                                    + "version word (offset 0) and the region (file tail) are different "
                                    + "pages with independent writeback, so a crash here can leave a durable "
                                    + "version selecting a region that never reached the device -- and "
                                    + "BlockFileReader refuses the whole file on checksum mismatch instead "
                                    + "of falling back to the previous region. These files hold view and "
                                    + "mat-view DEFINITIONS, which nothing can re-derive.",
                            versionBefore,
                            (long) ff.versionsAtSync.get(0)
                    );
                }
            }
        });
    }

    private static final class VersionAtSyncFacade extends TestFilesFacadeImpl {
        final List<Long> versionsAtSync = new ArrayList<>();
        private long mappedAddr = -1;
        private String watched;

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            final long addr = super.mmap(fd, len, offset, flags, memoryTag);
            if (watched != null && offset == 0 && addr != -1) {
                mappedAddr = addr;
            }
            return addr;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (watched != null) {
                versionsAtSync.add(Unsafe.getUnsafe().getLong(addr + BlockFileUtils.HEADER_VERSION_OFFSET));
            }
            super.msync(addr, len, async);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            return super.openRW(name, opts);
        }

        long readVersion() {
            return mappedAddr != -1 ? Unsafe.getUnsafe().getLong(mappedAddr + BlockFileUtils.HEADER_VERSION_OFFSET) : -1;
        }

        void unwatch() {
            watched = null;
        }

        void watch(String path) {
            watched = path;
            versionsAtSync.clear();
            mappedAddr = -1;
        }
    }
}
