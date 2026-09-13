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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Establishes why {@link io.questdb.cairo.file.BlockFileReader} must keep FAILING on a checksum mismatch
 * instead of silently falling back to the previous region.
 * <p>
 * A fallback looks attractive -- the previous region is usually still intact on disk, so why refuse the whole
 * file? Because the WRITER has no part in that decision. It reads the header's version word and derives the
 * next write offset from THAT version's region, so it would keep treating the version the reader rejected as
 * current. The two would disagree about which region is live, and this test measures the consequence: the
 * next commit lands exactly on the region a fallback reader would have been relying on.
 * <p>
 * So a reader-side fallback would convert one bad commit into silent destruction of the last good one -- the
 * newest data is refused, the fallback serves the older region, and the very next write overwrites it. The
 * durable fix belongs on the write side, where it now is: {@code BlockFileWriter.commit()} makes the region
 * durable BEFORE the version word names it, so a durable version word implies durable region bytes and a
 * mismatch means real corruption rather than a torn commit. Failing loudly on real corruption is correct.
 * <p>
 * See {@link BlockFilePublishOrderTest} for the barrier that makes that guarantee.
 */
public class BlockFileRegionReuseTest extends AbstractCairoTest {

    private static final int MSG_TYPE = 1;

    @Test
    public void testNextCommitReusesTheAreaAFallbackReaderWouldDependOn() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat("region_reuse.d");

                commitRegion(path, 64);
                final long v1Offset = regionOffsetOf(path, 1);
                final long v1Length = regionLengthOf(path, 1);

                commitRegion(path, 64);
                final long v2Offset = regionOffsetOf(path, 2);
                Assert.assertNotEquals("the A/B scheme must place consecutive regions in different areas",
                        v1Offset, v2Offset);

                // The third commit is where a reader/writer disagreement would bite. The writer derives its
                // target from the version word, which still says 2.
                commitRegion(path, 64);
                final long v3Offset = regionOffsetOf(path, 3);

                final boolean overlapsV1 = v3Offset < v1Offset + v1Length
                        && v1Offset < v3Offset + regionLengthOf(path, 3);
                Assert.assertTrue(
                        "the third commit did NOT reuse version 1's area (v1=[" + v1Offset + ", +" + v1Length
                                + "), v3=" + v3Offset + "). That is the premise behind refusing to add a "
                                + "reader-side fallback: because regions alternate, a reader that quietly "
                                + "served version N after rejecting a torn N+1 would be serving bytes the "
                                + "next commit is about to overwrite -- the writer never learns the reader "
                                + "disagreed. If this assertion no longer holds, the fallback question is "
                                + "worth reopening.",
                        overlapsV1
                );
            }
        });
    }

    private void commitRegion(Path path, int longs) {
        try (BlockFileWriter writer = new BlockFileWriter(engine.getConfiguration().getFilesFacade(), CommitMode.SYNC)) {
            writer.of(path.$());
            final AppendableBlock block = writer.append();
            for (int i = 0; i < longs; i++) {
                block.putLong(i);
            }
            block.commit(MSG_TYPE);
            writer.commit();
        }
    }

    private long headerLong(Path path, int offset) {
        try (MemoryCMARW mem = Vm.getCMARWInstance()) {
            mem.smallFile(engine.getConfiguration().getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getLong(offset);
        }
    }

    private long regionLengthOf(Path path, long version) {
        return headerLong(path, (int) BlockFileUtils.getRegionLengthOffset(version));
    }

    private long regionOffsetOf(Path path, long version) {
        return headerLong(path, (int) BlockFileUtils.getRegionOffsetOffset(version));
    }
}
