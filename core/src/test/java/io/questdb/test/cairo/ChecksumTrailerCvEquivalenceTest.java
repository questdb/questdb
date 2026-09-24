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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Equivalence proof for Task 2: {@code ColumnVersionReader}'s presence/verify logic is refactored to
 * delegate to {@link io.questdb.cairo.ChecksumTrailer#classify}, and this test must observe IDENTICAL
 * verdicts before and after that refactor -- a healthy {@code _cv} still loads clean, and a torn one is
 * still rejected. See {@code .superpowers/sdd/task-2-report.md} for the before/after equivalence
 * evidence (this test run pre- and post-refactor).
 */
public class ChecksumTrailerCvEquivalenceTest extends AbstractCairoTest {

    @Test
    public void testHealthyCvLoadsClean() throws Exception {
        assertMemoryLeak(() -> {
            ColumnVersionReader.resetBodyChecksumFallbackCount();
            execute("create table cv_ok (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into cv_ok values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            assertQuery("select count() from cv_ok").noRandomAccess().expectSize().returns("count\n1\n");
            Assert.assertEquals("healthy _cv must not trigger a checksum fallback", 0L, ColumnVersionReader.getBodyChecksumFallbackCount());
        });
    }

    @Test
    public void testCorruptedCvAreaIsDetected() throws Exception {
        // Negative control for the fix: flip a byte inside the live _cv area and require the reader
        // to refuse it rather than serve column versions derived from rotted bytes.
        assertMemoryLeak(() -> {
            execute("create table cv_rot (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into cv_rot values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            // A single fresh column has no column-top / rename history, so its _cv area is empty (size
            // 0) -- nothing to flip. ADD COLUMN then inserting into the SAME already-populated partition
            // forces a real column-top entry into _cv, giving the live area actual bytes to corrupt.
            execute("alter table cv_rot add column v2 long");
            execute("insert into cv_rot values ('2024-01-01T01:00:00.000000Z', 2, 20)");
            drainWalQueue();
            CvCorruptionUtils.flipByteInLiveArea(engine, "cv_rot");
            try {
                CvCorruptionUtils.forceReload(engine, "cv_rot");
                Assert.fail("expected the _cv checksum to reject a flipped byte");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "_cv checksum mismatch in both A and B areas");
            }
        });
    }
}
