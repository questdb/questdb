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

package io.questdb.test;

import io.questdb.MemoryUsageLogJob;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MemoryUsageLogJobTest {
    @Test
    public void testAppendMemoryUsageIncludesCoreFieldsAndNonZeroTags() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int rssMemoryTag = MemoryTag.NATIVE_ND_ARRAY_DBG2;
            final long rssSize = 64;
            final int nonRssMemoryTag = MemoryTag.NATIVE_PATH;
            final long nonRssSize = 32;
            final long expectedTagValue = Unsafe.getMemUsedByTag(rssMemoryTag) + rssSize;
            final long expectedNonRssAccounted = Unsafe.getNonRssMemUsed() + nonRssSize;
            long rssPtr = 0;
            long nonRssPtr = 0;
            try {
                rssPtr = Unsafe.malloc(rssSize, rssMemoryTag);
                nonRssPtr = Unsafe.malloc(nonRssSize, nonRssMemoryTag);
                final StringSink sink = new StringSink();
                MemoryUsageLogJob.appendMemoryUsage(sink);

                TestUtils.assertContains(sink, "mem.accounted=");
                TestUtils.assertContains(sink, "mem.rss.accounted=");
                TestUtils.assertContains(sink, "mem.non.rss.accounted=" + expectedNonRssAccounted);
                TestUtils.assertContains(sink, "mem.rss.limit=");
                TestUtils.assertContains(sink, "rss.physical=");
                TestUtils.assertContains(sink, "jvm.heap.used=");
                TestUtils.assertContains(sink, "malloc.count=");
                TestUtils.assertContains(sink, "realloc.count=");
                TestUtils.assertContains(sink, "free.count=");
                TestUtils.assertContains(sink, MemoryTag.nameOf(rssMemoryTag) + "=" + expectedTagValue);
            } finally {
                Unsafe.free(rssPtr, rssSize, rssMemoryTag);
                Unsafe.free(nonRssPtr, nonRssSize, nonRssMemoryTag);
            }
        });
    }

    @Test
    public void testRunSeriallyHonoursInterval() {
        final long[] ticks = {0};
        final MemoryUsageLogJob job = new MemoryUsageLogJob(() -> ticks[0], 1000);

        Assert.assertTrue(job.run(0));
        Assert.assertFalse(job.run(0));

        ticks[0] = 999_999;
        Assert.assertFalse(job.run(0));

        ticks[0] = 1_000_000;
        Assert.assertTrue(job.run(0));
    }
}
