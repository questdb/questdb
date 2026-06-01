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

import io.questdb.MemoryUsageFormatter;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MemoryUsageFormatterTest {
    @Test
    public void testAppendTopTagsAboveLimitEmitsAndNMoreSuffix() {
        // 5 tags, limit 3 -> top 3 by abs value followed by ", and 2 more".
        final StringSink sink = new StringSink();
        final int[] indices = {
                MemoryTag.MMAP_DEFAULT,
                MemoryTag.NATIVE_DEFAULT,
                MemoryTag.NATIVE_LOGGER,
                MemoryTag.NATIVE_PATH,
                MemoryTag.NATIVE_O3,
        };
        final long[] values = {50, 500, 400, 300, 200};
        MemoryUsageFormatter.appendTopTagsByAbsoluteValue(sink, indices, values, 5, 3);
        Assert.assertEquals(
                "NATIVE_DEFAULT=500, NATIVE_LOGGER=400, NATIVE_PATH=300, and 2 more",
                sink.toString()
        );
    }

    @Test
    public void testAppendTopTagsAtLimitSortsAllByAbsoluteValue() {
        final StringSink sink = new StringSink();
        final int[] indices = {MemoryTag.NATIVE_DEFAULT, MemoryTag.NATIVE_LOGGER, MemoryTag.NATIVE_PATH};
        final long[] values = {200, 100, 300};
        MemoryUsageFormatter.appendTopTagsByAbsoluteValue(sink, indices, values, 3, 3);
        Assert.assertEquals(
                "NATIVE_PATH=300, NATIVE_DEFAULT=200, NATIVE_LOGGER=100",
                sink.toString()
        );
    }

    @Test
    public void testAppendTopTagsBelowLimitSortsByAbsoluteValue() {
        final StringSink sink = new StringSink();
        final int[] indices = {MemoryTag.NATIVE_DEFAULT, MemoryTag.NATIVE_LOGGER, MemoryTag.NATIVE_PATH};
        final long[] values = {100, 500, 200};
        MemoryUsageFormatter.appendTopTagsByAbsoluteValue(sink, indices, values, 3, 10);
        Assert.assertEquals(
                "NATIVE_LOGGER=500, NATIVE_PATH=200, NATIVE_DEFAULT=100",
                sink.toString()
        );
    }

    @Test
    public void testAppendTopTagsEmptyEmitsNothing() {
        final StringSink sink = new StringSink();
        MemoryUsageFormatter.appendTopTagsByAbsoluteValue(sink, new int[0], new long[0], 0, MemoryUsageFormatter.MAX_LOGGED_TAGS);
        Assert.assertEquals("", sink.toString());
    }

    @Test
    public void testAppendTopTagsRanksByAbsoluteValuePreservingSign() {
        // A negative value with the largest magnitude must sort first and keep its sign.
        final StringSink sink = new StringSink();
        final int[] indices = {MemoryTag.NATIVE_DEFAULT, MemoryTag.NATIVE_LOGGER, MemoryTag.NATIVE_PATH};
        final long[] values = {100, -500, 200};
        MemoryUsageFormatter.appendTopTagsByAbsoluteValue(sink, indices, values, 3, 3);
        Assert.assertEquals(
                "NATIVE_LOGGER=-500, NATIVE_PATH=200, NATIVE_DEFAULT=100",
                sink.toString()
        );
    }

    @Test
    public void testAppendTopTagsSingleEntryAtAndAboveLimit() {
        // count == limit == 1: no "more" suffix, no separator.
        final StringSink sink = new StringSink();
        final int[] indices = {MemoryTag.NATIVE_LOGGER, 0, 0};
        final long[] values = {42, 0, 0};
        MemoryUsageFormatter.appendTopTagsByAbsoluteValue(sink, indices, values, 1, 1);
        Assert.assertEquals("NATIVE_LOGGER=42", sink.toString());

        // count > limit == 1: only the largest, then "and N more".
        sink.clear();
        final int[] indices2 = {MemoryTag.NATIVE_DEFAULT, MemoryTag.NATIVE_LOGGER, MemoryTag.NATIVE_PATH};
        final long[] values2 = {10, 50, 20};
        MemoryUsageFormatter.appendTopTagsByAbsoluteValue(sink, indices2, values2, 3, 1);
        Assert.assertEquals("NATIVE_LOGGER=50, and 2 more", sink.toString());
    }

    @Test
    public void testFormatIncludesCoreCounters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final StringSink sink = new StringSink();
            new MemoryUsageFormatter().format(sink);

            TestUtils.assertContains(sink, "mem.accounted=");
            TestUtils.assertContains(sink, "mem.rss.accounted=");
            TestUtils.assertContains(sink, "mem.non.rss.accounted=");
            TestUtils.assertContains(sink, "mem.rss.limit=");
            TestUtils.assertContains(sink, "rss.physical=");
            TestUtils.assertContains(sink, "jvm.heap.used=");
            TestUtils.assertContains(sink, "jvm.heap.committed=");
            TestUtils.assertContains(sink, "jvm.heap.max=");
            TestUtils.assertContains(sink, "malloc.count=");
            TestUtils.assertContains(sink, "realloc.count=");
            TestUtils.assertContains(sink, "free.count=");
            TestUtils.assertContains(sink, "tags=[");
            Assert.assertTrue(
                    "expected closing bracket for tags=[...]",
                    sink.toString().endsWith("]")
            );
        });
    }
}
