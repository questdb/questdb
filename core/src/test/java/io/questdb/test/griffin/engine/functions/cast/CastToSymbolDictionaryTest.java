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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.cast.CastToSymbolDictionary;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CastToSymbolDictionaryTest extends AbstractCairoTest {

    @Test
    public void testClearFreesEverythingAndForgetsEveryKey() throws Exception {
        assertMemoryLeak(() -> {
            final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true);
            final DirectString view = new DirectString();
            Assert.assertEquals("an empty dictionary allocates nothing", baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
            for (int cycle = 0; cycle < 3; cycle++) {
                for (int i = 0; i < 1000; i++) {
                    Assert.assertEquals(i, dictionary.put(i, "v" + i));
                }
                Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > baseline);
                dictionary.clear();
                Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                Assert.assertEquals(0, dictionary.size());
                // a released dictionary answers null rather than reading the memory it freed
                Assert.assertNull(dictionary.valueOf(0, view));
                Assert.assertEquals(-1, dictionary.keyOf(0));
            }
        });
    }

    @Test
    public void testScalarKeys() throws Exception {
        assertMemoryLeak(() -> {
            final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true);
            final DirectString view = new DirectString();
            try {
                Assert.assertEquals(-1, dictionary.keyOf(42));
                // No value is reserved as an empty-slot marker. Long.MIN_VALUE is also the bits of
                // -0.0d and Integer.MIN_VALUE the bits of -0.0f, both legal inputs of their casts.
                final long[] values = {0, -1, Long.MIN_VALUE, Long.MAX_VALUE, Integer.MIN_VALUE};
                for (int i = 0; i < values.length; i++) {
                    Assert.assertEquals(-1, dictionary.keyOf(values[i]));
                    Assert.assertEquals(i, dictionary.put(values[i], "text" + i));
                }
                // drive several rehashes and descriptor/chunk growths
                for (long v = 1; v <= 10_000; v++) {
                    Assert.assertEquals(-1, dictionary.keyOf(v * 1_000_003));
                    Assert.assertEquals(values.length + v - 1, dictionary.put(v * 1_000_003, Long.toString(v)));
                }
                Assert.assertEquals(values.length + 10_000, dictionary.size());
                for (int i = 0; i < values.length; i++) {
                    Assert.assertEquals(i, dictionary.keyOf(values[i]));
                    TestUtils.assertEquals("text" + i, dictionary.valueOf(i, view));
                }
                for (long v = 1; v <= 10_000; v++) {
                    final int key = dictionary.keyOf(v * 1_000_003);
                    Assert.assertEquals(values.length + v - 1, key);
                    TestUtils.assertEquals(Long.toString(v), dictionary.valueOf(key, view));
                }
                Assert.assertNull(dictionary.valueOf(SymbolTable.VALUE_IS_NULL, view));
                Assert.assertNull(dictionary.valueOf(dictionary.size(), view));
            } finally {
                dictionary.clear();
            }
        });
    }

    @Test
    public void testTextKeys() throws Exception {
        assertMemoryLeak(() -> {
            final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(false);
            final DirectString view = new DirectString();
            try {
                Assert.assertEquals(0, dictionary.intern("a"));
                Assert.assertEquals(1, dictionary.intern(""));
                Assert.assertEquals(2, dictionary.intern("żółw"));
                Assert.assertEquals(0, dictionary.intern(new StringBuilder("a")));
                Assert.assertEquals(2, dictionary.intern("żółw"));
                TestUtils.assertEquals("", dictionary.valueOf(1, view));
                TestUtils.assertEquals("żółw", dictionary.valueOf(2, view));
                Assert.assertEquals(3, dictionary.size());
            } finally {
                dictionary.clear();
            }
        });
    }

    @Test
    public void testTrackerChargesAndLimit() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4096L);
            final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    sqlExecutionContext.getSecurityContext(),
                    1L,
                    MemoryTrackerWorkload.QUERY
            );
            final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true);
            final DirectString view = new DirectString();
            try {
                dictionary.setMemoryTracker(tracker);
                int minted = 0;
                try {
                    for (long v = 0; v < 100_000; v++) {
                        Assert.assertEquals(minted, dictionary.put(v, "value_" + v));
                        minted++;
                    }
                    Assert.fail("expected the dictionary to reach the query memory limit");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                Assert.assertTrue("dictionary allocation must be charged to the query", tracker.getUsed() > 0);
                // the failed put left the dictionary consistent: every key minted before it resolves
                Assert.assertEquals(minted, dictionary.size());
                for (int key = 0; key < minted; key++) {
                    Assert.assertEquals(key, dictionary.keyOf(key));
                    TestUtils.assertEquals("value_" + key, dictionary.valueOf(key, view));
                }
                dictionary.clear();
                Assert.assertEquals("clear must release every charge", 0, tracker.getUsed());
            } finally {
                dictionary.clear();
                tracker.close();
            }
        });
    }

    @Test
    public void testUtf8Keys() throws Exception {
        assertMemoryLeak(() -> {
            final CastToSymbolDictionary dictionary = new CastToSymbolDictionary(true);
            final StringSink decodeSink = new StringSink();
            final DirectString view = new DirectString();
            try (DirectUtf8Sink direct = new DirectUtf8Sink(16)) {
                Assert.assertEquals(0, dictionary.intern(new Utf8String("short"), decodeSink));
                Assert.assertEquals(1, dictionary.intern(new Utf8String("longer than eight bytes"), decodeSink));
                Assert.assertEquals(2, dictionary.intern(new Utf8String("ąćęłńóśźż longer than eight"), decodeSink));
                Assert.assertEquals(3, dictionary.intern(new Utf8String(""), decodeSink));
                // the same bytes find the same key, whichever sequence carries them
                direct.put("longer than eight bytes");
                Assert.assertEquals(1, dictionary.intern(direct, decodeSink));
                direct.clear();
                direct.put("ąćęłńóśźż longer than eight");
                Assert.assertEquals(2, dictionary.intern(direct, decodeSink));
                // same size, same hash prefix, different tail
                Assert.assertEquals(4, dictionary.intern(new Utf8String("longer than eight bytez"), decodeSink));

                // malformed UTF-8 reads as NULL and mints no key
                final Utf8String malformed = new Utf8String(new byte[]{(byte) 0xC3}, false);
                Assert.assertEquals(SymbolTable.VALUE_IS_NULL, dictionary.intern(malformed, decodeSink));
                Assert.assertEquals(5, dictionary.size());

                TestUtils.assertEquals("short", dictionary.valueOf(0, view));
                TestUtils.assertEquals("longer than eight bytes", dictionary.valueOf(1, view));
                TestUtils.assertEquals("ąćęłńóśźż longer than eight", dictionary.valueOf(2, view));
                TestUtils.assertEquals("", dictionary.valueOf(3, view));
                TestUtils.assertEquals("longer than eight bytez", dictionary.valueOf(4, view));
            } finally {
                dictionary.clear();
            }
        });
    }
}
