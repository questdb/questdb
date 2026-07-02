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

import io.questdb.std.Chars;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.DirectString;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class DirectSymbolMapTest {

    @Test
    public void testClearResetsStateButKeepsBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < 8; i++) {
                    Assert.assertEquals(i, map.intern("sym" + i));
                }
                Assert.assertEquals(8, map.size());

                map.clear();
                Assert.assertEquals(0, map.size());
                Assert.assertNull(map.valueOf(0));
                Assert.assertEquals(-1, map.keyOf("sym3"));

                // Reuse after clear: keys restart at 0.
                Assert.assertEquals(0, map.intern("fresh0"));
                Assert.assertEquals(1, map.intern("fresh1"));
                Assert.assertEquals(0, map.intern("fresh0"));
                Assert.assertEquals("fresh1", Chars.toString(map.valueOf(1)));
            }
        });
    }

    @Test
    public void testCopyFromPreservesInternKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    DirectSymbolMap src = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT);
                    DirectSymbolMap dst = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)
            ) {
                Assert.assertEquals(0, src.intern("AAPL"));
                Assert.assertEquals(1, src.intern("MSFT"));
                Assert.assertEquals(2, src.intern("GOOG"));

                // dst already has unrelated contents; copyFrom should discard them.
                dst.intern("DELETE_ME");
                dst.copyFrom(src);

                Assert.assertEquals(3, dst.size());
                Assert.assertEquals("AAPL", Chars.toString(dst.valueOf(0)));
                Assert.assertEquals("MSFT", Chars.toString(dst.valueOf(1)));
                Assert.assertEquals("GOOG", Chars.toString(dst.valueOf(2)));
                Assert.assertEquals(0, dst.keyOf("AAPL"));
                Assert.assertEquals(2, dst.keyOf("GOOG"));
                Assert.assertEquals(-1, dst.keyOf("DELETE_ME"));
            }
        });
    }

    @Test
    public void testGrowthPreservesExistingValues() throws Exception {
        // Start with a tiny buffer and populate enough entries to force several reallocations.
        assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            final int n = 5_000;
            String[] values = new String[n];
            try (DirectSymbolMap map = new DirectSymbolMap(16, 4, MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < n; i++) {
                    values[i] = "sym_" + i + "_" + rnd.nextString(rnd.nextInt(32) + 1);
                    int key = map.intern(values[i]);
                    Assert.assertEquals(i, key);
                }
                Assert.assertEquals(n, map.size());

                for (int i = 0; i < n; i++) {
                    Assert.assertEquals(values[i], Chars.toString(map.valueOf(i)));
                    Assert.assertEquals(i, map.keyOf(values[i]));
                }
            }
        });
    }

    @Test
    public void testInternDedupsRepeatValues() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, map.intern("AAPL"));
                Assert.assertEquals(1, map.intern("MSFT"));
                Assert.assertEquals(0, map.intern("AAPL"));
                Assert.assertEquals(1, map.intern("MSFT"));
                Assert.assertEquals(2, map.intern("GOOG"));
                Assert.assertEquals(3, map.size());

                Assert.assertEquals("AAPL", Chars.toString(map.valueOf(0)));
                Assert.assertEquals("MSFT", Chars.toString(map.valueOf(1)));
                Assert.assertEquals("GOOG", Chars.toString(map.valueOf(2)));
                Assert.assertNull(map.valueOf(3));

                Assert.assertEquals(0, map.keyOf("AAPL"));
                Assert.assertEquals(2, map.keyOf("GOOG"));
                Assert.assertEquals(-1, map.keyOf("UNKNOWN"));
            }
        });
    }

    @Test
    public void testPutExternallyKeyedOverwritesExisting() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                map.put(10, "first");
                map.put(20, "second");
                Assert.assertEquals(2, map.size());
                Assert.assertEquals("first", Chars.toString(map.valueOf(10)));
                Assert.assertEquals("second", Chars.toString(map.valueOf(20)));

                // Overwrite existing key: size unchanged, value updated.
                map.put(10, "updated");
                Assert.assertEquals(2, map.size());
                Assert.assertEquals("updated", Chars.toString(map.valueOf(10)));

                // keyOf on put-only map returns -1 (no reverse index).
                Assert.assertEquals(-1, map.keyOf("updated"));
            }
        });
    }

    @Test
    public void testPutNullStoresNullSentinel() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                map.put(5, "real");
                map.put(7, null);
                Assert.assertEquals("real", Chars.toString(map.valueOf(5)));
                Assert.assertNull(map.valueOf(7));
                Assert.assertNull(map.valueOf(999));
            }
        });
    }

    @Test
    public void testValueOfIntoCallerView() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                map.intern("one");
                map.intern("two");
                map.intern("three");

                DirectString viewA = new DirectString();
                DirectString viewB = new DirectString();

                // Bind two views independently; each must survive the other's later overwrite.
                map.valueOf(0, viewA);
                map.valueOf(2, viewB);
                Assert.assertEquals("one", Chars.toString(viewA));
                Assert.assertEquals("three", Chars.toString(viewB));

                // Rebind B elsewhere; A remains untouched.
                map.valueOf(1, viewB);
                Assert.assertEquals("one", Chars.toString(viewA));
                Assert.assertEquals("two", Chars.toString(viewB));
            }
        });
    }

    @Test
    public void testInternResolvesHashCollision() throws Exception {
        // "Aa" and "BB" share the classic Java String hash (31 * 'A' + 'a' == 31 * 'B' + 'B' == 2112),
        // so both probes land on the same initial slot in ValueToKeyMap. Insertion and lookup must
        // walk past the colliding non-matching slot and resolve to the correct entry.
        assertMemoryLeak(() -> {
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, map.intern("Aa"));
                Assert.assertEquals(1, map.intern("BB"));
                Assert.assertEquals(0, map.intern("Aa"));
                Assert.assertEquals(1, map.intern("BB"));
                Assert.assertEquals(2, map.size());

                Assert.assertEquals("Aa", Chars.toString(map.valueOf(0)));
                Assert.assertEquals("BB", Chars.toString(map.valueOf(1)));
                Assert.assertEquals(0, map.keyOf("Aa"));
                Assert.assertEquals(1, map.keyOf("BB"));
            }
        });
    }

    @Test
    public void testEmptyStringRoundTrips() throws Exception {
        // Length-0 entries exercise the edge of append/matches/hashBytes where the
        // char loop body never runs. Verify both intern and put modes accept "" and
        // distinguish it from null and from a missing key.
        assertMemoryLeak(() -> {
            try (DirectSymbolMap intern = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, intern.intern(""));
                Assert.assertEquals(1, intern.intern("nonempty"));
                Assert.assertEquals(0, intern.intern(""));
                Assert.assertEquals("", Chars.toString(intern.valueOf(0)));
                Assert.assertEquals(0, intern.keyOf(""));
                Assert.assertEquals(-1, intern.keyOf("unknown"));
            }
            try (DirectSymbolMap extern = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                extern.put(42, "");
                extern.put(43, null);
                Assert.assertEquals("", Chars.toString(extern.valueOf(42)));
                Assert.assertNull(extern.valueOf(43));
            }
        });
    }

    @Test
    public void testCopyFromPutOnlySource() throws Exception {
        // Put-only source has no ValueToKeyMap; copyFrom should replicate the
        // (key, value) entries with the same int keys and leave the destination
        // in put-only mode too (keyOf returns -1).
        assertMemoryLeak(() -> {
            try (
                    DirectSymbolMap src = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT);
                    DirectSymbolMap dst = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)
            ) {
                src.put(0, "a");
                src.put(1, "b");
                src.put(2, "c");
                dst.copyFrom(src);

                Assert.assertEquals(3, dst.size());
                Assert.assertEquals("a", Chars.toString(dst.valueOf(0)));
                Assert.assertEquals("b", Chars.toString(dst.valueOf(1)));
                Assert.assertEquals("c", Chars.toString(dst.valueOf(2)));
                Assert.assertEquals(-1, dst.keyOf("a"));
                Assert.assertEquals(-1, dst.keyOf("b"));
            }
        });
    }

    @Test
    public void testNonAsciiContentRoundTrips() throws Exception {
        // End-to-end UTF-16: write via putChar, read via getChar, hash/equals by char value.
        // Non-BMP chars encoded as surrogate pairs are treated as two chars, which is
        // consistent with CharSequence.charAt — no special handling needed.
        assertMemoryLeak(() -> {
            String cyrillic = "Пример";
            String japanese = "日本語"; // nihongo
            String surrogatePair = "🚀"; // U+1F680 rocket
            try (DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT)) {
                Assert.assertEquals(0, map.intern(cyrillic));
                Assert.assertEquals(1, map.intern(japanese));
                Assert.assertEquals(2, map.intern(surrogatePair));
                Assert.assertEquals(0, map.intern(cyrillic));

                Assert.assertEquals(cyrillic, Chars.toString(map.valueOf(0)));
                Assert.assertEquals(japanese, Chars.toString(map.valueOf(1)));
                Assert.assertEquals(surrogatePair, Chars.toString(map.valueOf(2)));

                Assert.assertEquals(0, map.keyOf(cyrillic));
                Assert.assertEquals(1, map.keyOf(japanese));
                Assert.assertEquals(2, map.keyOf(surrogatePair));
            }
        });
    }

    @Test
    public void testReopenAfterClose() throws Exception {
        // Reopenable contract: a closed map can be re-initialized via reopen()
        // and used again. Prior state must not leak into the reopened instance.
        assertMemoryLeak(() -> {
            DirectSymbolMap map = new DirectSymbolMap(64, 4, MemoryTag.NATIVE_DEFAULT);
            try {
                map.intern("before");
                map.intern("close");
                map.close();

                map.reopen();
                Assert.assertEquals(0, map.size());
                Assert.assertNull(map.valueOf(0));
                Assert.assertEquals(-1, map.keyOf("before"));

                // Fresh interns start from key 0 on the reopened instance.
                Assert.assertEquals(0, map.intern("after"));
                Assert.assertEquals(1, map.intern("reopen"));
                Assert.assertEquals("after", Chars.toString(map.valueOf(0)));
                Assert.assertEquals(0, map.keyOf("after"));
            } finally {
                map.close();
            }
        });
    }
}
