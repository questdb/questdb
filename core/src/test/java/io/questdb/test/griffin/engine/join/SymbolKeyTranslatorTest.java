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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the two ways {@link SymbolKeyTranslator} fills its cache of build symbol keys by probe
 * symbol key. Lazily, a probe key looks its text up in the build dictionary the first time a
 * probe meets it. When the build dictionary is small next to the probe dictionary, the cache
 * opens filled instead: each build key looks its text up in the probe dictionary once. Either
 * way, every probe key must translate to the same build key. The fill relies on a probe
 * dictionary that holds each text once and looks a text up cheaply, so it runs only over a
 * probe dictionary that the caller found eligible.
 * <p>
 * The shared cache opens only for a probe that reads at least one row per 256 keys of its
 * dictionary. Below that, each view translates through a lazy cache of its own, and every probe
 * key must still translate to the build key that its text has in the build dictionary.
 */
public class SymbolKeyTranslatorTest extends AbstractCairoTest {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
    // The server default of cairo.sql.join.symbol.translation.cache.capacity: the entries past which a
    // view's own cache stops allocating.
    private static final int SLOT_CACHE_CAPACITY = 1_000_000;
    // The reach of an execution whose probe rows the caller does not know, which keeps the shared cache.
    private static final long UNKNOWN_REACH = Long.MAX_VALUE;

    @Test
    public void testAutoScaledProbeDictionaryIsEligible() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertTrue(configuration.autoScaleSymbolCapacity());
            execute("CREATE TABLE t (s SYMBOL CAPACITY 128)");
            // One commit of 2,000 texts outgrows the declared capacity fifteen times over, and
            // auto-scaling raises the capacity right after it.
            execute("INSERT INTO t SELECT 's' || x FROM long_sequence(2_000)");
            try (TableReader reader = getReader("t")) {
                Assert.assertEquals(2_000, reader.getSymbolMapReader(0).getSymbolCount());
                Assert.assertEquals(1_024, reader.getSymbolMapReader(0).getSymbolCapacity());
                Assert.assertTrue(SymbolKeyTranslator.isEligibleProbeDictionary(reader.getSymbolTable(0)));
            }
        });
    }

    @Test
    public void testBreakerTripWhileFillingFromTheBuildReleasesTheCache() throws Exception {
        assertMemoryLeak(() -> {
            // 300,000 probe keys clear in two MiB chunks, and 2,048 build keys resolve across
            // several breaker checks after them.
            final int buildKeyCount = 2_048;
            GeneratedDictionary probe = new GeneratedDictionary(300_000, 1);
            GeneratedDictionary build = new GeneratedDictionary(buildKeyCount, 7);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(16 << 20);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(NOOP);
                translator.of(probe, build, true, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, tracker, counting);
                Assert.assertEquals(buildKeyCount, probe.keyOfCalls);
                final long checkCount = counting.getCheckCount();
                translator.close();
                Assert.assertEquals(0, tracker.getUsed());

                boolean isTrippedWhileResolving = false;
                for (long tripAt = 1; tripAt <= checkCount; tripAt++) {
                    probe.keyOfCalls = 0;
                    TrippingCircuitBreaker breaker = new TrippingCircuitBreaker(tripAt);
                    CairoException error = Assert.assertThrows(CairoException.class, () -> translator.of(probe, build, true, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, tracker, breaker));
                    Assert.assertTrue(error.isCancellation());
                    // A trip releases the cache, whichever loop it lands in.
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, translator.getSizeInBytes());
                    Assert.assertFalse(translator.isFilledFromBuild());
                    isTrippedWhileResolving |= probe.keyOfCalls > 0 && probe.keyOfCalls < buildKeyCount;
                }
                // The breaker trips between build keys, not only while the cache clears.
                Assert.assertTrue(isTrippedWhileResolving);

                // The same translator opens again and translates every key.
                translator.of(probe, build, true, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, tracker, NOOP);
                Assert.assertTrue(translator.isFilledFromBuild());
                Assert.assertEquals(translator.getSizeInBytes(), tracker.getUsed());
                view.of(translator, probe, build);
                build.keyOfCalls = 0;
                for (int key = 0; key < 300_000; key++) {
                    Assert.assertEquals(key % 7 == 0 && key / 7 < buildKeyCount ? key / 7 : SymbolTable.VALUE_NOT_FOUND, view.translate(key));
                }
                Assert.assertEquals(0, build.keyOfCalls);
                view.close();
                translator.close();
                Assert.assertEquals(0, tracker.getUsed());

                // A memory limit the cache exceeds fails the fill before it starts and leaves the
                // translator reusable too.
                tracker.setLimit(1_024);
                probe.keyOfCalls = 0;
                CairoException error = Assert.assertThrows(CairoException.class, () -> translator.of(probe, build, true, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, tracker, NOOP));
                Assert.assertTrue(error.isOutOfMemory());
                Assert.assertEquals(0, probe.keyOfCalls);
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, translator.getSizeInBytes());
                Assert.assertFalse(translator.isFilledFromBuild());
            }
        });
    }

    @Test
    public void testFillFromTheBuildMatchesLazyResolution() throws Exception {
        assertMemoryLeak(() -> {
            // Each probe dictionary is at least eight times its build dictionary, so the cache
            // fills from the build, and the lazy cache over the same dictionaries is the oracle.
            assertFillMatchesLazyResolution("overlapping, with texts each side lacks and the empty string on both",
                    concat(new String[]{""}, sequence("p", 39)), "p5", "", "absent", "p38", "p0");
            assertFillMatchesLazyResolution("disjoint", sequence("p", 16), "q0", "q1");
            assertFillMatchesLazyResolution("shared texts under the same keys", sequence("p", 24), "p0", "p1", "p2");
            assertFillMatchesLazyResolution("shared texts under reversed keys", sequence("p", 24), "p2", "p1", "p0");
            assertFillMatchesLazyResolution("empty build", sequence("p", 8));
            assertFillMatchesLazyResolution("the empty string in the build only", sequence("p", 16), "", "p3");
            assertFillMatchesLazyResolution("the empty string in the probe only", concat(new String[]{""}, sequence("p", 15)), "q", "p3");
        });
    }

    @Test
    public void testFillFromTheBuildResolvesATextTheBuildHoldsTwiceAsKeyOfDoes() throws Exception {
        assertMemoryLeak(() -> {
            // A table's dictionary holds each text once. If a build dictionary held one twice,
            // the cache would still take the key that the build's keyOf() picks, as it does
            // lazily, whichever of the two that is.
            for (boolean isLastKeyPicked : new boolean[]{false, true}) {
                Dictionary probe = new Dictionary(sequence("p", 24));
                Dictionary build = new Dictionary(isLastKeyPicked, "p4", "p9", "p4");
                try (SymbolKeyTranslator translator = new SymbolKeyTranslator();
                     SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                    translator.of(probe, build, true, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, null, NOOP);
                    Assert.assertEquals(3, probe.keyOfCalls);
                    view.of(translator, probe, build);
                    Assert.assertEquals(isLastKeyPicked ? 2 : 0, view.translate(4));
                    Assert.assertEquals(1, view.translate(9));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, view.translate(5));
                }
                assertFillMatchesLazyResolution("a build text held twice, last key picked: " + isLastKeyPicked,
                        sequence("p", 24), isLastKeyPicked, "p4", "p9", "p4");
            }
        });
    }

    @Test
    public void testFillsFromTheBuildOnlyWhenTheBuildDictionaryIsSmall() throws Exception {
        assertMemoryLeak(() -> {
            // Build key b holds the text of probe key 9 * b, so some build texts lie past the
            // probe dictionary. The last argument is the number of probe lookups that opening
            // the cache makes: the build key count when the cache fills from the build, else 0.
            assertFill(1_000, 8_000, 1_000);
            assertFill(1_000, 7_999, 0);
            assertFill(32_768, 262_144, 32_768);
            assertFill(32_769, 262_152, 0);
            // A count far past the cap, whose product with the ratio would overflow an int. The
            // cap check rejects it first; without that check, the gate's long product does.
            assertFill(Integer.MAX_VALUE / 8 + 1, 64, 0);
            // An empty build dictionary fills the cache with misses and looks nothing up.
            assertFill(0, 1, 0);
            assertFill(0, 0, 0);
        });
    }

    @Test
    public void testIneligibleProbeDictionaryTranslatesLazily() throws Exception {
        assertMemoryLeak(() -> {
            // A live view served from memory can hold a text under two keys: here y under 9,
            // which keyOf() returns, and 10, which the rows carry. The counts call for a fill
            // from the build, but the dictionary is ineligible, so the cache opens unresolved
            // and both keys resolve to y's build key.
            Dictionary probe = new Dictionary(concat(sequence("k", 9), new String[]{"y", "y"}));
            Dictionary build = new Dictionary("y");
            try (SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                translator.of(probe, build, false, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, null, NOOP);
                Assert.assertFalse(translator.isFilledFromBuild());
                Assert.assertEquals(11 * Integer.BYTES, translator.getSizeInBytes());
                Assert.assertEquals(0, probe.keyOfCalls);
                view.of(translator, probe, build);
                for (int pass = 0; pass < 2; pass++) {
                    for (int key = 0; key < 9; key++) {
                        Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, view.translate(key));
                    }
                    Assert.assertEquals(0, view.translate(9));
                    Assert.assertEquals(0, view.translate(10));
                }
                // Each probe key looked its text up in the build dictionary once.
                Assert.assertEquals(11, build.keyOfCalls);
                Assert.assertEquals(0, probe.keyOfCalls);

                // Found eligible, the same dictionaries fill from the build, which writes y's
                // build key at key 9 alone. This is why only a dictionary that holds each text
                // once is eligible.
                translator.of(probe, build, true, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, null, NOOP);
                Assert.assertTrue(translator.isFilledFromBuild());
                Assert.assertEquals(1, probe.keyOfCalls);
                view.of(translator, probe, build);
                Assert.assertEquals(0, view.translate(9));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, view.translate(10));
            }
        });
    }

    @Test
    public void testProbeDictionaryEligibilityFollowsTheSymbolMapLoad() throws Exception {
        // Without auto-scaling, each column keeps its declared capacity of 128.
        setProperty(PropertyKey.CAIRO_AUTO_SCALE_SYMBOL_CAPACITY, "false");
        assertMemoryLeak(() -> {
            Assert.assertFalse(configuration.autoScaleSymbolCapacity());
            execute("CREATE TABLE t (at_limit SYMBOL CAPACITY 128, over_limit SYMBOL CAPACITY 128)");
            // at_limit holds 256 texts, two per unit of its capacity, and over_limit one more.
            execute("INSERT INTO t SELECT CASE WHEN x <= 256 THEN 'a' || x END, 'o' || x FROM long_sequence(257)");
            try (TableReader reader = getReader("t")) {
                for (int column = 0; column < 2; column++) {
                    Assert.assertEquals(128, reader.getSymbolMapReader(column).getSymbolCapacity());
                    Assert.assertEquals(256 + column, reader.getSymbolMapReader(column).getSymbolCount());
                    Assert.assertEquals(column == 0, SymbolKeyTranslator.isEligibleProbeDictionary(reader.getSymbolTable(column)));
                }
                // The check reads the shared symbol map; a view of it does not qualify.
                Assert.assertFalse(SymbolKeyTranslator.isEligibleProbeDictionary(reader.newSymbolTable(0)));
            }
            Assert.assertFalse(SymbolKeyTranslator.isEligibleProbeDictionary(null));
            Assert.assertFalse(SymbolKeyTranslator.isEligibleProbeDictionary(new Dictionary("a")));
        });
    }

    @Test
    public void testSharedCacheOpensAtOneProbeRowPer256Keys() throws Exception {
        assertMemoryLeak(() -> {
            // The shared cache opens once the probe reads ceil(P / 256) rows: 1,172 rows for 300,000
            // keys, one for 256 keys and two for 257. A dictionary without keys opens it, empty, for a
            // probe without rows.
            assertReach(300_000, 1_171, true);
            assertReach(300_000, 1_172, false);
            assertReach(300_000, UNKNOWN_REACH, false);
            assertReach(256, 1, false);
            assertReach(257, 1, true);
            assertReach(257, 2, false);
            assertReach(1, 0, true);
            assertReach(0, 0, false);
            // ceil(Integer.MAX_VALUE / 256) is 8,388,608, so a probe of one row fewer translates through
            // the views and allocates nothing. A rule that overflowed an int would open a shared cache
            // of 8 GiB, which the tracker's limit of 16 MiB fails.
            assertReach(Integer.MAX_VALUE, 8_388_607, true);
        });
    }

    @Test
    public void testSlotCacheMemoryLimitFailsTranslationAndReleases() throws Exception {
        assertMemoryLeak(() -> {
            // 1,000 probe keys and a probe of 3 rows: ceil(1,000 / 256) = 4 rows open the shared cache.
            GeneratedDictionary probe = new GeneratedDictionary(1_000, 1);
            GeneratedDictionary build = new GeneratedDictionary(1_000, 3);
            GeneratedDictionary oracleProbe = new GeneratedDictionary(1_000, 1);
            GeneratedDictionary oracleBuild = new GeneratedDictionary(1_000, 3);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(100);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                translator.of(probe, build, true, 3, SLOT_CACHE_CAPACITY, tracker, NOOP);
                Assert.assertTrue(translator.isSlotCached());
                view.of(translator, probe, build);
                Assert.assertEquals(0, tracker.getUsed());
                // The first miss opens the view's cache: a page table of 256 bytes, past the limit.
                CairoException error = Assert.assertThrows(CairoException.class, () -> view.translate(0));
                Assert.assertTrue(error.isOutOfMemory());
                Assert.assertEquals(0, tracker.getUsed());
                view.close();
                Assert.assertEquals(0, tracker.getUsed());

                // The page table and the page of keys 0 to 255 fit, and the 256-byte hash map that key
                // 600 waits in, its page not allowed yet, does not.
                tracker.setLimit(256 + 1_024 + 100);
                view.of(translator, probe, build);
                for (int key = 0; key < 4; key++) {
                    Assert.assertEquals(oracleBuild.keyOf(oracleProbe.valueOf(key)), view.translate(key));
                }
                Assert.assertEquals(256 + 1_024, tracker.getUsed());
                error = Assert.assertThrows(CairoException.class, () -> view.translate(600));
                Assert.assertTrue(error.isOutOfMemory());
                Assert.assertEquals(256 + 1_024, tracker.getUsed());
                view.close();
                Assert.assertEquals(0, tracker.getUsed());

                // Under a limit of 1 MiB, the same view binds again and translates every key.
                tracker.setLimit(1 << 20);
                view.of(translator, probe, build);
                for (int key = 0; key < 1_000; key++) {
                    Assert.assertEquals(oracleBuild.keyOf(oracleProbe.valueOf(key)), view.translate(key));
                }
                Assert.assertTrue(tracker.getUsed() > 0);
                view.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSlotCacheStartsEmptyOnEveryBinding() throws Exception {
        assertMemoryLeak(() -> {
            // A TRUNCATE renumbers a table's texts: the second probe dictionary holds other texts under
            // the keys of the first. The build holds b0 to b4 under keys 0 to 4 and a0 to a9 under 5 to 14.
            Dictionary first = new Dictionary(sequence("a", 10));
            Dictionary second = new Dictionary(sequence("b", 10));
            Dictionary build = new Dictionary(concat(sequence("b", 5), sequence("a", 10)));
            try (SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                translator.of(first, build, false, 0, SLOT_CACHE_CAPACITY, null, NOOP);
                Assert.assertTrue(translator.isSlotCached());
                view.of(translator, first, build);
                for (int key = 0; key < 10; key++) {
                    Assert.assertEquals(key + 5, view.translate(key));
                }
                translator.of(second, build, false, 0, SLOT_CACHE_CAPACITY, null, NOOP);
                Assert.assertTrue(translator.isSlotCached());
                view.of(translator, second, build);
                build.keyOfCalls = 0;
                for (int key = 0; key < 10; key++) {
                    Assert.assertEquals(key < 5 ? key : SymbolTable.VALUE_NOT_FOUND, view.translate(key));
                }
                // Every key resolved anew, through the second dictionary.
                Assert.assertEquals(10, build.keyOfCalls);
            }
        });
    }

    @Test
    public void testSlotCacheStopsAllocatingAtTheCapacity() throws Exception {
        assertMemoryLeak(() -> {
            // 1,000 probe keys, every one in the build under the same key, and a probe of 3 rows.
            GeneratedDictionary probe = new GeneratedDictionary(1_000, 1);
            GeneratedDictionary build = new GeneratedDictionary(1_000, 1);
            for (int capacity : new int[]{4, 0}) {
                try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                     SymbolKeyTranslator translator = new SymbolKeyTranslator();
                     SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                    translator.of(probe, build, true, 3, capacity, tracker, NOOP);
                    Assert.assertTrue(translator.isSlotCached());
                    view.of(translator, probe, build);
                    build.keyOfCalls = 0;
                    for (int pass = 0; pass < 2; pass++) {
                        for (int key = 0; key < 1_000; key++) {
                            Assert.assertEquals(key, view.translate(key));
                        }
                    }
                    // With a capacity of 4, the first four keys open the page table and the page of keys
                    // 0 to 255, which then keeps those keys at no cost, and keys 256 to 999 resolve again
                    // on the second pass: 744 lookups. With 0, the cache never opens, and every key
                    // resolves on both passes.
                    Assert.assertEquals("capacity " + capacity, capacity == 4 ? 1_000 + 744 : 2_000, build.keyOfCalls);
                    Assert.assertEquals("capacity " + capacity, capacity == 4 ? 256 + 1_024 : 0, tracker.getUsed());
                    view.close();
                    Assert.assertEquals(0, tracker.getUsed());
                }
            }
        });
    }

    @Test
    public void testSlotModeOverATextHeldUnderTwoKeys() throws Exception {
        assertMemoryLeak(() -> {
            // A live view served from memory can hold a text under two keys: y under 9, which keyOf()
            // returns, and 10, which rows carry. The counts call for a fill from the build, which would
            // write y's build key at key 9 alone, but a probe without rows translates through the views,
            // which never fill, so both keys resolve by their text.
            Dictionary probe = new Dictionary(concat(sequence("k", 9), new String[]{"y", "y"}));
            Dictionary build = new Dictionary("y");
            try (SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                translator.of(probe, build, true, 0, SLOT_CACHE_CAPACITY, null, NOOP);
                Assert.assertTrue(translator.isSlotCached());
                Assert.assertFalse(translator.isFilledFromBuild());
                Assert.assertEquals(0, translator.getSizeInBytes());
                Assert.assertEquals(0, probe.keyOfCalls);
                view.of(translator, probe, build);
                for (int key = 0; key < 9; key++) {
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, view.translate(key));
                }
                Assert.assertEquals(0, view.translate(9));
                Assert.assertEquals(0, view.translate(10));
                Assert.assertEquals(0, probe.keyOfCalls);
            }
        });
    }

    @Test
    public void testSlotModeTranslatesLikeTheDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            final String longText = "L".repeat(1_000);
            // The probe dictionary that the owner saw holds the empty string, a 4-byte UTF-8 text, a text
            // of 1,000 characters, texts the build lacks and texts the build holds under other keys. The
            // views read it grown by two texts: one the build lacks, and one it holds.
            final String[] probeTexts = {"", "a", "\uD83D\uDE00x", longText, "absent1", "shared", "absent2"};
            final String[] grownTexts = concat(probeTexts, new String[]{"late", "past"});
            final String[] buildTexts = {"past", "shared", "", "\uD83D\uDE00x", "a", longText, "other"};
            final int keyCount = grownTexts.length;
            Dictionary oracleProbe = new Dictionary(grownTexts);
            Dictionary oracleBuild = new Dictionary(buildTexts);
            Dictionary firstProbe = new Dictionary(grownTexts);
            Dictionary firstBuild = new Dictionary(buildTexts);
            Dictionary secondProbe = new Dictionary(grownTexts);
            Dictionary secondBuild = new Dictionary(buildTexts);
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View first = new SymbolKeyTranslator.View();
                 SymbolKeyTranslator.View second = new SymbolKeyTranslator.View()) {
                // A probe that reads no row translates through the views.
                translator.of(new Dictionary(probeTexts), new Dictionary(buildTexts), true, 0, SLOT_CACHE_CAPACITY, tracker, NOOP);
                Assert.assertTrue(translator.isSlotCached());
                Assert.assertEquals(0, translator.getSizeInBytes());
                first.of(translator, firstProbe, firstBuild);
                second.of(translator, secondProbe, secondBuild);
                Assert.assertEquals(0, tracker.getUsed());
                for (int pass = 0; pass < 2; pass++) {
                    for (int key = 0; key < keyCount; key++) {
                        Assert.assertEquals("pass " + pass + ", key " + key, oracleBuild.keyOf(oracleProbe.valueOf(key)), first.translate(key));
                    }
                    // Each key, those past the owner's dictionary too, looked its text up once.
                    Assert.assertEquals(keyCount, firstBuild.keyOfCalls);
                }
                Assert.assertEquals(0, secondBuild.keyOfCalls);
                for (int key = 0; key < keyCount; key++) {
                    Assert.assertEquals("key " + key, oracleBuild.keyOf(oracleProbe.valueOf(key)), second.translate(key));
                }
                // The second view caches its own lookups.
                Assert.assertEquals(keyCount, secondBuild.keyOfCalls);
                Assert.assertEquals(keyCount, firstBuild.keyOfCalls);
                // NULL translates without a lookup, and the other negative keys resolve uncached on every
                // call, as with the shared cache.
                for (int pass = 0; pass < 2; pass++) {
                    Assert.assertEquals(SymbolTable.VALUE_IS_NULL, first.translate(SymbolTable.VALUE_IS_NULL));
                    for (int key : new int[]{-1, -2}) {
                        Assert.assertEquals(oracleBuild.keyOf(oracleProbe.valueOf(key)), first.translate(key));
                    }
                }
                Assert.assertEquals(keyCount + 4, firstBuild.keyOfCalls);
                // Each view's cache took its page table of 256 bytes and the 1 KiB page of keys 0 to 255.
                Assert.assertEquals(2 * (256 + 1_024), tracker.getUsed());
                first.close();
                Assert.assertEquals(256 + 1_024, tracker.getUsed());
                second.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSlotModeTranslatesWithoutHeapAllocation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p AS (SELECT ('k' || x)::SYMBOL s FROM long_sequence(1_000))");
            execute("CREATE TABLE b AS (SELECT ('k' || (x * 2))::SYMBOL s FROM long_sequence(500))");
            try (
                    TableReader probeReader = getReader("p");
                    TableReader buildReader = getReader("b");
                    TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope();
                    SymbolKeyTranslator translator = new SymbolKeyTranslator();
                    SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()
            ) {
                final StaticSymbolTable probe = probeReader.getSymbolMapReader(0);
                final StaticSymbolTable build = buildReader.getSymbolMapReader(0);
                translator.of(probe, build, true, 0, SLOT_CACHE_CAPACITY, null, NOOP);
                Assert.assertTrue(translator.isSlotCached());
                long minAllocated = Long.MAX_VALUE;
                for (int attempt = 0; attempt < 10; attempt++) {
                    // Each attempt starts from an empty cache: 1,000 lookups, then 99,000 hits.
                    view.of(translator, probe, build);
                    long checksum = 0;
                    final long before = scope.getBean().getCurrentThreadAllocatedBytes();
                    for (int i = 0; i < 100_000; i++) {
                        checksum += view.translate(i % 1_000);
                    }
                    minAllocated = Math.min(minAllocated, scope.getBean().getCurrentThreadAllocatedBytes() - before);
                    // Probe key i holds k(i + 1), which build key (i + 1) / 2 - 1 holds when i + 1 is even.
                    long expected = 0;
                    for (int key = 0; key < 1_000; key++) {
                        expected += (key + 1) % 2 == 0 ? (key + 1) / 2 - 1 : SymbolTable.VALUE_NOT_FOUND;
                    }
                    Assert.assertEquals(100 * expected, checksum);
                }
                Assert.assertEquals(0, minAllocated);
            }
        });
    }

    @Test
    public void testSmallBuildDictionaryResolvesFromTheBuildSide() throws Exception {
        assertMemoryLeak(() -> {
            // 64 probe texts against 4 build texts, one of which the probe lacks: 4 * 8 <= 64.
            Dictionary probe = new Dictionary(sequence("c", 64));
            Dictionary build = new Dictionary("c7", "c1", "x", "c40");
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                translator.of(probe, build, true, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, tracker, NOOP);
                Assert.assertEquals(64 * Integer.BYTES, translator.getSizeInBytes());
                Assert.assertEquals(translator.getSizeInBytes(), tracker.getUsed());
                final int probeLookupsWhileOpening = probe.keyOfCalls;
                final int buildLookupsWhileOpening = build.keyOfCalls;
                view.of(translator, probe, build);
                for (int pass = 0; pass < 2; pass++) {
                    for (int key = 0; key < 64; key++) {
                        final int expected = key == 7 ? 0 : key == 1 ? 1 : key == 40 ? 3 : SymbolTable.VALUE_NOT_FOUND;
                        Assert.assertEquals(expected, view.translate(key));
                    }
                }
                Assert.assertEquals(SymbolTable.VALUE_IS_NULL, view.translate(SymbolTable.VALUE_IS_NULL));
                // Translating every probe key, twice, looks no text up in the build dictionary:
                // opening the cache looked each build text up in the probe dictionary once.
                Assert.assertEquals("build lookups while translating", 0, build.keyOfCalls - buildLookupsWhileOpening);
                Assert.assertEquals("probe lookups while opening", 4, probeLookupsWhileOpening);
                Assert.assertEquals("build lookups while opening", 0, buildLookupsWhileOpening);
                Assert.assertEquals(4, probe.keyOfCalls);
                Assert.assertEquals(0, probe.valueOfCalls);
                Assert.assertEquals(4, build.valueOfCalls);
                view.close();
                translator.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    private static void assertFill(int buildKeyCount, int probeKeyCount, int probeLookupsWhileOpening) {
        final String message = "build=" + buildKeyCount + ", probe=" + probeKeyCount;
        // An empty build dictionary fills from the build without a single lookup.
        final boolean isFilledFromBuild = probeLookupsWhileOpening > 0 || buildKeyCount == 0;
        GeneratedDictionary probe = new GeneratedDictionary(probeKeyCount, 1);
        GeneratedDictionary build = new GeneratedDictionary(buildKeyCount, 9);
        try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(16 << 20);
             SymbolKeyTranslator translator = new SymbolKeyTranslator();
             SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
            translator.of(probe, build, true, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, tracker, NOOP);
            Assert.assertEquals(message, (long) probeKeyCount * Integer.BYTES, translator.getSizeInBytes());
            Assert.assertEquals(message, translator.getSizeInBytes(), tracker.getUsed());
            Assert.assertEquals(message, probeLookupsWhileOpening, probe.keyOfCalls);
            Assert.assertEquals(message, isFilledFromBuild, translator.isFilledFromBuild());
            view.of(translator, probe, build);
            for (int key = 0; key < probeKeyCount; key++) {
                final int expected = key % 9 == 0 && key / 9 < buildKeyCount ? key / 9 : SymbolTable.VALUE_NOT_FOUND;
                Assert.assertEquals(message, expected, view.translate(key));
            }
            // A cache filled from the build answers every probe key; a lazy one looks each up once.
            Assert.assertEquals(message, isFilledFromBuild ? 0 : probeKeyCount, build.keyOfCalls);
            view.close();
            translator.close();
            Assert.assertFalse(message, translator.isFilledFromBuild());
            Assert.assertEquals(message, 0, tracker.getUsed());
        }
    }

    private static void assertFillMatchesLazyResolution(String scenario, String[] probeTexts, String... buildTexts) {
        assertFillMatchesLazyResolution(scenario, probeTexts, false, buildTexts);
    }

    private static void assertFillMatchesLazyResolution(String scenario, String[] probeTexts, boolean isLastKeyPicked, String... buildTexts) {
        // The cache covers the probe dictionary as the owner saw it. The views read a probe
        // dictionary that grew by two texts since, the second one a build text the snapshot
        // lacks when there is one, and their keys, like NULL and a negative key, resolve
        // without the cache.
        final int probeKeyCount = probeTexts.length;
        String lateText = "late2";
        for (String buildText : buildTexts) {
            if (new Dictionary(probeTexts).keyOf(buildText) == SymbolTable.VALUE_NOT_FOUND) {
                lateText = buildText;
                break;
            }
        }
        Dictionary snapshot = new Dictionary(probeTexts);
        Dictionary grown = new Dictionary(concat(probeTexts, new String[]{"late", lateText}));
        Dictionary eagerBuild = new Dictionary(isLastKeyPicked, buildTexts);
        Dictionary lazyBuild = new Dictionary(isLastKeyPicked, buildTexts);
        try (SymbolKeyTranslator eager = new SymbolKeyTranslator();
             SymbolKeyTranslator lazy = new SymbolKeyTranslator();
             SymbolKeyTranslator.View eagerView = new SymbolKeyTranslator.View();
             SymbolKeyTranslator.View lazyView = new SymbolKeyTranslator.View()) {
            eager.of(snapshot, eagerBuild, true, UNKNOWN_REACH, SLOT_CACHE_CAPACITY, null, NOOP);
            // The cache filled from the build: one probe lookup per build key.
            Assert.assertEquals(scenario, buildTexts.length, snapshot.keyOfCalls);
            lazy.of(probeKeyCount, null, NOOP);
            eagerView.of(eager, grown, eagerBuild);
            lazyView.of(lazy, grown, lazyBuild);
            final int buildLookupsWhileOpening = eagerBuild.keyOfCalls;
            for (int key = 0; key < probeKeyCount; key++) {
                Assert.assertEquals(scenario + ", probe key " + key, lazyView.translate(key), eagerView.translate(key));
            }
            // No probe key of the cache looks its text up in the build dictionary.
            Assert.assertEquals(scenario, buildLookupsWhileOpening, eagerBuild.keyOfCalls);
            for (int key : new int[]{probeKeyCount, probeKeyCount + 1, probeKeyCount + 2, -1, SymbolTable.VALUE_IS_NULL}) {
                Assert.assertEquals(scenario + ", probe key " + key, lazyView.translate(key), eagerView.translate(key));
            }
        }
    }

    // Opens a translator over a probe dictionary of probeKeyCount keys and a build dictionary of 1,000
    // keys, which fills a shared cache when the probe dictionary holds 8,000 keys or more, for a probe
    // of probeRowCount rows, and checks which way it translates.
    private static void assertReach(int probeKeyCount, long probeRowCount, boolean isSlotCached) {
        final String message = "probe keys " + probeKeyCount + ", probe rows " + probeRowCount;
        GeneratedDictionary probe = new GeneratedDictionary(probeKeyCount, 1);
        GeneratedDictionary build = new GeneratedDictionary(1_000, 9);
        try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(16 << 20);
             SymbolKeyTranslator translator = new SymbolKeyTranslator()) {
            CountingSqlExecutionCircuitBreaker counting = new CountingSqlExecutionCircuitBreaker(NOOP);
            translator.of(probe, build, true, probeRowCount, SLOT_CACHE_CAPACITY, tracker, counting);
            Assert.assertEquals(message, isSlotCached, translator.isSlotCached());
            if (isSlotCached) {
                // Nothing allocated, cleared, filled or checked.
                Assert.assertEquals(message, 0, translator.getSizeInBytes());
                Assert.assertEquals(message, 0, tracker.getUsed());
                Assert.assertEquals(message, 0, counting.getCheckCount());
                Assert.assertEquals(message, 0, probe.keyOfCalls);
                Assert.assertFalse(message, translator.isFilledFromBuild());
            } else {
                final boolean isFilledFromBuild = probeKeyCount >= 8_000;
                Assert.assertEquals(message, (long) probeKeyCount * Integer.BYTES, translator.getSizeInBytes());
                Assert.assertEquals(message, translator.getSizeInBytes(), tracker.getUsed());
                Assert.assertEquals(message, isFilledFromBuild, translator.isFilledFromBuild());
                Assert.assertEquals(message, isFilledFromBuild ? 1_000 : 0, probe.keyOfCalls);
            }
            translator.close();
            Assert.assertFalse(message, translator.isSlotCached());
            Assert.assertEquals(message, 0, tracker.getUsed());
        }
    }

    private static String[] concat(String[] head, String[] tail) {
        String[] texts = new String[head.length + tail.length];
        System.arraycopy(head, 0, texts, 0, head.length);
        System.arraycopy(tail, 0, texts, head.length, tail.length);
        return texts;
    }

    private static String[] sequence(String prefix, int count) {
        String[] texts = new String[count];
        for (int i = 0; i < count; i++) {
            texts[i] = prefix + i;
        }
        return texts;
    }

    /**
     * A dictionary of explicit texts that counts its lookups. It finds a text it holds twice
     * under the first of its keys, or under the last one when isLastKeyPicked is set.
     */
    private static class Dictionary implements StaticSymbolTable {
        private final boolean isLastKeyPicked;
        private final ObjList<String> texts = new ObjList<>();
        private int keyOfCalls;
        private int valueOfCalls;

        private Dictionary(String... texts) {
            this(false, texts);
        }

        private Dictionary(boolean isLastKeyPicked, String... texts) {
            this.isLastKeyPicked = isLastKeyPicked;
            for (String text : texts) {
                this.texts.add(text);
            }
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return texts.size();
        }

        @Override
        public int keyOf(CharSequence value) {
            keyOfCalls++;
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            int found = SymbolTable.VALUE_NOT_FOUND;
            for (int key = 0, n = texts.size(); key < n; key++) {
                if (Chars.equals(value, texts.getQuick(key))) {
                    found = key;
                    if (!isLastKeyPicked) {
                        break;
                    }
                }
            }
            return found;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            valueOfCalls++;
            return key >= 0 && key < texts.size() ? texts.getQuick(key) : null;
        }
    }

    /**
     * A dictionary of the texts "s" + key * stride, generated on demand rather than held, so that
     * it scales to the build and probe sizes the fill rule turns on. It counts its keyOf() calls.
     */
    private static class GeneratedDictionary implements StaticSymbolTable {
        private final int keyCount;
        private final StringSink sink = new StringSink();
        private final int stride;
        private int keyOfCalls;

        private GeneratedDictionary(int keyCount, int stride) {
            this.keyCount = keyCount;
            this.stride = stride;
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return keyCount;
        }

        @Override
        public int keyOf(CharSequence value) {
            keyOfCalls++;
            if (value == null) {
                return SymbolTable.VALUE_IS_NULL;
            }
            final int length = value.length();
            // Only the generated spelling matches: an "s" and digits without a leading zero.
            if (length < 2 || length > 12 || value.charAt(0) != 's' || (length > 2 && value.charAt(1) == '0')) {
                return SymbolTable.VALUE_NOT_FOUND;
            }
            long number = 0;
            for (int i = 1; i < length; i++) {
                final char c = value.charAt(i);
                if (c < '0' || c > '9') {
                    return SymbolTable.VALUE_NOT_FOUND;
                }
                number = number * 10 + (c - '0');
            }
            return number % stride == 0 && number / stride < keyCount ? (int) (number / stride) : SymbolTable.VALUE_NOT_FOUND;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            if (key < 0 || key >= keyCount) {
                return null;
            }
            sink.clear();
            sink.put('s').put((long) key * stride);
            return sink;
        }
    }

    // Trips on the given check, counting from one, and passes every other one.
    private static class TrippingCircuitBreaker extends CountingSqlExecutionCircuitBreaker {
        private final long tripAt;

        private TrippingCircuitBreaker(long tripAt) {
            super(NOOP);
            this.tripAt = tripAt;
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            super.statefulThrowExceptionIfTrippedTimeThrottled();
            if (getCheckCount() == tripAt) {
                throw CairoException.queryCancelled();
            }
        }
    }
}
