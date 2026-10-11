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
 */
public class SymbolKeyTranslatorTest extends AbstractCairoTest {
    private static final SqlExecutionCircuitBreaker NOOP = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;

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
                translator.of(probe, build, true, tracker, counting);
                Assert.assertEquals(buildKeyCount, probe.keyOfCalls);
                final long checkCount = counting.getCheckCount();
                translator.close();
                Assert.assertEquals(0, tracker.getUsed());

                boolean isTrippedWhileResolving = false;
                for (long tripAt = 1; tripAt <= checkCount; tripAt++) {
                    probe.keyOfCalls = 0;
                    TrippingCircuitBreaker breaker = new TrippingCircuitBreaker(tripAt);
                    CairoException error = Assert.assertThrows(CairoException.class, () -> translator.of(probe, build, true, tracker, breaker));
                    Assert.assertTrue(error.isCancellation());
                    // A trip releases the cache, whichever loop it lands in.
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(0, translator.getSizeInBytes());
                    isTrippedWhileResolving |= probe.keyOfCalls > 0 && probe.keyOfCalls < buildKeyCount;
                }
                // The breaker trips between build keys, not only while the cache clears.
                Assert.assertTrue(isTrippedWhileResolving);

                // The same translator opens again and translates every key.
                translator.of(probe, build, true, tracker, NOOP);
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
                CairoException error = Assert.assertThrows(CairoException.class, () -> translator.of(probe, build, true, tracker, NOOP));
                Assert.assertTrue(error.isOutOfMemory());
                Assert.assertEquals(0, probe.keyOfCalls);
                Assert.assertEquals(0, tracker.getUsed());
                Assert.assertEquals(0, translator.getSizeInBytes());
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
                    translator.of(probe, build, true, null, NOOP);
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
            // A count whose product with the ratio overflows an int.
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
                translator.of(probe, build, false, null, NOOP);
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
                translator.of(probe, build, true, null, NOOP);
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
    public void testSmallBuildDictionaryResolvesFromTheBuildSide() throws Exception {
        assertMemoryLeak(() -> {
            // 64 probe texts against 4 build texts, one of which the probe lacks: 4 * 8 <= 64.
            Dictionary probe = new Dictionary(sequence("c", 64));
            Dictionary build = new Dictionary("c7", "c1", "x", "c40");
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(1 << 20);
                 SymbolKeyTranslator translator = new SymbolKeyTranslator();
                 SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
                translator.of(probe, build, true, tracker, NOOP);
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
        GeneratedDictionary probe = new GeneratedDictionary(probeKeyCount, 1);
        GeneratedDictionary build = new GeneratedDictionary(buildKeyCount, 9);
        try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(16 << 20);
             SymbolKeyTranslator translator = new SymbolKeyTranslator();
             SymbolKeyTranslator.View view = new SymbolKeyTranslator.View()) {
            translator.of(probe, build, true, tracker, NOOP);
            Assert.assertEquals(message, (long) probeKeyCount * Integer.BYTES, translator.getSizeInBytes());
            Assert.assertEquals(message, translator.getSizeInBytes(), tracker.getUsed());
            Assert.assertEquals(message, probeLookupsWhileOpening, probe.keyOfCalls);
            view.of(translator, probe, build);
            for (int key = 0; key < probeKeyCount; key++) {
                final int expected = key % 9 == 0 && key / 9 < buildKeyCount ? key / 9 : SymbolTable.VALUE_NOT_FOUND;
                Assert.assertEquals(message, expected, view.translate(key));
            }
            // A cache filled from the build answers every probe key; a lazy one looks each up once.
            Assert.assertEquals(message, probeLookupsWhileOpening > 0 || buildKeyCount == 0 ? 0 : probeKeyCount, build.keyOfCalls);
            view.close();
            translator.close();
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
            eager.of(snapshot, eagerBuild, true, null, NOOP);
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
