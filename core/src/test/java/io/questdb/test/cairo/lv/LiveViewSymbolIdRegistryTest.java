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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewSymbolIdRegistry;
import io.questdb.cairo.lv.LiveViewSymbolIdSource;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The LV-private symbol-id namespace, on its own.
 * <p>
 * Every assertion here is about a key that would otherwise be wrong rather than slow. A
 * partition key written through this registry is an integer with no self-describing content:
 * an id produced from a stale transaction boundary, from a raw WAL id that was never
 * translated, or from another column's dictionary is in range for the dictionary it lands in,
 * so no map, no codec and no checkpoint decoder downstream can tell it from a correct one. The
 * error surfaces as wrong query results and nothing else. So the registry refuses rather than
 * guesses, and these tests pin what it refuses.
 * <p>
 * The end-to-end half - which cursor families actually arm, and which base column a view's
 * terms bind to - is in {@link LiveViewSymbolIdBindingTest}, against real live views.
 */
public class LiveViewSymbolIdRegistryTest extends AbstractCairoTest {

    private static final int BASE_TABLE_ID = 7;
    private static final int SCAN_COLUMN = 1;
    private static final int SLOT = 3;
    private static final int WRITER_COLUMN = 2;

    @Test
    public void testArmingOutsideArmForIsRefused() {
        // armWal/armStatic stamp the current epoch, so a caller that reached past armFor
        // could arm a slot against a source nothing verified - which is the whole failure
        // the epoch exists to prevent, reintroduced from the other side.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary source = new Dictionary("a");
            try {
                registry.armStatic(SLOT, 1, source);
                Assert.fail("arming outside armFor must be refused");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "armed outside armFor");
            }
        }
    }

    @Test
    public void testBindIsIdempotentAndRefusesAMovedColumn() {
        // A base-schema recompile builds a second factory over the same view and rebinds the
        // same slots. Rebinding to the same column has to be free; rebinding to a different
        // one means the term now reads another column, whose strings the dictionary does not
        // hold - so it invalidates rather than keys.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            registry.bind(SLOT, SCAN_COLUMN + 4, WRITER_COLUMN, BASE_TABLE_ID);
            Assert.assertEquals(1, registry.getBoundSlotCount());
            // The scan index is a property of the compiled plan, so a recompile may move it.
            Assert.assertEquals(SCAN_COLUMN + 4, registry.getBaseScanColumnIndex(SLOT));
            try {
                registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN + 1, BASE_TABLE_ID);
                Assert.fail("a slot must not rebind to a different base column");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "rebound to a different base column");
            }
        }
    }

    @Test
    public void testClearingTheBaseIdCacheKeepsTheDictionary() {
        // The cache is an accelerator over a durable dictionary. Dropping it must cost a
        // resolve, never an id: an id that moved would rename every key already written
        // under it.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary base = new Dictionary("a", "b");
            registry.armFor(source(base, 2, 0));
            final int a = registry.translate(SLOT, 0);
            final int b = registry.translate(SLOT, 1);
            Assert.assertEquals(2, registry.getInternCount());

            registry.clearBaseIdCaches();
            Assert.assertEquals(0, registry.getBaseIdCacheBytes());
            Assert.assertEquals(a, registry.translate(SLOT, 0));
            Assert.assertEquals(b, registry.translate(SLOT, 1));
            // Re-earned through the reverse index rather than assigned again.
            Assert.assertEquals(2, registry.getInternCount());
            Assert.assertEquals(2, registry.getDictionarySize(SLOT));
        }
    }

    @Test
    public void testDictionaryAllocationIsChargedToTheViewTracker() throws Exception {
        // The dictionary is the one resident structure that grows without a frontier sweep to
        // bound it, so it has to sit under the view's own limit rather than only under the
        // global counter. Releasing it under the SAME tracker matters as much: a tracker
        // returned to the pool with a non-zero balance trips the recycle guard in whichever
        // unrelated workload acquires it next.
        assertMemoryLeak(() -> {
            final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    sqlExecutionContext.getSecurityContext(),
                    1L,
                    MemoryTrackerWorkload.LIVE_VIEW_REFRESH
            );
            try {
                try (LiveViewSymbolIdRegistry registry = registry()) {
                    registry.setMemoryTracker(tracker);
                    registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
                    final Dictionary base = new Dictionary("a", "b", "c");
                    registry.armFor(source(base, 3, 0));
                    for (int rawId = 0; rawId < 3; rawId++) {
                        registry.translate(SLOT, rawId);
                    }
                    Assert.assertTrue("the dictionary must charge the view's tracker", tracker.getUsed() > 0);
                }
                Assert.assertEquals("closing the registry must release every byte it charged", 0L, tracker.getUsed());
            } finally {
                tracker.close();
            }
        });
    }

    @Test
    public void testDirtyBandIsEpochStampedRatherThanCleared() {
        // The scratch is reused across transactions without being rewritten, which is what
        // keeps arming O(1) when a diff holds a hundred thousand entries and the view reads
        // one row. A stamp from an earlier epoch must not read as this epoch's entry.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary first = new Dictionary("clean", "alpha", "beta");
            registry.armFor(source(first, 1, 2));
            final int alpha = registry.translate(SLOT, 1);
            final int beta = registry.translate(SLOT, 2);
            Assert.assertNotEquals(alpha, beta);

            // Same raw ids, different strings: the WAL writer restarts its local ids on every
            // commit, so this is the ordinary case rather than a corner one.
            final Dictionary second = new Dictionary("clean", "gamma", "alpha");
            registry.armFor(source(second, 1, 2));
            final int gamma = registry.translate(SLOT, 1);
            Assert.assertNotEquals(alpha, gamma);
            // And a string seen under an earlier epoch keeps the id it was given then.
            Assert.assertEquals(alpha, registry.translate(SLOT, 2));
            Assert.assertEquals(3, registry.getDictionarySize(SLOT));
        }
    }

    @Test
    public void testExhaustedDictionaryRefusesANewIdButStillAnswersAnOldOne() {
        // The ceiling is a capacity guard, not a policy limit: it refuses rather than wrapping
        // into a negative id or reusing one, because either would rename keys already written.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            registry.setMaxDictionarySize(2);
            final Dictionary base = new Dictionary("a", "b", "c");
            registry.armFor(source(base, 3, 0));
            final int a = registry.translate(SLOT, 0);
            registry.translate(SLOT, 1);
            try {
                registry.translate(SLOT, 2);
                Assert.fail("an exhausted dictionary must refuse the next id");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "dictionary is exhausted");
            }
            // An id it already assigned is still answerable, so the refusal is about growth
            // rather than about the view falling over.
            registry.clearBaseIdCaches();
            Assert.assertEquals(a, registry.translate(SLOT, 0));
            Assert.assertEquals(2, registry.getDictionarySize(SLOT));
        }
    }

    @Test
    public void testFootprintsAreReportedApart() {
        // Only the forward half is required by the durable format; the other two are
        // accelerators, and the design asks for the gap between them to be visible before it
        // is a support ticket.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary base = new Dictionary("aa", "bb", "cc");
            registry.armFor(source(base, 3, 0));
            registry.translate(SLOT, 2);
            Assert.assertTrue(registry.getForwardDictionaryBytes() > 0);
            Assert.assertTrue(registry.getReverseDictionaryBytes() > 0);
            // Grown to the highest base id actually touched, not to the clean count.
            Assert.assertEquals(12, registry.getBaseIdCacheBytes());
            Assert.assertEquals(0, registry.getDirtyBandBytes());
        }
    }

    @Test
    public void testKeyOfReverseResolvesAnInternedStringAndRefusesTheRest() {
        // A keyed repair holds a logical string from the change set and needs the id back
        // without minting one - keyOf is the reverse half of translate/lookup that does
        // that, and it must never intern.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary base = new Dictionary("acct-1", "acct-2");
            registry.armFor(source(base, 2, 0));
            final int id1 = registry.translate(SLOT, 0);
            final int id2 = registry.translate(SLOT, 1);
            final long internedBefore = registry.getInternCount();

            Assert.assertEquals(id1, registry.keyOf(SLOT, "acct-1"));
            Assert.assertEquals(id2, registry.keyOf(SLOT, "acct-2"));
            // A string the dictionary was never asked to intern names no id, and asking
            // must not create one.
            Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, registry.keyOf(SLOT, "acct-3"));
            // An unbound slot names no dictionary at all.
            Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, registry.keyOf(SLOT + 1, "acct-1"));
            Assert.assertEquals(internedBefore, registry.getInternCount());
            Assert.assertEquals(2, registry.getDictionarySize(SLOT));
        }
    }

    @Test
    public void testNullTranslatesToItselfAndIsNeverInterned() {
        // VALUE_IS_NULL is the only NULL encoding. A dictionary entry for it would give NULL
        // two spellings in one key, and the second would compare unequal to the first.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            registry.armFor(source(new Dictionary("a"), 1, 0));
            Assert.assertEquals(SymbolTable.VALUE_IS_NULL, registry.translate(SLOT, SymbolTable.VALUE_IS_NULL));
            Assert.assertEquals(0, registry.getDictionarySize(SLOT));
            Assert.assertEquals(0, registry.getInternCount());
        }
    }

    @Test
    public void testRawIdAboveTheArmedBandIsRefused() {
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            registry.armFor(source(new Dictionary("clean", "dirty"), 1, 1));
            registry.translate(SLOT, 1);
            try {
                registry.translate(SLOT, 2);
                Assert.fail("an id above the armed band must be refused");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "above the source's symbol count");
            }
        }
    }

    @Test
    public void testSlotsHaveIndependentNamespaces() {
        // Two terms over different base columns never share ids, even when both are the first
        // component of their own key and both name the same string.
        final int otherSlot = SLOT + 2;
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            registry.bind(otherSlot, SCAN_COLUMN + 1, WRITER_COLUMN + 1, BASE_TABLE_ID);
            final Dictionary first = new Dictionary("x", "y");
            final Dictionary second = new Dictionary("y");
            registry.armFor((r, slot, scan, writer) -> {
                final Dictionary d = slot == SLOT ? first : second;
                r.armStatic(slot, d.size(), d);
            });
            // Ids are assigned per slot in the order that slot first sees a string, so the
            // same value lands on a different id in each - and neither key is ever compared
            // against the other's.
            Assert.assertEquals(0, registry.translate(SLOT, 0));
            Assert.assertEquals(1, registry.translate(SLOT, 1));
            Assert.assertEquals(0, registry.translate(otherSlot, 0));
            Assert.assertEquals("y", registry.lookup(SLOT, 1).toString());
            Assert.assertEquals("y", registry.lookup(otherSlot, 0).toString());
        }
    }

    @Test
    public void testSourceThatArmsNoSlotIsRefused() {
        // The registry drives the walk so that a source cannot arm the slots it happens to
        // know about and leave the rest holding the previous cursor's boundary.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            try {
                registry.armFor((r, slot, scan, writer) -> {
                });
                Assert.fail("a source that arms no slot must be refused");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "armed no dictionary slot");
            }
        }
    }

    @Test
    public void testStaleWalBandDoesNotSurviveIntoAReplay() {
        // The single most important case. A WAL transaction's boundary carried into an
        // applied-base replay puts a legitimate base id inside a stale dirty band, where it
        // resolves to a plausible id for the wrong string. Re-arming moves the boundary;
        // failing to arm at all refuses.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary txn = new Dictionary("clean0", "dirtyA", "dirtyB");
            registry.armFor(source(txn, 1, 2));
            final int dirtyA = registry.translate(SLOT, 1);

            // The replay's pinned reader holds every string committed, so id 1 is a clean id
            // there and names something else entirely.
            final Dictionary applied = new Dictionary("clean0", "committed1", "committed2");
            registry.armFor(sourceStatic(applied));
            Assert.assertEquals(0, registry.getArmedDirtyBandSize(SLOT));
            Assert.assertEquals(3, registry.getArmedCleanSymbolCount(SLOT));
            final int committed1 = registry.translate(SLOT, 1);
            Assert.assertNotEquals(dirtyA, committed1);
            Assert.assertEquals("committed1", registry.lookup(SLOT, committed1).toString());

            // And with no arming at all it refuses rather than reusing the last boundary.
            registry.disarm();
            try {
                registry.translate(SLOT, 1);
                Assert.fail("an unarmed slot must refuse to translate");
            } catch (CairoException e) {
                // A real branch rather than a Java assert: an AssertionError would not be
                // caught here, and QuestDB does not run with -ea in the builds where a silent
                // wrong key costs the most.
                TestUtils.assertContains(e.getFlyweightMessage(), "not armed for the current source");
            }
        }
    }

    @Test
    public void testTranslatingAnUnboundSlotIsRefused() {
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            registry.armFor(source(new Dictionary("a"), 1, 0));
            try {
                registry.translate(SLOT + 1, 0);
                Assert.fail("an unbound slot must refuse to translate");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "is not bound");
            }
        }
    }

    @Test
    public void testUnexpectedNegativeIdsAreRefused() {
        // -1 and VALUE_NOT_FOUND are both in range as a cache index and neither is a value.
        // Reading one as an id is how a wrong key becomes unnoticeable.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            registry.armFor(source(new Dictionary("a"), 1, 0));
            for (int rawId : new int[]{-1, SymbolTable.VALUE_NOT_FOUND, -1000}) {
                try {
                    registry.translate(SLOT, rawId);
                    Assert.fail("a negative raw id must be refused: " + rawId);
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "reason=negative");
                }
            }
        }
    }

    @Test
    public void testUnresolvableIdIsRefusedRatherThanInterned() {
        // The source claimed the id is in band and then could not name it. Interning a
        // placeholder would put a key in the dictionary that no base row ever carried.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            final Dictionary base = new Dictionary("a");
            // A clean count wider than the strings the table can name.
            registry.armFor(source(base, 4, 0));
            try {
                registry.translate(SLOT, 3);
                Assert.fail("an unresolvable id must be refused");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "unresolvable in the armed source");
            }
            Assert.assertEquals(0, registry.getDictionarySize(SLOT));
        }
    }

    @Test
    public void testDuplicateStringsAcrossSourcesShareOneId() {
        // The dictionary is a value namespace, so the same string is the same key no matter
        // which transaction or replay first produced it - which is the property that makes a
        // persisted key readable on a later cycle at all.
        try (LiveViewSymbolIdRegistry registry = registry()) {
            registry.bind(SLOT, SCAN_COLUMN, WRITER_COLUMN, BASE_TABLE_ID);
            registry.armFor(source(new Dictionary("clean", "shared"), 1, 1));
            final int shared = registry.translate(SLOT, 1);

            registry.armFor(sourceStatic(new Dictionary("clean", "shared", "extra")));
            Assert.assertEquals(shared, registry.translate(SLOT, 1));
            // One id, though the two sources reached it through different bands: the first
            // as a dirty WAL id, the second as a committed base id.
            Assert.assertEquals(1, registry.getDictionarySize(SLOT));
            Assert.assertEquals(1, registry.getInternCount());
            Assert.assertEquals(2, registry.getArmCount());
        }
    }

    private static LiveViewSymbolIdRegistry registry() {
        return new LiveViewSymbolIdRegistry(new TableToken("lv", "lv~1", null, 1, true, false, false));
    }

    private static LiveViewSymbolIdSource source(Dictionary dictionary, int cleanSymbolCount, int dirtyBandSize) {
        return (registry, slot, scan, writer) -> registry.armWal(slot, cleanSymbolCount, dirtyBandSize, dictionary);
    }

    private static LiveViewSymbolIdSource sourceStatic(Dictionary dictionary) {
        return (registry, slot, scan, writer) -> registry.armStatic(slot, dictionary.size(), dictionary);
    }

    /**
     * One column's symbol space, in the shape the registry consumes it: an id-to-string
     * resolver with a count. It stands in for the WAL cursor's per-transaction table and for a
     * pinned reader's symbol map alike, which is the point - the registry is written against
     * the {@link SymbolTable} contract and not against either of them.
     */
    private static final class Dictionary implements StaticSymbolTable, SymbolTableSource {
        private final ObjList<String> values = new ObjList<>();

        Dictionary(String... values) {
            for (String value : values) {
                this.values.add(value);
            }
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            return values.size();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return this;
        }

        @Override
        public int keyOf(CharSequence value) {
            final int index = values.indexOf(value);
            return index < 0 ? SymbolTable.VALUE_NOT_FOUND : index;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return this;
        }

        int size() {
            return values.size();
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key >= 0 && key < values.size() ? values.getQuick(key) : null;
        }
    }
}
