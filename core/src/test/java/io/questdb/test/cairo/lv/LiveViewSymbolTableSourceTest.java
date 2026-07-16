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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewSymbolCache;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.lv.LiveViewSymbolTableSource;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit coverage for {@link LiveViewSymbolTableSource}, the SYMBOL resolution a
 * live-view read binds while it routes through the in-memory tier. No CREATE LIVE
 * VIEW or refresh worker involved: the source is driven against a real
 * {@link LiveViewSymbolCache} and a real {@link LiveViewInMemoryBuffer} slot over a
 * stub disk symbol source, so the two facts the higher layers depend on can be pinned
 * without query noise.
 * <p>
 * Those facts are the tier column keying and the per-slot horizon. Both have a failure
 * mode that reads as an empty result rather than a wrong one: a source keyed by OUTPUT
 * column builds no overlay at all for a pruned or reordered projection (the tier column
 * it probes is not the SYMBOL one), and a lead-only value then matches nothing. The
 * query-level tests in {@code LiveViewInMemReadTest} cover the keying end to end; these
 * pin it at the unit the page-frame cursor will bind next, where a filter worker - not a
 * record - resolves the symbol.
 * <p>
 * The buffers allocate native memory at construction, so every test runs under
 * {@code assertMemoryLeak}.
 */
public class LiveViewSymbolTableSourceTest extends AbstractCairoTest {

    // Output column -> tier column for SELECT g, ts FROM lv over the (ts, x, g, rn)
    // tier schema below: the projection prunes around the SYMBOL and moves it off its
    // tier index, so output and tier numbering disagree for both columns.
    private static final int OUT_SYM_COL = 0;
    private static final int OUT_TS_COL = 1;
    private static final long PAGE_SIZE = 4096L;
    private static final int TIER_SYM_COL = 2;
    private static final int TIER_TS_COL = 0;

    @Test
    public void testNewSymbolTableClonesOwnTheirBase() throws Exception {
        // Parallel execution clones a symbol table per worker and frees it directly, so
        // newSymbolTable() must hand back an overlay that OWNS the base clone
        // underneath and closes it in turn - otherwise every worker strands one. The
        // shared getSymbolTable() view is the mirror image: it borrows the disk
        // cursor's own table, which the disk cursor closes, so the source must not
        // close it.
        assertMemoryLeak(() -> {
            final IntList tierTypes = tierSchema();
            try (
                    LiveViewSymbolCache cache = new LiveViewSymbolCache(tierTypes);
                    LiveViewInMemoryBuffer slot = new LiveViewInMemoryBuffer(tierTypes, TIER_TS_COL, PAGE_SIZE);
                    LiveViewSymbolTableSource source = new LiveViewSymbolTableSource()
            ) {
                final TestSymbolTableSource base = new TestSymbolTableSource(2);
                base.shared.getQuick(OUT_SYM_COL).values.add("committed");
                internLead(cache, slot, base, "lead-only");
                source.of(base, cache, slot, tierColumns());

                final SymbolTable first = source.newSymbolTable(OUT_SYM_COL);
                final SymbolTable second = source.newSymbolTable(OUT_SYM_COL);
                Assert.assertNotSame("each caller must get its own flyweight", first, second);
                Assert.assertEquals(2, base.clones.size());
                Assert.assertNotSame(base.clones.getQuick(0), base.clones.getQuick(1));

                // Both clones resolve committed and lead ids independently.
                Assert.assertEquals("committed", first.valueOf(0).toString());
                Assert.assertEquals("lead-only", first.valueOf(1).toString());
                Assert.assertEquals("lead-only", second.valueOf(1).toString());

                // Freeing a returned table must free the base clone it wraps, and only
                // that one.
                Misc.freeIfCloseable(first);
                Assert.assertTrue("the freed overlay must close its own base clone", base.clones.getQuick(0).isClosed);
                Assert.assertFalse("it must not close another caller's clone", base.clones.getQuick(1).isClosed);
                Misc.freeIfCloseable(second);
                Assert.assertTrue(base.clones.getQuick(1).isClosed);

                // The shared view borrows instead: closing the source drops the overlay
                // but must leave the disk cursor's own table open for the disk cursor.
                Assert.assertNotNull(source.getSymbolTable(OUT_SYM_COL));
                source.close();
                Assert.assertFalse(
                        "the shared overlay borrows the disk table; closing it here would close it twice",
                        base.shared.getQuick(OUT_SYM_COL).isClosed
                );
            }
        });
    }

    @Test
    public void testNonSymbolTierColumnResolvesFromBase() throws Exception {
        // A column whose TIER column is not a SYMBOL has no lead band to overlay, so
        // both views must hand back the base's own tables untouched.
        assertMemoryLeak(() -> {
            final IntList tierTypes = tierSchema();
            try (
                    LiveViewSymbolCache cache = new LiveViewSymbolCache(tierTypes);
                    LiveViewInMemoryBuffer slot = new LiveViewInMemoryBuffer(tierTypes, TIER_TS_COL, PAGE_SIZE);
                    LiveViewSymbolTableSource source = new LiveViewSymbolTableSource()
            ) {
                final TestSymbolTableSource base = new TestSymbolTableSource(2);
                internLead(cache, slot, base, "lead-only");
                source.of(base, cache, slot, tierColumns());

                Assert.assertSame(base.shared.getQuick(OUT_TS_COL), source.getSymbolTable(OUT_TS_COL));
                final SymbolTable clone = source.newSymbolTable(OUT_TS_COL);
                Assert.assertSame(base.clones.getQuick(0), clone);
                Misc.freeIfCloseable(clone);
            }
        });
    }

    @Test
    public void testOverlayResolvesLeadKeyedByTierColumn() throws Exception {
        // The projection reads the SYMBOL at OUTPUT column 0 but TIER column 2. The
        // cache and the slot's horizon key off the tier column, so the source must
        // resolve through the mapping. Keyed by output column instead, isSymbolColumn(0)
        // reports the TIMESTAMP tier column as plain, no overlay is built, and the
        // lead-only value silently matches nothing rather than reading wrong.
        assertMemoryLeak(() -> {
            final IntList tierTypes = tierSchema();
            try (
                    LiveViewSymbolCache cache = new LiveViewSymbolCache(tierTypes);
                    LiveViewInMemoryBuffer slot = new LiveViewInMemoryBuffer(tierTypes, TIER_TS_COL, PAGE_SIZE);
                    LiveViewSymbolTableSource source = new LiveViewSymbolTableSource()
            ) {
                final TestSymbolTableSource base = new TestSymbolTableSource(2);
                base.shared.getQuick(OUT_SYM_COL).values.add("committed");
                internLead(cache, slot, base, "lead-only");
                source.of(base, cache, slot, tierColumns());

                final StaticSymbolTable overlay = (StaticSymbolTable) source.getSymbolTable(OUT_SYM_COL);
                Assert.assertNotSame("a SYMBOL column must resolve through an overlay", base.shared.getQuick(OUT_SYM_COL), overlay);

                // A committed id resolves against the disk table, a lead-only id against
                // the cache - one LV-table id space across both bands.
                Assert.assertEquals(0, overlay.keyOf("committed"));
                Assert.assertEquals("committed", overlay.valueOf(0).toString());
                Assert.assertEquals(1, overlay.keyOf("lead-only"));
                Assert.assertEquals("lead-only", overlay.valueOf(1).toString());
                Assert.assertEquals(2, overlay.getSymbolCount());
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, overlay.keyOf("never-seen"));

                // The shared view is cached per output column: one flyweight, one probe.
                Assert.assertSame(overlay, source.getSymbolTable(OUT_SYM_COL));
            }
        });
    }

    @Test
    public void testPassThroughBindingResolvesFromBase() throws Exception {
        // A read that does not route through the tier binds a null cache: it serves no
        // lead rows, so it has no lead symbols to resolve. It must then resolve every
        // column from the disk source alone and touch neither the slot nor the mapping -
        // the statically-disk-only branch in LiveViewRecordCursor.of already RELEASED
        // that slot, so reaching for it would be a read of a slot this cursor no longer
        // pins. Passing null for both proves the source never looks.
        assertMemoryLeak(() -> {
            try (LiveViewSymbolTableSource source = new LiveViewSymbolTableSource()) {
                final TestSymbolTableSource base = new TestSymbolTableSource(2);
                base.shared.getQuick(OUT_SYM_COL).values.add("committed");
                source.of(base, null, null, null);

                Assert.assertSame(base.shared.getQuick(OUT_SYM_COL), source.getSymbolTable(OUT_SYM_COL));
                Assert.assertSame(base.shared.getQuick(OUT_TS_COL), source.getSymbolTable(OUT_TS_COL));
                final SymbolTable clone = source.newSymbolTable(OUT_SYM_COL);
                Assert.assertSame(base.clones.getQuick(0), clone);
                Misc.freeIfCloseable(clone);
            }
        });
    }

    @Test
    public void testRebindDropsPreviousSlotOverlays() throws Exception {
        // An overlay is stamped with the slot it was built against, so a rebind must
        // drop it. Left in place, the next read would resolve its symbols against the
        // PREVIOUS read's slot horizon and disk table - a slot it does not pin. The
        // rebind closes the stale overlay rather than merely unlinking it, so a leaked
        // reference to one fails fast instead of quietly resolving the old band.
        assertMemoryLeak(() -> {
            final IntList tierTypes = tierSchema();
            try (
                    LiveViewSymbolCache cache = new LiveViewSymbolCache(tierTypes);
                    LiveViewInMemoryBuffer firstSlot = new LiveViewInMemoryBuffer(tierTypes, TIER_TS_COL, PAGE_SIZE);
                    LiveViewInMemoryBuffer secondSlot = new LiveViewInMemoryBuffer(tierTypes, TIER_TS_COL, PAGE_SIZE);
                    LiveViewSymbolTableSource source = new LiveViewSymbolTableSource()
            ) {
                final TestSymbolTableSource base = new TestSymbolTableSource(2);
                internLead(cache, firstSlot, base, "first-lead");
                source.of(base, cache, firstSlot, tierColumns());
                final StaticSymbolTable first = (StaticSymbolTable) source.getSymbolTable(OUT_SYM_COL);
                Assert.assertEquals(0, first.keyOf("first-lead"));

                // A later cycle interns another value and publishes a slot that carries
                // it; the first slot's horizon never moves.
                internLead(cache, secondSlot, base, "second-lead");
                source.of(base, cache, secondSlot, tierColumns());

                final StaticSymbolTable second = (StaticSymbolTable) source.getSymbolTable(OUT_SYM_COL);
                Assert.assertNotSame("a rebind must not reuse the previous slot's overlay", first, second);
                // Reused, the stale overlay would still carry the FIRST slot's horizon of
                // 1 and so miss the second slot's value at id 1 - the resolution the
                // rebind exists to restore.
                Assert.assertEquals(1, second.keyOf("second-lead"));
                Assert.assertEquals(2, second.getSymbolCount());
            }
        });
    }

    @Test
    public void testSlotHorizonBoundsLeadResolution() throws Exception {
        // The pinned slot's horizon - not the cache's live size - bounds what a read may
        // resolve, so a value a LATER refresh cycle interns stays invisible. This must
        // hold for a clone taken after the cache already grew: a parallel filter worker
        // calls newSymbolTable() at an arbitrary point in the read, and sourcing the
        // bound from the cache's live newSymbolMaxIdExclusive() there would let it
        // resolve ids its slot never carried.
        assertMemoryLeak(() -> {
            final IntList tierTypes = tierSchema();
            try (
                    LiveViewSymbolCache cache = new LiveViewSymbolCache(tierTypes);
                    LiveViewInMemoryBuffer slot = new LiveViewInMemoryBuffer(tierTypes, TIER_TS_COL, PAGE_SIZE);
                    LiveViewSymbolTableSource source = new LiveViewSymbolTableSource()
            ) {
                final TestSymbolTableSource base = new TestSymbolTableSource(2);
                internLead(cache, slot, base, "in-band");
                source.of(base, cache, slot, tierColumns());

                // A later cycle interns past this slot's stamped horizon.
                cache.anchor(TIER_SYM_COL, base.shared.getQuick(OUT_SYM_COL).getSymbolCount());
                Assert.assertEquals(1, cache.intern(TIER_SYM_COL, "out-of-band", base.shared.getQuick(OUT_SYM_COL)));
                Assert.assertEquals(2, cache.newSymbolMaxIdExclusive(TIER_SYM_COL));
                Assert.assertEquals(1, slot.newSymbolMaxId(TIER_SYM_COL));

                final StaticSymbolTable shared = (StaticSymbolTable) source.getSymbolTable(OUT_SYM_COL);
                Assert.assertEquals(0, shared.keyOf("in-band"));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, shared.keyOf("out-of-band"));
                Assert.assertEquals(1, shared.getSymbolCount());

                // A worker cloning after the growth is bounded the same way.
                final SymbolTable clone = source.newSymbolTable(OUT_SYM_COL);
                Assert.assertEquals(0, ((StaticSymbolTable) clone).keyOf("in-band"));
                Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, ((StaticSymbolTable) clone).keyOf("out-of-band"));
                Assert.assertEquals(1, ((StaticSymbolTable) clone).getSymbolCount());
                Misc.freeIfCloseable(clone);
            }
        });
    }

    // Interns value as a new lead symbol of the tier's SYMBOL column, anchored above the
    // disk table's committed count exactly as a drain does, and stamps the resulting
    // horizon onto slot the way the tier does at publish.
    private static void internLead(
            LiveViewSymbolCache cache,
            LiveViewInMemoryBuffer slot,
            TestSymbolTableSource base,
            CharSequence value
    ) {
        final TestSymbolTable committed = base.shared.getQuick(OUT_SYM_COL);
        cache.anchor(TIER_SYM_COL, committed.getSymbolCount());
        cache.intern(TIER_SYM_COL, value, committed);
        slot.setNewSymbolMaxId(TIER_SYM_COL, cache.newSymbolMaxIdExclusive(TIER_SYM_COL));
    }

    // Output column -> tier column for SELECT g, ts FROM lv.
    private static IntList tierColumns() {
        final IntList mapping = new IntList();
        mapping.add(TIER_SYM_COL);
        mapping.add(TIER_TS_COL);
        return mapping;
    }

    // The live view's full output row as the tier stores it: (ts, x, g SYMBOL, rn).
    // The SYMBOL sits at index 2 so a projection can prune AROUND it - pruning only
    // trailing columns would leave an identity mapping and test nothing.
    private static IntList tierSchema() {
        final IntList types = new IntList();
        types.add(ColumnType.TIMESTAMP);
        types.add(ColumnType.LONG);
        types.add(ColumnType.SYMBOL);
        types.add(ColumnType.LONG);
        return types;
    }

    // A stand-in for the LV table's committed symbol map. Doubles as the disk cursor's
    // symbol table (a SymbolMapReader IS a StaticSymbolTable) so one stub covers both
    // roles the source layers over. Ids are list positions, which matches the id space
    // LiveViewSymbolCache anchors its lead band above.
    private static class TestSymbolTable implements SymbolMapReader, QuietCloseable {
        private final ObjList<String> values = new ObjList<>();
        private boolean isClosed;

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCapacity() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSymbolCount() {
            return values.size();
        }

        @Override
        public MemoryR getSymbolOffsetsColumn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MemoryR getSymbolValuesColumn() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCached() {
            return false;
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public int keyOf(CharSequence value) {
            for (int i = 0, n = values.size(); i < n; i++) {
                if (Chars.equalsNc(value, values.getQuick(i))) {
                    return i;
                }
            }
            return SymbolTable.VALUE_NOT_FOUND;
        }

        @Override
        public StaticSymbolTable newSymbolTableView() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateSymbolCount(int count) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            // null for an out-of-band id, as the disk table does: that is the miss the
            // overlay falls back to the lead's cache on.
            return key >= 0 && key < values.size() ? values.getQuick(key) : null;
        }
    }

    // The disk cursor's symbol source. Records every clone it hands out so a test can
    // assert who closes what.
    private static class TestSymbolTableSource implements SymbolTableSource {
        private final ObjList<TestSymbolTable> clones = new ObjList<>();
        private final ObjList<TestSymbolTable> shared = new ObjList<>();

        private TestSymbolTableSource(int columnCount) {
            for (int i = 0; i < columnCount; i++) {
                shared.add(new TestSymbolTable());
            }
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return shared.getQuick(columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            final TestSymbolTable clone = new TestSymbolTable();
            clone.values.addAll(shared.getQuick(columnIndex).values);
            clones.add(clone);
            return clone;
        }
    }
}
