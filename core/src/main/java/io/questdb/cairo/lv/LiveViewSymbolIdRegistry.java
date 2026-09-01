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


package io.questdb.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The live view's own symbol-id namespace: one append-only {@code lvId -> string}
 * dictionary per distinct base SYMBOL column its PARTITION BY terms key by, plus the
 * runtime machinery that turns a raw id a base scan produced into this view's private id
 * for the same string.
 * <p>
 * One registry per {@link LiveViewInstance}. It outlives the compiled factory - a base
 * schema recompile rebuilds the factory and rebinds the same slots, and the ids already
 * handed out stay valid because they name strings rather than base ids - and dies with the
 * view's cached refresh state, before the memory tracker it charges.
 *
 * <h2>Why a private id at all</h2>
 * A WAL segment's symbol id is stable for one transaction. Above the transaction's clean
 * count the WAL writer restarts its local ids on every commit, so sibling transactions give
 * the same raw id to different strings; below it the id names a position in the base table's
 * dictionary, which is a namespace the view does not control and cannot resolve once the
 * segment is pruned. Keying a partition map or a checkpoint root on either means the key
 * written on one cycle is read back as a different key on the next. An LV-private id is what
 * survives a WAL drain, an applied-base replay, a seal and a restore, and what makes a
 * partition key a fixed-width int instead of a resolved string.
 *
 * <h2>Slots</h2>
 * A slot is one <i>source</i> column's dictionary, and its ordinal is the term's window-input
 * column index - see {@link LiveViewPartitionKeyClassifier} for why that index rather than a
 * dense counter. Two terms over one base column share a slot and an id namespace; two terms
 * over different base columns never do. Each slot also records the two indexes the arming
 * families name that base column by: a base-scan index for a pinned-reader page-frame cursor,
 * and a base-table writer index for a WAL segment.
 *
 * <h2>Arming, and why it is a real throw</h2>
 * {@link #translate} splits a raw id on {@code rawId < cleanSymbolCount}, and that boundary
 * belongs to the cursor that produced the row rather than to the slot. A slot carrying a WAL
 * transaction's boundary into an applied-base replay puts a legitimate base id inside a stale
 * dirty band, where it resolves to a plausible id for the wrong string - in range for the
 * dictionary, so nothing downstream rejects it. Every cursor open therefore calls
 * {@link #armFor}, which advances the source epoch and walks every bound slot, and
 * {@link #translate} refuses a slot not stamped with the current epoch. That check is an
 * ordinary branch and not a Java {@code assert}, because QuestDB does not run with
 * {@code -ea} and a silent wrong key costs the most in exactly the builds an assert would
 * skip. {@link #disarm} on cursor close leaves an unarmed slot as the default state rather
 * than a stale-but-plausible one.
 *
 * <h2>Interning is lazy</h2>
 * A transaction's {@code SymbolMapDiff} is sized once at arming and interned per row, not
 * per diff entry. Bounded and filtered views read a handful of rows out of transactions that
 * introduce thousands of symbols, and the dictionary is durable and append-only: eager
 * interning would make it track base ingestion rather than the view's own key domain. The
 * dirty band is epoch-stamped rather than cleared per transaction, so reusing the scratch
 * costs nothing and arming stays O(1) instead of memsetting the largest diff seen.
 *
 * <h2>Growth</h2>
 * Ids are never renumbered or reused, so a dictionary only grows for the life of the view's
 * history. For a bounded-cardinality key column that is strictly cheaper than repeating the
 * strings; for an unbounded one it grows without limit, and no frontier sweep bounds it.
 * {@link #MAX_DICTIONARY_SIZE} is the capacity guard rather than a policy limit: it sits at
 * the boundary of the non-negative int key space, so reaching it is a bug report and not a
 * supported outcome. Hitting it refuses the id rather than wrapping into a negative or
 * reusing an existing one.
 *
 * <h2>Threading</h2>
 * This registry mutates per row and takes no locks. What makes that sound is
 * {@code LiveViewInstance}'s refresh latch: a refresh cycle CAS-acquires it for the whole
 * cycle, and so do the two teardown paths that can free this registry
 * ({@code tryCloseIfDropped}, {@code tryFreeRuntimeStateIfInvalid}), so at most one thread is
 * ever inside these methods for a given view. It is exclusion, not thread affinity - and the
 * difference matters, because the owning THREAD is not stable. {@code ownsViewShard} shards
 * views across workers by table id for the <i>idle sweep</i> only; a notification-driven
 * refresh reaches whichever worker takes the notification, a single-worker pool owns every
 * view, and the segment-yield path exists precisely so a foreign worker can hand a parked loop
 * back to its owner. A same-thread assertion here therefore fails on ordinary, correct
 * schedules - it was tried, and {@code LiveViewConcurrencyTest} is where it fails.
 */
public final class LiveViewSymbolIdRegistry implements LiveViewSymbolIdTranslator, QuietCloseable {
    /**
     * The id-capacity guard from the design's section 8. The dictionary hands out
     * non-negative ints, {@link SymbolTable#VALUE_IS_NULL} is the only NULL encoding and
     * {@link SymbolTable#VALUE_NOT_FOUND} is a cache sentinel, so the space ends here.
     */
    public static final int MAX_DICTIONARY_SIZE = Integer.MAX_VALUE;
    /**
     * The epoch of a slot no cursor has armed. Real epochs start at 1, so a zero-filled
     * dirty-band stamp is automatically stale rather than accidentally current.
     */
    public static final long UNARMED_EPOCH = 0;
    private static final int INITIAL_DICTIONARY_BUF_BYTES = 4096;
    private static final int INITIAL_DICTIONARY_ENTRIES = 64;
    private static final Log LOG = LogFactory.getLog(LiveViewSymbolIdRegistry.class);
    private final IntList boundSlots = new IntList();
    // Returned by armFor so a cursor open can clear its own arming from a try-with-resources
    // rather than remembering a finally. Reused: arming happens once per cursor open, and a
    // handle per open would allocate on the repair path's every scan.
    private final DisarmHandle disarmHandle = new DisarmHandle();
    // Indexed by slot, which is a window-input column index, so the list is sparse and
    // mostly null. boundSlots is the dense inventory everything that iterates reads.
    private final ObjList<Slot> slots = new ObjList<>();
    private final StaticSource staticSource = new StaticSource();
    private final TableToken viewToken;
    private long armCount;
    private long internCount;
    // True only inside armFor. armWal/armStatic stamp the current epoch, so calling one
    // outside the loop would arm a slot against a source nothing verified.
    private boolean isArming;
    private int maxDictionarySize = MAX_DICTIONARY_SIZE;
    private @Nullable MemoryTracker memoryTracker;
    private long sourceEpoch = UNARMED_EPOCH;

    public LiveViewSymbolIdRegistry(@NotNull TableToken viewToken) {
        this.viewToken = viewToken;
    }

    /**
     * Advances the source epoch and arms every bound slot through {@code source}, refusing a
     * source that leaves one unanswered.
     * <p>
     * The registry drives the walk rather than the caller so that arming is structural: a
     * cursor family cannot arm the slots it happens to know about and leave the rest holding
     * the previous cursor's boundary. A family that forgets to call this at all is caught by
     * {@link #translate}'s epoch check instead, one row later.
     *
     * @return a handle that clears this arming, so a caller can hold it in a
     * try-with-resources beside the cursor it armed for
     */
    public QuietCloseable armFor(@NotNull LiveViewSymbolIdSource source) {
        final long epoch = ++sourceEpoch;
        armCount++;
        isArming = true;
        try {
            for (int i = 0, n = boundSlots.size(); i < n; i++) {
                final Slot slot = slots.getQuick(boundSlots.getQuick(i));
                source.armSlot(this, slot.slot, slot.baseScanColumnIndex, slot.baseWriterColumnIndex);
                if (slot.armedEpoch != epoch) {
                    throw CairoException.critical(0)
                            .put("live view partition key source armed no dictionary slot [view=").put(viewToken.getTableName())
                            .put(", slot=").put(slot.slot)
                            .put(']');
                }
            }
        } finally {
            isArming = false;
        }
        return disarmHandle;
    }

    /**
     * Arms every bound slot from a pinned-reader cursor: the applied-base, seed, O3 replay
     * and repair-bounds families, which carry no dirty band because every id they produce is
     * already committed to the base table's dictionary. A convenience over
     * {@link #armFor(LiveViewSymbolIdSource)}, which stays the one place arming happens.
     */
    public QuietCloseable armForPinnedReader(@NotNull SymbolTableSource cursor) {
        return armFor(staticSource.of(cursor));
    }

    /**
     * Arms one slot against a pinned reader's dictionary. Called by a
     * {@link LiveViewSymbolIdSource} from inside {@link #armFor}.
     *
     * @param symbolCount the reader's symbol count, which bounds the ids its rows can carry
     * @param resolver    the table the epoch's ids resolve through
     */
    public void armStatic(int slot, int symbolCount, @NotNull SymbolTable resolver) {
        arm(slot, symbolCount, 0, resolver);
    }

    /**
     * Arms one slot against a WAL transaction. Called by a {@link LiveViewSymbolIdSource}
     * from inside {@link #armFor}.
     * <p>
     * {@code resolver} must be the cursor's own symbol table rather than a base
     * {@code SymbolMapReader} the refresh happens to hold: only the cursor's table probes
     * this transaction's overlay before falling through to the segment's clean files, and a
     * pinned base reader is a different object with a different count that would resolve the
     * clean band against a dictionary the epoch did not come from.
     *
     * @param cleanSymbolCount this transaction's clean symbol count for the column, which is
     *                         where its dirty band starts
     * @param dirtyBandSize    the transaction's diff size for the column, and 0 when it
     *                         carries no diff - never the previous transaction's width
     */
    public void armWal(int slot, int cleanSymbolCount, int dirtyBandSize, @NotNull SymbolTable resolver) {
        arm(slot, cleanSymbolCount, dirtyBandSize, resolver);
    }

    /**
     * Binds one slot to the base column whose dictionary it keys through, creating that
     * dictionary on first sight. This is stage 2 of the design's section 3.2: stage 1 admits
     * a term locally, at the point the key type has to be fixed, and this resolves the source
     * the term really reads once the compiled plan can trace it.
     * <p>
     * A rebind - a base schema recompile builds a second factory over the same view - has to
     * name the same base column. A slot that moved to a different column would key rows
     * through a dictionary holding another column's strings, which is in range and so passes
     * every check below it; refusing is what turns that into an invalidation. The base-scan
     * index is re-recorded on every bind because it is a property of the compiled plan rather
     * than of the base table.
     *
     * @param slot                  the classifier's slot, which is a window-input column index
     * @param baseScanColumnIndex   the column's index in the plan's base-scan metadata
     * @param baseWriterColumnIndex the column's base-table writer index
     * @param baseTableId           the base table's id, which changes when the id space is replaced
     */
    public void bind(int slot, int baseScanColumnIndex, int baseWriterColumnIndex, int baseTableId) {
        if (slot < 0 || baseScanColumnIndex < 0 || baseWriterColumnIndex < 0) {
            throw CairoException.critical(0)
                    .put("live view partition key slot cannot be bound [view=").put(viewToken.getTableName())
                    .put(", slot=").put(slot)
                    .put(", baseScanColumn=").put(baseScanColumnIndex)
                    .put(", baseWriterColumn=").put(baseWriterColumnIndex)
                    .put(']');
        }
        Slot existing = slot < slots.size() ? slots.getQuick(slot) : null;
        if (existing != null) {
            if (existing.baseWriterColumnIndex != baseWriterColumnIndex || existing.baseTableId != baseTableId) {
                throw CairoException.critical(0)
                        .put("live view partition key slot rebound to a different base column [view=").put(viewToken.getTableName())
                        .put(", slot=").put(slot)
                        .put(", boundWriterColumn=").put(existing.baseWriterColumnIndex)
                        .put(", writerColumn=").put(baseWriterColumnIndex)
                        .put(']');
            }
            existing.baseScanColumnIndex = baseScanColumnIndex;
            return;
        }
        final Slot created = new Slot(slot, baseScanColumnIndex, baseWriterColumnIndex, baseTableId);
        try {
            created.dictionary = new DirectSymbolMap(
                    INITIAL_DICTIONARY_BUF_BYTES,
                    INITIAL_DICTIONARY_ENTRIES,
                    MemoryTag.NATIVE_LIVE_VIEW_IN_MEM
            );
            created.dictionary.setMemoryTracker(memoryTracker);
        } catch (Throwable th) {
            created.close();
            throw th;
        }
        slots.extendAndSet(slot, created);
        boundSlots.add(slot);
    }

    /**
     * Drops every {@code baseId -> lvId} entry, keeping the dictionaries. Call it only when
     * the base table's symbol id space can have been replaced or renumbered, never when a
     * reader is merely reopened over the same append-only symbol files: those ids still name
     * the strings they named, and re-earning them costs a resolve per distinct id.
     */
    public void clearBaseIdCaches() {
        for (int i = 0, n = boundSlots.size(); i < n; i++) {
            slots.getQuick(boundSlots.getQuick(i)).baseIdToLvId.clear();
        }
    }

    @Override
    public void close() {
        for (int i = 0, n = boundSlots.size(); i < n; i++) {
            Misc.free(slots.getQuick(boundSlots.getQuick(i)));
        }
        boundSlots.clear();
        slots.clear();
        memoryTracker = null;
    }

    /**
     * Clears every slot's epoch, which is what a cursor close owes the next one. An unarmed
     * slot then fails loudly rather than translating through whatever boundary the closed
     * cursor left behind.
     */
    public void disarm() {
        for (int i = 0, n = boundSlots.size(); i < n; i++) {
            final Slot slot = slots.getQuick(boundSlots.getQuick(i));
            slot.armedEpoch = UNARMED_EPOCH;
            slot.resolver = null;
        }
    }

    /**
     * How many cursor opens have armed this registry. Every source binding a row can key
     * through is one of them, so a family that never arms shows up here as a count that does
     * not move rather than as a wrong key.
     */
    public long getArmCount() {
        return armCount;
    }

    /**
     * The clean count the last {@link #armFor} gave a slot. Survives {@link #disarm}, which
     * clears the epoch rather than the band, so a test can read what a cursor open produced.
     */
    @TestOnly
    public int getArmedCleanSymbolCount(int slot) {
        final Slot s = slotOf(slot);
        return s != null ? s.cleanSymbolCount : -1;
    }

    /**
     * The dirty-band width the last {@link #armFor} gave a slot. Zero for every source but a
     * WAL transaction that carries a diff for the slot's column.
     */
    @TestOnly
    public int getArmedDirtyBandSize(int slot) {
        final Slot s = slotOf(slot);
        return s != null ? s.dirtyBandSize : -1;
    }

    /**
     * Bytes the {@code baseId -> lvId} caches hold. A pure accelerator: the durable format
     * needs none of it, and a restart re-earns it one resolve per distinct id.
     */
    public long getBaseIdCacheBytes() {
        long bytes = 0;
        for (int i = 0, n = boundSlots.size(); i < n; i++) {
            bytes += 4L * slots.getQuick(boundSlots.getQuick(i)).baseIdToLvId.size();
        }
        return bytes;
    }

    public int getBaseScanColumnIndex(int slot) {
        final Slot s = slotOf(slot);
        return s != null ? s.baseScanColumnIndex : -1;
    }

    public int getBaseWriterColumnIndex(int slot) {
        final Slot s = slotOf(slot);
        return s != null ? s.baseWriterColumnIndex : -1;
    }

    /**
     * Returns the {@code n}-th bound slot. The inventory is dense while the slot ordinals
     * themselves are window-input column indexes and so are not.
     */
    public int getBoundSlot(int n) {
        return boundSlots.getQuick(n);
    }

    public int getBoundSlotCount() {
        return boundSlots.size();
    }

    /**
     * The number of ids one slot's dictionary has handed out, which is also the next id it
     * would assign.
     */
    public int getDictionarySize(int slot) {
        final Slot s = slotOf(slot);
        return s != null ? s.dictionary.size() : 0;
    }

    /**
     * Bytes the dirty-band scratch holds. It grows to the widest band a row has actually
     * reached and is never released between transactions, which is what keeps arming O(1).
     */
    public long getDirtyBandBytes() {
        long bytes = 0;
        for (int i = 0, n = boundSlots.size(); i < n; i++) {
            final Slot slot = slots.getQuick(boundSlots.getQuick(i));
            bytes += 4L * slot.dirtyToLvId.size() + 8L * slot.dirtyEpoch.size();
        }
        return bytes;
    }

    /**
     * Bytes the append-only {@code lvId -> string} halves hold. This is the one resident
     * structure the durable format requires; the other two footprints are accelerators.
     */
    public long getForwardDictionaryBytes() {
        long bytes = 0;
        for (int i = 0, n = boundSlots.size(); i < n; i++) {
            bytes += slots.getQuick(boundSlots.getQuick(i)).dictionary.getForwardMemoryBytes();
        }
        return bytes;
    }

    /**
     * How many strings this registry has interned since it was created. A repair walk that
     * resolves keys it must not add to the durable domain leaves this unchanged.
     */
    public long getInternCount() {
        return internCount;
    }

    /**
     * Bytes the {@code string -> lvId} reverse indexes hold. Rebuildable from the forward
     * half, which is why the design keeps it a candidate for lazy construction.
     */
    public long getReverseDictionaryBytes() {
        long bytes = 0;
        for (int i = 0, n = boundSlots.size(); i < n; i++) {
            bytes += slots.getQuick(boundSlots.getQuick(i)).dictionary.getReverseMemoryBytes();
        }
        return bytes;
    }

    /**
     * The ids every bound dictionary has handed out. Reported per view beside the live key
     * count, because the gap between the two is what a bounded frontier no longer bounds.
     */
    public long getTotalDictionarySize() {
        long total = 0;
        for (int i = 0, n = boundSlots.size(); i < n; i++) {
            total += slots.getQuick(boundSlots.getQuick(i)).dictionary.size();
        }
        return total;
    }

    public boolean isBound(int slot) {
        return slotOf(slot) != null;
    }

    /**
     * Resolves one slot's string for an id this registry handed out. The seal reads the
     * dictionary through here, and so does a repair resolving a persisted key.
     *
     * @return the string, or null when the id is not one this dictionary assigned
     */
    public @Nullable CharSequence lookup(int slot, int lvId) {
        final Slot s = slotOf(slot);
        return s != null && lvId >= 0 ? s.dictionary.valueOf(lvId) : null;
    }

    /**
     * Lowers the id ceiling so a test can drive exhaustion without interning two billion
     * strings. Production runs at {@link #MAX_DICTIONARY_SIZE}.
     */
    @TestOnly
    public void setMaxDictionarySize(int maxDictionarySize) {
        this.maxDictionarySize = maxDictionarySize;
    }

    /**
     * Binds the per-view tracker every dictionary allocation charges. Bind it before the
     * first {@link #bind}: rebinding a live dictionary would have to free its blocks under
     * the tracker that charged them, so it discards their contents instead.
     */
    public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
        for (int i = 0, n = boundSlots.size(); i < n; i++) {
            slots.getQuick(boundSlots.getQuick(i)).dictionary.setMemoryTracker(memoryTracker);
        }
    }

    @Override
    public int translate(int slot, int rawId) {
        final Slot s = slot >= 0 && slot < slots.size() ? slots.getQuick(slot) : null;
        if (s == null || s.armedEpoch != sourceEpoch) {
            // Two failures with one branch on the hot path: a slot nothing bound, and a slot
            // whose cursor did not arm it for this source. Both are wrong-key bugs rather
            // than slow paths, and both stay branches rather than asserts because a build
            // without -ea is where a wrong key costs the most.
            throw unarmed(slot, s);
        }
        if (rawId == SymbolTable.VALUE_IS_NULL) {
            // The only NULL encoding, and never interned: a dictionary entry for it would
            // give NULL two spellings in the same key.
            return SymbolTable.VALUE_IS_NULL;
        }
        if (rawId < 0) {
            // VALUE_NOT_FOUND and -1 land here. Neither is a value, and both are in range as
            // a cache index, so reading one as an id is how a wrong key becomes unnoticeable.
            throw rejectRawId(s, rawId, "negative");
        }
        if (rawId < s.cleanSymbolCount) {
            return translateClean(s, rawId);
        }
        final int dirtyIndex = rawId - s.cleanSymbolCount;
        if (dirtyIndex >= s.dirtyBandSize) {
            throw rejectRawId(s, rawId, "above the source's symbol count");
        }
        return translateDirty(s, dirtyIndex, rawId);
    }

    private static void extendWith(IntList list, int size, int fill) {
        for (int i = list.size(); i < size; i++) {
            list.add(fill);
        }
    }

    private static void extendWith(LongList list, int size, long fill) {
        for (int i = list.size(); i < size; i++) {
            list.add(fill);
        }
    }

    private void arm(int slot, int cleanSymbolCount, int dirtyBandSize, @NotNull SymbolTable resolver) {
        if (!isArming) {
            // Arming outside armFor would stamp the current epoch on a slot no one verified,
            // which is the stale-boundary bug the epoch exists to prevent.
            throw CairoException.critical(0)
                    .put("live view partition key slot armed outside armFor [view=").put(viewToken.getTableName())
                    .put(", slot=").put(slot)
                    .put(']');
        }
        final Slot s = slotOf(slot);
        if (s == null) {
            throw CairoException.critical(0)
                    .put("live view partition key source armed an unbound slot [view=").put(viewToken.getTableName())
                    .put(", slot=").put(slot)
                    .put(']');
        }
        if (cleanSymbolCount < 0 || dirtyBandSize < 0) {
            throw CairoException.critical(0)
                    .put("live view partition key source produced a negative symbol band [view=").put(viewToken.getTableName())
                    .put(", slot=").put(slot)
                    .put(", cleanSymbolCount=").put(cleanSymbolCount)
                    .put(", dirtyBandSize=").put(dirtyBandSize)
                    .put(']');
        }
        s.cleanSymbolCount = cleanSymbolCount;
        s.dirtyBandSize = dirtyBandSize;
        s.resolver = resolver;
        s.armedEpoch = sourceEpoch;
    }

    private int intern(Slot slot, CharSequence value) {
        if (slot.dictionary.size() >= maxDictionarySize) {
            final int existing = slot.dictionary.keyOf(value);
            if (existing >= 0) {
                return existing;
            }
            LOG.error().$("live view partition key dictionary is exhausted [view=").$(viewToken.getTableName())
                    .$(", slot=").$(slot.slot)
                    .$(", baseWriterColumn=").$(slot.baseWriterColumnIndex)
                    .$(", size=").$(slot.dictionary.size())
                    .$(", max=").$(maxDictionarySize).I$();
            throw CairoException.critical(0)
                    .put("live view partition key dictionary is exhausted [view=").put(viewToken.getTableName())
                    .put(", slot=").put(slot.slot)
                    .put(", size=").put(slot.dictionary.size())
                    .put(']');
        }
        final int before = slot.dictionary.size();
        final int lvId = slot.dictionary.intern(value);
        if (slot.dictionary.size() != before) {
            // Counted per id assigned, not per call: a string the dictionary already holds
            // comes back with the id it was given, which is the property that makes a key
            // written on one cycle readable on the next.
            internCount++;
        }
        return lvId;
    }

    private CairoException rejectRawId(Slot slot, int rawId, CharSequence why) {
        return CairoException.critical(0)
                .put("live view partition key cannot translate a raw symbol id [view=").put(viewToken.getTableName())
                .put(", slot=").put(slot.slot)
                .put(", rawId=").put(rawId)
                .put(", cleanSymbolCount=").put(slot.cleanSymbolCount)
                .put(", dirtyBandSize=").put(slot.dirtyBandSize)
                .put(", reason=").put(why)
                .put(']');
    }

    private CharSequence resolve(Slot slot, int rawId) {
        final CharSequence value = slot.resolver.valueOf(rawId);
        if (value == null) {
            // The source claimed the id is in band and then could not name it. Interning a
            // placeholder would put a key in the dictionary that no base row ever carried.
            throw rejectRawId(slot, rawId, "unresolvable in the armed source");
        }
        return value;
    }

    private @Nullable Slot slotOf(int slot) {
        return slot >= 0 && slot < slots.size() ? slots.getQuick(slot) : null;
    }

    private int translateClean(Slot slot, int rawId) {
        // Keyed by base id and kept across epochs: a base symbol map is append-only, so an id
        // resolved under one epoch names the same string under every later one. Only the
        // string SOURCE is per epoch.
        if (rawId < slot.baseIdToLvId.size()) {
            final int cached = slot.baseIdToLvId.getQuick(rawId);
            if (cached != SymbolTable.VALUE_NOT_FOUND) {
                return cached;
            }
        } else {
            extendWith(slot.baseIdToLvId, rawId + 1, SymbolTable.VALUE_NOT_FOUND);
        }
        final int lvId = intern(slot, resolve(slot, rawId));
        slot.baseIdToLvId.setQuick(rawId, lvId);
        return lvId;
    }

    private int translateDirty(Slot slot, int dirtyIndex, int rawId) {
        // Grown on first touch rather than at arming: a bounded or filtered view reads a
        // handful of rows out of a transaction that introduces thousands of symbols, and
        // sizing the scratch to the diff would charge it for every one of them.
        if (dirtyIndex >= slot.dirtyToLvId.size()) {
            extendWith(slot.dirtyToLvId, dirtyIndex + 1, SymbolTable.VALUE_NOT_FOUND);
            extendWith(slot.dirtyEpoch, dirtyIndex + 1, UNARMED_EPOCH);
        } else if (slot.dirtyEpoch.getQuick(dirtyIndex) == sourceEpoch) {
            // Stamped rather than cleared per transaction: the entry is this epoch's, so the
            // scratch a previous transaction left behind is reused without being rewritten.
            return slot.dirtyToLvId.getQuick(dirtyIndex);
        }
        final int lvId = intern(slot, resolve(slot, rawId));
        slot.dirtyToLvId.setQuick(dirtyIndex, lvId);
        slot.dirtyEpoch.setQuick(dirtyIndex, sourceEpoch);
        return lvId;
    }

    private CairoException unarmed(int slot, @Nullable Slot s) {
        if (s == null) {
            return CairoException.critical(0)
                    .put("live view partition key slot is not bound [view=").put(viewToken.getTableName())
                    .put(", slot=").put(slot)
                    .put(']');
        }
        return CairoException.critical(0)
                .put("live view partition key slot is not armed for the current source [view=").put(viewToken.getTableName())
                .put(", slot=").put(slot)
                .put(", slotEpoch=").put(s.armedEpoch)
                .put(", sourceEpoch=").put(sourceEpoch)
                .put(']');
    }

    private final class DisarmHandle implements QuietCloseable {
        @Override
        public void close() {
            disarm();
        }
    }

    private static final class Slot implements QuietCloseable {
        final int baseTableId;
        final int baseWriterColumnIndex;
        final int slot;
        // Lazy baseId -> lvId cache for the clean band, VALUE_NOT_FOUND until earned.
        final IntList baseIdToLvId = new IntList();
        // Dense dirtyRawId - cleanSymbolCount -> lvId remap, valid only for the epoch its
        // parallel stamp names.
        final LongList dirtyEpoch = new LongList();
        final IntList dirtyToLvId = new IntList();
        long armedEpoch = UNARMED_EPOCH;
        int baseScanColumnIndex;
        int cleanSymbolCount;
        DirectSymbolMap dictionary;
        int dirtyBandSize;
        SymbolTable resolver;

        Slot(int slot, int baseScanColumnIndex, int baseWriterColumnIndex, int baseTableId) {
            this.slot = slot;
            this.baseScanColumnIndex = baseScanColumnIndex;
            this.baseWriterColumnIndex = baseWriterColumnIndex;
            this.baseTableId = baseTableId;
        }

        @Override
        public void close() {
            dictionary = Misc.free(dictionary);
            baseIdToLvId.clear();
            dirtyToLvId.clear();
            dirtyEpoch.clear();
            resolver = null;
        }
    }

    /**
     * The arming source every pinned-reader family shares. Reused rather than allocated per
     * cursor open, because the refresh path opens one per repair scan and per replay.
     */
    private static final class StaticSource implements LiveViewSymbolIdSource {
        private SymbolTableSource cursor;

        @Override
        public void armSlot(LiveViewSymbolIdRegistry registry, int slot, int baseScanColumnIndex, int baseWriterColumnIndex) {
            final SymbolTable table = cursor.getSymbolTable(baseScanColumnIndex);
            if (!(table instanceof StaticSymbolTable staticTable)) {
                // The clean-band bound comes from the table's own count, so a source that
                // cannot state one cannot bound the ids its rows carry either.
                throw CairoException.critical(0)
                        .put("live view partition key source has no static symbol table [slot=").put(slot)
                        .put(", baseScanColumn=").put(baseScanColumnIndex)
                        .put(']');
            }
            registry.armStatic(slot, staticTable.getSymbolCount(), staticTable);
        }

        StaticSource of(SymbolTableSource cursor) {
            this.cursor = cursor;
            return this;
        }
    }
}
