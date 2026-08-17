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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * The runtime owner of one window Map group: one {@link Map} keyed by the window's PARTITION
 * BY columns, whose value carries every accumulator component a {@link WindowAccumulatorPlan}
 * laid out, and the one lookup per row that serves all of them.
 * <p>
 * It is the ordinary-query counterpart of what {@code LiveViewWindow} does for an anchored
 * live view, and the row sequence is deliberately the same one: load the value once, put a new
 * entry's components to identity, run every contributor, then run every projection. What it
 * does not carry is anything durable - no anchor, no bucket, no dirty set, no manifest - so
 * the whole of its state is a map that lives and dies with the cursor.
 * <p>
 * Two owners drive it. {@link WindowRecordCursorFactory} runs a group per row of the base
 * scan, ahead of the {@code computeNext} dispatch a bound member no-ops in; the cached
 * factories run it inside the traversal of the sort bucket its members belong to, ahead of
 * their {@code pass1} - see {@link CachedWindowMapGroups}. The group is the same object in
 * both, and what differs is only which record the row arrives on.
 * <p>
 * A cached factory adds one thing the streaming one has no use for. A group whose functions
 * read the whole partition rather than the rows the traversal has already passed is driven
 * twice: {@link #computeNext(Record)} absorbs the row in pass 1 and projects nothing, and
 * {@link #projectPass2(Record)} materializes every output in pass 2 from the accumulator
 * pass 1 left final. That split is what replaces the destructive finalization those
 * functions perform on their own maps - {@code avg}'s {@code preparePass2} overwrites the
 * sum slot a {@code sum} projection still needs - with arithmetic each projection does for
 * itself, off state the group never rewrites.
 *
 * <h2>Ownership</h2>
 * The group owns its map and its key projection and nothing else. The functions it binds keep
 * owning their arguments, their compiled PARTITION BY terms and the private partition map they
 * were built with; that map merely stays closed for the factory's lifetime, so the function's
 * {@code close()} still frees exactly what it always freed and the group's
 * {@link #close()} frees exactly one thing more.
 * <p>
 * The key is written one of two ways, and which one is the spec's answer rather than the
 * owner's:
 * <ul>
 *     <li><b>direct columns</b> - a sink over the record's own column indexes, called with the
 *     row record itself. It borrows nothing and has nothing to bind to a symbol source, which
 *     is what keeps such a group out of the cursor's initialization order entirely;</li>
 *     <li><b>an expression key</b> - a sink over the compiled PARTITION BY terms, called with
 *     a {@link VirtualRecord} of this group's own positioned on the row. The terms are
 *     <b>borrowed</b> from the function the spec was snapshotted for and are never freed here,
 *     which is why the wrapper is this group's rather than that function's: a
 *     {@code VirtualRecord}'s own {@code close()} frees the functions inside it, and this one
 *     must never be closed. That the borrowed terms are initialized by the time a row arrives
 *     follows from the owner's order - every window function's {@code init} runs before the
 *     first row of any traversal, and the terms are that function's own.</li>
 * </ul>
 * Which record the row arrives on belongs to the owner: the base record on the streaming
 * cursor, and the sorted chain record on a cached one, both of which the group's PARTITION BY
 * terms were resolved against. One evaluation of an expression key a row serves the whole
 * group, which is a saving the direct-column case has no equivalent of.
 *
 * <h2>What this binds</h2>
 * Every plan the compiler produces, whether its outputs each keep a component of their own -
 * {@code sum(x)} beside {@code count(y)} - or share one. A shared component is the second half
 * of the optimization and the larger one: {@code sum(x) + avg(x) + count(x)} is one running
 * {@code (sum, nonNullCount)} pair that one contributor maintains, so the group removes two
 * accumulator updates and one argument evaluation a row on top of the two maps and two probes.
 * The relations it may share through are the plan's - see
 * {@link WindowAccumulatorDescriptor#derivedSlotOffset} - and are never inferred here.
 * <p>
 * What a shared component costs the row sequence below is nothing: a projection reads slots and
 * writes no state, so it does not matter to it whether the slots it reads are its own function's
 * or a host's.
 *
 * <h2>What declines</h2>
 * One thing stops a compiled plan from getting a runtime: {@code cairo.sql.window.map.fusion.enabled},
 * the operational escape hatch. Every plan the compiler produces binds otherwise. In particular a
 * fused value that crosses {@code cairo.sql.unordered.map.max.entry.size} still binds: the wider
 * value may move the group's map from {@link io.questdb.cairo.map.Unordered4Map} to
 * {@link io.questdb.cairo.map.OrderedMap}, and measurement says that trade is worth taking,
 * because the unordered maps lead only while the key domain is small and a window map's cost is
 * concentrated where it is not.
 */
public final class WindowMapState implements QuietCloseable, Reopenable {
    private final int componentCount;
    private final boolean isTwoPass;
    /**
     * The group's own wrapper over the borrowed PARTITION BY terms, or null when the key is
     * direct columns and the row record carries it. Deliberately never closed: closing a
     * {@link VirtualRecord} frees the functions inside it, and these are the compiling
     * function's.
     */
    private final VirtualRecord keyRecord;
    private final RecordSink keySink;
    private final Map map;
    private final WindowAccumulatorPlan plan;
    private final int projectionCount;
    private final int unorderedMapMaxEntrySize;
    private long lookupCount;
    private long projectionWriteCount;
    private long updateCount;

    private WindowMapState(
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @NotNull WindowAccumulatorPlan plan,
            @NotNull ColumnTypes recordTypes
    ) {
        this.plan = plan;
        this.componentCount = plan.getComponentCount();
        this.projectionCount = plan.getProjectionCount();
        this.unorderedMapMaxEntrySize = configuration.getSqlUnorderedMapMaxEntrySize();
        // A group's map is keyed by its spec, so only a plan that carries one is bindable
        // here. A live view's plan does not - it is owned by LiveViewWindow, which keys the
        // fused entry off its own anchor map - and never reaches this constructor.
        final WindowMapSpec spec = plan.getSpec();
        assert spec != null;
        // Every member of a group agrees with its spec on how many passes the traversal takes,
        // so this is the group's pass structure and not one function's.
        this.isTwoPass = spec.getPassCount() > WindowFunction.ONE_PASS;
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        appendKeyTypes(spec, keyTypes);
        final ObjList<? extends Function> keyFunctions = spec.getPartitionByFunctions();
        final ListColumnFilter keyColumnFilter = new ListColumnFilter();
        final ColumnTypes sinkTypes;
        if (keyFunctions != null) {
            // An expression key: the sink reads the group's own virtual record, whose columns
            // are the terms themselves and whose types are therefore the key's own - so the
            // filter is the identity over them, exactly as the compiler builds each function's.
            this.keyRecord = new VirtualRecord(keyFunctions);
            sinkTypes = keyTypes;
            for (int i = 0, n = spec.getKeyColumnCount(); i < n; i++) {
                keyColumnFilter.add(i + 1);
            }
        } else {
            this.keyRecord = null;
            sinkTypes = recordTypes;
            for (int i = 0, n = spec.getPartitionColumnCount(); i < n; i++) {
                // The RecordSink contract: the filter holds 1-based indexes into the source
                // record's metadata, and the ColumnTypes argument carries that whole metadata's
                // types rather than the filtered subset's.
                keyColumnFilter.add(spec.getPartitionColumnIndex(i) + 1);
            }
        }
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        plan.buildMapValueTypes(valueTypes);
        // Built before the map so a failure here cannot strand a tracked allocation.
        this.keySink = RecordSinkFactory.getInstance(configuration, asm, sinkTypes, keyColumnFilter, null);
        // Lazily opened, like every other tracker-aware window state: the owning cursor binds
        // the per-query MemoryTracker and only then reopens, so the backing's malloc and its
        // free are charged to the same counter.
        this.map = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes, false, false);
    }

    /**
     * Builds one runtime group per compiled plan, or null when the query compiled none and when
     * the kill switch is off.
     * <p>
     * Binding happens here rather than at cursor start because it is a compile-time fact: the
     * slots a projection reads are the plan's, the plan is the factory's, and a function bound
     * once stays bound for the factory's whole life. That is also what leaves the private maps
     * closed - {@code reopen()} skips a map whose function reports
     * {@link WindowFunction#isWindowStateOwned()}.
     *
     * @param plans       the compiled groups, or null when the query formed none
     * @param recordTypes the types, by index, of the record the group is driven with - the
     *                    base record for a streaming compile and the chain record for a
     *                    cached one, both of which the group's PARTITION BY terms were
     *                    resolved against. Read to build a direct-column key sink; an
     *                    expression key's sink is built over the terms' own types instead
     */
    public static @Nullable ObjList<WindowMapState> createGroups(
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @Nullable ObjList<WindowAccumulatorPlan> plans,
            @NotNull ColumnTypes recordTypes
    ) {
        // The switch gates the binding rather than the compile: what it turns off is a runtime
        // that owns a map, and a plan that no runtime reads costs a query nothing. So the group
        // this query forms stays visible either way, and the two settings differ in exactly one
        // thing - whether the functions keep their own maps.
        if (plans == null || plans.size() == 0 || !configuration.isSqlWindowMapFusionEnabled()) {
            return null;
        }
        final ObjList<WindowMapState> states = new ObjList<>(plans.size());
        try {
            for (int i = 0, n = plans.size(); i < n; i++) {
                states.add(new WindowMapState(configuration, asm, plans.getQuick(i), recordTypes));
            }
        } catch (Throwable th) {
            Misc.freeObjList(states);
            throw th;
        }
        for (int i = 0, n = states.size(); i < n; i++) {
            states.getQuick(i).bindProjectionFunctions();
        }
        return states;
    }

    /**
     * Empties the group's key domain, leaving the map's backing allocated. The cursor's
     * {@code toTop} calls it exactly once per group - not once per bound function, which is
     * the same shared state seen several times.
     */
    public void clear() {
        if (map.isOpen()) {
            map.clear();
        }
        resetStructuralCounters();
    }

    @Override
    public void close() {
        Misc.free(map);
    }

    /**
     * Absorbs one row into every component of the group and, for a group whose outputs are
     * final by the end of the traversal, materializes every one of them from the result. The
     * one lookup this makes is the whole point of the group.
     * <p>
     * Three things about the sequence are load-bearing:
     * <ul>
     *     <li><b>a new entry is put to identity before anything reads it.</b> No {@link Map}
     *     implementation promises a fresh value's slots are zero-filled - cleared or
     *     reallocated backing can carry whatever the previous occupant left - and only the
     *     first component would notice through {@code isNew()} anyway;</li>
     *     <li><b>contributors run before projections, in two loops.</b> One component may
     *     serve several outputs, so interleaving them would make an output's value depend on
     *     where its call sits in the SELECT list;</li>
     *     <li><b>the value handle is used and dropped.</b> Nothing here rebuilds or rehashes
     *     the map behind the loops, so the handle stays valid for the whole row.</li>
     * </ul>
     * <p>
     * A <b>two-pass group</b> - one whose functions read the whole partition rather than the
     * rows the traversal has already passed - projects nothing here. Its accumulator is not
     * final until the last row of pass 1 has been absorbed, so every output it could
     * materialize now would be overwritten by {@link #projectPass2(Record)}, and the group is
     * a projection loop a row better off not running one. Which of the two this is follows
     * from the spec every member shares, so it is the group's shape rather than a caller's
     * choice.
     */
    public void computeNext(Record record) {
        final MapKey key = map.withKey();
        putKey(key, record);
        final MapValue value = key.createValue();
        if (value.isNew()) {
            for (int c = 0; c < componentCount; c++) {
                plan.getComponent(c).resetState(value, plan.getComponentSlotBase(c));
            }
        }
        for (int c = 0; c < componentCount; c++) {
            plan.getContributor(c).accumulateWindowState(record, value);
        }
        if (!isTwoPass) {
            for (int p = 0; p < projectionCount; p++) {
                plan.getProjectionFunction(p).projectWindowState(record, value);
            }
            projectionWriteCount += projectionCount;
        }
        lookupCount++;
        updateCount += componentCount;
    }

    /**
     * The number of times a contributor absorbed a row, which is the lookup count times the
     * component count. Beside {@link #getLookupCount()} it is what says a group removed
     * updates rather than only maps.
     */
    @TestOnly
    public long getContributorUpdateCount() {
        return updateCount;
    }

    /**
     * The number of rows this group looked its key up for - one per row, however many outputs
     * read the value back, and two per row for a two-pass group, which probes once in each
     * traversal. Structural rather than timed: a lookup reduction that is only visible in
     * elapsed time is not a measurement.
     */
    @TestOnly
    public long getLookupCount() {
        return lookupCount;
    }

    /**
     * The concrete {@link Map} implementation {@code MapFactory} selected for the group's
     * key and its widened value. Reported beside {@link #getUnorderedMapMaxEntrySize()},
     * because the selection is a function of both and neither alone explains it.
     */
    @TestOnly
    public String getMapImplementation() {
        return map.getClass().getSimpleName();
    }

    public WindowAccumulatorPlan getPlan() {
        return plan;
    }

    @TestOnly
    public long getProjectionWriteCount() {
        return projectionWriteCount;
    }

    /**
     * The configured {@code cairo.sql.unordered.map.max.entry.size} this group's map was
     * selected under. It is 16 in {@code DefaultCairoConfiguration}, which embedded use and the
     * benchmarks take, and 32 by default in a server - enough to move a shape between
     * {@code Unordered4Map} and {@code OrderedMap}, so a claim about the implementation has to
     * name which of the two it holds for.
     */
    @TestOnly
    public int getUnorderedMapMaxEntrySize() {
        return unorderedMapMaxEntrySize;
    }

    /**
     * Whether the group's map currently holds native backing. The cursor's open/close is what
     * moves it, and it is what says a failed open left nothing allocated behind it.
     */
    @TestOnly
    public boolean isMapOpen() {
        return map.isOpen();
    }

    /**
     * Whether this group's outputs are written by a second traversal - which is what makes it
     * the owner's business, since a two-pass group has to be driven from the pass-2 loop as
     * well as the pass-1 one. See {@link CachedWindowMapGroups}, the only owner that has both.
     */
    public boolean isTwoPass() {
        return isTwoPass;
    }

    /**
     * Materializes every output of a two-pass group from the accumulator pass 1 left final,
     * for the row {@code record} is positioned on.
     * <p>
     * The lookup is a {@link MapKey#findValue()} rather than a {@code createValue()}: pass 1
     * created an entry for every row the two passes walk - it creates one unconditionally,
     * where a function's own {@code pass1} may not - so there is nothing here to insert, and
     * inserting would grow a key domain that is supposed to be closed by now.
     * <p>
     * There is deliberately no accumulation. A projection reads slots and writes no state, so
     * running this over a row a second time is idempotent, which is what a cached cursor's
     * random access and its second drain need.
     */
    public void projectPass2(Record record) {
        final MapKey key = map.withKey();
        putKey(key, record);
        final MapValue value = key.findValue();
        // Pass 1 walked the same rows and created an entry for each, so the key is there.
        assert value != null;
        for (int p = 0; p < projectionCount; p++) {
            plan.getProjectionFunction(p).projectWindowState(record, value);
        }
        lookupCount++;
        projectionWriteCount += projectionCount;
    }

    /**
     * Allocates the group's map backing. The owning cursor calls it once per open, after
     * {@link #setMemoryTracker} has bound the per-query tracker, so the allocation is charged
     * where {@link #reset()} will later give it back.
     */
    @Override
    public void reopen() {
        map.reopen();
        resetStructuralCounters();
    }

    /**
     * Hands the group's map backing back at cursor close, leaving the group reusable: a
     * subsequent {@link #reopen()} allocates it again under whatever tracker is bound then.
     */
    public void reset() {
        map.close();
        resetStructuralCounters();
    }

    public void setMemoryTracker(@Nullable MemoryTracker tracker) {
        map.setMemoryTracker(tracker);
    }

    private static void appendKeyTypes(@NotNull WindowMapSpec spec, @NotNull ArrayColumnTypes types) {
        for (int i = 0, n = spec.getKeyColumnCount(); i < n; i++) {
            types.add(spec.getKeyColumnType(i));
        }
    }

    /**
     * Hands every output of the group the slots it reads out of the shared value. Done once,
     * at compile time: a bound function's {@code computeNext} is a no-op from here on and its
     * private map stays closed, both of which the factory relies on for its whole life.
     */
    private void bindProjectionFunctions() {
        for (int i = 0; i < projectionCount; i++) {
            plan.getProjectionFunction(i).bindWindowStateSlots(plan.getProjection(i));
        }
    }

    /**
     * Writes the row's key onto {@code key}, through the compiled PARTITION BY terms where the
     * key is an expression and off the record's own columns where it is not.
     * <p>
     * The virtual record is positioned on every row rather than once: it is this group's, but
     * the row it reads is the traversal's, and a group is driven from more than one of them.
     */
    private void putKey(MapKey key, Record record) {
        if (keyRecord != null) {
            keyRecord.of(record);
            key.put(keyRecord, keySink);
        } else {
            key.put(record, keySink);
        }
    }

    private void resetStructuralCounters() {
        lookupCount = 0;
        updateCount = 0;
        projectionWriteCount = 0;
    }
}
