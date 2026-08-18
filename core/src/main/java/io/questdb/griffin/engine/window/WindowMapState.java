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
import io.questdb.std.Numbers;
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
 * The sequence a row runs through is always the same: load the value once, put a new entry's
 * components to identity, run every contributor, then run every projection. What the group
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
 * <h2>What a refused row costs</h2>
 * The one lookup a row is a saving over the several a group replaces, and it is not a saving
 * over none. Unfused {@code sum} and {@code avg} test their DOUBLE argument before they touch
 * their maps, so a NULL-heavy partition costs them an argument evaluation a row and no probe at
 * all, while the fused sequence above writes the key, creates the value and dispatches to the
 * contributor before the contributor decides the row was never its business. Measurement on
 * mostly-NULL DOUBLE input found that unconditional work outweighed the probe the group
 * consolidates, which is what {@link #isPass1SkipEnabled()} answers to: a two-pass group whose
 * components are every one
 * {@link WindowAccumulatorDescriptor#isRefusedRowInert() inert on a refused row} evaluates the
 * predicate itself and leaves the row alone when the answer is no. That the group may then have
 * no entry for a partition is pass 2's to settle - see {@link #projectPass2(Record)} - and it is
 * the whole of what the skip changes about the map.
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
 * the operational escape hatch. It is reloadable and read once per compile, so flipping it moves
 * the next compile and nothing already compiled - a cached select plan keeps the binding it was
 * built with until the cache is flushed. Every plan the compiler produces binds otherwise. In
 * particular a fused value that crosses {@code cairo.sql.unordered.map.max.entry.size} still binds: the wider
 * value may move the group's map from {@link io.questdb.cairo.map.Unordered4Map} to
 * {@link io.questdb.cairo.map.OrderedMap}, and measurement says that trade is worth taking,
 * because the unordered maps lead only while the key domain is small and a window map's cost is
 * concentrated where it is not.
 */
public final class WindowMapState implements QuietCloseable, Reopenable {
    private final int componentCount;
    /**
     * The contributors' arguments, one per component, in component order - borrowed exactly as
     * {@link #keyRecord}'s terms are, and null unless {@link #isPass1SkipEnabled()} holds. Read
     * only to evaluate the components' shared contribution predicate ahead of the map work; the
     * window functions own these and free them.
     */
    private final ObjList<Function> contributorArguments;
    private final boolean isPass1SkipEnabled;
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
        // here. Every plan carries one: WindowAccumulatorPlanBuilder.compileGroups skips a
        // function whose spec is null and opens a builder per spec over the rest.
        final WindowMapSpec spec = plan.getSpec();
        assert spec != null;
        // Every member of a group agrees with its spec on how many passes the traversal takes,
        // so this is the group's pass structure and not one function's.
        this.isTwoPass = spec.getPassCount() > WindowFunction.ONE_PASS;
        // Only a two-pass group can leave a row's key out of pass 1: a one-pass group projects
        // from the value it just loaded, so it needs the entry for every row whether the row
        // contributed to it or not.
        final ObjList<Function> skipArguments = isTwoPass ? contributorArgumentsForSkip(plan) : null;
        this.contributorArguments = skipArguments;
        this.isPass1SkipEnabled = skipArguments != null;
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
     * <p>
     * A two-pass group whose components are all {@link WindowAccumulatorDescriptor#isRefusedRowInert()
     * inert on a refused row} skips the whole sequence for a row not one of them would absorb -
     * the key write, the lookup, the identity put and the contributor dispatch alike - because
     * every one of those would leave the value exactly as it found it. What the row costs then
     * is one evaluation of the shared contribution predicate, which is what the unfused
     * functions charge a refused row and less than the fused probe was charging it. Pass 2
     * answers for the partitions this leaves out of the map: see {@link #projectPass2}.
     */
    public void computeNext(Record record) {
        if (isPass1SkipEnabled && isRowRefusedByEveryComponent(record)) {
            return;
        }
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
        }
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

    /**
     * The number of distinct keys the group currently holds. Tests use this to observe the
     * pass-1 key domain directly without adding work to the per-row production path.
     */
    @TestOnly
    public long getMapSize() {
        return map.size();
    }

    public WindowAccumulatorPlan getPlan() {
        return plan;
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
     * Whether this group skips the pass-1 map work for a row every one of its components
     * refuses. A compile-time fact: it holds for a two-pass group whose components are every one
     * {@link WindowAccumulatorDescriptor#isRefusedRowInert() inert on a refused row}, and the
     * group's pass 2 creates the entries it leaves out - see {@link #projectPass2}.
     */
    @TestOnly
    public boolean isPass1SkipEnabled() {
        return isPass1SkipEnabled;
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
     * The lookup is a {@link MapKey#findValue()}: pass 1 created an entry for every row the two
     * passes walk - it creates one unconditionally, where a function's own {@code pass1} may
     * not - so there is nothing to insert for a group that skips nothing, and inserting would
     * grow a key domain that is supposed to be closed by now.
     * <p>
     * A group that does skip closes its key domain here instead, and only where the lookup
     * misses. Pass 1 leaves out exactly the partitions nothing contributed to, so a missing
     * entry is that partition's own answer rather than a lost one, and what it projects is the
     * identity every component would still be sitting at had pass 1 created it - a NULL sum, a
     * NULL average, a zero count. Creating it now rather than reading identity out of thin air
     * is what keeps the projections reading a real value and the key domain the same one the
     * unskipped group ends with, so the two bindings agree on the map's size as well as on its
     * answers.
     * <p>
     * There is deliberately no accumulation. A projection reads slots and writes no state, so
     * running this over a row a second time is idempotent, which is what a cached cursor's
     * random access and its second drain need - and the create above is idempotent for the same
     * reason, since the second visit finds the entry the first one left.
     */
    public void projectPass2(Record record) {
        final MapKey key = map.withKey();
        putKey(key, record);
        MapValue value = key.findValue();
        if (value == null) {
            // A group that skips nothing created an entry for every row the two passes walk, so
            // only a skipping group can miss here - and it misses exactly on the partitions
            // whose every row its components refused.
            assert isPass1SkipEnabled;
            value = putIdentity(record);
        }
        for (int p = 0; p < projectionCount; p++) {
            plan.getProjectionFunction(p).projectWindowState(record, value);
        }
    }

    /**
     * Allocates the group's map backing. The owning cursor calls it once per open, after
     * {@link #setMemoryTracker} has bound the per-query tracker, so the allocation is charged
     * where {@link #reset()} will later give it back.
     */
    @Override
    public void reopen() {
        map.reopen();
    }

    /**
     * Hands the group's map backing back at cursor close, leaving the group reusable: a
     * subsequent {@link #reopen()} allocates it again under whatever tracker is bound then.
     */
    public void reset() {
        map.close();
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
     * Returns the contributors' arguments, in component order, for a plan whose pass 1 may skip
     * a refused row - or null for one whose may not, which is the answer for every plan holding
     * a component that is not
     * {@link WindowAccumulatorDescriptor#isRefusedRowInert() inert on a refused row}.
     * <p>
     * One component that is not inert disables the skip for the whole group rather than for
     * itself: the group makes one decision per row, and a row {@code sum(x)} refuses is still a
     * row {@code count(*)} beside it counts.
     * <p>
     * The argument is the <b>contributor's</b> and not the component's, which is the same column
     * read through the object that already reads it: an inert component's identity carries a
     * direct column reference of the record's own type, and the contributor's own
     * {@code accumulateWindowState} evaluates exactly this function. A contributor that reports
     * no argument declines the group, which cannot happen for the families admitted here - every
     * one of them takes an argument - and is the honest answer rather than a cast.
     */
    private static @Nullable ObjList<Function> contributorArgumentsForSkip(@NotNull WindowAccumulatorPlan plan) {
        final int componentCount = plan.getComponentCount();
        if (componentCount == 0) {
            return null;
        }
        final ObjList<Function> arguments = new ObjList<>(componentCount);
        for (int c = 0; c < componentCount; c++) {
            if (!plan.getComponent(c).isRefusedRowInert()) {
                return null;
            }
            final Function argument = plan.getContributor(c).windowAccumulatorArgument();
            if (argument == null) {
                return null;
            }
            arguments.add(argument);
        }
        return arguments;
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
     * Whether no component of the group would absorb this row, which is what lets pass 1 leave
     * the row's key out of the map.
     * <p>
     * The predicate is {@link WindowAccumulatorDescriptor#CONTRIBUTION_FINITE_DOUBLE}'s, which
     * every component of a skipping group carries, so one expression answers for all of them.
     * The walk stops at the first component that would absorb the row, so the dense case pays
     * for one argument evaluation and the row goes on to the ordinary sequence.
     */
    private boolean isRowRefusedByEveryComponent(Record record) {
        for (int c = 0; c < componentCount; c++) {
            if (Numbers.isFinite(contributorArguments.getQuick(c).getDouble(record))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates the entry for a partition pass 1 skipped whole and puts every component of it to
     * identity, returning the value the caller's projections then read.
     * <p>
     * A second key write rather than a {@code createValue()} on the key
     * {@link #projectPass2(Record)} just looked up with: pass 2's hot path is the lookup that
     * finds an entry, and a {@code findValue()} is the cheaper of the two ways to make it. What
     * this costs is one extra key write on the rows of a partition no row contributed to, which
     * is the population the skip exists for.
     */
    private MapValue putIdentity(Record record) {
        final MapKey key = map.withKey();
        putKey(key, record);
        final MapValue value = key.createValue();
        if (value.isNew()) {
            for (int c = 0; c < componentCount; c++) {
                plan.getComponent(c).resetState(value, plan.getComponentSlotBase(c));
            }
        }
        return value;
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

}
