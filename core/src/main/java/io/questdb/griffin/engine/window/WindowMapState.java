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
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
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
 *
 * <h2>Ownership</h2>
 * The group owns its map and its key projection and nothing else. The functions it binds keep
 * owning their arguments, their compiled PARTITION BY terms and the private partition map they
 * were built with; that map merely stays closed for the factory's lifetime, so the function's
 * {@code close()} still frees exactly what it always freed and the group's
 * {@link #close()} frees exactly one thing more.
 * <p>
 * The key projection reads the base record's own columns rather than a function's
 * {@code partitionByRecord}, which is what makes that separation possible - and what keeps the
 * group out of the cursor's initialization order, since a sink over base columns has nothing
 * to bind to a symbol source. The first slice admits only direct-column partition keys (see
 * {@link WindowMapSpec}), so there is nothing else such a sink could need.
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
 * the operational escape hatch. Every plan the compiler produces binds otherwise.
 * <p>
 * This build shipped a second rule and then removed it, which is worth stating so it is not
 * written again. It declined a group whose fused value crossed
 * {@code cairo.sql.unordered.map.max.entry.size} while every member's own value stayed under it -
 * two counters over one INT key, {@code 4 + 8 = 12} each against {@code 4 + 16 = 20} fused - on
 * the premise that trading several {@link io.questdb.cairo.map.Unordered4Map} probes for one
 * {@link io.questdb.cairo.map.OrderedMap} probe is a bad trade. Measured over 2e6 rows, it is
 * not: that shape runs at 65.2 ns/row fused against 132.2 unfused over 1e6 keys and 33.2 against
 * 34.7 over 1e3, and {@code sum(x) + sum(y)} - the same trade at the limit a server defaults to -
 * runs at 75.5 against 209.1 and 39.4 against 44.7. A single {@code sum(x)}, no group in the
 * picture at all, says why: 55.5 ns/row on an {@code OrderedMap} against 77.5 on an
 * {@code Unordered4Map} over 1e6 keys, and 22.1 against 19.3 over 1e3. The unordered maps are the
 * faster ones only while the key domain is small, and a window map's cost is concentrated where
 * it is not, so the rule turned down its largest win to buy nothing at the cardinality it was
 * protecting. The same holds for a VARCHAR key and {@link io.questdb.cairo.map.UnorderedVarcharMap}.
 */
public final class WindowMapState implements QuietCloseable, Reopenable {
    private final int componentCount;
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
            @NotNull RecordMetadata baseMetadata
    ) {
        this.plan = plan;
        this.componentCount = plan.getComponentCount();
        this.projectionCount = plan.getProjectionCount();
        this.unorderedMapMaxEntrySize = configuration.getSqlUnorderedMapMaxEntrySize();
        final WindowMapSpec spec = plan.getSpec();
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        appendKeyTypes(spec, keyTypes);
        final ListColumnFilter keyColumnFilter = new ListColumnFilter();
        for (int i = 0, n = spec.getPartitionColumnCount(); i < n; i++) {
            // The RecordSink contract: the filter holds 1-based indexes into the source
            // record's metadata, and the ColumnTypes argument carries that whole metadata's
            // types rather than the filtered subset's.
            keyColumnFilter.add(spec.getPartitionColumnIndex(i) + 1);
        }
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        plan.buildMapValueTypes(valueTypes);
        // Built before the map so a failure here cannot strand a tracked allocation.
        this.keySink = RecordSinkFactory.getInstance(configuration, asm, baseMetadata, keyColumnFilter, null);
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
     * @param plans        the compiled groups, or null when the query formed none
     * @param baseMetadata the metadata the window functions and their PARTITION BY terms were
     *                     compiled against, and the record the key projection reads
     */
    public static @Nullable ObjList<WindowMapState> createGroups(
            @NotNull CairoConfiguration configuration,
            @NotNull BytecodeAssembler asm,
            @Nullable ObjList<WindowAccumulatorPlan> plans,
            @NotNull RecordMetadata baseMetadata
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
                states.add(new WindowMapState(configuration, asm, plans.getQuick(i), baseMetadata));
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
     * Absorbs one row into every component of the group and materializes every output from the
     * result. The one lookup this makes is the whole point of the group.
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
     */
    public void computeNext(Record record) {
        final MapKey key = map.withKey();
        key.put(record, keySink);
        final MapValue value = key.createValue();
        if (value.isNew()) {
            for (int c = 0; c < componentCount; c++) {
                plan.getComponent(c).resetState(value, plan.getComponentSlotBase(c));
            }
        }
        for (int c = 0; c < componentCount; c++) {
            plan.getContributor(c).accumulateWindowState(record, value);
        }
        for (int p = 0; p < projectionCount; p++) {
            plan.getProjectionFunction(p).projectWindowState(record, value);
        }
        lookupCount++;
        updateCount += componentCount;
        projectionWriteCount += projectionCount;
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
     * read the value back. Structural rather than timed: a lookup reduction that is only
     * visible in elapsed time is not a measurement.
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

    private void resetStructuralCounters() {
        lookupCount = 0;
        updateCount = 0;
        projectionWriteCount = 0;
    }
}
