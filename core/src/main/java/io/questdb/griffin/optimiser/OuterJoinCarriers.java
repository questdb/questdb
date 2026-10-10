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

package io.questdb.griffin.optimiser;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;


/**
 * Satisfies the outer columns of a block whose source join has a step that null-extends its master: a RIGHT, FULL or
 * SPLICE join. Every outer column gets a carrier, a column that holds the outer value on every row the join holds at a
 * step and that the join has not null-extended there. The first step that reads an outer column introduces the
 * carriers: a domain crossed into the leading input, or the slave's own when the step is a RIGHT join. Every RIGHT or
 * FULL step from there on gives its slave carriers keyed on the carriers before it, so each row it preserves still
 * carries the outer values, and its ON condition reads the slave's carriers. A FULL step then splits the join: the
 * steps up to it become a nested join under a projection that takes each carrier from the master side, unless a
 * marker column the master side holds reads NULL, and from the slave side otherwise.
 */
final class OuterJoinCarriers implements Mutable {
    static final String SPLICE_CORRELATION = "outer column reference at or before a SPLICE join is not supported in a LATERAL sub-query";
    private final IntList carrierIds = new IntList();
    private final DecorrelationContext ctx;
    private final DecorrelationDomains domains;
    private final IntIntHashMap forwardedIds = new IntIntHashMap();
    private final CorrelationKeys keys;
    private final IntList slaveIds = new IntList();
    private int markerId;
    private int slaveMarkerId;

    OuterJoinCarriers(DecorrelationContext ctx, DecorrelationDomains domains, CorrelationKeys keys) {
        this.ctx = ctx;
        this.domains = domains;
        this.keys = keys;
    }

    @Override
    public void clear() {
        carrierIds.clear();
        forwardedIds.clear();
        slaveIds.clear();
        markerId = -1;
        slaveMarkerId = -1;
    }

    /**
     * True when the first step at or after {@code from} and up to {@code last} that null-extends its master is a FULL
     * join, whose carriers need a marker on its master side.
     */
    private static boolean isFullNext(ObjList<JoinInput> steps, int from, int last) {
        for (int i = from; i <= last; i++) {
            final JoinKind type = steps.getQuick(i).getJoinType();
            if (type.isMasterNulling()) {
                return type.isSlaveNulling();
            }
        }
        return false;
    }

    private void addCarrierKey(JoinInput step, int masterId, int slaveId, CharSequence name) {
        step.addKey(masterId, slaveId, name, name, step.getPosition());
        ctx.addCarrier(step, masterId);
        ctx.addCarrier(step, slaveId);
    }

    /**
     * A domain of the outer columns whose columns {@link #slaveIds} holds, with a marker column when
     * {@code isMarked}; the mapping stack stays as it was.
     */
    private AggregatePlan carrierDomain(boolean isMarked, int position) throws SqlException {
        final AggregatePlan domain = domains.buildDomain(position);
        final int count = domains.domainOuterIds.size();
        final int lo = ctx.mappedOuterIds.size() - count;
        slaveIds.clear();
        for (int i = 0; i < count; i++) {
            slaveIds.add(ctx.mappedColumnIds.getQuick(lo + i));
        }
        ctx.mappedOuterIds.setPos(lo);
        ctx.mappedColumnIds.setPos(lo);
        slaveMarkerId = -1;
        if (isMarked) {
            ctx.context.getCallArguments().clear();
            final FunctionExpression marker = (FunctionExpression) ctx.context.bindCall("count", position, domain.getInput().getOutput());
            domain.getAggregates().add(marker);
            slaveMarkerId = ctx.context.newColumnId();
            domain.getOutput().add(slaveMarkerId, markerName(), marker.getDataType(), false);
        }
        return domain;
    }

    private int carrierOf(int outerId, int base) {
        final int index = domains.domainOuterIds.indexOf(outerId, 0, domains.domainOuterIds.size());
        return index > -1 ? carrierIds.getQuick(index) : ctx.mappedColumn(outerId, base, ctx.mappedOuterIds.size());
    }

    /**
     * Gives the input carriers: its own mapped columns when they map every outer column and no marker is needed,
     * otherwise a domain crossed into it and keyed on the columns it maps. {@link #slaveIds} holds the carriers.
     */
    private void cross(JoinPlan join, JoinInput input, int deferredBase, boolean isMarked, int position) throws SqlException {
        final IntList outerIds = domains.domainOuterIds;
        if (!isMarked && mapsEvery(input, deferredBase)) {
            slaveIds.clear();
            for (int i = 0, n = outerIds.size(); i < n; i++) {
                slaveIds.add(keys.deferredColumn(input, outerIds.getQuick(i), deferredBase));
            }
            slaveMarkerId = -1;
            return;
        }
        final AggregatePlan domain = carrierDomain(isMarked, position);
        final JoinPlan crossed = domains.crossDomain(input.getInput(), domain, position);
        final JoinInput domainStep = crossed.getInputs().getQuick(1);
        for (int i = 0, n = outerIds.size(); i < n; i++) {
            final int columnId = keys.deferredColumn(input, outerIds.getQuick(i), deferredBase);
            if (columnId > -1) {
                addCarrierKey(domainStep, columnId, slaveIds.getQuick(i), ctx.outerRefName(outerIds.getQuick(i)));
            }
        }
        input.setInput(crossed);
        join.addMissingInputColumns();
    }

    /**
     * Keys a deferred input on the carriers of the outer columns it maps; {@code isOutsideDomain} limits the keys
     * to the outer columns WHERE equalities map.
     */
    private void keyDeferred(JoinInput input, int base, int deferredBase, boolean isOutsideDomain) {
        for (int i = deferredBase, n = keys.deferredInputs.size(); i < n; i++) {
            if (keys.deferredInputs.getQuick(i) != input) {
                continue;
            }
            final int outerId = keys.deferredOuterIds.getQuick(i);
            if (isOutsideDomain && domains.domainOuterIds.contains(outerId)) {
                continue;
            }
            final int carrierId = carrierOf(outerId, base);
            if (carrierId > -1 && input.getSourceOutput().getColumnIndexById(carrierId) < 0) {
                addCarrierKey(input, carrierId, keys.deferredColumnIds.getQuick(i), ctx.outerRefName(outerId));
            }
        }
    }

    private boolean mapsEvery(JoinInput input, int deferredBase) {
        final IntList outerIds = domains.domainOuterIds;
        for (int i = 0, n = outerIds.size(); i < n; i++) {
            if (keys.deferredColumn(input, outerIds.getQuick(i), deferredBase) < 0) {
                return false;
            }
        }
        return true;
    }

    private CharSequence markerName() {
        final CharacterStoreEntry name = ctx.characterStore.newEntry();
        name.put(DecorrelationContext.OUTER_REF_PREFIX).put("marker_").put(ctx.carrierSequence++);
        return name.toImmutable();
    }

    /**
     * Reads, in the step's ON condition, key filter and, when {@code isFilterRead}, post-join filter, each outer column
     * a WHERE equality maps through its mapped column and each other outer column through {@code carriers}.
     */
    private void remap(JoinInput step, IntList carriers, int base, boolean isFilterRead) {
        ctx.loadMappedSubstitution(base);
        final IntList outerIds = domains.domainOuterIds;
        for (int i = 0, n = outerIds.size(); i < n; i++) {
            if (carriers.getQuick(i) > -1) {
                ctx.substitution.put(outerIds.getQuick(i), carriers.getQuick(i));
            }
        }
        ctx.remapStepConditions(step, isFilterRead);
    }

    /**
     * Moves the steps up to the FULL step at {@code full} into a nested join under a projection that forwards their
     * columns and computes each carrier, and the step's post-join filter above the projection. Returns the projection,
     * or the filter, when the FULL step is the last step; otherwise the join, which leads with that plan.
     */
    private LogicalPlan split(JoinPlan join, int full, int base, int chainBase, boolean isMarked, int position) throws SqlException {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final ObjList<JoinInput> inputs = join.getInputs();
        final JoinInput fullStep = ordered.getQuick(full);
        final JoinPlan nested = ctx.planNodes.joins.next().of(position);
        nested.setExplicitTimestamp(false);
        for (int i = 0; i <= full; i++) {
            nested.getOrderedInputs().add(ordered.getQuick(i));
        }
        for (int i = 0, n = inputs.size(); i < n; i++) {
            final JoinInput input = inputs.getQuick(i);
            if (nested.getOrderedInputs().indexOf(input) > -1) {
                nested.getInputs().add(input);
                nested.getOutput().addColumnsFrom(input.getSourceOutput(), input.getBindingAlias());
            }
        }
        final OutputSchema nestedOutput = nested.getOutput();
        forwardedIds.clear();
        final ProjectPlan project = ctx.forwardingProjection(nested, forwardedIds, position);
        final OutputSchema output = project.getOutput();
        ctx.uniqueNames(output);
        final IntList outerIds = domains.domainOuterIds;
        for (int i = 0, n = outerIds.size(); i < n; i++) {
            final BoundExpression isSlaveRow = ctx.context.bindCall("=", position, ctx.column(nestedOutput, markerId, position),
                    ctx.planNodes.constants.next().ofNull(position), nestedOutput);
            final BoundExpression carrier = ctx.caseWhen(isSlaveRow, ctx.column(nestedOutput, slaveIds.getQuick(i), position),
                    ctx.column(nestedOutput, carrierIds.getQuick(i), position), nestedOutput, position);
            project.getExpressions().add(carrier);
            final int carrierId = ctx.context.newColumnId();
            output.add(carrierId, ctx.outerRefName(outerIds.getQuick(i)), carrier.getDataType(), false);
            carrierIds.setQuick(i, carrierId);
        }
        markerId = -1;
        if (isMarked) {
            project.getExpressions().add(ctx.planNodes.constants.next().ofLong(1, position));
            markerId = ctx.context.newColumnId();
            output.add(markerId, markerName(), ColumnType.LONG, false);
        }
        for (int i = base, n = ctx.mappedColumnIds.size(); i < n; i++) {
            final int forwardedId = forwardedIds.get(ctx.mappedColumnIds.getQuick(i));
            if (forwardedId > -1) {
                ctx.mappedColumnIds.setQuick(i, forwardedId);
            }
        }
        LogicalPlan top = project;
        final BoundExpression where = fullStep.getPostJoinFilter();
        if (where != null) {
            fullStep.setPostJoinFilter(null);
            ctx.loadMappedSubstitution(base);
            for (int i = 0, n = outerIds.size(); i < n; i++) {
                ctx.substitution.put(outerIds.getQuick(i), carrierIds.getQuick(i));
            }
            final BoundExpression predicate = ctx.context.getRewriter().remapColumns(ctx.context.getRewriter().remapColumns(where, forwardedIds), ctx.substitution);
            top = ctx.planNodes.filters.next().of(project, predicate, position);
        }
        for (int i = chainBase, n = ctx.chain.size(); i < n; i++) {
            ctx.copier.remap(ctx.chain.getQuick(i), forwardedIds);
        }
        if (full == ordered.size() - 1) {
            return top;
        }
        for (int i = inputs.size() - 1; i > -1; i--) {
            if (nested.getInputs().indexOf(inputs.getQuick(i)) > -1) {
                inputs.remove(i);
            }
        }
        ordered.remove(0, full);
        final JoinInput leading = ctx.planNodes.joinInputs.next().of(top, JoinKind.CROSS, null, position);
        inputs.insert(0, 1, leading);
        ordered.insert(0, 1, leading);
        ctx.copier.remap(join, forwardedIds);
        join.addMissingInputColumns();
        return join;
    }

    /**
     * Places the carriers of the outer columns {@link DecorrelationDomains#domainOuterIds} holds in the source join of
     * a block and keys the block's deferred inputs on them; the mapping stack then maps each outer column to its
     * carrier at the join's output. Returns the block's new source.
     */
    LogicalPlan place(JoinPlan join, int base, int deferredBase, int chainBase) throws SqlException {
        final IntList outerIds = domains.domainOuterIds;
        final int position = join.getPosition();
        carrierIds.setAll(outerIds.size(), -1);
        markerId = -1;
        int last = LogicalPlans.lastMasterNullingStep(join);
        final int first = outerIds.size() > 0 ? keys.firstOuterRead(join, outerIds, last, true) : Integer.MAX_VALUE;
        final boolean isTrailing = first > last && outerIds.size() > 0;
        int from = first;
        if (first <= last) {
            for (int i = first; i <= last; i++) {
                final JoinInput step = join.getOrderedInputs().getQuick(i);
                if (step.getJoinType().isMasterNulling() && step.getJoinType().isTemporal()) {
                    throw SqlException.$(step.getPosition(), SPLICE_CORRELATION);
                }
            }
            final ObjList<JoinInput> steps = join.getOrderedInputs();
            final JoinKind firstType = steps.getQuick(first).getJoinType();
            if (first == 0 || !firstType.isMasterNulling() || firstType.isSlaveNulling()) {
                final boolean isMarked = isFullNext(steps, first, last);
                if (first < 2) {
                    cross(join, steps.getQuick(0), deferredBase, isMarked, position);
                } else {
                    domains.insertDomainStep(join, carrierDomain(isMarked, position), first, position);
                    from = first + 1;
                    last++;
                }
                carrierIds.clear();
                carrierIds.addAll(slaveIds);
                markerId = slaveMarkerId;
            }
        }
        LogicalPlan source = join;
        int p = 1;
        while (p <= last) {
            final ObjList<JoinInput> steps = join.getOrderedInputs();
            final JoinInput step = steps.getQuick(p);
            final JoinKind type = step.getJoinType();
            if (p < from || !type.isMasterNulling()) {
                remap(step, carrierIds, base, p < last);
                keys.keyOuterConditions(join, step);
                keyDeferred(step, base, deferredBase, false);
                p++;
                continue;
            }
            final boolean isFull = type.isSlaveNulling();
            final boolean isMarked = isFullNext(steps, p + 1, last);
            cross(join, step, deferredBase, !isFull && isMarked, position);
            keyDeferred(step, base, deferredBase, true);
            if (carrierIds.size() > 0 && carrierIds.getQuick(0) > -1) {
                for (int i = 0, n = outerIds.size(); i < n; i++) {
                    addCarrierKey(step, carrierIds.getQuick(i), slaveIds.getQuick(i), ctx.outerRefName(outerIds.getQuick(i)));
                }
            }
            for (int i = 0, n = slaveIds.size(); i < n; i++) {
                ctx.addCarrier(step, slaveIds.getQuick(i));
            }
            remap(step, slaveIds, base, !isFull && p < last);
            keys.keyOuterConditions(join, step);
            if (!isFull) {
                carrierIds.clear();
                carrierIds.addAll(slaveIds);
                markerId = slaveMarkerId;
                p++;
                continue;
            }
            source = split(join, p, base, chainBase, isMarked, position);
            if (source != join) {
                break;
            }
            last = LogicalPlans.lastMasterNullingStep(join);
            from = 0;
            p = 1;
        }
        if (source == join) {
            final ObjList<JoinInput> steps = join.getOrderedInputs();
            if (isTrailing) {
                final JoinInput domainStep = domains.insertDomainStep(join, carrierDomain(false, position), last + 1, position);
                final JoinInput lastStep = steps.getQuick(last);
                lastStep.setPostJoinFilter(domains.moveDomainConjuncts(lastStep.getPostJoinFilter(), domainStep));
                carrierIds.clear();
                carrierIds.addAll(slaveIds);
            }
            for (int i = Math.max(last + 1, 1), n = steps.size(); i < n; i++) {
                keyDeferred(steps.getQuick(i), base, deferredBase, false);
            }
            join.addMissingInputColumns();
        }
        for (int i = 0, n = outerIds.size(); i < n; i++) {
            ctx.addMapping(outerIds.getQuick(i), carrierIds.getQuick(i));
        }
        keys.truncateDeferred(deferredBase);
        return source;
    }
}
