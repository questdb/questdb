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

package io.questdb.griffin.codegen;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncFilterResources;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.HashJoinAggregation;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * Builds the GROUP BY fused into a hash join as the {@link HashJoinAggregation} operator planning records: each input
 * as it decided to read it, under the gate when it recorded one, and the other build when it recorded one.
 */
final class HashJoinGroupByFactoryGenerator {
    private final BytecodeAssembler asm;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final FilterFactoryGenerator filterGenerator;
    private final FunctionInstantiator instantiator;

    HashJoinGroupByFactoryGenerator(CairoConfiguration configuration, SqlCodeGenerator codeGenerator,
                                    FilterFactoryGenerator filterGenerator, FunctionInstantiator instantiator, BytecodeAssembler asm) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.filterGenerator = filterGenerator;
        this.instantiator = instantiator;
        this.asm = asm;
    }

    RecordCursorFactory generate(GenerationFrame frame, HashJoinAggregation hashJoin, SqlExecutionContext executionContext) throws SqlException {
        final BoundExpression gate = hashJoin.getGate();
        final AsyncHashJoinGroupByRecordCursorFactory primary = generateOrientation(frame, hashJoin, gate, executionContext);
        final HashJoinAggregation alternateJoin = hashJoin.getAlternate();
        if (alternateJoin == null) {
            return primary;
        }
        AsyncHashJoinGroupByRecordCursorFactory alternate = null;
        try {
            alternate = generateOrientation(frame, alternateJoin, gate, executionContext);
            if (!HashJoinGroupByBuildChoiceRecordCursorFactory.isSameOutput(primary.getMetadata(), alternate.getMetadata())) {
                throw new IllegalStateException("hash join orientations have different output metadata");
            }
            return new HashJoinGroupByBuildChoiceRecordCursorFactory(primary, alternate);
        } catch (Throwable th) {
            Misc.free(alternate, th);
            Misc.free(primary, th);
            throw th;
        }
    }

    private static IntList columnIds(OutputSchema input) {
        IntList ids = new IntList(input.getColumnCount());
        for (int i = 0; i < input.getColumnCount(); i++) {
            ids.add(input.getColumnId(i));
        }
        return ids;
    }

    private AsyncFilterContext filterContext(PreparedFilter prepared, int workerCount) {
        try (AsyncFilterResources resources = prepared.adopt()) {
            return new AsyncFilterContext(configuration, resources, workerCount, 0, 0, 0);
        }
    }

    /**
     * Gates the input on the runtime constant the ordinary plan evaluates over the join: a false gate serves no page
     * frames, so the operator aggregates no rows, and an aggregate without keys still returns its row.
     */
    private RecordCursorFactory gate(BoundExpression gate, RecordCursorFactory input, OutputSchema output,
                                     SqlExecutionContext executionContext) throws SqlException {
        return new RuntimeConstGateRecordCursorFactory(input, instantiator.instantiate(gate, output, executionContext));
    }

    private AsyncHashJoinGroupByRecordCursorFactory generateOrientation(GenerationFrame frame, HashJoinAggregation hashJoin,
                                                                        BoundExpression gate, SqlExecutionContext executionContext) throws SqlException {
        RecordCursorFactory probe = null;
        RecordCursorFactory build = null;
        HashJoinGroupByFunctions functions = null;
        Function buildOnFilter = null;
        ObjList<Function> workerBuildOnFilters = null;
        AsyncFilterContext probeContext = null;
        AsyncFilterContext buildContext = null;
        final PreparedFilter probeFilter = frame.pushPreparedFilter();
        final PreparedFilter buildFilter = frame.pushPreparedFilter();
        Throwable primary = null;
        try {
            // The ordinary plan builds the master first, and the first build of a bind variable without a type infers it.
            final boolean isMasterBuilt = hashJoin.getBuildSide() == JoinInput.BuildSide.MASTER;
            if (isMasterBuilt) {
                build = generateInput(frame, hashJoin.getBuildInput(), hashJoin.getBuildRead(), buildFilter, executionContext);
            }
            probe = generateInput(frame, hashJoin.getProbeInput(), hashJoin.getProbeRead(), probeFilter, executionContext);
            if (!isMasterBuilt) {
                build = generateInput(frame, hashJoin.getBuildInput(), hashJoin.getBuildRead(), buildFilter, executionContext);
            }
            final int workers = executionContext.getSharedQueryWorkerCount();
            final OutputSchema buildInput = hashJoin.getBuildOutput();
            try (HashJoinGroupByMetadata metadata = new HashJoinGroupByMetadata(configuration, asm, hashJoin,
                    probe.getMetadata(), columnIds(hashJoin.getProbeOutput()), build.getMetadata(), columnIds(buildInput))) {
                functions = new HashJoinGroupByFunctions(configuration, asm, instantiator, hashJoin, metadata, workers, executionContext);
                final BoundExpression on = hashJoin.getBuildOnFilter();
                if (on != null) {
                    buildOnFilter = instantiator.instantiate(on, buildInput, build.getMetadata(), executionContext);
                    workerBuildOnFilters = instantiator.instantiateWorkers(on, buildInput, build.getMetadata(), buildOnFilter, workers, executionContext);
                }
                if (hashJoin.getProbeRead() == HashJoinAggregation.InputRead.STOLEN_FILTER) {
                    filterGenerator.prepareParallel(frame, probeFilter, probe, executionContext);
                }
                if (hashJoin.getBuildRead() == HashJoinAggregation.InputRead.STOLEN_FILTER) {
                    filterGenerator.prepareParallel(frame, buildFilter, build, executionContext);
                }
                probeContext = filterContext(probeFilter, workers);
                buildContext = filterContext(buildFilter, workers);
                if (gate != null) {
                    probe = gate(gate, probe, hashJoin.getProbeOutput(), executionContext);
                    build = gate(gate, build, buildInput, executionContext);
                }
                RecordCursorFactory ownedProbe = probe;
                RecordCursorFactory ownedBuild = build;
                HashJoinGroupByFunctions ownedFunctions = functions;
                Function ownedOn = buildOnFilter;
                ObjList<Function> ownedOnWorkers = workerBuildOnFilters;
                AsyncFilterContext ownedProbeContext = probeContext;
                AsyncFilterContext ownedBuildContext = buildContext;
                probe = null;
                build = null;
                functions = null;
                buildOnFilter = null;
                workerBuildOnFilters = null;
                probeContext = null;
                buildContext = null;
                return new AsyncHashJoinGroupByRecordCursorFactory(executionContext.getCairoEngine(), ownedProbe, ownedBuild,
                        ownedBuildContext, ownedOn, ownedOnWorkers, metadata, ownedFunctions, ownedProbeContext,
                        hashJoin.getJoinType() != JoinKind.INNER, workers, hashJoin.getJoinType(), isMasterBuilt);
            }
        } catch (Throwable th) {
            primary = th;
            throw th;
        } finally {
            Throwable failure = Misc.freeBestEffort(primary, functions);
            failure = Misc.freeBestEffort(failure, buildOnFilter);
            failure = Misc.freeObjListBestEffort(failure, workerBuildOnFilters);
            failure = Misc.freeBestEffort(failure, probeContext);
            failure = Misc.freeBestEffort(failure, buildContext);
            failure = Misc.freeBestEffort(failure, probe);
            failure = Misc.freeBestEffort(failure, build);
            if (failure != null) {
                frame.popPreparedFilter(failure);
                frame.popPreparedFilter(failure);
            } else {
                frame.popPreparedFilter();
                frame.popPreparedFilter();
            }
            if (primary == null) {
                CairoException.rethrowCleanupFailure(failure);
            }
        }
    }

    /**
     * Builds the factory of a frame source the fused operator reads as {@code read} records: whole, for its page
     * frames, or without the filter it steals into {@code stolen}.
     */
    private RecordCursorFactory generateInput(GenerationFrame frame, LogicalPlan source, HashJoinAggregation.InputRead read,
                                              PreparedFilter stolen, SqlExecutionContext executionContext) throws SqlException {
        return codeGenerator.generateSource(frame, source, read == HashJoinAggregation.InputRead.STOLEN_FILTER ? stolen : null, executionContext);
    }
}
