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
import io.questdb.griffin.HashJoinGroupByCandidate;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncFilterResources;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/** Compiles the fused operator chosen by physical planning without rebuilding or rebinding a query model. */
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

    RecordCursorFactory generate(GenerationFrame frame, HashJoinGroupByCandidate candidate, SqlExecutionContext executionContext) throws SqlException {
        AsyncHashJoinGroupByRecordCursorFactory primary = generateOrientation(frame, candidate, executionContext);
        if (candidate.getAlternate() == null || !primary.hasIntervalScan()) {
            return primary;
        }
        AsyncHashJoinGroupByRecordCursorFactory alternate = null;
        try {
            alternate = generateOrientation(frame, candidate.getAlternate(), executionContext);
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

    private AsyncHashJoinGroupByRecordCursorFactory generateOrientation(GenerationFrame frame, HashJoinGroupByCandidate candidate,
                                                                       SqlExecutionContext executionContext) throws SqlException {
        RecordCursorFactory probe = null;
        RecordCursorFactory build = null;
        HashJoinGroupByFunctions functions = null;
        Function buildOnFilter = null;
        ObjList<Function> workerBuildOnFilters = null;
        AsyncFilterContext probeContext = null;
        AsyncFilterContext buildContext = null;
        Throwable primary = null;
        try (PreparedFilter probeFilter = new PreparedFilter(); PreparedFilter buildFilter = new PreparedFilter()) {
            if (candidate.isInputSwapped()) {
                build = generateSource(frame, candidate.getBuildInput(), buildFilter, executionContext);
            }
            probe = generateSource(frame, candidate.getProbeInput(), probeFilter, executionContext);
            if (!candidate.isInputSwapped()) {
                build = generateSource(frame, candidate.getBuildInput(), buildFilter, executionContext);
            }
            final int workers = executionContext.getSharedQueryWorkerCount();
            final OutputSchema probeInput = HashJoinGroupByCandidate.frameOutput(candidate.getProbeInput());
            final OutputSchema buildInput = HashJoinGroupByCandidate.frameOutput(candidate.getBuildInput());
            try (HashJoinGroupByMetadata metadata = new HashJoinGroupByMetadata(configuration, asm, candidate,
                    probe.getMetadata(), columnIds(probeInput), build.getMetadata(), columnIds(buildInput))) {
                functions = new HashJoinGroupByFunctions(configuration, asm, instantiator, candidate, metadata, workers, executionContext);
                final BoundExpression on = candidate.getBuildOnFilter();
                if (on != null) {
                    buildOnFilter = instantiator.instantiate(on, buildInput, build.getMetadata(), executionContext);
                    workerBuildOnFilters = instantiator.instantiateWorkers(on, buildInput, build.getMetadata(), buildOnFilter, workers, executionContext);
                }
                if (probeFilter.getFilter() != null) {
                    filterGenerator.prepareParallel(frame, probeFilter, probe, executionContext);
                }
                if (buildFilter.getFilter() != null) {
                    filterGenerator.prepareParallel(frame, buildFilter, build, executionContext);
                }
                probeContext = filterContext(probeFilter, workers);
                buildContext = filterContext(buildFilter, workers);
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
                        candidate.getLogicalJoinType() != JoinKind.INNER, workers, candidate.getLogicalJoinType(), candidate.isInputSwapped());
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
            if (primary == null) {
                CairoException.rethrowCleanupFailure(failure);
            }
        }
    }

    private RecordCursorFactory generateSource(GenerationFrame frame, LogicalPlan input, PreparedFilter prepared,
                                               SqlExecutionContext executionContext) throws SqlException {
        if (input instanceof FilterPlan filter) {
            final ScanPlan scan = PlanToFactoryRules.fusedScan(filter);
            if (!HashJoinGroupByCandidate.isConstantGate(scan == null ? filter.getPredicate() : scan.getResidual())
                    && PlanToFactoryRules.stolenFilter(input) != null && (scan == null || scan.getResidual() != null)) {
                return codeGenerator.generateSource(frame, input, prepared, executionContext);
            }
        } else if (input instanceof ScanPlan scan && scan.getResidual() != null
                && !HashJoinGroupByCandidate.isConstantGate(scan.getResidual())) {
            return codeGenerator.generateFilteredScan(frame, scan, prepared, executionContext);
        }
        return codeGenerator.generate(frame, input, executionContext);
    }
}
