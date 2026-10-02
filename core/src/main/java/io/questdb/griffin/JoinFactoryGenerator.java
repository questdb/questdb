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

package io.questdb.griffin;

import io.questdb.TelemetryEvent;
import io.questdb.TelemetryOrigin;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.map.RecordValueSink;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastSymbolToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToStrFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.TimestampSamplerFactory;
import io.questdb.griffin.engine.join.ArrayUnnestSource;
import io.questdb.griffin.engine.join.AsOfJoinDenseRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinDenseSingleSymbolRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinIndexedRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightNoKeyRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinMemoizedRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinNoKeyFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsOfJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.AsyncWindowJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.ChainedSymbolShortCircuit;
import io.questdb.griffin.engine.join.CrossJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.FilteredAsOfJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.FilteredAsOfJoinNoKeyFastRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinFilteredRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.JsonUnnestSource;
import io.questdb.griffin.engine.join.LtJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.LtJoinNoKeyFastRecordCursorFactory;
import io.questdb.griffin.engine.join.LtJoinNoKeyRecordCursorFactory;
import io.questdb.griffin.engine.join.LtJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.MarkoutHorizonRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopFullJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopLeftJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopRightJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NoopSymbolShortCircuit;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.griffin.engine.join.SpliceJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.StringToSymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.SymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.SymbolKeyMappingRecordCopier;
import io.questdb.griffin.engine.join.SymbolShortCircuit;
import io.questdb.griffin.engine.join.SymbolToSymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.UnnestRecordCursorFactory;
import io.questdb.griffin.engine.join.UnnestSource;
import io.questdb.griffin.engine.join.VarcharToSymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.WindowJoinFastRecordCursorFactory;
import io.questdb.griffin.engine.join.WindowJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.ExtraNullColumnCursorFactory;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowContextImpl;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.questdb.cairo.ColumnType.*;

final class JoinFactoryGenerator {
    private static final FullFatJoinGenerator CREATE_FULL_FAT_AS_OF_JOIN = JoinFactoryGenerator::createFullFatAsOfJoin;
    private static final FullFatJoinGenerator CREATE_FULL_FAT_LT_JOIN = JoinFactoryGenerator::createFullFatLtJoin;
    private final BytecodeAssembler asm;
    private final SqlCodeGenerator codeGenerator;
    private final StringSink conditionSink;
    private final CairoConfiguration configuration;
    private final EntityColumnFilter entityColumnFilter;
    private final FilterFactoryGenerator filterGenerator;
    private final FunctionParser functionParser;
    private final IntHashSet intHashSet;
    private final ArrayColumnTypes keyTypes;
    private final ListColumnFilter listColumnFilterA;
    private final ListColumnFilter listColumnFilterB;
    private final IntList masterKeyIndexes;
    private final IntList masterSymbolKeyColumns;
    private final PageFrameReduceTaskFactory reduceTaskFactory;
    private final IntList slaveKeyIndexes;
    private final IntList slaveSymbolKeyColumns;
    private final IntList slaveValueIndexes;
    private final IntList symbolJoinKeyFlags;
    private final ArrayColumnTypes valueTypes;
    // a bitset of string/symbol columns forced to be serialised as varchar
    private final BitSet writeStringAsVarcharA = new BitSet();
    private final BitSet writeStringAsVarcharB = new BitSet();
    // bitsets of symbol join key columns serialised as strings, for the slave (A) and master (B)
    // key copiers; master and slave column indexes come from different metadata, so each side
    // needs its own bitset
    private final BitSet writeSymbolAsStringA;
    private final BitSet writeSymbolAsStringB = new BitSet();
    // bitsets for timestamp conversion to higher precision type
    private final BitSet writeTimestampAsNanosA = new BitSet();
    private final BitSet writeTimestampAsNanosB = new BitSet();

    JoinFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            FilterFactoryGenerator filterGenerator,
            FunctionParser functionParser,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter,
            ArrayColumnTypes keyTypes,
            ArrayColumnTypes valueTypes,
            ListColumnFilter listColumnFilterA,
            ListColumnFilter listColumnFilterB,
            PageFrameReduceTaskFactory reduceTaskFactory,
            StringSink conditionSink,
            IntList indexScratch,
            IntList valueScratch,
            IntHashSet intHashSet,
            BitSet writeSymbolAsStringA,
            IntList masterSymbolKeyColumns,
            IntList slaveSymbolKeyColumns
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.filterGenerator = filterGenerator;
        this.functionParser = functionParser;
        this.asm = asm;
        this.entityColumnFilter = entityColumnFilter;
        this.keyTypes = keyTypes;
        this.valueTypes = valueTypes;
        this.listColumnFilterA = listColumnFilterA;
        this.listColumnFilterB = listColumnFilterB;
        this.reduceTaskFactory = reduceTaskFactory;
        this.conditionSink = conditionSink;
        this.masterKeyIndexes = indexScratch;
        this.slaveValueIndexes = indexScratch;
        this.symbolJoinKeyFlags = indexScratch;
        this.slaveKeyIndexes = valueScratch;
        this.intHashSet = intHashSet;
        this.writeSymbolAsStringA = writeSymbolAsStringA;
        this.masterSymbolKeyColumns = masterSymbolKeyColumns;
        this.slaveSymbolKeyColumns = slaveSymbolKeyColumns;
    }

    static boolean isTemporalJoin(int type) {
        return type == QueryModel.JOIN_ASOF || type == QueryModel.JOIN_LT || type == QueryModel.JOIN_SPLICE;
    }

    /** Consumes both input factories on entry, including failure. */
    RecordCursorFactory generate(
            GenerationFrame frame,
            JoinInput step,
            OutputSchema masterOutput,
            CharSequence masterAlias,
            RecordCursorFactory master,
            RecordCursorFactory slave,
            FunctionBinder binder,
            SqlExecutionContext executionContext
    ) throws SqlException {
        JoinRecordMetadata metadata = null;
        Function onFilter = null;
        Function postFilter = null;
        RecordCursorFactory result = null;
        try {
            final int joinType = step.getJoinType();
            if (joinType != QueryModel.JOIN_CROSS && joinType != QueryModel.JOIN_INNER
                    && joinType != QueryModel.JOIN_LEFT_OUTER && joinType != QueryModel.JOIN_RIGHT_OUTER
                    && joinType != QueryModel.JOIN_FULL_OUTER) {
                throw SqlException.$(step.getPosition(), "unsupported logical join type");
            }
            final boolean isOuter = joinType == QueryModel.JOIN_LEFT_OUTER || joinType == QueryModel.JOIN_RIGHT_OUTER
                    || joinType == QueryModel.JOIN_FULL_OUTER;
            final int keyCount = step.getMasterKeyColumnIds().size();
            if (keyCount != step.getSlaveKeyColumnIds().size() || keyCount != step.getKeyPositions().size()
                    || keyCount != step.getMasterKeyNames().size() || keyCount != step.getSlaveKeyNames().size()) {
                throw new IllegalStateException("unaligned logical join keys");
            }
            if (joinType == QueryModel.JOIN_CROSS && (keyCount != 0 || step.getOnResidual() != null)) {
                throw SqlException.$(step.getPosition(), "CROSS join cannot have a join condition");
            }
            final RecordMetadata masterMetadata = master.getMetadata();
            final RecordMetadata slaveMetadata = slave.getMetadata();
            final boolean isSelfJoin = master.getTableToken() != null && master.getTableToken().equals(slave.getTableToken());
            final int markoutTimestampIndex = masterOutput.getColumnIndexById(step.getMarkoutTimestampColumnId());
            final boolean isMarkout = markoutTimestampIndex >= 0 && markoutTimestampIndex == masterMetadata.getTimestampIndex()
                    && master.recordCursorSupportsRandomAccess() && isLongSequence(slave);
            metadata = createJoinMetadata(masterAlias, masterMetadata, step.getBindingAlias(),
                    slaveMetadata, isMarkout || joinType == QueryModel.JOIN_RIGHT_OUTER || joinType == QueryModel.JOIN_FULL_OUTER
                            ? -1 : masterMetadata.getTimestampIndex());
            final BoundExpression onResidual = step.getOnResidual();
            if (onResidual != null) {
                onFilter = binder.instantiate(onResidual, step.getOutput(), metadata, executionContext);
            }
            if (isMarkout) {
                final JoinRecordMetadata ownedMetadata = metadata;
                final RecordCursorFactory ownedMaster = master;
                final RecordCursorFactory ownedSlave = slave;
                metadata = null;
                master = null;
                slave = null;
                result = createMarkoutHorizonJoin(ownedMetadata, ownedMaster, ownedSlave, markoutTimestampIndex,
                        step.getInput().getOutput().getColumnIndexById(step.getMarkoutSequenceColumnId()));
            } else if (keyCount == 0) {
                final JoinRecordMetadata ownedMetadata = metadata;
                final RecordCursorFactory ownedMaster = master;
                final RecordCursorFactory ownedSlave = slave;
                final Function ownedFilter = isOuter ? onFilter : null;
                metadata = null;
                master = null;
                slave = null;
                if (isOuter) {
                    onFilter = null;
                }
                result = createNestedLoopJoin(ownedMetadata, ownedMaster, ownedSlave, joinType, ownedFilter);
            } else {
                resolveKeys(masterOutput, step.getMasterKeyColumnIds(), masterKeyIndexes);
                resolveKeys(step.getInput().getOutput(), step.getSlaveKeyColumnIds(), slaveKeyIndexes);
                prepareLogicalJoinKeys(masterMetadata, slaveMetadata, masterKeyIndexes, slaveKeyIndexes,
                        step.getKeyPositions(), isSelfJoin);
                // Retain unmatched master rows when a keyed LEFT ON is constant FALSE.
                // Copy before closing: a derived slave may own closeable join metadata.
                if (joinType == QueryModel.JOIN_LEFT_OUTER && onFilter != null && onFilter.isConstant() && !onFilter.getBool(null)) {
                    final RecordCursorFactory empty = new EmptyTableRecordCursorFactory(GenericRecordMetadata.copyOfNew(slaveMetadata));
                    final RecordCursorFactory oldSlave = slave;
                    slave = empty;
                    oldSlave.close();
                } else if (joinType == QueryModel.JOIN_RIGHT_OUTER && onFilter != null && onFilter.isConstant() && !onFilter.getBool(null)) {
                    final RecordCursorFactory empty = new EmptyTableRecordCursorFactory(GenericRecordMetadata.copyOfNew(masterMetadata));
                    final RecordCursorFactory oldMaster = master;
                    master = empty;
                    oldMaster.close();
                }
                final Plannable condition = createCondition(step);
                final JoinRecordMetadata ownedMetadata = metadata;
                final RecordCursorFactory ownedMaster = master;
                final RecordCursorFactory ownedSlave = slave;
                final Function ownedFilter = isOuter ? onFilter : null;
                metadata = null;
                master = null;
                slave = null;
                if (isOuter) {
                    onFilter = null;
                }
                result = createHashJoin(ownedMetadata, ownedMaster, ownedSlave, joinType, ownedFilter, condition);
            }
            BoundExpression postJoinFilter = step.getPostJoinFilter();
            if (onFilter != null) {
                // INNER residual ON gates matched pairs, together with the post-join filter.
                // Outer residual ON is owned by the join and must run before unmatched rows are NULL-extended.
                BoundExpression predicate = onResidual;
                if (postJoinFilter != null) {
                    predicate = binder.combineConjunction(onResidual, postJoinFilter, onResidual.getPosition());
                    postJoinFilter = null;
                    final Function residualFilter = onFilter;
                    onFilter = null;
                    residualFilter.close();
                    onFilter = binder.instantiate(predicate, step.getOutput(), result.getMetadata(), executionContext);
                }
                final RecordCursorFactory owned = result;
                final Function ownedFilter = onFilter;
                result = null;
                onFilter = null;
                result = filterGenerator.generatePostJoin(frame, predicate, step.getOutput(), owned, ownedFilter, binder, executionContext);
            }
            if (postJoinFilter != null) {
                postFilter = binder.instantiate(postJoinFilter, step.getOutput(), result.getMetadata(), executionContext);
                final RecordCursorFactory owned = result;
                final Function ownedFilter = postFilter;
                result = null;
                postFilter = null;
                result = filterGenerator.generatePostJoin(frame, postJoinFilter, step.getOutput(), owned, ownedFilter, binder, executionContext);
            }
            return result;
        } catch (Throwable th) {
            Misc.free(postFilter, th);
            Misc.free(onFilter, th);
            Misc.free(metadata, th);
            Misc.free(master, th);
            Misc.free(slave, th);
            Misc.free(result, th);
            throw th;
        }
    }

    int generateJoin(
            GenerationFrame frame, JoinPlan join, int requiredOrderColumnId, int requiredScanDirection, SortPlan orderAdvice,
            int orderByMnemonic, SqlExecutionContext executionContext
    ) throws SqlException {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final JoinInput first = ordered.getQuick(0);
        boolean isMasterTimestampRequired = false;
        for (int i = 1, n = ordered.size(); i < n; i++) {
            isMasterTimestampRequired |= isTemporalJoin(ordered.getQuick(i).getJoinType());
        }
        final boolean isMasterOrderPreserved = SortFactoryGenerator.isMasterOrderPreserved(join);
        final OutputSchema firstOutput = first.getInput().getOutput();
        int masterSlot;
        if (isMasterOrderPreserved && requiredOrderColumnId >= 0 && firstOutput.getColumnIndexById(requiredOrderColumnId) >= 0) {
            masterSlot = codeGenerator.generateJoinInput(frame, first.getInput(), executionContext, false, orderByMnemonic, requiredOrderColumnId,
                    requiredScanDirection, SqlCodeGenerator.hasColumns(firstOutput, orderAdvice) ? orderAdvice : null);
        } else if (isMasterOrderPreserved && SqlCodeGenerator.hasColumns(firstOutput, orderAdvice)) {
            masterSlot = codeGenerator.generateJoinInput(frame, first.getInput(), executionContext, false, orderByMnemonic, -1,
                    RecordCursorFactory.SCAN_DIRECTION_OTHER, orderAdvice);
        } else {
            masterSlot = codeGenerator.generateJoinInput(frame, first.getInput(), executionContext, isMasterTimestampRequired, orderByMnemonic);
        }
        addSharedSource(frame, first, masterSlot);
        CharSequence masterAlias = first.getBindingAlias();
        OutputSchema masterOutput = first.getOutput();
        for (int i = 1, n = ordered.size(); i < n; i++) {
            final JoinInput step = ordered.getQuick(i);
            if (step.getJoinType() == QueryModel.JOIN_UNNEST) {
                int slot = frame.resources.reserve();
                final RecordCursorFactory master = (RecordCursorFactory) frame.resources.detach(masterSlot);
                frame.resources.own(slot, generateUnnest(step.getUnnest(), masterOutput, master,
                        masterAlias, step.getBindingAlias(), frame.functionBinder, executionContext));
                final BoundExpression predicate = step.getPostJoinFilter();
                if (predicate != null) {
                    final RecordCursorFactory base = (RecordCursorFactory) frame.resources.resources.getQuick(slot);
                    final int filterSlot = frame.resources.reserve();
                    final Function filter = frame.functionBinder.instantiate(predicate, step.getOutput(), base.getMetadata(), executionContext);
                    frame.resources.own(filterSlot, filter);
                    final int filteredSlot = frame.resources.reserve();
                    frame.resources.detach(slot);
                    frame.resources.detach(filterSlot);
                    frame.resources.own(filteredSlot, filterGenerator.generate(frame, predicate, step.getOutput(), base, filter,
                            frame.functionBinder, executionContext, frame.isUpdate && SqlCodeGenerator.hasUpdateScan(join)));
                    slot = filteredSlot;
                }
                masterSlot = slot;
                masterOutput = step.getOutput();
                masterAlias = null;
                continue;
            }
            final boolean wasJoinIntervalCapture = frame.isJoinIntervalCapture;
            final boolean wasJoinSlaveInput = frame.isJoinSlaveInput;
            frame.isJoinIntervalCapture = false;
            frame.isJoinSlaveInput = true;
            final int slaveSlot;
            try {
                slaveSlot = codeGenerator.generateJoinInput(frame, step.getInput(), executionContext, isTemporalJoin(step.getJoinType()), orderByMnemonic);
            } finally {
                frame.isJoinIntervalCapture = wasJoinIntervalCapture;
                frame.isJoinSlaveInput = wasJoinSlaveInput;
            }
            addSharedSource(frame, step, slaveSlot);
            final int slot = frame.resources.reserve();
            final RecordCursorFactory master = (RecordCursorFactory) frame.resources.detach(masterSlot);
            final RecordCursorFactory slave = (RecordCursorFactory) frame.resources.detach(slaveSlot);
            frame.resources.own(slot, switch (step.getJoinType()) {
                case QueryModel.JOIN_ASOF, QueryModel.JOIN_LT ->
                        generateTemporal(frame, step, masterOutput, masterAlias, master, slave, frame.functionBinder, executionContext,
                                (step.getHints() & JoinInput.HINT_ASOF_LINEAR) != 0,
                                (step.getHints() & JoinInput.HINT_ASOF_DENSE) != 0,
                                (step.getHints() & JoinInput.HINT_ASOF_INDEX) != 0,
                                (step.getHints() & JoinInput.HINT_ASOF_MEMOIZED) != 0,
                                (step.getHints() & JoinInput.HINT_ASOF_MEMOIZED_DRIVEBY) != 0);
                case QueryModel.JOIN_SPLICE ->
                        generateSplice(frame, step, masterOutput, masterAlias, master, slave, frame.functionBinder, executionContext);
                default ->
                        generate(frame, step, masterOutput, masterAlias, master, slave, frame.functionBinder, executionContext);
            });
            masterSlot = slot;
            masterOutput = step.getOutput();
            masterAlias = null;
        }
        return masterSlot;
    }

    /** Consumes both factories on entry; SPLICE checks input order before rejecting residual ON. */
    RecordCursorFactory generateSplice(
            GenerationFrame frame,
            JoinInput step,
            OutputSchema masterOutput,
            CharSequence masterAlias,
            RecordCursorFactory master,
            RecordCursorFactory slave,
            FunctionBinder binder,
            SqlExecutionContext executionContext
    ) throws SqlException {
        RecordCursorFactory result = null;
        Function postFilter = null;
        try {
            if (step.getJoinType() != QueryModel.JOIN_SPLICE) {
                throw SqlException.$(step.getPosition(), "unsupported logical splice join type");
            }
            final RecordMetadata masterMetadata = master.getMetadata();
            final RecordMetadata slaveMetadata = slave.getMetadata();
            validateBothTimestamps(step.getPosition(), masterMetadata, slaveMetadata);
            validateBothTimestampOrders(master, slave, step.getPosition());
            if (step.getUnsupportedOnExpression() != null) {
                throw SqlException.$(step.getUnsupportedOnPosition(), "unsupported SPLICE join expression [expr='")
                        .put(step.getUnsupportedOnExpression()).put("']");
            }
            if (step.getOnResidual() != null) {
                throw new IllegalStateException("SPLICE join has an ON residual");
            }
            final boolean isSelfJoin = master.getTableToken() != null && master.getTableToken().equals(slave.getTableToken());
            resolveKeys(masterOutput, step.getMasterKeyColumnIds(), masterKeyIndexes);
            resolveKeys(step.getInput().getOutput(), step.getSlaveKeyColumnIds(), slaveKeyIndexes);
            prepareLogicalJoinKeys(masterMetadata, slaveMetadata, masterKeyIndexes, slaveKeyIndexes,
                    step.getKeyPositions(), isSelfJoin);
            // Unlike ASOF/LT, designated-timestamp equality remains an actual SPLICE key.
            final Plannable condition = masterKeyIndexes.size() == 0 ? null : createCondition(step);
            final RecordCursorFactory ownedMaster = master;
            final RecordCursorFactory ownedSlave = slave;
            master = null;
            slave = null;
            result = generateJoinSplice(ownedMaster, masterMetadata, masterAlias, ownedSlave,
                    slaveMetadata, step.getBindingAlias(), step.getPosition(), condition);
            if (step.getPostJoinFilter() != null) {
                postFilter = binder.instantiate(step.getPostJoinFilter(), step.getOutput(), result.getMetadata(), executionContext);
                final RecordCursorFactory owned = result;
                final Function ownedFilter = postFilter;
                result = null;
                postFilter = null;
                result = filterGenerator.generatePostJoin(frame, step.getPostJoinFilter(), step.getOutput(), owned, ownedFilter, binder, executionContext);
            }
            return result;
        } catch (Throwable th) {
            Misc.free(postFilter, th);
            Misc.free(master, th);
            Misc.free(slave, th);
            Misc.free(result, th);
            throw th;
        }
    }

    /** Consumes both factories on entry. Parses tolerance after timestamp, order and key validation. */
    RecordCursorFactory generateTemporal(
            GenerationFrame frame,
            JoinInput step,
            OutputSchema masterOutput,
            CharSequence masterAlias,
            RecordCursorFactory master,
            RecordCursorFactory slave,
            FunctionBinder binder,
            SqlExecutionContext executionContext,
            boolean hasLinearHint,
            boolean hasDenseHint,
            boolean hasIndexHint,
            boolean hasMemoizedHint,
            boolean hasMemoizedDrivebyHint
    ) throws SqlException {
        RecordCursorFactory result = null;
        Function postFilter = null;
        try {
            final int joinType = step.getJoinType();
            if (joinType != QueryModel.JOIN_ASOF && joinType != QueryModel.JOIN_LT) {
                throw SqlException.$(step.getPosition(), "unsupported logical temporal join type");
            }
            final RecordMetadata masterMetadata = master.getMetadata();
            final RecordMetadata slaveMetadata = slave.getMetadata();
            validateBothTimestamps(step.getPosition(), masterMetadata, slaveMetadata);
            if (step.getOnResidual() != null) {
                throw new IllegalStateException("temporal join has an ON residual");
            }
            validateBothTimestampOrders(master, slave, step.getPosition());
            final boolean isSelfJoin = master.getTableToken() != null && master.getTableToken().equals(slave.getTableToken());
            resolveKeys(masterOutput, step.getMasterKeyColumnIds(), masterKeyIndexes);
            resolveKeys(step.getInput().getOutput(), step.getSlaveKeyColumnIds(), slaveKeyIndexes);
            prepareLogicalJoinKeys(masterMetadata, slaveMetadata, masterKeyIndexes, slaveKeyIndexes,
                    step.getKeyPositions(), isSelfJoin);
            validateTimestampNotInJoinKeys(step.getPosition(), masterMetadata, slaveMetadata);
            final long toleranceInterval = tolerance(step.getToleranceToken(), step.getTolerancePosition(),
                    masterMetadata.getTimestampType(), slaveMetadata.getTimestampType());
            // The sole designated-timestamp pair adds no equality key to a temporal join.
            if (masterKeyIndexes.size() == 1 && masterKeyIndexes.getQuick(0) == masterMetadata.getTimestampIndex()
                    && slaveKeyIndexes.getQuick(0) == slaveMetadata.getTimestampIndex()) {
                masterKeyIndexes.clear();
                slaveKeyIndexes.clear();
                prepareLogicalJoinKeys(masterMetadata, slaveMetadata, masterKeyIndexes, slaveKeyIndexes,
                        step.getKeyPositions(), isSelfJoin);
            }
            final boolean isFullFat = isFullFatTemporalJoin(slave);
            final Plannable condition = createCondition(step);
            final RecordCursorFactory ownedMaster = master;
            final RecordCursorFactory ownedSlave = slave;
            master = null;
            slave = null;
            result = joinType == QueryModel.JOIN_ASOF
                    ? generateJoinAsof(isFullFat, isSelfJoin, ownedMaster, masterMetadata, masterAlias,
                    ownedSlave, slaveMetadata, step.getBindingAlias(), step.getPosition(), condition, toleranceInterval,
                    hasLinearHint, hasDenseHint, hasIndexHint, hasMemoizedHint, hasMemoizedDrivebyHint)
                    : generateJoinLt(isFullFat, ownedMaster, masterMetadata, masterAlias, ownedSlave, slaveMetadata,
                    step.getBindingAlias(), step.getPosition(), condition, toleranceInterval, hasLinearHint);
            if (isFullFat) {
                final RecordCursorFactory raw = result;
                result = null;
                result = restoreTemporalOutput(raw, masterMetadata.getColumnCount(), step.getInput().getOutput(),
                        slaveMetadata.getTimestampIndex(), step.getOutput());
            }
            if (step.getPostJoinFilter() != null) {
                postFilter = binder.instantiate(step.getPostJoinFilter(), step.getOutput(), result.getMetadata(), executionContext);
                final RecordCursorFactory owned = result;
                final Function ownedFilter = postFilter;
                result = null;
                postFilter = null;
                result = filterGenerator.generatePostJoin(frame, step.getPostJoinFilter(), step.getOutput(), owned, ownedFilter, binder, executionContext);
            }
            return result;
        } catch (Throwable th) {
            Misc.free(postFilter, th);
            Misc.free(master, th);
            Misc.free(slave, th);
            Misc.free(result, th);
            throw th;
        }
    }

    int generateWindowJoin(GenerationFrame frame, WindowJoinPlan windowJoin, ProjectPlan projection, SqlExecutionContext executionContext, int orderByMnemonic) throws SqlException {
        final boolean wasJoinIntervalCapture = frame.isJoinIntervalCapture;
        final RuntimeIntrinsicIntervalModel previousJoinIntervals = frame.joinIntervals;
        frame.isJoinIntervalCapture = true;
        frame.joinIntervals = null;
        try {
            int masterSlot = codeGenerator.generateJoinInput(frame, windowJoin.getMaster(), executionContext, true, orderByMnemonic);
            frame.isJoinIntervalCapture = false;
            for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
                final WindowJoinStep step = windowJoin.getSteps().getQuick(i);
                frame.joinIntervalStep = frame.joinIntervals != null && step.isTableSource() && !step.isDynamic() ? step : null;
                final int slaveSlot = codeGenerator.generateJoinInput(frame, step.getSlave(), executionContext, true, orderByMnemonic);
                frame.joinIntervalStep = null;
                final int slot = frame.resources.reserve();
                final RecordCursorFactory master = (RecordCursorFactory) frame.resources.detach(masterSlot);
                final RecordCursorFactory slave = (RecordCursorFactory) frame.resources.detach(slaveSlot);
                frame.resources.own(slot, generateWindowJoin(windowJoin, i, i == n - 1 ? projection : null, master, slave,
                        i == 0 ? AggregateFactoryGenerator.findFilterPlan(windowJoin.getMaster()) : null, frame.functionBinder, executionContext));
                masterSlot = slot;
            }
            if (windowJoin.isEmpty()) {
                final RecordCursorFactory factory = (RecordCursorFactory) frame.resources.detach(masterSlot);
                final int slot = frame.resources.reserve();
                try {
                    frame.resources.own(slot, new EmptyTableRecordCursorFactory(GenericRecordMetadata.copyOfNew(factory.getMetadata())));
                } finally {
                    Misc.free(factory);
                }
                return slot;
            }
            return masterSlot;
        } finally {
            frame.isJoinIntervalCapture = wasJoinIntervalCapture;
            frame.joinIntervals = previousJoinIntervals;
            frame.joinIntervalStep = null;
        }
    }

    /**
     * Consumes the raw factory, including failure. Maps the bound output onto the full-fat layout: master
     * columns, slave value columns, then slave keys. The full-fat map exposes a slave key with its master
     * key's type, so a SYMBOL key paired with a STRING or VARCHAR master key is cast back to SYMBOL.
     */
    private RecordCursorFactory restoreTemporalOutput(
            RecordCursorFactory base,
            int masterColumnCount,
            OutputSchema slaveOutput,
            int slaveTimestampIndex,
            OutputSchema output
    ) throws SqlException {
        ObjList<Function> functions = null;
        try {
            final int slaveColumnCount = slaveOutput.getColumnCount();
            final IntList valueIndexes = slaveValueIndexes;
            valueIndexes.clear();
            int valueCount = 0;
            for (int i = 0; i < slaveColumnCount; i++) {
                valueIndexes.add(i == slaveTimestampIndex || slaveKeyIndexes.indexOf(i, 0, slaveKeyIndexes.size()) < 0 ? valueCount++ : -1);
            }
            final int columnCount = output.getColumnCount();
            final IntList indexes = new IntList(columnCount);
            for (int i = 0; i < masterColumnCount; i++) {
                indexes.add(i);
            }
            for (int i = masterColumnCount; i < columnCount; i++) {
                final int slaveIndex = slaveOutput.getColumnIndexById(output.getColumnId(i));
                final int valueIndex = valueIndexes.getQuick(slaveIndex);
                indexes.add(masterColumnCount + (valueIndex < 0
                        ? valueCount + slaveKeyIndexes.indexOf(slaveIndex, 0, slaveKeyIndexes.size())
                        : valueIndex));
            }
            final RecordMetadata raw = base.getMetadata();
            boolean isTypeRestored = false;
            for (int i = 0; i < columnCount; i++) {
                final int rawType = raw.getColumnType(indexes.getQuick(i));
                final int outputType = output.getColumnType(i);
                if (rawType != outputType) {
                    // A full-fat join exposes a shared slave key with the type of its first map key slot.
                    if (!(ColumnType.isSymbol(outputType) && (rawType == ColumnType.STRING || rawType == ColumnType.VARCHAR))
                            && !(outputType == ColumnType.STRING && (rawType == ColumnType.VARCHAR || ColumnType.isSymbol(rawType)))) {
                        throw new IllegalStateException("temporal join output type differs from its factory");
                    }
                    isTypeRestored = true;
                }
            }
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            if (isTypeRestored) {
                functions = new ObjList<>(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    final int rawIndex = indexes.getQuick(i);
                    final Function column = FunctionParser.createColumn(0, rawIndex, raw);
                    final int rawType = raw.getColumnType(rawIndex);
                    if (rawType == output.getColumnType(i)) {
                        functions.add(column);
                        metadata.add(raw.getColumnMetadata(rawIndex));
                    } else if (output.getColumnType(i) == ColumnType.STRING) {
                        functions.add(ColumnType.isSymbol(rawType)
                                ? new CastSymbolToStrFunctionFactory.Func(column)
                                : new CastVarcharToStrFunctionFactory.Func(column));
                        metadata.add(new TableColumnMetadata(raw.getColumnName(rawIndex), ColumnType.STRING));
                    } else {
                        functions.add(rawType == ColumnType.STRING
                                ? new CastStrToSymbolFunctionFactory.Func(column)
                                : new CastVarcharToSymbolFunctionFactory.Func(column));
                        metadata.add(new TableColumnMetadata(raw.getColumnName(rawIndex), ColumnType.SYMBOL,
                                IndexType.NONE, 0, false, null));
                    }
                }
                metadata.setTimestampIndex(raw.getTimestampIndex());
                final ObjList<Function> ownedFunctions = functions;
                final RecordCursorFactory ownedBase = base;
                functions = null;
                base = null;
                return new VirtualRecordCursorFactory(metadata, new PriorityMetadata(0, raw), ownedFunctions, ownedBase, 0);
            }
            for (int i = 0; i < columnCount; i++) {
                metadata.add(raw.getColumnMetadata(indexes.getQuick(i)));
            }
            metadata.setTimestampIndex(raw.getTimestampIndex());
            return SelectedRecordCursorFactory.isCrossedIndex(indexes) || raw.getColumnCount() != columnCount
                    ? new SelectedRecordCursorFactory(metadata, indexes, base) : base;
        } catch (Throwable th) {
            Misc.freeObjList(functions, th);
            Misc.free(base, th);
            throw th;
        }
    }

    private void addSharedSource(GenerationFrame frame, JoinInput input, int slot) {
        frame.sharedSources.add(input);
        frame.sharedFactories.add((RecordCursorFactory) frame.resources.resources.getQuick(slot));
        frame.sharedConsumerCounts.add(1);
    }

    private Plannable createCondition(JoinInput step) {
        conditionSink.clear();
        for (int i = 0, n = step.getMasterKeyColumnIds().size(); i < n; i++) {
            if (i > 0) {
                conditionSink.put(" and ");
            }
            conditionSink.put(step.getSlaveKeyNames().getQuick(i)).put('=').put(step.getMasterKeyNames().getQuick(i));
        }
        return new JoinCondition(conditionSink.toString());
    }

    private static void resolveKeys(OutputSchema input, IntList ids, IntList indexes) {
        indexes.clear();
        for (int i = 0, n = ids.size(); i < n; i++) {
            final int index = input.getColumnIndexById(ids.getQuick(i));
            if (index < 0) {
                throw new IllegalStateException("bound join input has changed");
            }
            indexes.add(index);
        }
    }

    static boolean isLongSequence(RecordCursorFactory factory) {
        while (factory != null && !factory.getClass().getSimpleName().contains("LongSequence")) {
            factory = factory.getBaseFactory();
        }
        return factory != null;
    }

    static long tolerance(CharSequence token, int position, int leftTimestamp, int rightTimestampType) throws SqlException {
        long toleranceInterval = Numbers.LONG_NULL;
        if (token != null) {
            int k = TimestampSamplerFactory.findPositiveIntervalEndIndex(token, position, "tolerance");
            assert token.length() > k;
            char unit = token.charAt(k);
            TimestampDriver timestampDriver = getTimestampDriver(getHigherPrecisionTimestampType(leftTimestamp, rightTimestampType));
            if (unit == 'n') {
                toleranceInterval = TimestampSamplerFactory.parsePositiveInterval(token, k, position, "tolerance", Integer.MAX_VALUE, unit);
                return timestampDriver.fromNanos(toleranceInterval);
            }
            final long multiplier = switch (unit) {
                case 'U' -> timestampDriver.fromMicros(1);
                case 'T' -> timestampDriver.fromMillis(1);
                case 's' -> timestampDriver.fromSeconds(1);
                case 'm' -> timestampDriver.fromMinutes(1);
                case 'h' -> timestampDriver.fromHours(1);
                case 'd' -> timestampDriver.fromDays(1);
                case 'w' -> timestampDriver.fromWeeks(1);
                default -> throw SqlException.$(position, "unsupported TOLERANCE unit [unit=").put(unit).put(']');
            };
            int maxValue = (int) Math.min(Long.MAX_VALUE / multiplier, Integer.MAX_VALUE);
            toleranceInterval = TimestampSamplerFactory.parsePositiveInterval(token, k, position, "tolerance", maxValue, unit);
            toleranceInterval *= multiplier;
        }
        return toleranceInterval;
    }

    static void validateBothTimestampOrders(RecordCursorFactory masterFactory, RecordCursorFactory slaveFactory, int position) throws SqlException {
        if (masterFactory.getScanDirection() != RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
            throw SqlException.$(position, "left side of time series join doesn't have ASC timestamp order");
        }
        if (slaveFactory.getScanDirection() != RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
            throw SqlException.$(position, "right side of time series join doesn't have ASC timestamp order");
        }
    }

    static void validateBothTimestamps(int position, RecordMetadata masterMetadata, RecordMetadata slaveMetadata) throws SqlException {
        if (masterMetadata.getTimestampIndex() == -1) {
            throw SqlException.$(position, "left side of time series join has no timestamp");
        }
        if (slaveMetadata.getTimestampIndex() == -1) {
            throw SqlException.$(position, "right side of time series join has no timestamp");
        }
    }

    private static boolean canStealTemporalFilter(RecordCursorFactory factory) {
        // ASOF adopts the existing closure; unlike worker reconstruction it needs no AST.
        // Keep serial wrappers on their existing route and retain the LIMIT barrier.
        return FilterFactoryGenerator.isParallelFilter(factory)
                || factory instanceof RuntimeConstGateRecordCursorFactory && factory.getBaseFactory().supportsPageFrameCursor();
    }

    private static RecordCursorFactory createFullFatAsOfJoin(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes mapKeyTypes,
            @Transient ColumnTypes mapValueTypes,
            @Transient ColumnTypes slaveColumnTypes,
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit,
            RecordValueSink slaveValueSink,
            IntList columnIndex,
            Plannable joinContext,
            ColumnFilter masterTableKeyColumns,
            long toleranceInterval,
            int slaveValueTimestampIndex
    ) {
        return new AsOfJoinRecordCursorFactory(
                configuration,
                metadata,
                masterFactory,
                slaveFactory,
                mapKeyTypes,
                mapValueTypes,
                slaveColumnTypes,
                masterKeySink,
                slaveKeySink,
                columnSplit,
                slaveValueSink,
                columnIndex,
                joinContext,
                masterTableKeyColumns,
                toleranceInterval,
                slaveValueTimestampIndex
        );
    }

    private static RecordCursorFactory createFullFatLtJoin(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes mapKeyTypes,
            @Transient ColumnTypes mapValueTypes,
            @Transient ColumnTypes slaveColumnTypes,
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit,
            RecordValueSink slaveValueSink,
            IntList columnIndex,
            Plannable joinContext,
            ColumnFilter masterTableKeyColumns,
            long toleranceInterval,
            int slaveValueTimestampIndex
    ) {
        return new LtJoinRecordCursorFactory(
                configuration,
                metadata,
                masterFactory,
                slaveFactory,
                mapKeyTypes,
                mapValueTypes,
                slaveColumnTypes,
                masterKeySink,
                slaveKeySink,
                columnSplit,
                slaveValueSink,
                columnIndex,
                joinContext,
                masterTableKeyColumns,
                toleranceInterval,
                slaveValueTimestampIndex
        );
    }

    private static FunctionExpression findWindowJoinSymbolEquality(BoundExpression predicate, OutputSchema scope, RecordMetadata metadata, int splitIndex) {
        if (!(predicate instanceof FunctionExpression call)) {
            return null;
        }
        if (call.isAnd() && call.getArgumentCount() == 2) {
            final FunctionExpression left = findWindowJoinSymbolEquality(call.argumentAt(0), scope, metadata, splitIndex);
            return left != null ? left : findWindowJoinSymbolEquality(call.argumentAt(1), scope, metadata, splitIndex);
        }
        if (!"=".equals(call.getName()) || call.getArgumentCount() != 2
                || !(call.argumentAt(0) instanceof ColumnExpression left) || !(call.argumentAt(1) instanceof ColumnExpression right)) {
            return null;
        }
        final int leftIndex = scope.getColumnIndexById(left.getColumnId());
        final int rightIndex = scope.getColumnIndexById(right.getColumnId());
        return leftIndex >= 0 && rightIndex >= 0
                && metadata.getColumnType(leftIndex) == ColumnType.SYMBOL && metadata.getColumnType(rightIndex) == ColumnType.SYMBOL
                && (leftIndex < splitIndex) != (rightIndex < splitIndex)
                && metadata.isSymbolTableStatic(leftIndex) && metadata.isSymbolTableStatic(rightIndex) ? call : null;
    }

    private static ObjList<ObjList<GroupByFunction>> instantiateWorkerAggregates(
            ObjList<FunctionExpression> aggregates, ObjList<GroupByFunction> owners, OutputSchema scope, RecordMetadata metadata,
            int workerCount, FunctionBinder binder, SqlExecutionContext executionContext
    ) throws SqlException {
        boolean isThreadSafe = true;
        for (int i = 0, n = owners.size(); i < n && isThreadSafe; i++) {
            isThreadSafe = owners.getQuick(i).isThreadSafe();
        }
        if (isThreadSafe || workerCount == 0) {
            return null;
        }
        final ObjList<ObjList<GroupByFunction>> workers = new ObjList<>(workerCount);
        try {
            for (int w = 0; w < workerCount; w++) {
                final PerWorkerFunctionList<GroupByFunction> functions = new PerWorkerFunctionList<>(owners.size());
                workers.add(functions);
                for (int i = 0, n = owners.size(); i < n; i++) {
                    final GroupByFunction owner = owners.getQuick(i);
                    if (owner.isThreadSafe()) {
                        functions.add(owner, false);
                    } else {
                        final GroupByFunction function = (GroupByFunction) binder.instantiateAggregate(aggregates.getQuick(i), scope, metadata, executionContext);
                        functions.add(function, true);
                        function.initValueIndex(owner.getValueIndex());
                    }
                }
            }
            return workers;
        } catch (Throwable th) {
            for (int w = 0, n = workers.size(); w < n; w++) {
                PerWorkerFunctionList.close(workers.getQuick(w), th);
            }
            throw th;
        }
    }

    private static ObjList<Function> instantiateWorkers(
            BoundExpression expression, OutputSchema scope, RecordMetadata metadata, int workerCount,
            FunctionBinder binder, SqlExecutionContext executionContext
    ) throws SqlException {
        if (workerCount == 0) {
            return null;
        }
        final ObjList<Function> workers = new ObjList<>(workerCount);
        try {
            for (int i = 0; i < workerCount; i++) {
                workers.add(binder.instantiate(expression, scope, metadata, executionContext));
            }
            return workers;
        } catch (Throwable th) {
            Misc.freeObjList(workers, th);
            throw th;
        }
    }

    private static boolean isSingleSymbolJoin(SymbolShortCircuit symbolShortCircuit, ListColumnFilter joinColumns) {
        return joinColumns.getColumnCount() == 1 &&
                symbolShortCircuit != NoopSymbolShortCircuit.INSTANCE &&
                !(symbolShortCircuit instanceof ChainedSymbolShortCircuit);
    }

    private static boolean isWindowJoinSlaveOnly(BoundExpression expression, OutputSchema scope, int splitIndex) {
        if (expression instanceof ColumnExpression column) {
            return scope.getColumnIndexById(column.getColumnId()) >= splitIndex;
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!isWindowJoinSlaveOnly(call.argumentAt(i), scope, splitIndex)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Fills the outer metadata of a column-only projection over the window join and returns the
     * projected column indexes, or null when the projection keeps every column in place.
     */
    private static @Nullable IntList projectWindowJoinOutput(
            ProjectPlan projection,
            OutputSchema output,
            GenericRecordMetadata innerMetadata,
            int splitIndex,
            GenericRecordMetadata outerMetadata
    ) {
        final IntList columnIndex = new IntList(projection.getExpressions().size());
        boolean isIdentity = projection.getExpressions().size() == innerMetadata.getColumnCount();
        boolean isMasterPrefix = true;
        for (int i = 0, n = projection.getExpressions().size(); i < n; i++) {
            final int index = output.getColumnIndexById(((ColumnExpression) projection.getExpressions().getQuick(i)).getColumnId());
            columnIndex.add(index);
            isIdentity &= index == i;
            final TableColumnMetadata column = innerMetadata.getColumnMetadata(index);
            outerMetadata.add(new TableColumnMetadata(Chars.toString(projection.getOutput().getColumnName(i)), column.getColumnType(),
                    column.getIndexType(), column.getIndexValueBlockCapacity(), column.isSymbolTableStatic(), column.getMetadata()));
            if (index == innerMetadata.getTimestampIndex() && isMasterPrefix) {
                outerMetadata.setTimestampIndex(i);
            }
            isMasterPrefix &= index < splitIndex;
        }
        return isIdentity ? null : columnIndex;
    }

    private static BoundExpression removeConjunct(BoundExpression predicate, BoundExpression target, FunctionBinder binder) {
        if (predicate == target) {
            return null;
        }
        if (predicate instanceof FunctionExpression call && call.isAnd() && call.getArgumentCount() == 2) {
            return binder.replaceConjunction(call, removeConjunct(call.argumentAt(0), target, binder),
                    removeConjunct(call.argumentAt(1), target, binder));
        }
        return predicate;
    }

    private static int validateAndGetSlaveTimestampIndex(RecordMetadata slaveMetadata, RecordCursorFactory slaveBase) {
        int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
        // If slave is just a filter, then it must have the same metadata as its base,
        // that includes the timestamp index.
        assert slaveBase.getMetadata().getTimestampIndex() == slaveTimestampIndex;
        return slaveTimestampIndex;
    }

    private static void validateUnnestArgument(Function function, int position, boolean isJson) throws SqlException {
        final int type = function.getType();
        if (isJson ? ColumnType.tagOf(type) != ColumnType.VARCHAR : !ColumnType.isArray(type)) {
            throw SqlException.$(position, isJson ? "VARCHAR expected for JSON UNNEST" : "array type expected in UNNEST")
                    .put(", got ").put(ColumnType.nameOf(type));
        }
    }

    /**
     * The master columns this step passes through followed by its aggregate columns.
     */
    private static GenericRecordMetadata windowJoinInnerMetadata(
            WindowJoinPlan plan,
            int stepIndex,
            RecordMetadata masterMetadata,
            JoinRecordMetadata joinMetadata,
            ObjList<GroupByFunction> groupByFunctions
    ) {
        final OutputSchema output = plan.getOutput();
        final int splitIndex = masterMetadata.getColumnCount();
        final GenericRecordMetadata innerMetadata;
        if (stepIndex < plan.getSteps().size() - 1) {
            innerMetadata = GenericRecordMetadata.copyOfNew(joinMetadata, splitIndex);
        } else if (stepIndex > 0) {
            innerMetadata = new GenericRecordMetadata();
            for (int i = 0; i < splitIndex; i++) {
                final TableColumnMetadata column = masterMetadata.getColumnMetadata(i);
                innerMetadata.add(new TableColumnMetadata(Chars.toString(output.getColumnName(i)), column.getColumnType(),
                        column.getIndexType(), column.getIndexValueBlockCapacity(), column.isSymbolTableStatic(), column.getMetadata()));
            }
            innerMetadata.setTimestampIndex(masterMetadata.getTimestampIndex());
        } else {
            innerMetadata = GenericRecordMetadata.copyOfNew(masterMetadata);
        }
        final IntList aggregateColumnIds = plan.getSteps().getQuick(stepIndex).getAggregateColumnIds();
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            final int index = output.getColumnIndexById(aggregateColumnIds.getQuick(i));
            final GroupByFunction function = groupByFunctions.getQuick(i);
            innerMetadata.add(new TableColumnMetadata(Chars.toString(output.getColumnName(index)), function.getType(), IndexType.NONE, 0,
                    function instanceof SymbolFunction symbol && symbol.isSymbolTableStatic(), function.getMetadata()));
        }
        innerMetadata.setTimestampIndex(masterMetadata.getTimestampIndex());
        return innerMetadata;
    }

    // Consumes metadata, both inputs and the optional filter on entry.
    RecordCursorFactory createHashJoin(
            JoinRecordMetadata metadata,
            RecordCursorFactory master,
            RecordCursorFactory slave,
            int joinType,
            Function filter,
            Plannable context
    ) {
        boolean isTransferred = false;
        try {
            final RecordMetadata masterMetadata = master.getMetadata();
            final RecordMetadata slaveMetadata = slave.getMetadata();
            final int[][] symbolKeyIndices = convertSymbolJoinKeysToInt(masterMetadata, slaveMetadata);
            final RecordSink masterKeyCopier = createRecordCopierMaster(masterMetadata);
            final RecordSink slaveKeyCopier = createRecordCopierSlave(slaveMetadata);
            final int[] masterSymbolKeyCols = symbolKeyIndices != null ? symbolKeyIndices[0] : null;
            final int[] slaveSymbolKeyCols = symbolKeyIndices != null ? symbolKeyIndices[1] : null;

            if (slave.recordCursorSupportsRandomAccess() && !codeGenerator.isFullFatJoins()) {
                valueTypes.clear();
                valueTypes.add(INT); // chain tail offset

                if (joinType == QueryModel.JOIN_INNER) {
                    // For inner join we can also store per-key count to speed up size calculation.
                    valueTypes.add(INT); // record count for the key

                    isTransferred = true;
                    return new HashJoinLightRecordCursorFactory(
                            configuration,
                            metadata,
                            master,
                            slave,
                            keyTypes,
                            valueTypes,
                            masterKeyCopier,
                            slaveKeyCopier,
                            masterMetadata.getColumnCount(),
                            context,
                            masterSymbolKeyCols,
                            slaveSymbolKeyCols
                    );
                }

                if (joinType == QueryModel.JOIN_RIGHT_OUTER || joinType == QueryModel.JOIN_FULL_OUTER) {
                    valueTypes.add(BOOLEAN);
                }
                if (filter != null) {
                    isTransferred = true;
                    return new HashOuterJoinFilteredLightRecordCursorFactory(
                            configuration,
                            metadata,
                            master,
                            slave,
                            keyTypes,
                            valueTypes,
                            masterKeyCopier,
                            slaveKeyCopier,
                            masterMetadata.getColumnCount(),
                            filter,
                            context,
                            joinType,
                            masterSymbolKeyCols,
                            slaveSymbolKeyCols
                    );
                }

                isTransferred = true;
                return new HashOuterJoinLightRecordCursorFactory(
                        configuration,
                        metadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeyCopier,
                        slaveKeyCopier,
                        masterMetadata.getColumnCount(),
                        context,
                        joinType,
                        masterSymbolKeyCols,
                        slaveSymbolKeyCols
                );
            }

            valueTypes.clear();
            valueTypes.add(LONG); // chain head offset
            valueTypes.add(LONG); // chain tail offset
            valueTypes.add(LONG); // record count for the key
            if (filter == null && (joinType == QueryModel.JOIN_RIGHT_OUTER || joinType == QueryModel.JOIN_FULL_OUTER)) {
                valueTypes.add(BOOLEAN);
            }

            entityColumnFilter.of(slaveMetadata.getColumnCount());
            RecordSink slaveSink = RecordSinkFactory.getInstance(configuration, asm, slaveMetadata, entityColumnFilter);

            if (joinType == QueryModel.JOIN_INNER) {
                isTransferred = true;
                return new HashJoinRecordCursorFactory(
                        configuration,
                        metadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeyCopier,
                        slaveKeyCopier,
                        slaveSink,
                        masterMetadata.getColumnCount(),
                        context,
                        masterSymbolKeyCols,
                        slaveSymbolKeyCols
                );
            }

            if (filter != null) {
                isTransferred = true;
                return new HashOuterJoinFilteredRecordCursorFactory(
                        configuration,
                        metadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeyCopier,
                        slaveKeyCopier,
                        slaveSink,
                        masterMetadata.getColumnCount(),
                        filter,
                        context,
                        joinType,
                        masterSymbolKeyCols,
                        slaveSymbolKeyCols
                );
            }

            isTransferred = true;
            return new HashOuterJoinRecordCursorFactory(
                    configuration,
                    metadata,
                    master,
                    slave,
                    keyTypes,
                    valueTypes,
                    masterKeyCopier,
                    slaveKeyCopier,
                    slaveSink,
                    masterMetadata.getColumnCount(),
                    context,
                    joinType,
                    masterSymbolKeyCols,
                    slaveSymbolKeyCols
            );
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.free(filter, th);
                Misc.free(metadata, th);
                Misc.free(master, th);
                Misc.free(slave, th);
            }
            throw th;
        }
    }

    @NotNull
    JoinRecordMetadata createJoinMetadata(
            CharSequence masterAlias,
            RecordMetadata masterMetadata,
            CharSequence slaveAlias,
            RecordMetadata slaveMetadata,
            int timestampIndex
    ) {
        JoinRecordMetadata metadata;
        metadata = new JoinRecordMetadata(
                configuration,
                masterMetadata.getColumnCount() + slaveMetadata.getColumnCount()
        );

        try {
            metadata.copyColumnMetadataFrom(masterAlias, masterMetadata);
            metadata.copyColumnMetadataFrom(slaveAlias, slaveMetadata);
        } catch (Throwable th) {
            Misc.free(metadata);
            throw th;
        }

        if (timestampIndex != -1) {
            metadata.setTimestampIndex(timestampIndex);
        }
        return metadata;
    }

    /**
     * Consumes the metadata and both factories on entry, including on failure.
     */
    RecordCursorFactory createMarkoutHorizonJoin(
            JoinRecordMetadata metadata,
            RecordCursorFactory master,
            RecordCursorFactory slave,
            int masterTimestampIndex,
            int slaveSequenceIndex
    ) {
        try {
            entityColumnFilter.of(slave.getMetadata().getColumnCount());
            final RecordSink slaveSink = RecordSinkFactory.getInstance(configuration, asm, slave.getMetadata(), entityColumnFilter);
            return new MarkoutHorizonRecordCursorFactory(configuration, metadata, master, slave,
                    master.getMetadata().getColumnCount(), masterTimestampIndex, slaveSequenceIndex, slaveSink);
        } catch (Throwable th) {
            Misc.free(metadata, th);
            Misc.free(master, th);
            Misc.free(slave, th);
            throw th;
        }
    }

    /**
     * Consumes metadata, both inputs and the optional outer ON filter on entry, including failure.
     */
    RecordCursorFactory createNestedLoopJoin(
            JoinRecordMetadata metadata,
            RecordCursorFactory master,
            RecordCursorFactory slave,
            int joinType,
            @Nullable Function filter
    ) {
        boolean isTransferred = false;
        try {
            final int columnSplit = master.getMetadata().getColumnCount();
            switch (joinType) {
                case QueryModel.JOIN_CROSS, QueryModel.JOIN_INNER -> {
                    // INNER residual ON belongs to the caller's filter over matched pairs.
                    if (filter != null) {
                        throw new IllegalArgumentException("non-outer nested loop join cannot own an ON filter");
                    }
                    return new CrossJoinRecordCursorFactory(metadata, master, slave, columnSplit);
                }
                case QueryModel.JOIN_LEFT_OUTER, QueryModel.JOIN_CROSS_LEFT -> {
                    filter = filter != null ? filter : BooleanConstant.TRUE;
                    return new NestedLoopLeftJoinRecordCursorFactory(metadata, master, slave, columnSplit,
                            filter, NullRecordFactory.getInstance(slave.getMetadata()));
                }
                case QueryModel.JOIN_RIGHT_OUTER, QueryModel.JOIN_CROSS_RIGHT -> {
                    filter = filter != null ? filter : BooleanConstant.TRUE;
                    return new NestedLoopRightJoinRecordCursorFactory(metadata, master, slave, columnSplit,
                            filter, NullRecordFactory.getInstance(master.getMetadata()));
                }
                case QueryModel.JOIN_FULL_OUTER, QueryModel.JOIN_CROSS_FULL -> {
                    filter = filter != null ? filter : BooleanConstant.TRUE;
                    final Record masterNull = NullRecordFactory.getInstance(master.getMetadata());
                    final Record slaveNull = NullRecordFactory.getInstance(slave.getMetadata());
                    // FULL owns its inputs if map construction fails; the other constructors borrow on failure.
                    isTransferred = true;
                    return new NestedLoopFullJoinRecordCursorFactory(configuration, metadata, master, slave,
                            columnSplit, filter, masterNull, slaveNull);
                }
                default -> throw new IllegalArgumentException("unsupported nested loop join type: " + joinType);
            }
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.free(filter, th);
                Misc.free(metadata, th);
                Misc.free(master, th);
                if (slave != master) {
                    Misc.free(slave, th);
                }
            }
            throw th;
        }
    }

    /**
     * Prepared keys must be installed before entry. Consumes both factories, including failure.
     */
    RecordCursorFactory generateJoinAsof(
            boolean isFullFat,
            boolean isSelfJoin,
            RecordCursorFactory master,
            RecordMetadata masterMetadata,
            CharSequence masterAlias,
            RecordCursorFactory slave,
            RecordMetadata slaveMetadata,
            CharSequence slaveAlias,
            int joinPosition,
            Plannable condition,
            long toleranceInterval,
            boolean hasLinearHint,
            boolean hasDenseHint,
            boolean hasIndexHint,
            boolean hasMemoizedHint,
            boolean hasMemoizedDrivebyHint
    ) throws SqlException {
        JoinRecordMetadata joinMetadata = null;
        Function ownedStolenFilter = null;
        boolean isTransferred = false;
        try {
            if (isFullFat || codeGenerator.isFullFatJoins() || !slave.recordCursorSupportsRandomAccess()) {
                isTransferred = true;
                return createFullFatJoin(
                        master,
                        masterMetadata,
                        masterAlias,
                        slave,
                        slaveMetadata,
                        slaveAlias,
                        joinPosition,
                        CREATE_FULL_FAT_AS_OF_JOIN,
                        condition,
                        toleranceInterval
                );
            }

            joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveAlias, slaveMetadata);
            if (isKeyedTemporalJoin(masterMetadata, slaveMetadata)) {
                SymbolShortCircuit symbolShortCircuit = createSymbolShortCircuit(masterMetadata, slaveMetadata, isSelfJoin);
                int joinColumnSplit = masterMetadata.getColumnCount();
                if (!hasLinearHint) {
                    if (slave.supportsTimeFrameCursor()) {
                        boolean isSingleSymbolJoin = isSingleSymbolJoin(symbolShortCircuit, listColumnFilterA);
                        if (hasDenseHint) {
                            if (isSingleSymbolJoin) {
                                int slaveSymbolColumnIndex = listColumnFilterA.getColumnIndexFactored(0);
                                isTransferred = true;
                                return new AsOfJoinDenseSingleSymbolRecordCursorFactory(
                                        configuration,
                                        joinMetadata,
                                        master,
                                        slave,
                                        joinColumnSplit,
                                        slaveSymbolColumnIndex,
                                        (SymbolJoinKeyMapping) symbolShortCircuit,
                                        condition,
                                        toleranceInterval
                                );
                            }
                            int[][] denseSymbolKeyIndices = convertSymbolJoinKeysToInt(masterMetadata, slaveMetadata);
                            final RecordSink masterKeyCopier = createRecordCopierMaster(masterMetadata);
                            final RecordSink slaveKeyCopier = createRecordCopierSlave(slaveMetadata);
                            isTransferred = true;
                            return new AsOfJoinDenseRecordCursorFactory(
                                    configuration,
                                    joinMetadata,
                                    master,
                                    masterKeyCopier,
                                    slave,
                                    slaveKeyCopier,
                                    joinColumnSplit,
                                    keyTypes,
                                    condition,
                                    toleranceInterval,
                                    denseSymbolKeyIndices != null ? denseSymbolKeyIndices[0] : null,
                                    denseSymbolKeyIndices != null ? denseSymbolKeyIndices[1] : null
                            );
                        }
                        if (isSingleSymbolJoin) {
                            SymbolJoinKeyMapping symbolJoinKeyMapping = (SymbolJoinKeyMapping) symbolShortCircuit;
                            int slaveSymbolColumnIndex = listColumnFilterA.getColumnIndexFactored(0);
                            if (hasIndexHint && slaveMetadata.isColumnIndexed(slaveSymbolColumnIndex)) {
                                return new AsOfJoinIndexedRecordCursorFactory(
                                        configuration,
                                        joinMetadata,
                                        master,
                                        slave,
                                        joinColumnSplit,
                                        slaveSymbolColumnIndex,
                                        symbolJoinKeyMapping,
                                        condition,
                                        toleranceInterval
                                );
                            }
                            if (hasMemoizedHint || hasMemoizedDrivebyHint) {
                                isTransferred = true;
                                return new AsOfJoinMemoizedRecordCursorFactory(
                                        configuration,
                                        joinMetadata,
                                        master,
                                        slave,
                                        joinColumnSplit,
                                        slaveSymbolColumnIndex,
                                        symbolJoinKeyMapping,
                                        condition,
                                        toleranceInterval,
                                        hasMemoizedDrivebyHint
                                );
                            }

                            // We're falling back to the default Fast scan. We can still optimize one thing:
                            // join key equality check. Instead of comparing symbols as strings, compare symbol keys.
                            // For that to work, we need code that maps master symbol key to slave symbol key.
                            writeSymbolAsStringA.unset(slaveSymbolColumnIndex);
                            return new AsOfJoinFastRecordCursorFactory(
                                    configuration,
                                    joinMetadata,
                                    master,
                                    new SymbolKeyMappingRecordCopier((SymbolJoinKeyMapping) symbolShortCircuit),
                                    slave,
                                    createRecordCopierSlave(slaveMetadata),
                                    joinColumnSplit,
                                    symbolShortCircuit,
                                    condition,
                                    toleranceInterval,
                                    null,
                                    null
                            );
                        } else {
                            int[][] fastSymbolKeyIndices = convertSymbolJoinKeysToInt(masterMetadata, slaveMetadata);
                            return new AsOfJoinFastRecordCursorFactory(
                                    configuration,
                                    joinMetadata,
                                    master,
                                    createRecordCopierMaster(masterMetadata),
                                    slave,
                                    createRecordCopierSlave(slaveMetadata),
                                    joinColumnSplit,
                                    fastSymbolKeyIndices != null ? NoopSymbolShortCircuit.INSTANCE : symbolShortCircuit,
                                    condition,
                                    toleranceInterval,
                                    fastSymbolKeyIndices != null ? fastSymbolKeyIndices[0] : null,
                                    fastSymbolKeyIndices != null ? fastSymbolKeyIndices[1] : null
                            );
                        }
                    } else if (canStealTemporalFilter(slave) && slave.getBaseFactory().supportsTimeFrameCursor()) {
                        RecordCursorFactory slaveBase = slave.getBaseFactory();
                        int slaveTimestampIndex = validateAndGetSlaveTimestampIndex(slaveMetadata, slaveBase);

                        Function stolenFilter = slave.getFilter();
                        assert stolenFilter != null;

                        Misc.free(slave.getCompiledFilter());
                        Misc.free(slave.getBindVarMemory());
                        Misc.freeObjList(slave.getBindVarFunctions());
                        slave.halfClose();
                        slave = slaveBase;
                        ownedStolenFilter = stolenFilter;

                        int[][] filteredSymbolKeyIndices = convertSymbolJoinKeysToInt(masterMetadata, slaveMetadata);
                        return new FilteredAsOfJoinFastRecordCursorFactory(
                                configuration,
                                joinMetadata,
                                master,
                                createRecordCopierMaster(masterMetadata),
                                slaveBase,
                                createRecordCopierSlave(slaveMetadata),
                                stolenFilter,
                                masterMetadata.getColumnCount(),
                                NullRecordFactory.getInstance(slaveMetadata),
                                null,
                                slaveTimestampIndex,
                                toleranceInterval,
                                filteredSymbolKeyIndices != null ? filteredSymbolKeyIndices[0] : null,
                                filteredSymbolKeyIndices != null ? filteredSymbolKeyIndices[1] : null
                        );
                    } else if (slave.isProjection()) {
                        RecordCursorFactory projectionBase = slave.getBaseFactory();
                        // We know projectionBase does not support supportsTimeFrameCursor, because
                        // Projections forward this call to its base factory and if we are in this branch
                        // then slave.supportsTimeFrameCursor() returned false in one the previous branches.
                        // There is still chance that projectionBase is just a filter
                        // and its own base supports timeFrameCursor. let's see.
                        if (canStealTemporalFilter(projectionBase)) {
                            // ok cool, it's used only as a filter.
                            RecordCursorFactory filterStealingBase = projectionBase.getBaseFactory();
                            if (filterStealingBase.supportsTimeFrameCursor()) {
                                IntList stolenCrossIndex = slave.getColumnCrossIndex();
                                assert stolenCrossIndex != null;
                                Function stolenFilter = projectionBase.getFilter();
                                assert stolenFilter != null;

                                // index *after* applying the projection
                                int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
                                assert stolenCrossIndex.get(slaveTimestampIndex) == filterStealingBase.getMetadata().getTimestampIndex();

                                Misc.free(projectionBase.getCompiledFilter());
                                Misc.free(projectionBase.getBindVarMemory());
                                Misc.freeObjList(projectionBase.getBindVarFunctions());
                                projectionBase.halfClose();
                                slave = filterStealingBase;
                                ownedStolenFilter = stolenFilter;

                                int[][] projFilteredSymbolKeyIndices = convertSymbolJoinKeysToInt(masterMetadata, slaveMetadata);
                                return new FilteredAsOfJoinFastRecordCursorFactory(
                                        configuration,
                                        joinMetadata,
                                        master,
                                        createRecordCopierMaster(masterMetadata),
                                        filterStealingBase,
                                        createRecordCopierSlave(slaveMetadata),
                                        stolenFilter,
                                        masterMetadata.getColumnCount(),
                                        NullRecordFactory.getInstance(slaveMetadata),
                                        stolenCrossIndex,
                                        slaveTimestampIndex,
                                        toleranceInterval,
                                        projFilteredSymbolKeyIndices != null ? projFilteredSymbolKeyIndices[0] : null,
                                        projFilteredSymbolKeyIndices != null ? projFilteredSymbolKeyIndices[1] : null
                                );
                            }
                        }
                    }
                }

                // fallback for keyed join when no optimizations are applicable, or when asof_linear hint is used:
                if (isSingleSymbolJoin(symbolShortCircuit, listColumnFilterA)) {
                    // We're falling back to the default Light scan. We can still optimize one thing:
                    // join key equality check. Instead of comparing symbols as strings, compare symbol keys.
                    // For that to work, we need code that maps master symbol key to slave symbol key.
                    int slaveSymbolColumnIndex = listColumnFilterA.getColumnIndexFactored(0);
                    writeSymbolAsStringA.unset(slaveSymbolColumnIndex);
                    SymbolJoinKeyMapping joinKeyMapping = (SymbolJoinKeyMapping) symbolShortCircuit;
                    keyTypes.clear();
                    keyTypes.add(ColumnType.INT);
                    final RecordSink masterKeyCopier = new SymbolKeyMappingRecordCopier(joinKeyMapping);
                    final RecordSink slaveKeyCopier = createRecordCopierSlave(slaveMetadata);
                    isTransferred = true;
                    return new AsOfJoinLightRecordCursorFactory(
                            configuration,
                            joinMetadata,
                            master,
                            slave,
                            keyTypes,
                            masterKeyCopier,
                            slaveKeyCopier,
                            joinKeyMapping,
                            joinColumnSplit,
                            condition,
                            toleranceInterval,
                            null,
                            null
                    );
                } else {
                    int[][] lightSymbolKeyIndices = convertSymbolJoinKeysToInt(masterMetadata, slaveMetadata);
                    final RecordSink masterKeyCopier = createRecordCopierMaster(masterMetadata);
                    final RecordSink slaveKeyCopier = createRecordCopierSlave(slaveMetadata);
                    isTransferred = true;
                    return new AsOfJoinLightRecordCursorFactory(
                            configuration,
                            joinMetadata,
                            master,
                            slave,
                            keyTypes,
                            masterKeyCopier,
                            slaveKeyCopier,
                            null,
                            joinColumnSplit,
                            condition,
                            toleranceInterval,
                            lightSymbolKeyIndices != null ? lightSymbolKeyIndices[0] : null,
                            lightSymbolKeyIndices != null ? lightSymbolKeyIndices[1] : null
                    );
                }
            }

            // reaching this point means the join is non-keyed
            if (!hasLinearHint) {
                if (slave.supportsTimeFrameCursor()) {
                    return new AsOfJoinNoKeyFastRecordCursorFactory(
                            configuration,
                            joinMetadata,
                            master,
                            slave,
                            masterMetadata.getColumnCount(),
                            toleranceInterval
                    );
                }
                if (canStealTemporalFilter(slave) && slave.getBaseFactory().supportsTimeFrameCursor()) {
                    // Try to steal the filter from the slave. This downgrades to
                    // single-threaded Java-level filtering, so it's only worth it if the filter
                    // selectivity is low. We don't have statistics to tell selectivity, so
                    // we allow the user to disable this with the asof_linear_search hint.
                    RecordCursorFactory slaveBase = slave.getBaseFactory();
                    int slaveTimestampIndex = validateAndGetSlaveTimestampIndex(slaveMetadata, slaveBase);

                    Function stolenFilter = slave.getFilter();
                    assert stolenFilter != null;

                    Misc.free(slave.getCompiledFilter());
                    Misc.free(slave.getBindVarMemory());
                    Misc.freeObjList(slave.getBindVarFunctions());
                    slave.halfClose();
                    slave = slaveBase;
                    ownedStolenFilter = stolenFilter;
                    return new FilteredAsOfJoinNoKeyFastRecordCursorFactory(
                            configuration,
                            joinMetadata,
                            master,
                            slaveBase,
                            stolenFilter,
                            masterMetadata.getColumnCount(),
                            NullRecordFactory.getInstance(slaveMetadata),
                            null,
                            slaveTimestampIndex,
                            toleranceInterval
                    );
                }
                if (slave.isProjection()) {
                    RecordCursorFactory projectionBase = slave.getBaseFactory();
                    // We know projectionBase does not support supportsTimeFrameCursor, because
                    // projections forward this call to its base factory, and if we are in this branch,
                    // slave.supportsTimeFrameCursor() returned false in a previous branch.
                    // There is still chance that projectionBase is just a filter
                    // and its own base supports timeFrameCursor. Let's see.
                    if (canStealTemporalFilter(projectionBase)) {
                        // ok, cool, it's used only as a filter
                        RecordCursorFactory filterStealingBase = projectionBase.getBaseFactory();
                        if (filterStealingBase.supportsTimeFrameCursor()) {
                            IntList stolenCrossIndex = slave.getColumnCrossIndex();
                            assert stolenCrossIndex != null;
                            Function stolenFilter = projectionBase.getFilter();
                            assert stolenFilter != null;

                            // index *after* applying the projection
                            int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
                            assert stolenCrossIndex.get(slaveTimestampIndex) == filterStealingBase.getMetadata().getTimestampIndex();

                            Misc.free(projectionBase.getCompiledFilter());
                            Misc.free(projectionBase.getBindVarMemory());
                            Misc.freeObjList(projectionBase.getBindVarFunctions());
                            projectionBase.halfClose();
                            slave = filterStealingBase;
                            ownedStolenFilter = stolenFilter;

                            return new FilteredAsOfJoinNoKeyFastRecordCursorFactory(
                                    configuration,
                                    joinMetadata,
                                    master,
                                    filterStealingBase,
                                    stolenFilter,
                                    masterMetadata.getColumnCount(),
                                    NullRecordFactory.getInstance(slaveMetadata),
                                    stolenCrossIndex,
                                    slaveTimestampIndex,
                                    toleranceInterval
                            );
                        }
                    }
                }
            }
            // fallback for non-keyed join when no optimizations are applicable, or the asof_linear hint is used:
            return new AsOfJoinLightNoKeyRecordCursorFactory(
                    joinMetadata,
                    master,
                    slave,
                    masterMetadata.getColumnCount(),
                    toleranceInterval
            );
        } catch (Throwable t) {
            if (!isTransferred) {
                Misc.free(ownedStolenFilter, t);
                Misc.free(joinMetadata, t);
                Misc.free(master, t);
                Misc.free(slave, t);
            }
            throw t;
        }
    }

    /**
     * Prepared keys must be installed before entry. Consumes both factories, including failure.
     */
    RecordCursorFactory generateJoinLt(
            boolean isFullFat,
            RecordCursorFactory master,
            RecordMetadata masterMetadata,
            CharSequence masterAlias,
            RecordCursorFactory slave,
            RecordMetadata slaveMetadata,
            CharSequence slaveAlias,
            int joinPosition,
            Plannable condition,
            long toleranceInterval,
            boolean hasLinearHint
    ) throws SqlException {
        JoinRecordMetadata joinMetadata = null;
        boolean isTransferred = false;
        try {
            if (isFullFat || codeGenerator.isFullFatJoins() || !slave.recordCursorSupportsRandomAccess()) {
                isTransferred = true;
                return createFullFatJoin(
                        master,
                        masterMetadata,
                        masterAlias,
                        slave,
                        slaveMetadata,
                        slaveAlias,
                        joinPosition,
                        CREATE_FULL_FAT_LT_JOIN,
                        condition,
                        toleranceInterval
                );
            }

            joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveAlias, slaveMetadata);
            if (isKeyedTemporalJoin(masterMetadata, slaveMetadata)) {
                int[][] ltSymbolKeyIndices = convertSymbolJoinKeysToInt(masterMetadata, slaveMetadata);
                RecordSink masterKeyCopier = createRecordCopierMaster(masterMetadata);
                RecordSink slaveKeyCopier = createRecordCopierSlave(slaveMetadata);
                int columnSplit = masterMetadata.getColumnCount();
                valueTypes.clear();
                valueTypes.add(LONG);
                isTransferred = true;
                return new LtJoinLightRecordCursorFactory(
                        configuration,
                        joinMetadata,
                        master,
                        slave,
                        keyTypes,
                        valueTypes,
                        masterKeyCopier,
                        slaveKeyCopier,
                        columnSplit,
                        condition,
                        toleranceInterval,
                        ltSymbolKeyIndices != null ? ltSymbolKeyIndices[0] : null,
                        ltSymbolKeyIndices != null ? ltSymbolKeyIndices[1] : null
                );
            }

            if (!hasLinearHint && slave.supportsTimeFrameCursor()) {
                return new LtJoinNoKeyFastRecordCursorFactory(
                        configuration,
                        joinMetadata,
                        master,
                        slave,
                        masterMetadata.getColumnCount(),
                        toleranceInterval
                );
            }

            return new LtJoinNoKeyRecordCursorFactory(
                    joinMetadata,
                    master,
                    slave,
                    masterMetadata.getColumnCount(),
                    toleranceInterval
            );
        } catch (Throwable t) {
            if (!isTransferred) {
                Misc.free(joinMetadata, t);
                Misc.free(master, t);
                Misc.free(slave, t);
            }
            throw t;
        }
    }

    /**
     * Prepared keys must be installed before entry. Consumes both factories, including failure.
     */
    RecordCursorFactory generateJoinSplice(
            RecordCursorFactory master,
            RecordMetadata masterMetadata,
            CharSequence masterAlias,
            RecordCursorFactory slave,
            RecordMetadata slaveMetadata,
            CharSequence slaveAlias,
            int joinPosition,
            Plannable condition
    ) throws SqlException {
        JoinRecordMetadata joinMetadata = null;
        boolean isTransferred = false;
        try {
            if (!master.recordCursorSupportsRandomAccess()) {
                throw SqlException.$(joinPosition, "left side of splice join doesn't support random access");
            }
            if (!slave.recordCursorSupportsRandomAccess()) {
                throw SqlException.$(joinPosition, "right side of splice join doesn't support random access");
            }
            if (codeGenerator.isFullFatJoins()) {
                throw SqlException.$(joinPosition, "splice join doesn't support full fat mode");
            }
            // Neither side's timestamp describes the combined SPLICE stream.
            joinMetadata = createJoinMetadata(masterAlias, masterMetadata, slaveAlias, slaveMetadata, -1);
            final RecordSink masterKeySink = createRecordCopierMaster(masterMetadata);
            final RecordSink slaveKeySink = createRecordCopierSlave(slaveMetadata);
            valueTypes.clear();
            valueTypes.add(LONG); // master previous
            valueTypes.add(LONG); // master current
            valueTypes.add(LONG); // slave previous
            valueTypes.add(LONG); // slave current
            final int columnSplit = masterMetadata.getColumnCount();
            isTransferred = true;
            return new SpliceJoinLightRecordCursorFactory(configuration, joinMetadata, master, slave, keyTypes,
                    valueTypes, masterKeySink, slaveKeySink, columnSplit, condition);
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.free(joinMetadata, th);
                Misc.free(master, th);
                Misc.free(slave, th);
            }
            throw th;
        }
    }

    /**
     * Consumes the master and argument functions on entry, including failure.
     */
    RecordCursorFactory generateUnnest(
            RecordCursorFactory masterFactory,
            CharSequence masterAlias,
            CharSequence unnestAlias,
            ObjList<Function> functions,
            ObjList<BoundExpression> expressions,
            ObjList<ObjList<CharSequence>> jsonColumnNames,
            ObjList<IntList> jsonColumnTypes,
            ObjList<CharSequence> columnAliases,
            boolean isStandalone,
            boolean hasOrdinality
    ) throws SqlException {
        JoinRecordMetadata outputMetadata = null;
        final ObjList<UnnestSource> sources = new ObjList<>(functions.size());
        try {
            final int exprCount = functions.size();
            int outputCount = 0;
            for (int i = 0; i < exprCount; i++) {
                final ObjList<CharSequence> names = jsonColumnNames.getQuiet(i);
                outputCount += names == null ? 1 : names.size();
            }
            final int totalUnnestColumns = outputCount + (hasOrdinality ? 1 : 0);
            final RecordMetadata masterMetadata = masterFactory.getMetadata();
            final int columnSplit = isStandalone ? 0 : masterMetadata.getColumnCount();
            outputMetadata = new JoinRecordMetadata(configuration, columnSplit + totalUnnestColumns);
            if (!isStandalone) {
                outputMetadata.copyColumnMetadataFrom(masterAlias, masterMetadata);
                outputMetadata.setTimestampIndex(masterMetadata.getTimestampIndex());
            }
            final ObjList<CharSequence> columnNames = new ObjList<>(totalUnnestColumns);
            int aliasIndex = 0;
            for (int i = 0; i < exprCount; i++) {
                final Function function = functions.getQuick(i);
                final ObjList<CharSequence> declaredNames = jsonColumnNames.getQuiet(i);
                validateUnnestArgument(function, expressions.getQuick(i).getPosition(), declaredNames != null);
                if (declaredNames != null) {
                    final ObjList<CharSequence> names = new ObjList<>(declaredNames.size());
                    for (int j = 0; j < declaredNames.size(); j++) {
                        names.add(Chars.toString(declaredNames.getQuick(j)));
                    }
                    final IntList types = jsonColumnTypes.getQuick(i);
                    sources.add(new JsonUnnestSource(function, names, types, configuration.getJsonUnnestMaxValueSize()));
                    for (int j = 0; j < names.size(); j++) {
                        final String name = Chars.toString(aliasIndex < columnAliases.size()
                                ? columnAliases.getQuick(aliasIndex) : names.getQuick(j));
                        columnNames.add(name);
                        outputMetadata.add(unnestAlias, name, types.getQuick(j), IndexType.NONE, 0, false, null);
                        aliasIndex++;
                    }
                } else {
                    sources.add(new ArrayUnnestSource(function));
                    final String name = aliasIndex < columnAliases.size()
                            ? Chars.toString(columnAliases.getQuick(aliasIndex))
                            : outputCount == 1 ? "value" : "value" + (aliasIndex + 1);
                    columnNames.add(name);
                    final int type = function.getType();
                    final int elementType = ColumnType.decodeArrayElementType(type);
                    final int dimensions = ColumnType.decodeArrayDimensionality(type);
                    final int outputType = dimensions > 1
                            ? ColumnType.encodeArrayType((short) elementType, dimensions - 1) : elementType;
                    outputMetadata.add(unnestAlias, name, outputType, IndexType.NONE, 0, false, null);
                    aliasIndex++;
                }
            }
            if (hasOrdinality) {
                final String name = columnAliases.size() == totalUnnestColumns
                        ? Chars.toString(columnAliases.getQuick(outputCount)) : "ordinality";
                columnNames.add(name);
                outputMetadata.add(unnestAlias, name, ColumnType.LONG, IndexType.NONE, 0, false, null);
            }
            // This constructor borrows its children if construction fails.
            return new UnnestRecordCursorFactory(outputMetadata, masterFactory, functions, sources,
                    columnSplit, hasOrdinality, columnNames);
        } catch (Throwable th) {
            Misc.free(outputMetadata, th);
            Misc.freeObjList(functions, th);
            Misc.freeObjListIfCloseable(sources, th);
            Misc.free(masterFactory, th);
            throw th;
        }
    }

    /**
     * Consumes the master on entry; bound arguments read its final physical column layout.
     */
    RecordCursorFactory generateUnnest(
            UnnestSpec spec,
            OutputSchema input,
            RecordCursorFactory masterFactory,
            CharSequence masterAlias,
            CharSequence unnestAlias,
            FunctionBinder binder,
            SqlExecutionContext executionContext
    ) throws SqlException {
        ObjList<Function> functions = new ObjList<>(spec.getExpressions().size());
        try {
            final RecordMetadata metadata = masterFactory.getMetadata();
            for (int i = 0; i < spec.getExpressions().size(); i++) {
                final BoundExpression expression = spec.getExpressions().getQuick(i);
                final Function function = binder.instantiate(expression, input, metadata, executionContext);
                functions.add(function);
                validateUnnestArgument(function, expression.getPosition(), spec.getJsonColumnNames().getQuiet(i) != null);
            }
            final RecordCursorFactory ownedMaster = masterFactory;
            final ObjList<Function> ownedFunctions = functions;
            masterFactory = null;
            functions = null;
            return generateUnnest(ownedMaster, masterAlias, unnestAlias, ownedFunctions, spec.getExpressions(),
                    spec.getJsonColumnNames(), spec.getJsonColumnTypes(), spec.getColumnAliases(), spec.isStandalone(), spec.hasOrdinality());
        } catch (Throwable th) {
            Misc.freeObjList(functions, th);
            Misc.free(masterFactory, th);
            throw th;
        }
    }

    RecordCursorFactory generateWindowJoin(
            WindowJoinPlan plan,
            int stepIndex,
            @Nullable ProjectPlan projection,
            RecordCursorFactory master,
            RecordCursorFactory slave,
            @Nullable FilterPlan masterFilterPlan,
            FunctionBinder binder,
            SqlExecutionContext executionContext
    ) throws SqlException {
        final WindowJoinStep step = plan.getSteps().getQuick(stepIndex);
        JoinRecordMetadata joinMetadata = null;
        ObjList<GroupByFunction> groupByFunctions = null;
        Function joinFilter = null;
        Function windowLoFunc = null;
        Function windowHiFunc = null;
        ObjList<Function> workerLoFuncs = null;
        ObjList<Function> workerHiFuncs = null;
        try {
            final RecordMetadata masterMetadata = master.getMetadata();
            final RecordMetadata slaveMetadata = slave.getMetadata();
            validateBothTimestamps(step.getPosition(), masterMetadata, slaveMetadata);
            validateBothTimestampOrders(master, slave, step.getPosition());
            final int masterTimestampType = masterMetadata.getTimestampType();
            final TimestampDriver timestampDriver = getTimestampDriver(masterTimestampType);
            final boolean isDynamicWindow = step.isDynamic();
            long lo = step.getLo();
            long hi = step.getHi();
            if (step.getLoExpression() == null && step.getLoTimeUnit() != 0) {
                lo = WindowContextImpl.toTimestampUnits(masterTimestampType, lo, step.getLoTimeUnit(), step.getLoPosition(), "start");
            }
            if (step.getHiExpression() == null && step.getHiTimeUnit() != 0) {
                hi = WindowContextImpl.toTimestampUnits(masterTimestampType, hi, step.getHiTimeUnit(), step.getHiPosition(), "end");
            }
            if (step.getLoExpression() != null) {
                windowLoFunc = binder.instantiate(step.getLoExpression(), step.getMasterScope(), masterMetadata, executionContext);
            }
            if (step.getHiExpression() != null) {
                windowHiFunc = binder.instantiate(step.getHiExpression(), step.getMasterScope(), masterMetadata, executionContext);
            }
            if (!isDynamicWindow && hi < lo * -1) {
                throw SqlException.position(Math.max(step.getHiPosition(), step.getLoPosition())).put("WINDOW join hi value cannot be less than lo value");
            }

            final OutputSchema scope = step.getScope();
            final int splitIndex = masterMetadata.getColumnCount();
            joinMetadata = createJoinMetadata(stepIndex == 0 ? step.getMasterAlias() : null, masterMetadata, step.getSlaveAlias(),
                    slaveMetadata, masterMetadata.getTimestampIndex());
            final ObjList<FunctionExpression> aggregates = step.getAggregates();
            groupByFunctions = new ObjList<>(aggregates.size());
            instantiateWindowJoinAggregates(aggregates, scope, joinMetadata, masterMetadata.getTimestampIndex(), groupByFunctions,
                    binder, executionContext);
            final GenericRecordMetadata innerMetadata = windowJoinInnerMetadata(plan, stepIndex, masterMetadata, joinMetadata, groupByFunctions);
            GenericRecordMetadata outerMetadata = innerMetadata;
            IntList columnIndex = null;
            if (projection != null) {
                outerMetadata = new GenericRecordMetadata();
                columnIndex = projectWindowJoinOutput(projection, plan.getOutput(), innerMetadata, splitIndex, outerMetadata);
            }

            BoundExpression filter = step.getFilter();
            int leftSymbolIndex = -1;
            int rightSymbolIndex = -1;
            if (filter != null && !isDynamicWindow) {
                final FunctionExpression equality = findWindowJoinSymbolEquality(filter, scope, joinMetadata, splitIndex);
                if (equality != null) {
                    final int left = scope.getColumnIndexById(((ColumnExpression) equality.argumentAt(0)).getColumnId());
                    final int right = scope.getColumnIndexById(((ColumnExpression) equality.argumentAt(1)).getColumnId());
                    leftSymbolIndex = Math.min(left, right);
                    rightSymbolIndex = Math.max(left, right) - splitIndex;
                    filter = removeConjunct(filter, equality, binder);
                }
            }
            if (filter != null) {
                joinFilter = binder.instantiate(filter, scope, joinMetadata, executionContext);
                if (joinFilter.isConstant()) {
                    joinFilter.init(null, executionContext);
                    if (!joinFilter.getBool(null)) {
                        final RecordCursorFactory factory = columnIndex == null
                                ? new ExtraNullColumnCursorFactory(outerMetadata, splitIndex, master)
                                : new SelectedRecordCursorFactory(outerMetadata, columnIndex, new ExtraNullColumnCursorFactory(innerMetadata, splitIndex, master));
                        Misc.free(slave);
                        Misc.free(joinMetadata);
                        Misc.free(joinFilter);
                        Misc.free(windowLoFunc);
                        Misc.free(windowHiFunc);
                        Misc.freeObjList(groupByFunctions);
                        return factory;
                    }
                    joinFilter = Misc.free(joinFilter);
                    filter = null;
                }
            }

            final boolean isVectorized = joinFilter == null && isWindowJoinVectorized(plan, stepIndex, groupByFunctions, splitIndex);

            final int workerCount = executionContext.getSharedQueryWorkerCount();
            final boolean isFilterStealable = masterFilterPlan != null && !master.supportsPageFrameCursor()
                    && FilterFactoryGenerator.isParallelFilter(master) && master.getBaseFactory().supportsPageFrameCursor();
            final boolean masterSupportsPageFrames = master.supportsPageFrameCursor() || isFilterStealable;
            if (executionContext.isParallelWindowJoinEnabled()
                    && masterSupportsPageFrames
                    && GroupByUtils.isParallelismSupported(groupByFunctions)
                    && slave.supportsTimeFrameCursor()) {
                CompiledFilter compiledFilter = null;
                MemoryCARW bindVarMemory = null;
                ObjList<Function> bindVarFunctions = null;
                Function masterFilter = null;
                IntHashSet masterFilterUsedColumnIndexes = null;
                ObjList<Function> workerMasterFilters = null;
                boolean isFilterStolen = false;
                boolean isStolenFilterAdopted = false;
                try {
                    if (isFilterStealable) {
                        final RecordCursorFactory filterFactory = master;
                        final OutputSchema filterInput = masterFilterPlan.getInput().getOutput();
                        masterFilterUsedColumnIndexes = new IntHashSet();
                        FilterFactoryGenerator.collectColumnIndexes(masterFilterPlan.getPredicate(), filterInput, masterFilterUsedColumnIndexes);
                        masterFilter = FilterFactoryGenerator.stolenFilter(filterFactory);
                        if (!masterFilter.isThreadSafe()) {
                            workerMasterFilters = new ObjList<>(workerCount);
                            for (int w = 0; w < workerCount; w++) {
                                workerMasterFilters.add(binder.instantiate(masterFilterPlan.getPredicate(), filterInput,
                                        filterFactory.getBaseFactory().getMetadata(), executionContext));
                            }
                        }
                        filterFactory.halfClose();
                        compiledFilter = filterFactory.getCompiledFilter();
                        bindVarMemory = filterFactory.getBindVarMemory();
                        bindVarFunctions = filterFactory.getBindVarFunctions();
                        master = filterFactory.getBaseFactory();
                        isFilterStolen = true;
                    }
                    master.changePageFrameSizes(configuration.getSqlSmallPageFrameMinRows(), configuration.getSqlSmallPageFrameMaxRows());
                    final ObjList<Function> workerJoinFilters = joinFilter == null || joinFilter.isThreadSafe() ? null
                            : instantiateWorkers(filter, scope, joinMetadata, workerCount, binder, executionContext);
                    final ObjList<ObjList<GroupByFunction>> workerGroupByFunctions = instantiateWorkerAggregates(
                            aggregates, groupByFunctions, scope, joinMetadata, workerCount, binder, executionContext);
                    final Function ownedJoinFilter = joinFilter;
                    final ObjList<GroupByFunction> ownedGroupByFunctions = groupByFunctions;
                    joinFilter = null;
                    groupByFunctions = null;
                    isStolenFilterAdopted = true;
                    if (leftSymbolIndex != -1) {
                        master = new AsyncWindowJoinFastRecordCursorFactory(
                                executionContext.getCairoEngine(), configuration, asm, executionContext.getMessageBus(),
                                joinMetadata, outerMetadata, columnIndex, master, slave, ownedJoinFilter, workerJoinFilters,
                                step.isIncludePrevailing(), leftSymbolIndex, rightSymbolIndex, lo, hi, valueTypes,
                                ownedGroupByFunctions, workerGroupByFunctions, compiledFilter, bindVarMemory, bindVarFunctions,
                                masterFilter, workerMasterFilters, masterFilterUsedColumnIndexes, isVectorized,
                                reduceTaskFactory, workerCount
                        );
                    } else {
                        workerLoFuncs = windowLoFunc == null || windowLoFunc.isThreadSafe() ? null
                                : instantiateWorkers(step.getLoExpression(), step.getMasterScope(), master.getMetadata(), workerCount, binder, executionContext);
                        workerHiFuncs = windowHiFunc == null || windowHiFunc.isThreadSafe() ? null
                                : instantiateWorkers(step.getHiExpression(), step.getMasterScope(), master.getMetadata(), workerCount, binder, executionContext);
                        final Function ownedLo = windowLoFunc;
                        final Function ownedHi = windowHiFunc;
                        final ObjList<Function> ownedWorkerLo = workerLoFuncs;
                        final ObjList<Function> ownedWorkerHi = workerHiFuncs;
                        windowLoFunc = null;
                        windowHiFunc = null;
                        workerLoFuncs = null;
                        workerHiFuncs = null;
                        master = new AsyncWindowJoinRecordCursorFactory(
                                executionContext.getCairoEngine(), configuration, asm, executionContext.getMessageBus(),
                                joinMetadata, outerMetadata, columnIndex, master, slave, step.isIncludePrevailing(),
                                ownedJoinFilter, workerJoinFilters, lo, hi, ownedLo, ownedHi, ownedWorkerLo, ownedWorkerHi,
                                step.getLoSign(), step.getHiSign(), step.getLoTimeUnit(), step.getHiTimeUnit(),
                                isDynamicWindow ? timestampDriver : null, valueTypes, ownedGroupByFunctions, workerGroupByFunctions,
                                compiledFilter, bindVarMemory, bindVarFunctions, masterFilter, workerMasterFilters,
                                masterFilterUsedColumnIndexes, isVectorized, reduceTaskFactory, workerCount
                        );
                    }
                } catch (Throwable th) {
                    if (!isStolenFilterAdopted) {
                        Misc.freeObjList(workerMasterFilters, th);
                        if (isFilterStolen) {
                            Misc.free(masterFilter, th);
                            Misc.free(compiledFilter, th);
                            Misc.free(bindVarMemory, th);
                            Misc.freeObjList(bindVarFunctions, th);
                        }
                    }
                    throw th;
                }
                executionContext.storeTelemetry(TelemetryEvent.PARALLEL_WINDOW_JOIN, TelemetryOrigin.NO_MATTERS);
                return master;
            }
            if (!slave.supportsTimeFrameCursor()) {
                throw SqlException.position(step.getPosition()).put("right side of window join must be a table, not sub-query");
            }
            final Function ownedJoinFilter = joinFilter;
            final ObjList<GroupByFunction> ownedGroupByFunctions = groupByFunctions;
            joinFilter = null;
            groupByFunctions = null;
            final RecordCursorFactory factory;
            if (leftSymbolIndex != -1) {
                factory = new WindowJoinFastRecordCursorFactory(asm, configuration, outerMetadata, joinMetadata, master, slave, columnIndex,
                        step.isIncludePrevailing(), lo, hi, ownedGroupByFunctions, valueTypes, rightSymbolIndex, leftSymbolIndex,
                        ownedJoinFilter, isVectorized);
            } else {
                final Function ownedLo = windowLoFunc;
                final Function ownedHi = windowHiFunc;
                windowLoFunc = null;
                windowHiFunc = null;
                factory = new WindowJoinRecordCursorFactory(asm, configuration, outerMetadata, joinMetadata, master, slave,
                        step.isIncludePrevailing(), columnIndex, lo, hi, ownedLo, ownedHi, step.getLoSign(), step.getHiSign(),
                        step.getLoTimeUnit(), step.getHiTimeUnit(), isDynamicWindow ? timestampDriver : null,
                        ownedGroupByFunctions, valueTypes, ownedJoinFilter);
            }
            executionContext.storeTelemetry(TelemetryEvent.SINGLE_THREAD_WINDOW_JOIN, TelemetryOrigin.NO_MATTERS);
            return factory;
        } catch (Throwable th) {
            Misc.free(joinFilter, th);
            Misc.free(windowLoFunc, th);
            Misc.free(windowHiFunc, th);
            Misc.freeObjList(workerLoFuncs, th);
            Misc.freeObjList(workerHiFuncs, th);
            Misc.freeObjList(groupByFunctions, th);
            Misc.free(joinMetadata, th);
            Misc.free(master, th);
            Misc.free(slave, th);
            throw th;
        }
    }

    boolean isFullFatTemporalJoin(RecordCursorFactory slave) {
        return codeGenerator.isFullFatJoins() || !slave.recordCursorSupportsRandomAccess();
    }

    void prepareLogicalJoinKeys(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            IntList masterIndexes,
            IntList slaveIndexes,
            IntList keyPositions,
            boolean isSelfJoin
    ) throws SqlException {
        listColumnFilterA.clear();
        listColumnFilterB.clear();
        for (int i = 0, n = slaveIndexes.size(); i < n; i++) {
            listColumnFilterA.add(slaveIndexes.getQuick(i) + 1);
            listColumnFilterB.add(masterIndexes.getQuick(i) + 1);
        }
        processJoinKeyTypes(isSelfJoin, masterMetadata, slaveMetadata, keyPositions);
    }

    void validateTimestampNotInJoinKeys(int position, RecordMetadata masterMetadata, RecordMetadata slaveMetadata) throws SqlException {
        // When timestamp is the ONLY join key, isKeyedTemporalJoin() will return false and simplify
        // "ASOF JOIN ON (ts)" to a non-keyed join, which is harmless. But when timestamp is combined
        // with other keys like "ON (sym, ts)", it causes problems in the join implementation.
        int keyCount = listColumnFilterA.getColumnCount();
        if (keyCount <= 1) {
            return; // Single key (or no key) is fine - handled by isKeyedTemporalJoin()
        }
        // listColumnFilterA contains slave column indices, listColumnFilterB contains master column indices
        int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
        int masterTimestampIndex = masterMetadata.getTimestampIndex();
        for (int i = 0; i < keyCount; i++) {
            if (listColumnFilterA.getColumnIndexFactored(i) == slaveTimestampIndex ||
                    listColumnFilterB.getColumnIndexFactored(i) == masterTimestampIndex) {
                throw SqlException.$(position,
                        "ASOF/LT JOIN cannot use designated timestamp as a join key");
            }
        }
    }

    private void alignSharedJoinKeyTypes(RecordMetadata masterMetadata, RecordMetadata slaveMetadata) {
        boolean isChanged = true;
        while (isChanged) {
            isChanged = false;
            for (int k = 0, m = listColumnFilterA.getColumnCount(); k < m; k++) {
                final int columnIndexA = listColumnFilterA.getColumnIndexFactored(k);
                final int columnIndexB = listColumnFilterB.getColumnIndexFactored(k);
                final int keyType = keyTypes.getColumnType(k);
                if (keyType == STRING || keyType == ColumnType.SYMBOL) {
                    if (writeStringAsVarcharA.get(columnIndexA) || writeStringAsVarcharB.get(columnIndexB)) {
                        keyTypes.set(k, VARCHAR);
                        if (!isVarchar(slaveMetadata.getColumnType(columnIndexA))) {
                            writeStringAsVarcharA.set(columnIndexA);
                        }
                        if (!isVarchar(masterMetadata.getColumnType(columnIndexB))) {
                            writeStringAsVarcharB.set(columnIndexB);
                        }
                        writeSymbolAsStringA.set(columnIndexA);
                        writeSymbolAsStringB.set(columnIndexB);
                        isChanged = true;
                    } else if (keyType == ColumnType.SYMBOL
                            && (writeSymbolAsStringA.get(columnIndexA) || writeSymbolAsStringB.get(columnIndexB))) {
                        keyTypes.set(k, STRING);
                        writeSymbolAsStringA.set(columnIndexA);
                        writeSymbolAsStringB.set(columnIndexB);
                        isChanged = true;
                    }
                } else if (isTimestamp(keyType) && !isTimestampNano(keyType)
                        && (writeTimestampAsNanosA.get(columnIndexA) || writeTimestampAsNanosB.get(columnIndexB))) {
                    keyTypes.set(k, TIMESTAMP_NANO);
                    writeTimestampAsNanosA.set(columnIndexA);
                    writeTimestampAsNanosB.set(columnIndexB);
                    isChanged = true;
                }
            }
        }
    }

    /**
     * Converts SYMBOL-SYMBOL join key pairs from string-based comparison to integer-based
     * comparison using SymbolTranslatingRecord. For each SYMBOL-SYMBOL pair where both
     * writeSymbolAsStringB (master) and writeSymbolAsStringA (slave) are currently set
     * (i.e., non-self-join pairs), this method:
     * <ul>
     *   <li>Unsets writeSymbolAsStringB for the master column and writeSymbolAsStringA for the slave column</li>
     *   <li>Changes the keyTypes entry from STRING to INT</li>
     *   <li>Collects master/slave column indices into arrays</li>
     * </ul>
     * Must be called after createSymbolShortCircuit() and before createRecordCopierMaster/Slave().
     *
     * @return null if no SYMBOL-SYMBOL pairs found, otherwise [masterIndices, slaveIndices]
     */
    private int @Nullable [][] convertSymbolJoinKeysToInt(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata
    ) {
        final int keyCount = listColumnFilterA.getColumnCount();
        symbolJoinKeyFlags.setAll(keyCount, 0);
        for (int k = 0; k < keyCount; k++) {
            final int slaveColIndex = listColumnFilterA.getColumnIndexFactored(k);
            final int masterColIndex = listColumnFilterB.getColumnIndexFactored(k);
            if (keyTypes.getColumnType(k) == STRING
                    && masterMetadata.getColumnType(masterColIndex) == ColumnType.SYMBOL
                    && slaveMetadata.getColumnType(slaveColIndex) == ColumnType.SYMBOL
                    && masterMetadata.isSymbolTableStatic(masterColIndex)
                    && slaveMetadata.isSymbolTableStatic(slaveColIndex)
                    && writeSymbolAsStringB.get(masterColIndex)
                    && writeSymbolAsStringA.get(slaveColIndex)) {
                symbolJoinKeyFlags.setQuick(k, 1);
            }
        }
        // Record sinks convert per column, so a column shared with a key pair that keeps
        // string comparison must keep it in every pair.
        boolean isChanged = true;
        while (isChanged) {
            isChanged = false;
            for (int k = 0; k < keyCount; k++) {
                if (symbolJoinKeyFlags.getQuick(k) == 1) {
                    for (int j = 0; j < keyCount; j++) {
                        if (symbolJoinKeyFlags.getQuick(j) == 0
                                && (listColumnFilterA.getColumnIndexFactored(j) == listColumnFilterA.getColumnIndexFactored(k)
                                || listColumnFilterB.getColumnIndexFactored(j) == listColumnFilterB.getColumnIndexFactored(k))) {
                            symbolJoinKeyFlags.setQuick(k, 0);
                            isChanged = true;
                            break;
                        }
                    }
                }
            }
        }
        final IntList masterSymbolKeyCols = masterSymbolKeyColumns;
        final IntList slaveSymbolKeyCols = slaveSymbolKeyColumns;
        masterSymbolKeyCols.clear();
        slaveSymbolKeyCols.clear();
        for (int k = 0; k < keyCount; k++) {
            if (symbolJoinKeyFlags.getQuick(k) == 1) {
                final int slaveColIndex = listColumnFilterA.getColumnIndexFactored(k);
                final int masterColIndex = listColumnFilterB.getColumnIndexFactored(k);
                keyTypes.set(k, ColumnType.INT);
                masterSymbolKeyCols.add(masterColIndex);
                slaveSymbolKeyCols.add(slaveColIndex);
            }
        }
        if (masterSymbolKeyCols.size() > 0) {
            // Unset the bits AFTER the loop, so that a column used by more than one
            // key pair keeps its bit until the loop has checked every pair
            for (int i = 0, n = masterSymbolKeyCols.size(); i < n; i++) {
                writeSymbolAsStringB.unset(masterSymbolKeyCols.getQuick(i));
                writeSymbolAsStringA.unset(slaveSymbolKeyCols.getQuick(i));
            }
            return new int[][]{masterSymbolKeyCols.toArray(), slaveSymbolKeyCols.toArray()};
        }
        return null;
    }

    /**
     * Consumes both factories on entry; the selected full-fat constructor consumes metadata as well.
     */
    @NotNull
    private RecordCursorFactory createFullFatJoin(
            RecordCursorFactory master,
            RecordMetadata masterMetadata,
            CharSequence masterAlias,
            RecordCursorFactory slave,
            RecordMetadata slaveMetadata,
            CharSequence slaveAlias,
            int joinPosition,
            FullFatJoinGenerator generator,
            Plannable joinContext,
            long toleranceInterval
    ) throws SqlException {
        JoinRecordMetadata metadata = null;
        boolean isTransferred = false;
        try {
            // create hash set of key columns to easily find them
            intHashSet.clear();
            for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
                intHashSet.add(listColumnFilterA.getColumnIndexFactored(i));
            }
            intHashSet.remove(slaveMetadata.getTimestampIndex());
            final int slaveOutputColumnCount = slaveMetadata.getColumnCount()
                    + listColumnFilterA.getColumnCount() - intHashSet.size();

            // map doesn't support variable length types in map value, which is ok
            // when we join tables on strings - technically string is the key,
            // and we do not need to store it in value, but we will still reject
            //
            // never mind, this is a stop-gap measure until I understand the problem
            // fully

            for (int k = 0, m = slaveMetadata.getColumnCount(); k < m; k++) {
                if (intHashSet.excludes(k)) {
                    // A non-key slave column is materialized into the map value, so it must be a
                    // type the value sink can store. That excludes variable-length types and
                    // fixed-size types the map value cannot hold (e.g. INTERVAL). Reject them here
                    // with a user-facing message instead of letting RecordValueSinkFactory throw a
                    // bare UnsupportedOperationException.
                    if (!RecordValueSinkFactory.isSupportedColumnType(slaveMetadata.getColumnType(k))) {
                        throw SqlException
                                .position(joinPosition).put("right side column '")
                                .put(slaveMetadata.getColumnName(k)).put("' is of unsupported type");
                    }
                }
            }

            // at this point listColumnFilterB has column indexes of the master record that are JOIN keys
            // so masterCopier writes key columns of master record to a sink
            RecordSink masterCopier = createRecordCopierMaster(masterMetadata);

            // This metadata allocates native memory, it has to be closed in case join
            // generation is unsuccessful. The exception can be thrown anywhere between
            // try...catch
            metadata = new JoinRecordMetadata(
                    configuration,
                    masterMetadata.getColumnCount() + slaveOutputColumnCount
            );

            // metadata will have master record verbatim
            metadata.copyColumnMetadataFrom(masterAlias, masterMetadata);

            // slave record is split across key and value of map
            // the rationale is not to store columns twice
            // especially when map value does not support variable
            // length types

            final IntList columnIndex = new IntList(slaveOutputColumnCount);
            // In map record value columns go first, so at this stage
            // we add to metadata all slave columns that are not keys.
            // Add the same columns to filter while we are in this loop.

            // We clear listColumnFilterB because after this loop it will
            // contain indexes of slave table columns that are not keys.
            ColumnFilter masterTableKeyColumns = listColumnFilterB.copy();
            listColumnFilterB.clear();
            valueTypes.clear();
            ArrayColumnTypes slaveTypes = new ArrayColumnTypes();
            int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
            int slaveValueTimestampIndex = -1;
            for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
                if (intHashSet.excludes(i)) {
                    // this is not a key column. Add it to metadata as it is. Symbols columns are kept as symbols
                    final TableColumnMetadata m = slaveMetadata.getColumnMetadata(i);
                    metadata.add(slaveAlias, m);
                    listColumnFilterB.add(i + 1);
                    columnIndex.add(i);
                    valueTypes.add(m.getColumnType());
                    slaveTypes.add(m.getColumnType());
                    if (i == slaveTimestampIndex) {
                        slaveValueTimestampIndex = valueTypes.getColumnCount() - 1;
                    }
                }
            }
            assert slaveValueTimestampIndex != -1;

            // now add key columns to metadata
            int internalKeySuffix = 0;
            for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
                int index = listColumnFilterA.getColumnIndexFactored(i);
                TableColumnMetadata m = slaveMetadata.getColumnMetadata(index);
                if (intHashSet.remove(index) < 0) {
                    // Every equality keeps its map key slot. Repeated slave columns
                    // get internal names until the logical output projection hides them.
                    String internalName;
                    do {
                        internalName = "__questdb_temporal_key_" + internalKeySuffix++;
                    } while (slaveMetadata.getColumnIndexQuiet(internalName) >= 0);
                    final TableColumnMetadata repeated = new TableColumnMetadata(internalName, m.getColumnType(),
                            m.getIndexType(), m.getIndexValueBlockCapacity(), m.isSymbolTableStatic(), m.getMetadata());
                    repeated.setParquetEncodingConfig(m.getParquetEncodingConfig());
                    m = repeated;
                }
                // Slave SYMBOL key paired with a non-SYMBOL master, or stored as VARCHAR
                // because the slave column also pairs with a VARCHAR key. The full-fat
                // join's SymbolWrapOverJoinRecord wraps slave-key reads over the master,
                // which has no matching symbol table here, so expose the column with the
                // map key type and read it from the map (where the slave key is stored).
                // slaveTypes must follow the rewritten type as well: it feeds
                // NullRecordFactory, and a SymbolConstant.NULL slot would throw from
                // getVarcharSize / getVarcharB on the outer-join no-match path.
                final int masterKeyColIdx = masterTableKeyColumns.getColumnIndexFactored(i);
                final int keyType = keyTypes.getColumnType(i);
                if (ColumnType.tagOf(m.getColumnType()) == ColumnType.SYMBOL
                        && (ColumnType.tagOf(masterMetadata.getColumnType(masterKeyColIdx)) != ColumnType.SYMBOL || keyType == VARCHAR)) {
                    metadata.add(slaveAlias, new TableColumnMetadata(
                            m.getColumnName(), keyType, IndexType.NONE, 0, false, m.getMetadata()));
                    slaveTypes.add(keyType);
                } else {
                    if (ColumnType.isSymbol(m.getColumnType())
                            && m.isSymbolTableStatic() != masterMetadata.isSymbolTableStatic(masterKeyColIdx)) {
                        // Slave symbol keys use the matching master's dictionary.
                        final TableColumnMetadata keyMetadata = new TableColumnMetadata(
                                m.getColumnName(), m.getColumnType(), m.getIndexType(), m.getIndexValueBlockCapacity(),
                                masterMetadata.isSymbolTableStatic(masterKeyColIdx), m.getMetadata());
                        keyMetadata.setParquetEncodingConfig(m.getParquetEncodingConfig());
                        m = keyMetadata;
                    }
                    metadata.add(slaveAlias, m);
                    slaveTypes.add(m.getColumnType());
                }
                columnIndex.add(index);
            }

            if (masterMetadata.getTimestampIndex() != -1) {
                metadata.setTimestampIndex(masterMetadata.getTimestampIndex());
            }
            final RecordSink slaveCopier = createRecordCopierSlave(slaveMetadata);
            final RecordValueSink slaveValueSink = RecordValueSinkFactory.getInstance(asm, slaveMetadata, listColumnFilterB);
            isTransferred = true;
            return generator.create(
                    configuration,
                    metadata,
                    master,
                    slave,
                    keyTypes,
                    valueTypes,
                    slaveTypes,
                    masterCopier,
                    slaveCopier,
                    masterMetadata.getColumnCount(),
                    slaveValueSink,
                    columnIndex,
                    joinContext,
                    masterTableKeyColumns,
                    toleranceInterval,
                    slaveValueTimestampIndex
            );

        } catch (Throwable e) {
            if (!isTransferred) {
                Misc.free(metadata, e);
                Misc.free(master, e);
                Misc.free(slave, e);
            }
            throw e;
        }
    }

    @NotNull
    private JoinRecordMetadata createJoinMetadata(
            CharSequence masterAlias,
            RecordMetadata masterMetadata,
            CharSequence slaveAlias,
            RecordMetadata slaveMetadata
    ) {
        return createJoinMetadata(
                masterAlias,
                masterMetadata,
                slaveAlias,
                slaveMetadata,
                masterMetadata.getTimestampIndex()
        );
    }

    private @NotNull RecordSink createRecordCopierMaster(RecordMetadata masterMetadata) {
        return RecordSinkFactory.getInstance(
                configuration,
                asm,
                masterMetadata,
                listColumnFilterB,
                writeSymbolAsStringB,
                writeStringAsVarcharB,
                writeTimestampAsNanosB
        );
    }

    private @NotNull RecordSink createRecordCopierSlave(RecordMetadata slaveMetadata) {
        return RecordSinkFactory.getInstance(
                configuration,
                asm,
                slaveMetadata,
                listColumnFilterA,
                writeSymbolAsStringA,
                writeStringAsVarcharA,
                writeTimestampAsNanosA
        );
    }

    private @NotNull SymbolShortCircuit createSymbolShortCircuit(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            boolean isSelfJoin
    ) {
        SymbolShortCircuit symbolShortCircuit = NoopSymbolShortCircuit.INSTANCE;
        assert listColumnFilterA.getColumnCount() == listColumnFilterB.getColumnCount();
        SymbolJoinKeyMapping[] mappings = null;
        for (int i = 0, n = listColumnFilterA.getColumnCount(); i < n; i++) {
            int masterIndex = listColumnFilterB.getColumnIndexFactored(i);
            int slaveIndex = listColumnFilterA.getColumnIndexFactored(i);
            if (slaveMetadata.getColumnType(slaveIndex) == ColumnType.SYMBOL && slaveMetadata.isSymbolTableStatic(slaveIndex)) {
                int masterColType = masterMetadata.getColumnType(masterIndex);
                SymbolJoinKeyMapping newMapping;
                switch (masterColType) {
                    case SYMBOL -> {
                        if (isSelfJoin && masterIndex == slaveIndex) {
                            // self join on the same column -> there is no point in attempting short-circuiting
                            // NOTE: This check is naive, it can generate false positives
                            //       For example 'select t1.s, t2.s2 from t as t1 asof join t as t2 on t1.s = t2.s2'
                            //       This is deemed as a self-join (which it is), and due to the way columns are projected
                            //       it will take this branch (even when it fact it's comparing different columns)
                            //       and won't create a short circuit. This is OK from correctness perspective,
                            //       but it is a missed opportunity for performance optimization. Doing a perfect check
                            //       would require a more complex logic, which is not worth it for now
                            continue;
                        }
                        newMapping = new SymbolToSymbolJoinKeyMapping(configuration, masterIndex, slaveIndex);
                    }
                    case VARCHAR, VARCHAR_SLICE -> newMapping = new VarcharToSymbolJoinKeyMapping(masterIndex, slaveIndex);
                    case STRING -> newMapping = new StringToSymbolJoinKeyMapping(masterIndex, slaveIndex);
                    default -> {
                        // unsupported type for short circuit
                        continue;
                    }
                }
                if (symbolShortCircuit == NoopSymbolShortCircuit.INSTANCE) {
                    // ok, a single symbol short circuit
                    symbolShortCircuit = (SymbolShortCircuit) newMapping;
                } else if (mappings == null) {
                    // 2 symbol mappings, we need to chain them
                    mappings = new SymbolJoinKeyMapping[2];
                    mappings[0] = (SymbolJoinKeyMapping) symbolShortCircuit;
                    mappings[1] = newMapping;
                    symbolShortCircuit = new ChainedSymbolShortCircuit(mappings);
                } else {
                    // ok, this is pretty uncommon - a join key with more than 2 symbol short circuits
                    // this allocates arrays, but it should be very rare
                    int size = mappings.length;
                    SymbolJoinKeyMapping[] newMappings = Arrays.copyOf(mappings, size + 1);
                    newMappings[size] = newMapping;
                    symbolShortCircuit = new ChainedSymbolShortCircuit(newMappings);
                    mappings = newMappings;
                }
            }
        }
        return symbolShortCircuit;
    }

    // The last window join treats an earlier aggregate's alias as an aggregate when it names a
    // group-by function; that alias reads a master column, which disables vectorization.
    private boolean hasAggregateNamedOutput(WindowJoinPlan plan, int stepIndex) {
        final OutputSchema output = plan.getOutput();
        for (int s = 0; s < stepIndex; s++) {
            final IntList ids = plan.getSteps().getQuick(s).getAggregateColumnIds();
            for (int i = 0, n = ids.size(); i < n; i++) {
                if (functionParser.getFunctionFactoryCache().isGroupBy(output.getColumnName(output.getColumnIndexById(ids.getQuick(i))))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Instantiates the step's aggregates into the given list, which the caller owns and frees on
     * failure, and declares their value types.
     */
    private void instantiateWindowJoinAggregates(
            ObjList<FunctionExpression> aggregates,
            OutputSchema scope,
            JoinRecordMetadata joinMetadata,
            int masterTimestampIndex,
            ObjList<GroupByFunction> groupByFunctions,
            FunctionBinder binder,
            SqlExecutionContext executionContext
    ) throws SqlException {
        valueTypes.clear();
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            final FunctionExpression call = aggregates.getQuick(i);
            final GroupByFunction function = (GroupByFunction) binder.instantiateAggregate(call, scope, joinMetadata, executionContext);
            groupByFunctions.add(function);
            GroupByUtils.validateTimestampOrder(function, masterTimestampIndex, false, call.getPosition());
            function.initValueTypes(valueTypes);
        }
    }

    private boolean isKeyedTemporalJoin(RecordMetadata masterMetadata, RecordMetadata slaveMetadata) {
        // Check if we can simplify ASOF JOIN ON (ts) to ASOF JOIN.
        if (listColumnFilterA.size() == 1 && listColumnFilterB.size() == 1) {
            int masterIndex = listColumnFilterB.getColumnIndexFactored(0);
            int slaveIndex = listColumnFilterA.getColumnIndexFactored(0);
            return masterIndex != masterMetadata.getTimestampIndex() || slaveIndex != slaveMetadata.getTimestampIndex();
        }
        return listColumnFilterA.size() > 0 && listColumnFilterB.size() > 0;
    }

    private boolean isWindowJoinVectorized(WindowJoinPlan plan, int stepIndex, ObjList<GroupByFunction> groupByFunctions, int splitIndex) {
        final WindowJoinStep step = plan.getSteps().getQuick(stepIndex);
        final ObjList<FunctionExpression> aggregates = step.getAggregates();
        boolean isVectorized = aggregates.size() > 0;
        for (int i = 0, n = aggregates.size(); i < n && isVectorized; i++) {
            isVectorized = groupByFunctions.getQuick(i).supportsBatchComputation()
                    && isWindowJoinSlaveOnly(aggregates.getQuick(i), step.getScope(), splitIndex);
        }
        if (isVectorized && stepIndex > 0 && stepIndex == plan.getSteps().size() - 1) {
            isVectorized = !hasAggregateNamedOutput(plan, stepIndex);
        }
        return isVectorized;
    }

    private void processJoinKeyTypes(
            boolean isSelfJoin,
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            IntList keyPositions
    ) throws SqlException {
        // compare types and populate keyTypes
        keyTypes.clear();
        writeSymbolAsStringA.clear();
        writeSymbolAsStringB.clear();
        writeStringAsVarcharA.clear();
        writeStringAsVarcharB.clear();
        writeTimestampAsNanosA.clear();
        writeTimestampAsNanosB.clear();
        for (int k = 0, m = listColumnFilterA.getColumnCount(); k < m; k++) {
            // Don't use tagOf(columnType) to compare the types.
            // Key types have too much exactly except SYMBOL and STRING special case
            final int columnIndexA = listColumnFilterA.getColumnIndexFactored(k);
            final int columnIndexB = listColumnFilterB.getColumnIndexFactored(k);
            final int columnTypeA = slaveMetadata.getColumnType(columnIndexA);
            final String columnNameA = slaveMetadata.getColumnName(columnIndexA);
            final int columnTypeB = masterMetadata.getColumnType(columnIndexB);
            final String columnNameB = masterMetadata.getColumnName(columnIndexB);
            if (columnTypeB != columnTypeA
                    && !(isSymbolOrStringOrVarchar(columnTypeB) && isSymbolOrStringOrVarchar(columnTypeA))
                    && !(isTimestamp(columnTypeB) && isTimestamp(columnTypeA))
            ) {
                // index in column filter and join context is the same
                throw SqlException.$(keyPositions.getQuick(k), "join column type mismatch");
            }
            if (isVarchar(columnTypeA) || isVarchar(columnTypeB)) {
                keyTypes.add(VARCHAR);
                if (isVarchar(columnTypeA)) {
                    writeStringAsVarcharB.set(columnIndexB);
                } else {
                    writeStringAsVarcharA.set(columnIndexA);
                }
                writeSymbolAsStringA.set(columnIndexA);
                writeSymbolAsStringB.set(columnIndexB);
            } else if (columnTypeB == ColumnType.SYMBOL) {
                if (isSelfJoin && Chars.equalsIgnoreCase(columnNameA, columnNameB)) {
                    keyTypes.add(ColumnType.SYMBOL);
                } else {
                    keyTypes.add(STRING);
                    writeSymbolAsStringA.set(columnIndexA);
                    writeSymbolAsStringB.set(columnIndexB);
                }
            } else if (isString(columnTypeA) || isString(columnTypeB)) {
                keyTypes.add(columnTypeB);
                writeSymbolAsStringA.set(columnIndexA);
                writeSymbolAsStringB.set(columnIndexB);
            } else if (columnTypeA != columnTypeB &&
                    isTimestamp(columnTypeA) && isTimestamp(columnTypeB)
            ) {
                keyTypes.add(TIMESTAMP_NANO);
                // Mark columns that need conversion to nanoseconds
                if (!isTimestampNano(columnTypeA)) {
                    writeTimestampAsNanosA.set(columnIndexA);
                }
                if (!isTimestampNano(columnTypeB)) {
                    writeTimestampAsNanosB.set(columnIndexB);
                }
            } else {
                keyTypes.add(columnTypeB);
            }
        }
        alignSharedJoinKeyTypes(masterMetadata, slaveMetadata);
    }

    private static final class JoinCondition implements Plannable {
        private final String text;

        private JoinCondition(String text) {
            this.text = text;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(text);
        }
    }

    @FunctionalInterface
    public interface FullFatJoinGenerator {
        RecordCursorFactory create(
                CairoConfiguration configuration,
                RecordMetadata metadata,
                RecordCursorFactory masterFactory,
                RecordCursorFactory slaveFactory,
                @Transient ColumnTypes mapKeyTypes,
                @Transient ColumnTypes mapValueTypes,
                @Transient ColumnTypes slaveColumnTypes,
                RecordSink masterKeySink,
                RecordSink slaveKeySink,
                int columnSplit,
                RecordValueSink slaveValueSink,
                IntList columnIndex,
                Plannable joinContext,
                ColumnFilter masterTableKeyColumns,
                long toleranceInterval,
                int slaveValueTimestampIndex
        );
    }
}
