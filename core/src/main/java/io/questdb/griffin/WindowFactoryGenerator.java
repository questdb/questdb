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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.lv.LiveViewCheckpointRangePlan;
import io.questdb.cairo.lv.LiveViewCheckpointRowsPlan;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowMapGroups;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.LiveViewCheckpointFunctionCompiler;
import io.questdb.griffin.engine.window.LiveViewWindowDescription;
import io.questdb.griffin.engine.window.WindowAccumulatorPlan;
import io.questdb.griffin.engine.window.WindowAccumulatorPlanBuilder;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowMapSpec;
import io.questdb.griffin.engine.window.WindowMapState;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import org.jetbrains.annotations.Nullable;

/**
 * Builds window-function factories: the cached and streaming window operators, their
 * input ordering and the fusion of a keep-flag filter into a cached window.
 */
final class WindowFactoryGenerator {
    // Read-only: WindowMapSpec.of copies directions, and only an ordered window mutates or borrows its own list.
    private static final IntList NO_DIRECTIONS = new IntList(0);
    private final BytecodeAssembler asm;
    private final SqlCodeGenerator codeGenerator;
    private final CairoConfiguration configuration;
    private final EntityColumnFilter entityColumnFilter;
    private final FunctionFactoryCache functionFactoryCache;
    private final RecordComparatorCompiler recordComparatorCompiler;

    WindowFactoryGenerator(
            CairoConfiguration configuration,
            SqlCodeGenerator codeGenerator,
            FunctionFactoryCache functionFactoryCache,
            BytecodeAssembler asm,
            EntityColumnFilter entityColumnFilter,
            RecordComparatorCompiler recordComparatorCompiler
    ) {
        this.configuration = configuration;
        this.codeGenerator = codeGenerator;
        this.functionFactoryCache = functionFactoryCache;
        this.asm = asm;
        this.entityColumnFilter = entityColumnFilter;
        this.recordComparatorCompiler = recordComparatorCompiler;
    }

    private static boolean hasNestedUnionAll(LogicalPlan plan) {
        return LogicalPlans.skipProjectsAndFilters(plan) instanceof SetOperationPlan operation
                && operation.getOperation() == SetOperationKind.UNION_ALL;
    }

    private static boolean isModelOrderPrefix(WindowSpec spec, @Nullable SortPlan modelOrder) {
        final int count = spec.getOrderByColumnIds().size();
        if (modelOrder == null || count == 0 || count > modelOrder.getColumnIds().size()) {
            return false;
        }
        for (int i = 0; i < count; i++) {
            if (spec.getOrderByColumnIds().getQuick(i) != modelOrder.getColumnIds().getQuick(i)
                    || spec.getOrderByDirections().getQuick(i) != modelOrder.getDirections().getQuick(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A live view resolves its anchor expression and partition keys against the window's input,
     * so projection aliases of input columns name that input.
     */
    private static RecordCursorFactory renameWindowInput(RecordCursorFactory base, OutputSchema input, ProjectPlan projection) {
        final GenericRecordMetadata renamed;
        final IntList mapping;
        try {
            final RecordMetadata metadata = base.getMetadata();
            final int columnCount = metadata.getColumnCount();
            final ObjList<CharSequence> names = new ObjList<>(columnCount);
            names.setPos(columnCount);
            boolean isRenamed = false;
            for (int i = 0, n = projection.getExpressions().size(); i < n; i++) {
                if (!(projection.getExpressions().getQuick(i) instanceof ColumnExpression column)) {
                    continue;
                }
                final int index = input.getColumnIndexById(column.getColumnId());
                final CharSequence name = projection.getOutput().getColumnName(i);
                if (index >= 0 && names.getQuick(index) == null && !Chars.equalsIgnoreCase(name, metadata.getColumnName(index))
                        && metadata.getColumnIndexQuiet(name) < 0) {
                    names.setQuick(index, name);
                    isRenamed = true;
                }
            }
            if (!isRenamed) {
                return base;
            }
            renamed = new GenericRecordMetadata();
            mapping = new IntList(columnCount);
            for (int i = 0; i < columnCount; i++) {
                final TableColumnMetadata column = metadata.getColumnMetadata(i);
                final CharSequence name = names.getQuick(i);
                renamed.add(name == null ? column : new TableColumnMetadata(Chars.toString(name), column.getColumnType(),
                        column.getIndexType(), column.getIndexValueBlockCapacity(), column.isSymbolTableStatic(), column.getMetadata()));
                mapping.add(i);
            }
            renamed.setTimestampIndex(metadata.getTimestampIndex());
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return new SelectedRecordCursorFactory(renamed, mapping, base);
    }

    private boolean hasGroupByWindowFunction(GenerationFrame frame, WindowPlan window) {
        for (int i = 0, n = window.getFunctions().size(); i < n; i++) {
            if (functionFactoryCache.isGroupBy(window.getFunctions().getQuick(i).getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * A projection the window factory can emit directly: plain column references that select every
     * window output once, at unchanged types.
     */
    static boolean isWindowOutputProjection(ProjectPlan project, WindowPlan window) {
        if (project.hasTimestampDeclaration()) {
            return false;
        }
        final OutputSchema input = window.getOutput();
        int windowCount = 0;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column)) {
                return false;
            }
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index < 0 || column.isCast() || !column.isDirectReference() || input.getColumnType(index) != project.getOutput().getColumnType(i)) {
                return false;
            }
            if (window.getFunctionColumnIds().indexOf(column.getColumnId(), 0, window.getFunctionColumnIds().size()) >= 0) {
                if (SqlCodeGenerator.isColumnSelectedBefore(project, i, column.getColumnId())) {
                    return false;
                }
                windowCount++;
            }
        }
        return windowCount == window.getFunctionColumnIds().size();
    }

    // Borrows the factory. Callers must pass a bare column predicate. The internal marker guarantees
    // the outer projection drops the keep flag: fusion skips its boolean write, so a user-visible
    // row-selecting boolean must retain the ordinary window and filter path.
    static boolean tryFuseKeepFlagFilter(RecordCursorFactory factory, int columnIndex) {
        if (!(factory instanceof CachedWindowLightRecordCursorFactory windowFactory)) {
            return false;
        }
        final WindowFunction fn = windowFactory.getSingleRowSelectingFunction();
        if (fn == null) {
            return false;
        }
        final RecordMetadata metadata = windowFactory.getMetadata();
        if (columnIndex < 0 || columnIndex != fn.getColumnIndex() || metadata.getColumnType(columnIndex) != ColumnType.BOOLEAN) {
            return false;
        }
        windowFactory.enableRowSelecting(fn);
        return true;
    }

    /**
     * Consumes the base and window functions; both representations share cached selection.
     */
    RecordCursorFactory generateCachedWindow(
            RecordCursorFactory base,
            GenericRecordMetadata factoryMetadata,
            GenericRecordMetadata chainMetadata,
            ArrayColumnTypes chainTypes,
            IntList columnIndexes,
            ListColumnFilter listColumnFilterA,
            ListColumnFilter listColumnFilterB,
            ObjObjHashMap<IntList, ObjList<WindowFunction>> groupedWindow,
            ObjList<WindowFunction> naturalOrderFunctions,
            ObjList<WindowFunction> cachedWindowSpecFunctions,
            ObjList<WindowMapSpec> cachedWindowMapSpecs,
            ObjList<TableColumnMetadata> deferredWindowMetadata,
            ObjList<SymbolFunction> windowSymbolFunctions,
            boolean isAllWindowOutputFixedWidth
    ) throws SqlException {
        CachedWindowMapGroups cachedWindowMapGroups = null;
        boolean isTransferred = false;
        try {
            final ObjList<RecordComparator> windowComparators = new ObjList<>(groupedWindow.size());
            final ObjList<ObjList<WindowFunction>> functionGroups = new ObjList<>(groupedWindow.size());
            final ObjList<IntList> keys = new ObjList<>();
            final boolean isSortEnabled = configuration.isSqlOrderBySortEnabled();
            boolean isAllGroupsEncodedEligible = isSortEnabled;
            for (ObjObjHashMap.Entry<IntList, ObjList<WindowFunction>> e : groupedWindow) {
                final boolean isEncodedEligible = isSortEnabled && SortKeyEncoder.isSupported(chainMetadata, e.key);
                final RecordComparator comparator = isEncodedEligible
                        ? null
                        : recordComparatorCompiler.newInstance(chainMetadata, e.key);
                windowComparators.add(comparator);
                functionGroups.add(e.value);
                keys.add(e.key);
                isAllGroupsEncodedEligible &= isEncodedEligible;
            }

            // The Map subgroups the sort groups' functions form, compiled one bucket at a
            // time so a group is by construction driven by one traversal. Built into a local
            // the outer catch can free: each group owns a map, and whichever factory is built
            // below takes ownership on entry to its constructor.
            if (cachedWindowMapSpecs != null) {
                // Give the groups' key projection its own snapshot of the assembled chain layout.
                final ArrayColumnTypes chainRecordTypes = new ArrayColumnTypes();
                for (int c = 0, n = chainTypes.getColumnCount(); c < n; c++) {
                    chainRecordTypes.add(chainTypes.getColumnType(c));
                }
                cachedWindowMapGroups = CachedWindowMapGroups.of(
                        configuration,
                        asm,
                        functionGroups,
                        naturalOrderFunctions,
                        cachedWindowSpecFunctions,
                        cachedWindowMapSpecs,
                        chainRecordTypes
                );
            }

            // LIGHT path is restricted to queries where every ordered group can use the encoded
            // sort buffer. Tree-fallback in LIGHT would do O(N log N) random base reads per
            // compare, which can regress 10-100x on cold/partitioned bases.
            // It also requires every window-output column to be fixed-width: the narrow chain
            // never initializes var-size aux pointers, so a var-size output column would read
            // uninitialized offsets and crash. No window function returns a var-size type today;
            // this guard keeps the path safe if one is ever added.
            if (configuration.isSqlWindowCachedLightEnabled()
                    && base.recordCursorSupportsRandomAccess()
                    && isAllGroupsEncodedEligible
                    && isAllWindowOutputFixedWidth) {
                final IntList sourceMap = new IntList();
                final ArrayColumnTypes narrowChainTypes = new ArrayColumnTypes();
                int narrowIdx = 0;
                for (int c = 0, chainColCount = chainTypes.getColumnCount(); c < chainColCount; c++) {
                    final TableColumnMetadata m = deferredWindowMetadata.getQuiet(c);
                    if (m != null) {
                        sourceMap.add(-narrowIdx - 1);
                        narrowChainTypes.add(m.getColumnType());
                        narrowIdx++;
                    } else {
                        sourceMap.add(columnIndexes.getQuick(c));
                    }
                }
                isTransferred = true;
                final CachedWindowLightRecordCursorFactory lightFactory = new CachedWindowLightRecordCursorFactory(
                        configuration,
                        base,
                        factoryMetadata,
                        narrowChainTypes,
                        functionGroups,
                        naturalOrderFunctions,
                        columnIndexes,
                        keys,
                        chainMetadata,
                        sourceMap,
                        cachedWindowMapGroups,
                        windowSymbolFunctions
                );
                cachedWindowMapGroups = null;
                return lightFactory;
            }

            final RecordSink recordSink = RecordSinkFactory.getInstance(
                    configuration,
                    asm,
                    chainTypes,
                    listColumnFilterA,
                    null,
                    listColumnFilterB,
                    null,
                    null
            );

            isTransferred = true;
            final CachedWindowRecordCursorFactory cachedFactory = new CachedWindowRecordCursorFactory(
                    configuration,
                    base,
                    recordSink,
                    factoryMetadata,
                    chainTypes,
                    windowComparators,
                    functionGroups,
                    naturalOrderFunctions,
                    columnIndexes,
                    keys,
                    chainMetadata,
                    cachedWindowMapGroups,
                    windowSymbolFunctions
            );
            cachedWindowMapGroups = null;
            return cachedFactory;
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.free(base, th);
                Misc.free(cachedWindowMapGroups, th);
                for (ObjObjHashMap.Entry<IntList, ObjList<WindowFunction>> entry : groupedWindow) {
                    Misc.freeObjList(entry.value, th);
                }
                Misc.freeObjList(naturalOrderFunctions, th);
            }
            throw th;
        } finally {
            // Factories retain the function lists, not this compilation lookup.
            groupedWindow.clear();
        }
    }

    /**
     * Consumes the base, functions and optional checkpoint rows plan.
     */
    RecordCursorFactory generateStreamingWindow(
            RecordCursorFactory base,
            GenericRecordMetadata metadata,
            ObjList<Function> functions,
            ObjList<WindowMapSpec> specs,
            ObjList<WindowFunction> anchorableFunctions,
            LiveViewCheckpointRangePlan rangePlan,
            LiveViewCheckpointRowsPlan rowsPlan
    ) {
        ObjList<WindowMapState> states = null;
        final ObjList<WindowAccumulatorPlan> plans;
        try {
            plans = specs == null ? null : WindowAccumulatorPlanBuilder.compileGroups(functions, specs, base.getMetadata());
            states = WindowMapState.createGroups(configuration, asm, plans, base.getMetadata());
        } catch (Throwable th) {
            Misc.free(base, th);
            Misc.freeObjList(states, th);
            Misc.freeObjList(functions, th);
            Misc.free(rowsPlan, th);
            throw th;
        }
        return new WindowRecordCursorFactory(base, metadata, functions, anchorableFunctions, rangePlan, rowsPlan, plans, states);
    }

    RecordCursorFactory generateWindow(GenerationFrame frame, WindowPlan window, ProjectPlan projection, int requiredOrderColumnId, int requiredScanDirection,
                                       SortPlan orderAdvice, boolean isModelOrder, int orderByMnemonic,
                                       SqlExecutionContext executionContext) throws SqlException {
        int orderId = -1;
        int direction = RecordCursorFactory.SCAN_DIRECTION_OTHER;
        // Order advice, or else a uniform window order, lets a nested UNION ALL merge its branches in that order.
        if (requiredOrderColumnId >= 0) {
            if (window.getInput().getOutput().getColumnIndexById(requiredOrderColumnId) >= 0) {
                orderId = requiredOrderColumnId;
                direction = requiredScanDirection;
            }
        } else if (hasNestedUnionAll(window.getInput())) {
            final ObjList<WindowSpec> specs = window.getSpecs();
            for (int i = 0, n = specs.size(); i < n; i++) {
                final WindowSpec spec = specs.getQuick(i);
                if (spec.getOrderByColumnIds().size() != 1) {
                    orderId = -1;
                    break;
                }
                final int id = spec.getOrderByColumnIds().getQuick(0);
                final int specDirection = spec.getOrderByDirections().getQuick(0) == SortDirection.DESCENDING
                        ? RecordCursorFactory.SCAN_DIRECTION_BACKWARD : RecordCursorFactory.SCAN_DIRECTION_FORWARD;
                if (i > 0 && (id != orderId || specDirection != direction)) {
                    orderId = -1;
                    break;
                }
                orderId = id;
                direction = specDirection;
            }
        }
        final int inputMnemonic = isModelOrder || orderByMnemonic == OrderByMnemonic.ORDER_BY_INVARIANT || hasGroupByWindowFunction(frame, window)
                ? OrderByMnemonic.ORDER_BY_INVARIANT : OrderByMnemonic.ORDER_BY_REQUIRED;
        final RecordCursorFactory base = codeGenerator.generate(frame, window.getInput(), executionContext, orderId,
                orderId < 0 ? RecordCursorFactory.SCAN_DIRECTION_OTHER : direction,
                SqlCodeGenerator.hasColumns(window.getInput().getOutput(), orderAdvice) && !orderAdvice.hasAliasedKey() ? orderAdvice : null, null, inputMnemonic);
        return generateWindow(frame, window, projection, base, isModelOrder ? orderAdvice : null, executionContext);
    }

    /**
     * Consumes the input and builds window functions against its final record layout. A column-only
     * projection, when given, orders the output; the cached chain keeps unselected inputs after it.
     */
    RecordCursorFactory generateWindow(GenerationFrame frame, WindowPlan plan, @Nullable ProjectPlan projection, RecordCursorFactory base,
                                       @Nullable SortPlan modelOrder, SqlExecutionContext executionContext) throws SqlException {
        final FunctionInstantiator instantiator = frame.functionInstantiator;
        final OutputSchema input = plan.getInput().getOutput();
        if (projection != null && executionContext.isLiveViewCompile()) {
            base = renameWindowInput(base, input, projection);
        }
        final RecordMetadata inputMetadata = base.getMetadata();
        final int inputCount = inputMetadata.getColumnCount();
        final OutputSchema output = projection == null ? plan.getOutput() : projection.getOutput();
        final int outputCount = output.getColumnCount();
        final IntList sources = new IntList(inputCount + plan.getFunctions().size());
        for (int i = 0; i < outputCount; i++) {
            final int columnId = projection == null ? output.getColumnId(i)
                    : ((ColumnExpression) projection.getExpressions().getQuick(i)).getColumnId();
            final int inputIndex = input.getColumnIndexById(columnId);
            sources.add(inputIndex >= 0 ? inputIndex : -plan.getFunctionColumnIds().indexOf(columnId, 0, plan.getFunctionColumnIds().size()) - 1);
        }
        for (int i = 0; i < inputCount; i++) {
            if (sources.indexOf(i, 0, sources.size()) < 0) {
                sources.add(i);
            }
        }
        final OutputSchema chainSchema = new OutputSchema();
        final GenericRecordMetadata chainMetadata = new GenericRecordMetadata();
        final ObjList<TableColumnMetadata> outputColumns = frame.windowOutputColumns;
        outputColumns.clear();
        outputColumns.setPos(outputCount);
        for (int i = 0, n = sources.size(); i < n; i++) {
            final int source = sources.getQuick(i);
            if (source >= 0) {
                final TableColumnMetadata column = inputMetadata.getColumnMetadata(source);
                chainSchema.add(input.getColumnId(source), input.getColumnName(source), input.getColumnType(source), input.getMetadata(source), true);
                if (i < outputCount) {
                    final CharSequence name = output.getColumnName(i);
                    outputColumns.setQuick(i, projection == null || Chars.equals(name, column.getColumnName()) ? column
                            : new TableColumnMetadata(Chars.toString(name), column.getColumnType(), column.getIndexType(),
                            column.getIndexValueBlockCapacity(), column.isSymbolTableStatic(), column.getMetadata()));
                }
                if (sources.indexOf(source, 0, i) < 0) {
                    chainMetadata.add(i, column);
                } else {
                    final String name = column.getColumnName();
                    String unique = name;
                    for (int k = 1; chainMetadata.getColumnIndexQuiet(unique) >= 0 || inputMetadata.getColumnIndexQuiet(unique) >= 0; k++) {
                        unique = name + "_" + k;
                    }
                    chainMetadata.add(i, new TableColumnMetadata(unique, column.getColumnType(), column.getIndexType(),
                            column.getIndexValueBlockCapacity(), column.isSymbolTableStatic(), column.getMetadata()));
                }
                if (source == inputMetadata.getTimestampIndex() && chainMetadata.getTimestampIndex() < 0) {
                    chainMetadata.setTimestampIndex(i);
                }
            } else {
                final int columnId = plan.getFunctionColumnIds().getQuick(-source - 1);
                final int index = plan.getOutput().getColumnIndexById(columnId);
                chainSchema.add(columnId, plan.getOutput().getColumnName(index), plan.getOutput().getColumnType(index), true);
            }
        }
        for (int i = 0, n = sources.size(); i < n; i++) {
            if (sources.getQuick(i) < 0) {
                final CharSequence name = chainSchema.getColumnName(i);
                String unique = Chars.toString(name);
                for (int k = 1; chainMetadata.getColumnIndexQuiet(unique) >= 0; k++) {
                    unique = name + "_" + k;
                }
                chainMetadata.add(i, new TableColumnMetadata(unique, chainSchema.getColumnType(i), IndexType.NONE, 0, false, null));
            }
        }
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        final ArrayColumnTypes chainTypes = new ArrayColumnTypes();
        final ObjObjHashMap<IntList, ObjList<WindowFunction>> groups = frame.windowGroups;
        groups.clear();
        final boolean isLiveView = executionContext.isLiveViewCompile();
        ObjList<Function> functions = null;
        ObjList<WindowFunction> naturalFunctions = null;
        ObjList<Function> partitionFunctions = null;
        WindowFunction pendingWindow = null;
        LiveViewCheckpointRowsPlan rowsPlan = null;
        try {
            if (isLiveView) {
                for (int i = 0, n = plan.getFunctions().size(); i < n; i++) {
                    LiveViewCheckpointFunctionCompiler.validateRange(plan.getSpecs().getQuick(i).getLiveViewDescription(),
                            plan.getFunctions().getQuick(i).getName(), inputMetadata);
                }
            }
            for (int pass = 0; pass < 2; pass++) {
                final boolean isStreaming = pass == 0;
                final OutputSchema bindSchema = isStreaming ? input : chainSchema;
                final RecordMetadata bindMetadata = isStreaming ? inputMetadata : chainMetadata;
                final ObjList<WindowMapSpec> specs = new ObjList<>();
                final ObjList<WindowFunction> specFunctions = new ObjList<>();
                final ObjList<TableColumnMetadata> windowMetadata = new ObjList<>();
                ObjList<SymbolFunction> symbolFunctions = null;
                boolean isAllWindowOutputFixedWidth = true;
                boolean isCachedRequired = false;
                chainTypes.clear();
                if (isStreaming) {
                    functions = new ObjList<>(outputCount);
                    functions.setPos(outputCount);
                    specs.setPos(outputCount);
                    for (int i = 0; i < outputCount; i++) {
                        if (sources.getQuick(i) >= 0) {
                            functions.setQuick(i, FunctionParser.createColumn(plan.getPosition(), sources.getQuick(i), inputMetadata));
                        }
                    }
                } else {
                    for (int i = 0, n = chainSchema.getColumnCount(); i < n; i++) {
                        chainTypes.add(chainSchema.getColumnType(i));
                    }
                }
                for (int i = 0, n = plan.getFunctions().size(); i < n; i++) {
                    final WindowSpec spec = plan.getSpecs().getQuick(i);
                    final IntList order = new IntList(spec.getOrderByColumnIds().size());
                    final IntList directions = spec.getOrderByColumnIds().size() == 0 ? NO_DIRECTIONS : new IntList(spec.getOrderByColumnIds().size());
                    final ObjList<CharSequence> orderNames = new ObjList<>(spec.getOrderByNames().size());
                    for (int k = 0; k < spec.getOrderByColumnIds().size(); k++) {
                        final int index = bindSchema.getColumnIndexById(spec.getOrderByColumnIds().getQuick(k));
                        if (index < 0) {
                            throw new IllegalStateException("bound window order input has changed");
                        }
                        final SortDirection direction = spec.getOrderByDirections().getQuick(k);
                        order.add(direction == SortDirection.ASCENDING ? index + 1 : -index - 1);
                        directions.add(SqlCodeGenerator.queryModelDirection(direction));
                        orderNames.add(Chars.toString(spec.getOrderByNames().getQuick(k)));
                    }
                    final boolean isOrderDismissed = base.followedOrderByAdvice() && isModelOrderPrefix(spec, modelOrder)
                            || order.size() == 1 && (modelOrder == null || modelOrder.getColumnIds().size() < 2)
                            && Math.abs(order.getQuick(0)) - 1 == bindMetadata.getTimestampIndex()
                            && (order.getQuick(0) > 0 && base.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_FORWARD
                            || order.getQuick(0) < 0 && base.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD);
                    keyTypes.clear();
                    BitSet symbolsAsStrings = null;
                    if (spec.getPartitionBy().size() > 0) {
                        partitionFunctions = new ObjList<>(spec.getPartitionBy().size());
                        for (int k = 0; k < spec.getPartitionBy().size(); k++) {
                            final Function function = instantiator.instantiate(spec.getPartitionBy().getQuick(k), bindSchema, bindMetadata, executionContext);
                            partitionFunctions.add(function);
                            // A live-view refresh reads WAL-segment-local symbol keys, so it partitions by the resolved string.
                            if (isLiveView && ColumnType.isSymbol(function.getType())) {
                                if (symbolsAsStrings == null) {
                                    symbolsAsStrings = new BitSet();
                                }
                                symbolsAsStrings.set(k);
                                keyTypes.add(ColumnType.STRING);
                            } else {
                                keyTypes.add(function.getType());
                            }
                        }
                    }
                    final VirtualRecord partitionRecord = partitionFunctions == null ? null : new VirtualRecord(partitionFunctions);
                    final RecordSink partitionSink;
                    if (partitionRecord == null) {
                        partitionSink = null;
                    } else {
                        entityColumnFilter.of(partitionFunctions.size());
                        partitionSink = RecordSinkFactory.getInstance(configuration, asm, keyTypes, entityColumnFilter, symbolsAsStrings);
                    }
                    final WindowMapSpec mapSpec;
                    try {
                        executionContext.configureWindowContext(partitionRecord, partitionSink, keyTypes, order.size() > 0,
                                isOrderDismissed ? base.getScanDirection() : RecordCursorFactory.SCAN_DIRECTION_OTHER,
                                order.size() == 0 ? -1 : spec.getOrderByPositions().getQuick(0),
                                base.recordCursorSupportsRandomAccess(), spec.getFramingMode(),
                                spec.getRowsLo(), spec.getRowsLoExprTimeUnit(), spec.getRowsLoExprPos(), spec.getRowsLoKindPos(),
                                spec.getRowsHi(), spec.getRowsHiExprTimeUnit(), spec.getRowsHiExprPos(), spec.getRowsHiKindPos(),
                                spec.getExclusionKind(), spec.getExclusionKindPos(), bindMetadata.getTimestampIndex(),
                                inputMetadata.getTimestampType(), spec.isIgnoreNulls(), spec.getNullsDescPos());
                        pendingWindow = instantiator.instantiateWindow(plan.getFunctions().getQuick(i), bindSchema, bindMetadata, executionContext);
                        partitionFunctions = null;
                        mapSpec = WindowMapSpec.of(executionContext.getWindowContext(), spec.getPartitionBy(), order,
                                directions, isOrderDismissed, pendingWindow, bindSchema, bindMetadata);
                    } finally {
                        executionContext.clearWindowContext();
                    }
                    final WindowFunction function = pendingWindow;
                    if (spec.isSubsampleKeepFlag()) {
                        function.markSubsampleKeepFlag();
                    }
                    final int outputIndex = sources.indexOf(-i - 1, 0, sources.size());
                    if (isStreaming) {
                        functions.setQuick(outputIndex, function);
                        pendingWindow = null;
                        if (order.size() > 0 && !isOrderDismissed || function.getPassCount() != WindowFunction.ZERO_PASS) {
                            isCachedRequired = true;
                            break;
                        }
                        specs.setQuick(outputIndex, mapSpec);
                        chainTypes.clear();
                    } else {
                        specFunctions.add(function);
                        specs.add(mapSpec);
                        if (order.size() > 0 && !isOrderDismissed) {
                            if (function.getPass1ScanDirection() == WindowFunction.Pass1ScanDirection.BACKWARD) {
                                for (int k = 0; k < order.size(); k++) {
                                    order.setQuick(k, -order.getQuick(k));
                                }
                            }
                            ObjList<WindowFunction> group = groups.get(order);
                            if (group == null) {
                                groups.put(order, group = new ObjList<>());
                            }
                            group.add(function);
                        } else {
                            if (naturalFunctions == null) {
                                naturalFunctions = new ObjList<>();
                            }
                            naturalFunctions.add(function);
                        }
                        pendingWindow = null;
                    }
                    if (order.size() > 0) {
                        if (!isStreaming && !isOrderDismissed
                                && function.getPass1ScanDirection() == WindowFunction.Pass1ScanDirection.BACKWARD) {
                            for (int k = 0; k < directions.size(); k++) {
                                directions.setQuick(k, 1 - directions.getQuick(k));
                            }
                        }
                        function.initRecordComparator(codeGenerator, bindMetadata, chainTypes, order,
                                spec.getOrderByPositions(), orderNames, directions);
                    }
                    function.setColumnIndex(outputIndex);
                    final TableColumnMetadata column = new TableColumnMetadata(
                            Chars.toString(output.getColumnName(outputIndex)), function.getType(), IndexType.NONE, 0,
                            function instanceof SymbolFunction symbol && symbol.isSymbolTableStatic(), null);
                    windowMetadata.extendAndSet(outputIndex, column);
                    isAllWindowOutputFixedWidth &= !ColumnType.isVarSize(function.getType());
                    if (ColumnType.isSymbol(function.getType())) {
                        if (!(function instanceof SymbolFunction symbol)) {
                            throw new IllegalStateException("SYMBOL window function does not implement SymbolFunction");
                        }
                        if (symbolFunctions == null) {
                            symbolFunctions = new ObjList<>();
                        }
                        symbolFunctions.extendAndSet(outputIndex, symbol);
                    }
                }
                if (isCachedRequired) {
                    final ObjList<Function> unused = functions;
                    functions = null;
                    Throwable failure = Misc.freeObjListBestEffort(null, unused);
                    CairoException.rethrowCleanupFailure(failure);
                    continue;
                }
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                for (int i = 0; i < outputCount; i++) {
                    metadata.add(sources.getQuick(i) >= 0 ? outputColumns.getQuick(i) : windowMetadata.getQuick(i));
                }
                metadata.setTimestampIndex(projection == null ? inputMetadata.getTimestampIndex() : output.getTimestampIndex());
                if (isStreaming) {
                    if (!isLiveView) {
                        final RecordCursorFactory ownedBase = base;
                        final ObjList<Function> ownedFunctions = functions;
                        base = null;
                        functions = null;
                        return generateStreamingWindow(ownedBase, metadata, ownedFunctions, specs, null, null, null);
                    }
                    final ObjList<LiveViewWindowDescription> descriptions = new ObjList<>(outputCount);
                    descriptions.setPos(outputCount);
                    ObjList<WindowFunction> anchorable = null;
                    for (int i = 0; i < outputCount; i++) {
                        final int source = sources.getQuick(i);
                        if (source >= 0) {
                            continue;
                        }
                        final LiveViewWindowDescription description = plan.getSpecs().getQuick(-source - 1).getLiveViewDescription();
                        descriptions.setQuick(i, description);
                        final WindowFunction function = (WindowFunction) functions.getQuick(i);
                        if (function.supportsCheckpointState() || function.isCheckpointStateless()) {
                            LiveViewCheckpointFunctionCompiler.configure(function, description,
                                    plan.getFunctions().getQuick(-source - 1).getOverload().getFactory().getSignature(),
                                    i, inputMetadata);
                        }
                        if (description.isAnchorReset() && (description.isAnchored() || function.isCheckpointStateless())) {
                            if (anchorable == null) {
                                anchorable = new ObjList<>();
                            }
                            anchorable.add(function);
                        }
                    }
                    try {
                        rowsPlan = LiveViewCheckpointFunctionCompiler.rowsPlan(functions, descriptions, inputMetadata, configuration, asm,
                                frame.windowPartitionKeys.of(instantiator, plan, sources, input, inputMetadata, executionContext));
                    } finally {
                        frame.windowPartitionKeys.clear();
                    }
                    final LiveViewCheckpointRangePlan rangePlan = LiveViewCheckpointFunctionCompiler.rangePlan(functions, descriptions);
                    final RecordCursorFactory ownedBase = base;
                    final ObjList<Function> ownedFunctions = functions;
                    final LiveViewCheckpointRowsPlan ownedRowsPlan = rowsPlan;
                    base = null;
                    functions = null;
                    rowsPlan = null;
                    return generateStreamingWindow(ownedBase, metadata, ownedFunctions, null, anchorable, rangePlan, ownedRowsPlan);
                }
                final ListColumnFilter sourceFilter = new ListColumnFilter();
                final ListColumnFilter copyFilter = new ListColumnFilter();
                final IntList indexes = new IntList();
                for (int i = 0, n = sources.size(); i < n; i++) {
                    final int source = sources.getQuick(i);
                    if (source >= 0) {
                        copyFilter.add(i + 1);
                        sourceFilter.add(source);
                        indexes.add(source);
                    } else {
                        chainTypes.add(i, windowMetadata.getQuick(i).getColumnType());
                        copyFilter.add(-i - 1);
                        sourceFilter.add(-1);
                        indexes.add(-1);
                    }
                }
                final RecordCursorFactory ownedBase = base;
                final ObjList<WindowFunction> ownedNatural = naturalFunctions;
                base = null;
                naturalFunctions = null;
                return generateCachedWindow(ownedBase, metadata, chainMetadata, chainTypes, indexes, copyFilter, sourceFilter,
                        groups, ownedNatural, specFunctions, specs, windowMetadata, symbolFunctions, isAllWindowOutputFixedWidth);
            }
            throw new IllegalStateException("window generation did not choose an implementation");
        } catch (Throwable th) {
            Misc.free(pendingWindow, th);
            Misc.freeObjList(partitionFunctions, th);
            Misc.freeObjList(functions, th);
            for (ObjObjHashMap.Entry<IntList, ObjList<WindowFunction>> entry : groups) {
                Misc.freeObjList(entry.value, th);
            }
            groups.clear();
            Misc.freeObjList(naturalFunctions, th);
            Misc.free(rowsPlan, th);
            Misc.free(base, th);
            throw th;
        }
    }

    static final class WindowPartitionKeys implements LiveViewCheckpointFunctionCompiler.PartitionKeyCompiler, Mutable {
        private SqlExecutionContext executionContext;
        private OutputSchema input;
        private RecordMetadata inputMetadata;
        private FunctionInstantiator instantiator;
        private WindowPlan plan;
        private IntList sources;

        @Override
        public void clear() {
            instantiator = null;
            executionContext = null;
            input = null;
            inputMetadata = null;
            plan = null;
            sources = null;
        }

        @Override
        public Function compile(int functionIndex, int keyIndex) throws SqlException {
            return instantiator.instantiate(plan.getSpecs().getQuick(-sources.getQuick(functionIndex) - 1).getPartitionBy().getQuick(keyIndex),
                    input, inputMetadata, executionContext);
        }

        WindowPartitionKeys of(FunctionInstantiator instantiator, WindowPlan plan, IntList sources, OutputSchema input,
                               RecordMetadata inputMetadata, SqlExecutionContext executionContext) {
            this.instantiator = instantiator;
            this.plan = plan;
            this.sources = sources;
            this.input = input;
            this.inputMetadata = inputMetadata;
            this.executionContext = executionContext;
            return this;
        }
    }
}
