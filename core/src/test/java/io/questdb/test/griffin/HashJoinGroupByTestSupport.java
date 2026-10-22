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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.HashJoinGroupByCandidate;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.PreparedFunctions;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.LogicalPlanPrinter;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import org.junit.Assert;

final class HashJoinGroupByTestSupport {
    static HashJoinGroupByCandidate candidate(SqlCompilerImpl compiler, String sql, SqlExecutionContext context) throws SqlException {
        try (RecordCursorFactory ignored = compiler.compile(sql, context).getRecordCursorFactory()) {
            final AggregatePlan aggregate = find(compiler.getPlanForTesting(), AggregatePlan.class);
            Assert.assertNotNull(sql, aggregate);
            final LogicalPlanPrinter printer = new LogicalPlanPrinter();
            final String before = printer.print(aggregate).toString();
            final IntList bindTypes = bindTypes(context.getBindVariableService());
            final HashJoinGroupByCandidate candidate = HashJoinGroupByCandidate.analyse(aggregate,
                    instantiator(compiler, new FunctionResolver(context.getCairoEngine().getConfiguration(), compiler.getFunctionFactoryCache())), context);
            Assert.assertEquals("candidate analysis mutated the plan", before, printer.print(aggregate).toString());
            Assert.assertEquals("candidate analysis changed bind types", bindTypes, bindTypes(context.getBindVariableService()));
            return candidate;
        }
    }

    static HashJoinGroupByMetadata metadata(CairoConfiguration configuration, BytecodeAssembler asm,
                                           HashJoinGroupByCandidate candidate, RecordMetadata probe, IntList probeColumns,
                                           RecordMetadata build, IntList buildColumns) throws SqlException {
        return new HashJoinGroupByMetadata(configuration, asm, candidate, probe, columnIds(candidate.getProbeInput(), probeColumns, 1_000_000),
                build, columnIds(candidate.getBuildInput(), buildColumns, 2_000_000));
    }

    static FunctionInstantiator instantiator(SqlCompilerImpl compiler, FunctionResolver resolver) {
        return new FunctionInstantiator(compiler, resolver, new PreparedFunctions(4), new OutputSchema(), 4);
    }

    static int sourceColumn(LogicalPlan input, int columnId) {
        final ScanPlan scan = find(input, ScanPlan.class);
        Assert.assertNotNull(scan);
        final int index = scan.getOutput().getColumnIndexById(columnId);
        Assert.assertTrue(index >= 0);
        return scan.getSourceColumnIndexes().getQuick(index);
    }

    static IntList sourceColumns(LogicalPlan input, IntList columnIds) {
        final IntList indexes = new IntList(columnIds.size());
        for (int i = 0; i < columnIds.size(); i++) {
            indexes.add(sourceColumn(input, columnIds.getQuick(i)));
        }
        return indexes;
    }

    static String tableName(LogicalPlan input) {
        final ScanPlan scan = find(input, ScanPlan.class);
        Assert.assertNotNull(scan);
        return scan.getTableToken().getTableName();
    }

    private static IntList bindTypes(BindVariableService service) {
        final IntList types = new IntList();
        for (int i = 0, n = service.getIndexedVariableCount(); i < n; i++) {
            final Function function = service.getFunction(i);
            types.add(function == null ? -1 : function.getType());
        }
        return types;
    }

    private static IntList columnIds(LogicalPlan input, IntList sourceColumns, int unusedBase) {
        final ScanPlan scan = find(input, ScanPlan.class);
        Assert.assertNotNull(scan);
        final IntList ids = new IntList(sourceColumns.size());
        for (int i = 0; i < sourceColumns.size(); i++) {
            final int sourceIndex = sourceColumns.getQuick(i);
            final int index = scan.getSourceColumnIndexes().indexOf(sourceIndex, 0, scan.getSourceColumnIndexes().size());
            ids.add(index < 0 ? unusedBase + sourceIndex : scan.getOutput().getColumnId(index));
        }
        return ids;
    }

    private static <T extends LogicalPlan> T find(LogicalPlan plan, Class<T> type) {
        if (type.isInstance(plan)) {
            return type.cast(plan);
        }
        for (int i = 0; i < plan.inputCount(); i++) {
            final T found = find(plan.inputAt(i), type);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}
