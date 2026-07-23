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

package io.questdb.griffin.engine;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class RoleGenerationRecordCursorFactory implements RecordCursorFactory {
    private final RecordCursorFactory base;
    private final CairoEngine engine;
    private final long roleGeneration;

    public RoleGenerationRecordCursorFactory(RecordCursorFactory base, CairoEngine engine, long roleGeneration) {
        this.base = base;
        this.engine = engine;
        this.roleGeneration = roleGeneration;
    }

    @Override
    public boolean canPeelForTopK() {
        return base.canPeelForTopK();
    }

    @Override
    public void changePageFrameSizes(int minRows, int maxRows) {
        base.changePageFrameSizes(minRows, maxRows);
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public SingleSymbolFilter convertToSampleByIndexPageFrameCursorFactory() {
        return base.convertToSampleByIndexPageFrameCursorFactory();
    }

    @Override
    public PageFrameSequence<?> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        validateRoleGeneration();
        return base.execute(executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return base.fragmentedSymbolTables();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return base.getBaseColumnName(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base.getBaseFactory();
    }

    @Override
    public @Nullable ObjList<Function> getBindVarFunctions() {
        return base.getBindVarFunctions();
    }

    @Override
    public @Nullable MemoryCARW getBindVarMemory() {
        return base.getBindVarMemory();
    }

    @Override
    public IntList getColumnCrossIndex() {
        return base.getColumnCrossIndex();
    }

    @Override
    public @Nullable CompiledFilter getCompiledFilter() {
        return base.getCompiledFilter();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        validateRoleGeneration();
        return base.getCursor(executionContext);
    }

    public RecordCursorFactory getDelegate() {
        return base;
    }

    @Override
    public @Nullable Function getFilter() {
        return base.getFilter();
    }

    @Override
    public RecordMetadata getMetadata() {
        return base.getMetadata();
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        validateRoleGeneration();
        return base.getPageFrameCursor(executionContext, order);
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public RecordCursor getSharedCursor(SqlExecutionContext executionContext, int sharedId) throws SqlException {
        validateRoleGeneration();
        return base.getSharedCursor(executionContext, sharedId);
    }

    @Override
    public ExpressionNode getStealFilterExpr() {
        return base.getStealFilterExpr();
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        validateRoleGeneration();
        return base.getTimeFrameCursor(executionContext);
    }

    @Override
    public void halfClose() {
        base.halfClose();
    }

    @Override
    public boolean hasParquetConvertedColumns(SqlExecutionContext executionContext) {
        return base.hasParquetConvertedColumns(executionContext);
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean isProjection() {
        return base.isProjection();
    }

    @Override
    public boolean mayHaveParquetPartitions(SqlExecutionContext executionContext) {
        return base.mayHaveParquetPartitions(executionContext);
    }

    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        validateRoleGeneration();
        return base.newTimeFrameCursor();
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        return base.recordCursorSupportsLongTopK(columnIndex);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void revertFromSampleByIndexPageFrameCursorFactory() {
        base.revertFromSampleByIndexPageFrameCursorFactory();
    }

    @Override
    public RecordCursorFactory rewrapOverTopK(RecordCursorFactory topK, RecordMetadata orderedMetadata) {
        return base.rewrapOverTopK(topK, orderedMetadata);
    }

    @Override
    public void setPushdownFilterCondition(ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions) {
        base.setPushdownFilterCondition(pushdownFilterConditions);
    }

    @Override
    public boolean supportsFilterStealing() {
        return base.supportsFilterStealing();
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
    }

    @Override
    public boolean supportsSharedCursors() {
        return base.supportsSharedCursors();
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        return base.supportsTimeFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableName) {
        return base.supportsUpdateRowId(tableName);
    }

    @Override
    public void toPlan(PlanSink sink) {
        base.toPlan(sink);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        base.toSink(sink);
    }

    @Override
    public int translateOrderByColumnToBase(int projectedIndex) {
        return base.translateOrderByColumnToBase(projectedIndex);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    public void validateRoleGeneration() {
        if (engine.getRoleGeneration() != roleGeneration) {
            throw TableReferenceOutOfDateException.of("node role");
        }
    }
}
