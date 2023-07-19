/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
package io.questdb.cairo;

import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.QueriedTables;
import io.questdb.mp.SCSequence;
import io.questdb.std.Transient;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

/**
 * Factory responsible for re-authorizing user permissions on getCursor() call.
 */
public class SecurityCheckFactory implements RecordCursorFactory {

    final RecordCursorFactory base;
    final QueriedTables queriedTables;

    public SecurityCheckFactory(RecordCursorFactory factory, @NotNull QueriedTables queriedTables) {
        this.base = factory;
        this.queriedTables = queriedTables;
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public SingleSymbolFilter convertToSampleByIndexDataFrameCursorFactory() {
        return base.convertToSampleByIndexDataFrameCursorFactory();
    }

    @Override
    public PageFrameSequence<?> execute(@Transient SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return base.execute(executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedLimitAdvice() {
        return base.followedLimitAdvice();
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return base.fragmentedSymbolTables();
    }

    public RecordCursorFactory getBase() {
        return base;
    }

    @Override
    public String getBaseColumnName(int idx) {
        return base.getBaseColumnName(idx);
    }

    @Override
    public String getBaseColumnNameNoRemap(int idx) {
        return base.getBaseColumnNameNoRemap(idx);
    }

    public RecordCursorFactory getBaseFactory() {
        return base.getBaseFactory();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        queriedTables.revalidate(executionContext.getSecurityContext());
        return base.getCursor(executionContext);
    }

    @Override
    public RecordMetadata getMetadata() {
        return base.getMetadata();
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        return base.getPageFrameCursor(executionContext, order);
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return this.base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void revertFromSampleByIndexDataFrameCursorFactory() {
        base.revertFromSampleByIndexDataFrameCursorFactory();
    }

    @Override
    public boolean supportPageFrameCursor() {
        return base.supportPageFrameCursor();
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
    public void toSink(CharSink sink) {
        base.toSink(sink);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }
}
