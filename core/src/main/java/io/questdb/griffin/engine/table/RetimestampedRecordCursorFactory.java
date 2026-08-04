/*******************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Relabels the designated timestamp of its base factory without changing records.
 */
public class RetimestampedRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RetimestampedFactoryConstructor FACTORY_CONSTRUCTOR = RetimestampedRecordCursorFactory::new;
    private final RecordCursorFactory base;

    /**
     * @param base           the wrapped factory; ownership transfers to this wrapper
     * @param timestampIndex the redesignated timestamp column index
     */
    private RetimestampedRecordCursorFactory(RecordCursorFactory base, int timestampIndex) {
        super(new RetimestampedMetadata(base.getMetadata(), timestampIndex));
        this.base = base;
    }

    public static RetimestampedRecordCursorFactory create(RecordCursorFactory base, int timestampIndex) {
        return create(base, timestampIndex, FACTORY_CONSTRUCTOR);
    }

    @TestOnly
    public static RetimestampedRecordCursorFactory create(
            RecordCursorFactory base,
            int timestampIndex,
            RetimestampedFactoryConstructor constructor
    ) {
        try {
            final RetimestampedRecordCursorFactory factory = constructor.create(base, timestampIndex);
            base = null;
            return factory;
        } finally {
            Misc.free(base);
        }
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
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Nullable
    @Override
    public ObjList<Function> getBindVarFunctions() {
        return base.getBindVarFunctions();
    }

    @Nullable
    @Override
    public MemoryCARW getBindVarMemory() {
        return base.getBindVarMemory();
    }

    @Nullable
    @Override
    public CompiledFilter getCompiledFilter() {
        return base.getCompiledFilter();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return base.getCursor(executionContext);
    }

    @Nullable
    @Override
    public Function getFilter() {
        return base.getFilter();
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        return base.getPageFrameCursor(executionContext, order);
    }

    @Override
    public int getScanDirection() {
        if (base.getMetadata().getTimestampIndex() == -1) {
            return SCAN_DIRECTION_OTHER;
        }
        return base.getScanDirection();
    }

    @Override
    public RecordCursor getSharedCursor(SqlExecutionContext executionContext, int sharedId) throws SqlException {
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
    public void halfClose() {
        base.halfClose();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean isProjection() {
        return false;
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
    public boolean supportsFilterStealing() {
        // Stealing would replace this factory with its physical base and lose the redesignated
        // timestamp metadata, including the prohibition on physical-domain time frames.
        return false;
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
        // The base's frame bounds and seeking use its physical designated timestamp. They cannot
        // safely describe an arbitrary redesignated timestamp column, so temporal consumers must
        // use their record-cursor fallback.
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Retimestamp");
        final RecordMetadata metadata = getMetadata();
        final int timestampIndex = metadata.getTimestampIndex();
        if (timestampIndex != -1) {
            sink.attr("designatedTimestamp").val(metadata.getColumnName(timestampIndex));
        }
        sink.child(base);
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

    @Override
    protected void _close() {
        Misc.free(base);
    }

    @FunctionalInterface
    public interface RetimestampedFactoryConstructor {
        RetimestampedRecordCursorFactory create(RecordCursorFactory base, int timestampIndex);
    }

    private static final class RetimestampedMetadata implements RecordMetadata {
        private final RecordMetadata base;
        private final int timestampIndex;

        private RetimestampedMetadata(RecordMetadata base, int timestampIndex) {
            this.base = base;
            this.timestampIndex = timestampIndex;
        }

        @Override
        public byte getColumnIndexType(int columnIndex) {
            return base.getColumnIndexType(columnIndex);
        }

        @Override
        public int getColumnCount() {
            return base.getColumnCount();
        }

        @Override
        public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
            return base.getColumnIndexQuiet(columnName, lo, hi);
        }

        @Override
        public TableColumnMetadata getColumnMetadata(int columnIndex) {
            return base.getColumnMetadata(columnIndex);
        }

        @Override
        public String getColumnName(int columnIndex) {
            return base.getColumnName(columnIndex);
        }

        @Override
        public int getColumnType(int columnIndex) {
            return base.getColumnType(columnIndex);
        }

        @Override
        public int getIndexValueBlockCapacity(int columnIndex) {
            return base.getIndexValueBlockCapacity(columnIndex);
        }

        @Override
        public RecordMetadata getMetadata(int columnIndex) {
            return base.getMetadata(columnIndex);
        }

        @Override
        public int getTimestampIndex() {
            return timestampIndex;
        }

        @Override
        public int getWriterIndex(int columnIndex) {
            return base.getWriterIndex(columnIndex);
        }

        @Override
        public boolean hasColumn(int columnIndex) {
            return base.hasColumn(columnIndex);
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            return base.isDedupKey(columnIndex);
        }

        @Override
        public boolean isSymbolTableStatic(int columnIndex) {
            return base.isSymbolTableStatic(columnIndex);
        }

        @Override
        public boolean isWalEnabled() {
            return base.isWalEnabled();
        }

        @Override
        public boolean splitsOnDot() {
            return base.splitsOnDot();
        }

        @Override
        public void toPlan(PlanSink sink) {
            base.toPlan(sink);
        }
    }
}
