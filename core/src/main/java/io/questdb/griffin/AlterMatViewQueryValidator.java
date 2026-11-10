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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewColumnMapping;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.ops.CreateMatViewOperationImpl;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.CommonUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Checks the new SQL of {@code ALTER MATERIALIZED VIEW ... AS} against the view, under the rules of the statement
 * that need the compiled query, past those the parse checks:
 * <ul>
 *     <li>the SAMPLE BY interval and unit of the view's, or, for a non-aggregating passthrough view, a passthrough SQL
 *     without one; the parse checks the time zone and offset;</li>
 *     <li>the view's designated timestamp produced under its name and type, as the query's designated timestamp;</li>
 *     <li>an implicit cast from every query column to the view column of its name, see
 *     {@link io.questdb.griffin.RecordToRowCopierUtils#isCopierConvertible(int, int)};</li>
 *     <li>no view column replaced by a differently named query column at its position.</li>
 * </ul>
 * The view's columns come from its sequencer metadata, which holds the column changes still in its WAL and lists the
 * view's columns in their order, with no dropped column. Errors point at the query column or the SAMPLE BY interval in
 * the statement. Once the checks pass, {@link #getColumnMappingSummary()} tells what the mapping leaves out.
 */
final class AlterMatViewQueryValidator implements MatViewQueryValidator {
    private BytecodeAssembler asm;
    private MatViewColumnMapping.Summary columnMappingSummary;
    private CairoConfiguration configuration;
    private SqlParser.AlterMatViewQuery query;
    private MatViewDefinition viewDefinition;
    private RecordMetadata viewMetadata;
    private TableToken viewToken;

    AlterMatViewQueryValidator of(
            TableToken viewToken,
            MatViewDefinition viewDefinition,
            RecordMetadata viewMetadata,
            SqlParser.AlterMatViewQuery query,
            BytecodeAssembler asm,
            CairoConfiguration configuration
    ) {
        this.viewToken = viewToken;
        this.viewDefinition = viewDefinition;
        this.viewMetadata = viewMetadata;
        this.query = query;
        this.asm = asm;
        this.configuration = configuration;
        this.columnMappingSummary = null;
        return this;
    }

    /**
     * Returns what the mapping of the checked query leaves out, see {@link MatViewColumnMapping#summarize}.
     */
    @Nullable
    MatViewColumnMapping.Summary getColumnMappingSummary() {
        return columnMappingSummary;
    }

    @Override
    public void validateMetadata(RecordMetadata queryMetadata, boolean hasGeneratedColumnNames) throws SqlException {
        final int viewTimestampIndex = viewMetadata.getTimestampIndex();
        final CharSequence viewTimestampName = viewMetadata.getColumnName(viewTimestampIndex);
        final int viewTimestampType = viewMetadata.getColumnType(viewTimestampIndex);
        // Refresh writes the rows in the order of the query's designated timestamp, which must be the view's, under its
        // name and type.
        final int queryTimestampIndex = queryMetadata.getTimestampIndex();
        if (queryTimestampIndex < 0
                || !Chars.equalsIgnoreCase(queryMetadata.getColumnName(queryTimestampIndex), viewTimestampName)
                || queryMetadata.getColumnType(queryTimestampIndex) != viewTimestampType) {
            throw SqlException.$(query.getColumnPosition(viewTimestampName), "materialized view query must produce the view's designated timestamp as its designated timestamp [column=")
                    .put(viewTimestampName)
                    .put(", type=").put(ColumnType.nameOf(viewTimestampType))
                    .put(']');
        }

        if (hasGeneratedColumnNames) {
            // A SQL that references a column name longer than 64 characters without an alias copies by position, as
            // refresh does: the query's columns must fit the view's one to one.
            try {
                MatViewRefreshJob.validateViewSchema(viewToken, queryMetadata, viewMetadata, false);
                MatViewColumnMapping.newRecordToRowCopier(asm, queryMetadata, viewMetadata, true, configuration);
            } catch (CairoException e) {
                throw SqlException.$(query.sqlPosition, e.getFlyweightMessage());
            }
            columnMappingSummary = MatViewColumnMapping.summarize(queryMetadata, viewMetadata, true);
            return;
        }

        for (int i = 0, n = queryMetadata.getColumnCount(); i < n; i++) {
            final CharSequence queryColumnName = queryMetadata.getColumnName(i);
            final int viewColumnIndex = viewMetadata.getColumnIndexQuiet(queryColumnName);
            if (viewColumnIndex < 0) {
                continue;
            }
            final int fromType = queryMetadata.getColumnType(i);
            final int toType = viewMetadata.getColumnType(viewColumnIndex);
            if (!RecordToRowCopierUtils.isCopierConvertible(fromType, toType)) {
                // the error refresh gives the same pair, see MatViewColumnMapping
                throw SqlException.$(query.getColumnPosition(queryColumnName), "inconvertible types: ")
                        .put(ColumnType.nameOf(fromType)).put(" -> ").put(ColumnType.nameOf(toType))
                        .put(" [from=").put(queryColumnName)
                        .put(", to=").put(viewMetadata.getColumnName(viewColumnIndex)).put(']');
            }
        }

        // A view column that the query stops producing, with a query column at its position that the view lacks, is
        // an accidental rename, or a drifted generated name of an older view: either keeps the view column NULL from
        // now on without a word.
        for (int i = 0, n = Math.min(viewMetadata.getColumnCount(), queryMetadata.getColumnCount()); i < n; i++) {
            final CharSequence viewColumnName = viewMetadata.getColumnName(i);
            final CharSequence queryColumnName = queryMetadata.getColumnName(i);
            if (queryMetadata.getColumnIndexQuiet(viewColumnName) < 0 && viewMetadata.getColumnIndexQuiet(queryColumnName) < 0) {
                throw SqlException.$(query.getColumnPosition(queryColumnName), "materialized view column does not match the query column at the same position [view column=")
                        .put(viewColumnName)
                        .put(", query column=").put(queryColumnName)
                        .put("]; to keep the view column, alias the query column to its name; to replace it, run ALTER MATERIALIZED VIEW ")
                        .put(viewToken.getTableName())
                        .put(" ADD COLUMN for the new column first");
            }
        }
        columnMappingSummary = MatViewColumnMapping.summarize(queryMetadata, viewMetadata, false);
    }

    @Override
    public void validateModel(IQueryModel queryModel, FunctionFactoryCache functionFactoryCache) throws SqlException {
        final ExpressionNode intervalNode = CreateMatViewOperationImpl.findSamplingIntervalNode(queryModel);
        if (viewDefinition.isPassthrough()) {
            if (intervalNode != null) {
                throw SqlException.$(query.sqlPosition + intervalNode.position, "materialized view without a sampling interval takes no SAMPLE BY or GROUP BY timestamp_floor() [view=")
                        .put(viewToken.getTableName()).put(']');
            }
            if (!CreateMatViewOperationImpl.isPassthroughQuery(functionFactoryCache, queryModel)) {
                throw SqlException.$(query.sqlPosition, "non-aggregating materialized view takes only a non-aggregating query over its base table [view=")
                        .put(viewToken.getTableName()).put(']');
            }
            return;
        }
        if (intervalNode == null) {
            throw SqlException.$(query.sqlPosition, "materialized view query must keep the view's sampling interval, use SAMPLE BY or GROUP BY timestamp_floor() [interval=")
                    .put(viewDefinition.getSamplingInterval()).put(viewDefinition.getSamplingIntervalUnit()).put(']');
        }
        final int intervalPosition = query.sqlPosition + intervalNode.position;
        final CharSequence interval = GenericLexer.unquote(intervalNode.token);
        final int intervalEnd = CommonUtils.findPositiveIntervalEndIndex(interval, intervalPosition, "sample");
        final long samplingInterval = CommonUtils.parsePositiveInterval(interval, intervalEnd, intervalPosition, "sample", Numbers.INT_NULL, ' ');
        final char samplingIntervalUnit = interval.charAt(intervalEnd);
        if (samplingInterval != viewDefinition.getSamplingInterval() || samplingIntervalUnit != viewDefinition.getSamplingIntervalUnit()) {
            throw SqlException.$(intervalPosition, "materialized view query must keep the view's sampling interval [interval=")
                    .put(viewDefinition.getSamplingInterval()).put(viewDefinition.getSamplingIntervalUnit())
                    .put(", queryInterval=").put(samplingInterval).put(samplingIntervalUnit)
                    .put(']');
        }
    }
}
