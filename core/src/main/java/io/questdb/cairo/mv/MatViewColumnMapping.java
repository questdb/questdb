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

package io.questdb.cairo.mv;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.LoopingRecordToRowCopier;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Builds the record-to-row copier that a materialized view refresh writes the view's rows with, from the
 * columns of the compiled view query to the view's columns.
 * <p>
 * A view whose SQL names its columns, see {@link io.questdb.griffin.SqlExecutionContext#hasViewSqlGeneratedColumnNames()},
 * maps by name: each query column goes to the view column of the same name, compared case-insensitively. A
 * view column that no query column names gets NULL, and a query column the view lacks is ignored. When every
 * query column goes to the view column at its own position and the view has no other column, the mapping is
 * the identity, and the copier is the one positional copy generates, with no projection, as for every view
 * whose columns match its SQL's output one to one. Otherwise a {@link SelectedRecord} projects the matched
 * query columns, in query order, and a {@link ListColumnFilter} names the view column each of them goes to.
 * <p>
 * A view whose SQL relies on generated column names copies by position: generated names change with the
 * server version and the {@code cairo.sql.column.alias.*} settings, so a view created under other rules has
 * column names its SQL no longer produces.
 * <p>
 * Every mapped pair must have a copier conversion, {@link RecordToRowCopierUtils#isCopierConvertible(int, int)};
 * a pair without one fails with the {@code inconvertible types} error, which names both columns and both types.
 */
public final class MatViewColumnMapping {

    private MatViewColumnMapping() {
    }

    /**
     * Replays a record that made the copier throw a value conversion error through a looping copier, into a row
     * that discards the values, to find the column whose value does not convert. The generated copiers do not
     * know which column they convert; the looping copier does. Returns the column's position in the copier, for
     * {@link #getQueryColumnIndex(RecordToRowCopier, int)} and {@link #getViewColumnIndex(RecordToRowCopier, int)},
     * or -1 when the replay converts every column.
     */
    public static int findFailingColumn(
            SqlExecutionContext context,
            RecordToRowCopier copier,
            RecordMetadata queryMetadata,
            RecordMetadata viewMetadata,
            Record record
    ) {
        final ColumnTypes fromTypes;
        final ColumnFilter toColumnFilter;
        final Record fromRecord;
        if (copier instanceof MappedRecordToRowCopier mapped) {
            fromTypes = mapped.fromTypes;
            toColumnFilter = mapped.viewColumnFilter;
            mapped.projection.of(record);
            fromRecord = mapped.projection;
        } else {
            fromTypes = queryMetadata;
            final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
            entityColumnFilter.of(queryMetadata.getColumnCount());
            toColumnFilter = entityColumnFilter;
            fromRecord = record;
        }
        return new LoopingRecordToRowCopier(fromTypes, viewMetadata, toColumnFilter)
                .findFailingColumn(context, fromRecord, TableWriter.NOOP_ROW);
    }

    /**
     * Returns the query column that the copier reads at the given position.
     */
    public static int getQueryColumnIndex(RecordToRowCopier copier, int position) {
        if (copier instanceof MappedRecordToRowCopier mapped) {
            return mapped.queryColumnIndexes.getQuick(position);
        }
        return position;
    }

    /**
     * Returns the projection that the copier reads the query's record through, or null when the copier reads the
     * record as it is, for an identity or positional mapping.
     */
    @TestOnly
    public static @Nullable SelectedRecord getRecordProjection(@Nullable RecordToRowCopier copier) {
        return copier instanceof MappedRecordToRowCopier mapped ? mapped.projection : null;
    }

    /**
     * Returns the view column that the copier writes at the given position.
     */
    public static int getViewColumnIndex(RecordToRowCopier copier, int position) {
        if (copier instanceof MappedRecordToRowCopier mapped) {
            return mapped.viewColumnFilter.getColumnIndexFactored(position);
        }
        return position;
    }

    /**
     * Returns the copier that writes the query's records into the view's rows. With {@code isPositional} set, query
     * column i goes to view column i, and the caller has already checked that the query has as many columns as the
     * view. Otherwise the columns map by name.
     *
     * @throws CairoException when a mapped pair has no copier conversion, or the query does not produce the view's
     *                        designated timestamp under its name
     */
    public static RecordToRowCopier newRecordToRowCopier(
            BytecodeAssembler asm,
            RecordMetadata queryMetadata,
            RecordMetadata viewMetadata,
            boolean isPositional,
            CairoConfiguration configuration
    ) {
        final int queryColumnCount = queryMetadata.getColumnCount();
        if (isPositional) {
            for (int i = 0; i < queryColumnCount; i++) {
                checkConvertible(queryMetadata, i, viewMetadata, i);
            }
            return newIdentityCopier(asm, queryMetadata, viewMetadata, configuration);
        }

        final IntList queryColumnIndexes = new IntList(queryColumnCount);
        final ListColumnFilter viewColumnFilter = new ListColumnFilter(queryColumnCount);
        boolean isIdentity = queryColumnCount == viewMetadata.getColumnCount();
        for (int i = 0; i < queryColumnCount; i++) {
            final int viewColumnIndex = viewMetadata.getColumnIndexQuiet(queryMetadata.getColumnName(i));
            if (viewColumnIndex < 0) {
                // the view has no column of this name, the copier skips the query column
                isIdentity = false;
                continue;
            }
            checkConvertible(queryMetadata, i, viewMetadata, viewColumnIndex);
            isIdentity &= viewColumnIndex == i;
            queryColumnIndexes.add(i);
            // ListColumnFilter holds 1-based column indexes
            viewColumnFilter.add(viewColumnIndex + 1);
        }

        // A materialized view always has a designated timestamp.
        final int viewTimestampIndex = viewMetadata.getTimestampIndex();
        assert viewTimestampIndex > -1;
        if (!viewColumnFilter.contains(viewTimestampIndex + 1)) {
            // A row cannot be written without its designated timestamp.
            throw CairoException.nonCritical()
                    .put("materialized view query does not produce the designated timestamp column [column=")
                    .put(viewMetadata.getColumnName(viewTimestampIndex))
                    .put(']');
        }

        if (isIdentity) {
            return newIdentityCopier(asm, queryMetadata, viewMetadata, configuration);
        }

        final ArrayColumnTypes fromTypes = new ArrayColumnTypes();
        for (int i = 0, n = queryColumnIndexes.size(); i < n; i++) {
            fromTypes.add(queryMetadata.getColumnType(queryColumnIndexes.getQuick(i)));
        }
        return new MappedRecordToRowCopier(
                RecordToRowCopierUtils.generateCopier(asm, fromTypes, viewMetadata, viewColumnFilter, configuration),
                fromTypes,
                queryColumnIndexes,
                viewColumnFilter
        );
    }

    private static void checkConvertible(RecordMetadata queryMetadata, int queryColumnIndex, RecordMetadata viewMetadata, int viewColumnIndex) {
        final int fromType = queryMetadata.getColumnType(queryColumnIndex);
        final int toType = viewMetadata.getColumnType(viewColumnIndex);
        if (!RecordToRowCopierUtils.isCopierConvertible(fromType, toType)) {
            // The error describes a view/query schema mismatch, not a SQL token, so it carries no position.
            throw CairoException.nonCritical().put("inconvertible types: ").put(ColumnType.nameOf(fromType))
                    .put(" -> ").put(ColumnType.nameOf(toType))
                    .put(" [from=").put(queryMetadata.getColumnName(queryColumnIndex))
                    .put(", to=").put(viewMetadata.getColumnName(viewColumnIndex)).put(']');
        }
    }

    // Query column i goes to view column i: the copier positional copy has always used.
    private static RecordToRowCopier newIdentityCopier(
            BytecodeAssembler asm,
            RecordMetadata queryMetadata,
            RecordMetadata viewMetadata,
            CairoConfiguration configuration
    ) {
        final EntityColumnFilter entityColumnFilter = new EntityColumnFilter();
        entityColumnFilter.of(queryMetadata.getColumnCount());
        return RecordToRowCopierUtils.generateCopier(asm, queryMetadata, viewMetadata, entityColumnFilter, configuration);
    }

    // Copies the query columns a name mapping matched, projected in query order, into the view columns the filter names.
    private static final class MappedRecordToRowCopier implements RecordToRowCopier {
        private final RecordToRowCopier copier;
        private final ArrayColumnTypes fromTypes;
        private final SelectedRecord projection;
        private final IntList queryColumnIndexes;
        private final ListColumnFilter viewColumnFilter;

        private MappedRecordToRowCopier(
                RecordToRowCopier copier,
                ArrayColumnTypes fromTypes,
                IntList queryColumnIndexes,
                ListColumnFilter viewColumnFilter
        ) {
            this.copier = copier;
            this.fromTypes = fromTypes;
            this.queryColumnIndexes = queryColumnIndexes;
            this.viewColumnFilter = viewColumnFilter;
            this.projection = new SelectedRecord(queryColumnIndexes);
        }

        @Override
        public void copy(SqlExecutionContext context, Record record, TableWriter.Row row) {
            projection.of(record);
            copier.copy(context, projection, row);
        }
    }
}
