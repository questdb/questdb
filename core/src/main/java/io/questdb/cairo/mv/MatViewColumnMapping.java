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
import io.questdb.cairo.wal.WriterRowUtils;
import io.questdb.griffin.LoopingRecordToRowCopier;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.Long256FromCharSequenceDecoder;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Uuid;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
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
     * or -1 when the replay names no column.
     * <p>
     * The copy stops at the first column whose value does not convert, and so does the replay. The replay names
     * that column when the copier's own conversion failed there, with the error's message. A conversion that the
     * writer row performs itself, such as text to UUID, keeps its error as it is: the row the replay copies into
     * runs those conversions, with the writer row's code, only to stop where the writer row stopped, and the
     * replay then names no column. Without that, the replay would pass over such a column and could name a later
     * one whose value fails with the same message.
     */
    public static int findFailingColumn(
            SqlExecutionContext context,
            RecordToRowCopier copier,
            RecordMetadata queryMetadata,
            RecordMetadata viewMetadata,
            Record record,
            CharSequence errorMessage
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
        final ConversionCheckingRow row = new ConversionCheckingRow(viewMetadata);
        final int position = new LoopingRecordToRowCopier(fromTypes, viewMetadata, toColumnFilter)
                .findFailingColumn(context, fromRecord, row, errorMessage);
        return row.hasConversionFailed ? -1 : position;
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

    /**
     * Returns what the mapping from the query's columns to the view's leaves out, see {@link Summary}: nothing for a
     * positional copy, which fills every view column from the query column at its position.
     */
    public static Summary summarize(RecordMetadata queryMetadata, RecordMetadata viewMetadata, boolean isPositional) {
        return isPositional ? Summary.EMPTY : summarize(queryMetadata, viewMetadata, null);
    }

    /**
     * Returns what a name mapping from the query's columns to the view's leaves out, see {@link Summary}. The view
     * has the columns of {@code viewMetadata}, and also {@code addedViewColumnName} when not null, a column the view
     * is about to gain.
     */
    public static Summary summarize(
            RecordMetadata queryMetadata,
            RecordMetadata viewMetadata,
            @Nullable CharSequence addedViewColumnName
    ) {
        final ObjList<String> queryColumnNames = new ObjList<>(queryMetadata.getColumnCount());
        for (int i = 0, n = queryMetadata.getColumnCount(); i < n; i++) {
            queryColumnNames.add(Chars.toString(queryMetadata.getColumnName(i)));
        }
        return summarize(queryColumnNames, viewMetadata, addedViewColumnName, null);
    }

    /**
     * Returns what the name mapping that {@code summary} describes leaves out once the view loses the column
     * {@code droppedViewColumnName}: the query columns stay those of the mapping's compile, and the view has the
     * columns of {@code viewMetadata} but the dropped one. Returns null, an unknown mapping, when {@code summary} is
     * null or describes a positional copy, which knows no query column names.
     */
    public static @Nullable Summary summarizeWithoutViewColumn(
            @Nullable Summary summary,
            RecordMetadata viewMetadata,
            CharSequence droppedViewColumnName
    ) {
        if (summary == null || summary.queryColumnNames == null) {
            return null;
        }
        return summarize(summary.queryColumnNames, viewMetadata, null, droppedViewColumnName);
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

    // True when the name is not null and names the column, without regard to case.
    private static boolean isNamed(@Nullable CharSequence name, CharSequence columnName) {
        return name != null && Chars.equalsIgnoreCase(name, columnName);
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

    // Appends the name to a comma-separated list, which starts on the first name.
    private static StringSink putListed(@Nullable StringSink list, CharSequence name) {
        if (list == null) {
            list = new StringSink();
        } else {
            list.put(',');
        }
        list.put(name);
        return list;
    }

    // The view has the columns of viewMetadata, also addedViewColumnName when not null and not droppedViewColumnName.
    // Names compare without regard to case, as column names do.
    private static Summary summarize(
            ObjList<String> queryColumnNames,
            RecordMetadata viewMetadata,
            @Nullable CharSequence addedViewColumnName,
            @Nullable CharSequence droppedViewColumnName
    ) {
        final LowerCaseCharSequenceHashSet queryColumnNameSet = new LowerCaseCharSequenceHashSet();
        for (int i = 0, n = queryColumnNames.size(); i < n; i++) {
            queryColumnNameSet.add(queryColumnNames.getQuick(i));
        }
        StringSink unmappedViewColumns = null;
        for (int i = 0, n = viewMetadata.getColumnCount(); i < n; i++) {
            // a dropped column keeps its slot with a negative type
            if (viewMetadata.getColumnType(i) > -1) {
                final CharSequence columnName = viewMetadata.getColumnName(i);
                if (!queryColumnNameSet.contains(columnName) && !isNamed(droppedViewColumnName, columnName)) {
                    unmappedViewColumns = putListed(unmappedViewColumns, columnName);
                }
            }
        }
        if (addedViewColumnName != null && !queryColumnNameSet.contains(addedViewColumnName)) {
            unmappedViewColumns = putListed(unmappedViewColumns, addedViewColumnName);
        }
        StringSink ignoredQueryColumns = null;
        for (int i = 0, n = queryColumnNames.size(); i < n; i++) {
            final String columnName = queryColumnNames.getQuick(i);
            if ((viewMetadata.getColumnIndexQuiet(columnName) < 0 || isNamed(droppedViewColumnName, columnName))
                    && !isNamed(addedViewColumnName, columnName)) {
                ignoredQueryColumns = putListed(ignoredQueryColumns, columnName);
            }
        }
        return new Summary(
                unmappedViewColumns != null ? unmappedViewColumns.toString() : "",
                ignoredQueryColumns != null ? ignoredQueryColumns.toString() : "",
                queryColumnNames
        );
    }

    /**
     * What the mapping from a view's query columns to its columns leaves out, as {@code materialized_views()} reports
     * it: the view columns no query column names, which get NULL, and the query columns no view column names, which
     * refresh ignores. Each is a comma-separated list of names, in column order, and empty when there is none. A
     * positional copy leaves nothing out. A name mapping keeps its query's column names, so that a view column
     * change can update it without a compile, see {@link #summarizeWithoutViewColumn}.
     */
    public static final class Summary {
        public static final Summary EMPTY = new Summary("", "", null);
        private final String ignoredQueryColumns;
        // the query's column names of a name mapping, in query order; null for a positional copy
        private final @Nullable ObjList<String> queryColumnNames;
        private final String unmappedViewColumns;

        private Summary(String unmappedViewColumns, String ignoredQueryColumns, @Nullable ObjList<String> queryColumnNames) {
            this.unmappedViewColumns = unmappedViewColumns;
            this.ignoredQueryColumns = ignoredQueryColumns;
            this.queryColumnNames = queryColumnNames;
        }

        public String getIgnoredQueryColumns() {
            return ignoredQueryColumns;
        }

        public String getUnmappedViewColumns() {
            return unmappedViewColumns;
        }
    }

    // A row that discards the values, but first runs the value conversions that the writer row performs itself for
    // the looping copier, from text to DECIMAL, GEOHASH, UUID and LONG256, with the writer row's code, see
    // TableWriter.RowImpl and WalWriter.RowImpl. A value that does not convert fails there as it fails in the writer
    // row, and the row records that the failure is its own. Column indexes are writer indexes, as in the writer row.
    private static final class ConversionCheckingRow extends TableWriter.NoOpRow {
        private static final Long256Acceptor NOOP_LONG256_ACCEPTOR = (l0, l1, l2, l3) -> {
        };
        private final Decimal256 decimal256 = new Decimal256();
        private final RecordMetadata metadata;
        private final Uuid uuid = new Uuid();
        private boolean hasConversionFailed;

        private ConversionCheckingRow(RecordMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public void putDecimalChar(int columnIndex, char decimalValue) {
            hasConversionFailed = true;
            WriterRowUtils.putDecimalChar(columnIndex, decimal256, decimalValue, metadata.getColumnType(columnIndex), this);
            hasConversionFailed = false;
        }

        @Override
        public void putDecimalStr(int columnIndex, CharSequence decimalValue) {
            hasConversionFailed = true;
            WriterRowUtils.putDecimalStr(columnIndex, decimal256, decimalValue, metadata.getColumnType(columnIndex), this);
            hasConversionFailed = false;
        }

        @Override
        public void putDecimalVarchar(int columnIndex, Utf8Sequence decimalValue) {
            hasConversionFailed = true;
            WriterRowUtils.putDecimalVarchar(columnIndex, decimal256, decimalValue, metadata.getColumnType(columnIndex), this);
            hasConversionFailed = false;
        }

        @Override
        public void putGeoStr(int columnIndex, CharSequence value) {
            hasConversionFailed = true;
            WriterRowUtils.putGeoStr(columnIndex, value, metadata.getColumnType(columnIndex), this);
            hasConversionFailed = false;
        }

        @Override
        public void putGeoVarchar(int columnIndex, Utf8Sequence value) {
            hasConversionFailed = true;
            WriterRowUtils.putGeoVarchar(columnIndex, value, metadata.getColumnType(columnIndex), this);
            hasConversionFailed = false;
        }

        // The writer row's column memory stores an empty or NULL text as NULL, and decodes any other text from its
        // third character on, past the "0x" prefix, with Long256FromCharSequenceDecoder.
        @Override
        public void putLong256Utf8(int columnIndex, Utf8Sequence hexString) {
            final int size;
            if (hexString != null && (size = hexString.size()) > 0) {
                hasConversionFailed = true;
                Long256FromCharSequenceDecoder.decode(hexString.asAsciiCharSequence(), 2, size, NOOP_LONG256_ACCEPTOR);
                hasConversionFailed = false;
            }
        }

        @Override
        public void putUuid(int columnIndex, CharSequence uuidStr) {
            hasConversionFailed = true;
            SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
            hasConversionFailed = false;
        }

        @Override
        public void putUuidUtf8(int columnIndex, Utf8Sequence uuidStr) {
            hasConversionFailed = true;
            SqlUtil.implicitCastStrAsUuid(uuidStr, uuid);
            hasConversionFailed = false;
        }
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
