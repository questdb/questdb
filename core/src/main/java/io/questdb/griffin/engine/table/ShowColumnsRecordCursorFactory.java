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
import io.questdb.cairo.CairoColumn;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoTable;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ShowColumnsRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_NAME_COL = 0;
    public static final int N_TYPE_COL = N_NAME_COL + 1;
    private static final int N_INDEXED_COL = N_TYPE_COL + 1;
    private static final int N_INDEX_BLOCK_CAPACITY_COL = N_INDEXED_COL + 1;
    private static final int N_SYMBOL_CACHED_COL = N_INDEX_BLOCK_CAPACITY_COL + 1;
    private static final int N_SYMBOL_CAPACITY_COL = N_SYMBOL_CACHED_COL + 1;
    private static final int N_SYMBOL_TABLE_SIZE_COL = N_SYMBOL_CAPACITY_COL + 1;
    private static final int N_DESIGNATED_COL = N_SYMBOL_TABLE_SIZE_COL + 1;
    private static final int N_UPSERT_KEY_COL = N_DESIGNATED_COL + 1;
    private static final RecordMetadata METADATA;
    private final ShowColumnsCursor cursor = new ShowColumnsCursor();
    private final TableToken tableToken;
    private final int tokenPosition;

    public ShowColumnsRecordCursorFactory(TableToken tableToken, int tokenPosition) {
        super(METADATA);
        this.tableToken = tableToken;
        this.tokenPosition = tokenPosition;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor.of(executionContext, tableToken, tokenPosition);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_columns");
        sink.meta("of").val(tableToken.getTableName());
    }

    private static class ShowColumnsCursor implements NoRandomAccessRecordCursor {
        private final ShowColumnsRecord record = new ShowColumnsRecord();
        private final IntList staticSymbolTableSizes = new IntList();
        private CairoColumn cairoColumn = new CairoColumn();
        private CairoTable cairoTable;
        private int columnIndex;

        @Override
        public void close() {
            cairoTable = null;
            cairoColumn = null;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (columnIndex < cairoTable.getColumnCount() - 1) {
                cairoColumn = cairoTable.getColumnQuiet(++columnIndex);
                return true;
            }
            return false;
        }

        public ShowColumnsCursor of(CairoTable cairoTable, @Transient @Nullable TableReader tableReader) {
            this.cairoTable = cairoTable;
            staticSymbolTableSizes.restoreInitialCapacity();
            staticSymbolTableSizes.setAll(cairoTable.getColumnCount(), 0);
            if (tableReader != null) {
                for (int i = 0, n = cairoTable.getColumnCount(); i < n; i++) {
                    final CairoColumn column = this.cairoTable.getColumnQuiet(i);
                    if (column.isSymbolTableStatic()) {
                        // there is a small chance that cairoTable and tableReader are out of sync with each other due to
                        // a concurrent schema change, so we must double-check presence of the column in table metadata
                        final int readerColumnIndex = tableReader.getMetadata().getColumnIndexQuiet(column.getName());
                        if (readerColumnIndex != -1) {
                            final StaticSymbolTable staticSymbolTable = tableReader.getSymbolTable(readerColumnIndex);
                            if (staticSymbolTable != null) {
                                staticSymbolTableSizes.setQuick(i, staticSymbolTable.getSymbolCount() + (staticSymbolTable.containsNullValue() ? 1 : 0));
                            }
                        }
                    }
                }
            }
            toTop();
            return this;
        }

        public ShowColumnsCursor of(SqlExecutionContext executionContext, TableToken tableToken, int tokenPosition) {
            final CairoEngine engine = executionContext.getCairoEngine();
            try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                final CairoTable cairoTable = metadataRO.getTable(tableToken);
                if (cairoTable != null) {
                    if (tableToken.isView()) {
                        return of(cairoTable, null);
                    }
                    try (TableReader tableReader = engine.getReader(tableToken)) {
                        return of(cairoTable, tableReader);
                    }
                }
            }
            throw CairoException.tableDoesNotExist(tableToken.getTableName()).position(tokenPosition);
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            columnIndex = -1;
            cairoColumn = null;
        }

        public class ShowColumnsRecord implements Record {

            @Override
            public boolean getBool(int col) {
                if (col == N_INDEXED_COL) {
                    return cairoColumn.isIndexed();
                }
                if (col == N_SYMBOL_CACHED_COL) {
                    return cairoColumn.isSymbolCached();
                }
                if (col == N_DESIGNATED_COL) {
                    return cairoColumn.isDesignated();
                }
                if (col == N_UPSERT_KEY_COL) {
                    return cairoColumn.isDedupKey();
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public int getInt(int col) {
                if (col == N_INDEX_BLOCK_CAPACITY_COL) {
                    return cairoColumn.getIndexBlockCapacity();
                }
                if (col == N_SYMBOL_CAPACITY_COL) {
                    if (ColumnType.isSymbol(cairoColumn.getType())) {
                        return cairoColumn.getSymbolCapacity();
                    } else {
                        return 0;
                    }
                }
                if (col == N_SYMBOL_TABLE_SIZE_COL) {
                    return staticSymbolTableSizes.getQuick(columnIndex);
                }
                throw new UnsupportedOperationException();
            }

            @Override
            @NotNull
            public CharSequence getStrA(int col) {
                if (col == N_NAME_COL) {
                    return cairoColumn.getName();
                }
                if (col == N_TYPE_COL) {
                    return ColumnType.nameOf(cairoColumn.getType());
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return getStrA(col).length();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("column", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("type", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("indexed", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indexBlockCapacity", ColumnType.INT));
        metadata.add(new TableColumnMetadata("symbolCached", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("symbolCapacity", ColumnType.INT));
        metadata.add(new TableColumnMetadata("symbolTableSize", ColumnType.INT));
        metadata.add(new TableColumnMetadata("designated", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("upsertKey", ColumnType.BOOLEAN));
        METADATA = metadata;
    }
}
