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

package io.questdb.cairo;

/**
 * Listener for DDL (Data Definition Language) events. The enterprise implementation
 * uses these callbacks to maintain permission metadata in sync with schema changes.
 * <p>
 * All callbacks fire only when the DDL actually takes effect. No-op statements
 * (e.g. {@code DROP TABLE IF EXISTS} for a non-existent table, or
 * {@code ADD COLUMN} for a duplicate column caught by ILP) do not trigger callbacks.
 */
public interface DdlListener {

    default void clear() {
    }

    /**
     * Called when a column is added to a table. The enterprise implementation grants
     * owner permissions on the new column to the principal that created it.
     *
     * @param securityContext the security context of the user adding the column
     * @param tableToken      the table token of the table the column is added to
     * @param columnName      the name of the new column
     */
    void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName);

    /**
     * Called when a column is dropped from a table.
     *
     * @param tableToken the table token of the table the column is dropped from
     * @param columnName the name of the dropped column
     */
    void onColumnDropped(TableToken tableToken, CharSequence columnName);

    /**
     * Called when a column is renamed. The enterprise implementation transfers all existing
     * permissions from the old column name to the new one.
     *
     * @param tableToken    the table token of the table containing the column
     * @param oldColumnName the original column name
     * @param newColumnName the new column name
     */
    void onColumnRenamed(TableToken tableToken, CharSequence oldColumnName, CharSequence newColumnName);

    /**
     * Called when a table, view or materialized view is created.
     *
     * @param securityContext the security context
     * @param tableToken      the table token
     * @param tableKind       the kind of table being created. See {@link TableUtils#TABLE_KIND_REGULAR_TABLE} for regular data tables
     *                        and {@link TableUtils#TABLE_KIND_TEMP_PARQUET_EXPORT} for parquet export tables. The parquet export
     *                        table kind is primarily used to allow table creation in read-only mode for parquet exports.
     *                        This table kind will be removed in the future when parquet export uses pure in-memory mode
     *                        instead of temporary tables.
     */
    void onTableOrViewOrMatViewCreated(SecurityContext securityContext, TableToken tableToken, int tableKind);

    /**
     * Called when a table, view or materialized view is dropped.
     * <p>
     * {@code DROP ALL} also calls this method once per successfully dropped entity, skipping
     * system tables and tables that could not be locked. Tables that fail to drop are collected
     * and reported in a single {@code CairoException} after the loop completes.
     *
     * @param tableToken table token of the dropped table, view or materialized view
     */
    void onTableOrViewOrMatViewDropped(TableToken tableToken);

    /**
     * Called when a table is renamed. The enterprise implementation transfers all existing
     * permissions from the old table name to the new one.
     *
     * @param oldTableToken the table token before the rename
     * @param newTableToken the table token after the rename
     */
    void onTableRenamed(TableToken oldTableToken, TableToken newTableToken);
}
