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

package io.questdb.cairo;

public interface DdlListener {

    void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName);

    void onColumnDropped(TableToken tableToken, CharSequence columnName, boolean cascadePermissions);

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

    void onTableDropped(String tableName, boolean cascadePermissions);

    void onTableRenamed(TableToken oldTableToken, TableToken newTableToken);
}
