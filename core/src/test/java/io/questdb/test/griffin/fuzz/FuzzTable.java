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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.ObjList;
import io.questdb.test.griffin.fuzz.FuzzTableFactory.ParquetMode;

/**
 * Generated table description: name, column list, and the designated
 * timestamp column name. The timestamp column is always present and is
 * also included as the last entry in {@link #getColumns()}.
 * <p>
 * Each primary table optionally points at a {@code shadow} sibling that
 * holds identical row-level content but was created with independently
 * random storage settings (parquet mode and per-column index flags).
 * The shadow's {@link #getShadow()} is {@code null} -- the relation is
 * one-way -- so iterating the test's primary list cannot accidentally
 * pull in the shadow as a query target.
 */
public final class FuzzTable {
    private final ObjList<FuzzColumn> columns;
    private final String name;
    private final ParquetMode parquetMode;
    private final FuzzTable shadow;
    private final String tsColumnName;

    public FuzzTable(String name, ObjList<FuzzColumn> columns, String tsColumnName) {
        this(name, columns, tsColumnName, ParquetMode.NONE, null);
    }

    public FuzzTable(String name, ObjList<FuzzColumn> columns, String tsColumnName, ParquetMode parquetMode, FuzzTable shadow) {
        this.name = name;
        this.columns = columns;
        this.tsColumnName = tsColumnName;
        this.parquetMode = parquetMode;
        this.shadow = shadow;
    }

    public FuzzColumn getColumn(int index) {
        return columns.getQuick(index);
    }

    public int getColumnCount() {
        return columns.size();
    }

    public ObjList<FuzzColumn> getColumns() {
        return columns;
    }

    public String getName() {
        return name;
    }

    public ParquetMode getParquetMode() {
        return parquetMode;
    }

    public FuzzTable getShadow() {
        return shadow;
    }

    public String getTsColumnName() {
        return tsColumnName;
    }
}
