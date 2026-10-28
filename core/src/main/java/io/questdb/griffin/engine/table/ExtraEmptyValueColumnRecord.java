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

package io.questdb.griffin.engine.table;

import io.questdb.std.LongList;

/**
 * Spliced-column record of a constant-false WINDOW JOIN whose aggregates include a function with a
 * non-NULL empty value, such as count(). {@link #getLong(int)} reports each spliced column's empty
 * value from {@code emptyValues}; a {@link io.questdb.std.Numbers#LONG_NULL} entry reproduces the
 * NULL that {@link ExtraNullColumnRecord} reports. Every other getter keeps the NULL behaviour.
 */
public class ExtraEmptyValueColumnRecord extends ExtraNullColumnRecord {
    private final int columnSplit;
    private final LongList emptyValues;

    public ExtraEmptyValueColumnRecord(int columnSplit, LongList emptyValues) {
        super(columnSplit);
        this.columnSplit = columnSplit;
        this.emptyValues = emptyValues;
    }

    @Override
    public long getLong(int col) {
        return col < columnSplit ? super.getLong(col) : emptyValues.getQuick(col - columnSplit);
    }
}
