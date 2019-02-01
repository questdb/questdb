/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.table;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.DataFrameCursorFactory;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.RecordMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestByAllIndexedFilteredRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    public LatestByAllIndexedFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @Nullable Function filter) {
        super(metadata, dataFrameCursorFactory, configuration);
        if (filter == null) {
            this.cursor = new LatestByAllIndexedRecordCursor(columnIndex, treeSet);
        } else {
            this.cursor = new LatestByAllIndexedFilteredRecordCursor(columnIndex, treeSet, filter);
        }
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }
}
