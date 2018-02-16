/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.RowCursorFactory;
import com.questdb.common.RowCursor;

public class MergingRowCursorFactory implements RowCursorFactory {
    private final RowCursorFactory lhs;
    private final RowCursorFactory rhs;
    private final MergingRowCursor cursor = new MergingRowCursor();

    public MergingRowCursorFactory(RowCursorFactory lhs, RowCursorFactory rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public RowCursor getCursor(DataFrame dataFrame) {
        cursor.of(lhs.getCursor(dataFrame), rhs.getCursor(dataFrame));
        return cursor;
    }
}
