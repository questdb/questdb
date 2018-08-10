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

import com.questdb.cairo.AbstractRecordCursorFactory;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.IntList;
import com.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public class VirtualRecordCursorFactory extends AbstractRecordCursorFactory {
    private final VirtualRecordCursor cursor;
    private final ObjList<Function> functions;
    private RecordCursorFactory base;

    public VirtualRecordCursorFactory(
            RecordMetadata metadata,
            ObjList<Function> functions,
            RecordCursorFactory baseFactory,
            @Nullable IntList symbolTableCrossIndex) {
        super(metadata);
        this.functions = functions;
        this.cursor = new VirtualRecordCursor(functions, symbolTableCrossIndex);
        this.base = baseFactory;
    }

    @Override
    public void close() {
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).close();
        }
        this.base.close();
    }

    @Override
    public RecordCursor getCursor(BindVariableService bindVariableService) {
        RecordCursor cursor = base.getCursor(bindVariableService);
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).init(cursor, bindVariableService);
        }
        this.cursor.of(cursor);
        return this.cursor;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return true;
    }
}
