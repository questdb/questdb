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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.AbstractVirtualFunctionRecordCursor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public abstract class VirtualFunctionSkewedSymbolRecordCursor extends AbstractVirtualFunctionRecordCursor {
    protected RecordCursor managedCursor;

    public VirtualFunctionSkewedSymbolRecordCursor(ObjList<Function> functions) {
        super(functions, true);
    }

    @Override
    public void close() {
        managedCursor = Misc.free(managedCursor);
        baseCursor = null;
    }

    public void of(RecordCursor managedCursor, RecordCursor baseCursor) {
        this.managedCursor = managedCursor;
        of(baseCursor);
    }
}
