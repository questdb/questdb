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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.ObjList;

public class UnionAllRecordCursorFactory extends AbstractSetRecordCursorFactory {

    public UnionAllRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB
    ) {
        super(metadata, factoryA, factoryB, castFunctionsA, castFunctionsB);
        try {
            this.cursor = new UnionAllRecordCursor(castFunctionsA, castFunctionsB);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return true;
    }

    @Override
    public boolean isColumnIntWidthStable(int columnIndex) {
        // UNION ALL is a live pass-through: UnionRecord.getInt/getLong (or the IntColumn-style cast
        // functions of UnionCastRecord) delegate to the active leg's record, so an overflowing INT
        // projection on a leg keeps its wide value at long width - like the join master and the other
        // transparent wrappers. But the copier reads ONE width for the whole column across both legs,
        // and getLong() is only safe to read on a width-unstable (function-backed) leg; a width-stable
        // leg may be a real INT column whose getLong() would over-read its 4-byte slot. So the union
        // may report unstable only when BOTH legs are unstable - then getLong() is safe on either side
        // and widens both. If either leg is width-stable, the column must be read at INT width, which
        // wraps an overflowing projection on the other leg (unavoidable without a per-leg-width read).
        return factoryA.isColumnIntWidthStable(columnIndex) || factoryB.isColumnIntWidthStable(columnIndex);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    protected String getOperation() {
        return "Union All";
    }
}
