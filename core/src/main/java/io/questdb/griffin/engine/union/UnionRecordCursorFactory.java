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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class UnionRecordCursorFactory extends AbstractSetRecordCursorFactory {

    public UnionRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            RecordSink recordSink,
            @Transient @NotNull ColumnTypes mapKeyTypes,
            @Transient @NotNull ColumnTypes mapValueTypes
    ) {
        super(metadata, factoryA, factoryB, castFunctionsA, castFunctionsB);
        Map map = null;
        try {
            map = MapFactory.createOrderedMap(configuration, mapKeyTypes, mapValueTypes, false);
            cursor = new UnionRecordCursor(map, recordSink, castFunctionsA, castFunctionsB);
        } catch (Throwable th) {
            Misc.free(map);
            close();
            throw th;
        }
    }

    @Override
    public void _close() {
        final AbstractSetRecordCursor cursor = this.cursor;
        this.cursor = null;
        closeSetOwnersBestEffort(cursor);
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return true;
    }

    @Override
    public boolean isColumnIntWidthStable(int columnIndex) {
        // UNION distinct is a live pass-through: UnionRecord/UnionCastRecord delegate getInt/getLong to
        // the active leg's record, so an overflowing INT projection on a leg keeps its wide value at long
        // width - exactly like UNION ALL. The copier reads ONE width for the whole column across both
        // legs, and getLong() is only safe on a width-unstable (function-backed) leg; a width-stable leg
        // may be a real INT column whose getLong() would over-read its 4-byte slot. So the column may be
        // reported unstable only when BOTH legs are unstable. The dedup map holds only membership keys and
        // never materialises the returned value, so it does not narrow the width - it is leg A / leg B
        // themselves that must both be function-backed.
        //
        // CAVEAT - same as UNION ALL's cast path: when a sibling column forces castIsRequired the cursor
        // uses UnionCastRecord, which routes an INT->INT column through IntColumn.getLong() = a re-wrap. So
        // a both-legs-unstable column that this reports unstable can still STORE the wrapped value on the
        // cast path. Safe (no over-read either way), but the stored value can then depend on whether a
        // sibling column forced the cast. Left documented rather than risked, exactly as in UNION ALL.
        return factoryA.isColumnIntWidthStable(columnIndex) || factoryB.isColumnIntWidthStable(columnIndex);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    protected CharSequence getOperation() {
        return "Union";
    }
}
