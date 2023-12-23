/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.DataFrameCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LatestByValuesIndexedFilteredRecordCursorFactory extends AbstractDeferredTreeSetRecordCursorFactory {

    private final Function filter;

    public LatestByValuesIndexedFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            @Transient ObjList<Function> keyValueFuncs,
            @Transient SymbolMapReader symbolMapReader,
            @Nullable Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(configuration, metadata, dataFrameCursorFactory, columnIndex, keyValueFuncs, symbolMapReader);
        if (filter != null) {
            cursor = new LatestByValuesIndexedFilteredRecordCursor(columnIndex, rows, symbolKeys, deferredSymbolKeys, filter, columnIndexes);
        } else {
            cursor = new LatestByValuesIndexedRecordCursor(columnIndex, symbolKeys, deferredSymbolKeys, rows, columnIndexes);
        }
        this.filter = filter;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index backward scan").meta("on").putColumnName(columnIndex);
        sink.optAttr("filter", filter);
        sink.attr("symbolFilter").putColumnName(columnIndex).val(" in ");
        if (symbolKeys.size() > 0) {
            sink.val(symbolKeys);
        }
        if (deferredSymbolFuncs != null && deferredSymbolFuncs.size() > 0) {
            if (symbolKeys.size() > 0) {
                sink.val(" or ").putColumnName(columnIndex).val(" in ");
            }
            sink.val(deferredSymbolFuncs);
        }
        sink.child(dataFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return true;
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(filter);
    }
}
