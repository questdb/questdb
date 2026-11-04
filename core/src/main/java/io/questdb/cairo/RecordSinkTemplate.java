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

package io.questdb.cairo;

import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Creates independent {@link RecordSink} instances for one key layout, for code that needs a sink
 * per worker or builds its sinks after code generation.
 * <p>
 * {@link RecordSinkFactory#getInstanceClass} returns null when the configuration or the key size
 * calls for a {@link LoopingRecordSink}. A bare class reference loses that case: it has no column
 * types, column filter or write flags to build the looping sink from. The template keeps them, so
 * {@link #newInstance()} returns a working sink either way.
 * <p>
 * The template keeps references to its arguments, so the caller must not modify them afterwards.
 * Don't pass the code generator's shared scratch filters or bit sets.
 */
public class RecordSinkTemplate {
    private final @Nullable Class<RecordSink> clazz;
    private final ListColumnFilter columnFilter;
    private final ColumnTypes columnTypes;
    private final @Nullable BitSet writeStringAsVarchar;
    private final @Nullable BitSet writeSymbolAsString;
    private final @Nullable BitSet writeTimestampAsNanos;

    public RecordSinkTemplate(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull ColumnTypes columnTypes,
            @NotNull ListColumnFilter columnFilter,
            @Nullable BitSet writeSymbolAsString,
            @Nullable BitSet writeStringAsVarchar,
            @Nullable BitSet writeTimestampAsNanos
    ) {
        this.clazz = RecordSinkFactory.getInstanceClass(
                configuration,
                asm,
                columnTypes,
                columnFilter,
                null,
                null,
                writeSymbolAsString,
                writeStringAsVarchar,
                writeTimestampAsNanos
        );
        this.columnTypes = columnTypes;
        this.columnFilter = columnFilter;
        this.writeSymbolAsString = writeSymbolAsString;
        this.writeStringAsVarchar = writeStringAsVarchar;
        this.writeTimestampAsNanos = writeTimestampAsNanos;
    }

    public RecordSink newInstance() {
        return RecordSinkFactory.getInstance(
                clazz,
                columnTypes,
                columnFilter,
                null,
                null,
                writeSymbolAsString,
                writeStringAsVarchar,
                writeTimestampAsNanos
        );
    }
}
