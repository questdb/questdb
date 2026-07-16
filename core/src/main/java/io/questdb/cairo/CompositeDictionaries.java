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

package io.questdb.cairo;

import io.questdb.std.ObjList;

/**
 * Non-owning holder for a composite table's write-side interners: the per-dimension dedicated
 * {@link MapWriter} dictionaries (for {@code TRUNCATE}/{@code EXPRESSION} dimensions) and the single
 * {@link CellRegistry} over the {@code _cell} symbol map.
 * <p>
 * <b>Ownership.</b> The underlying {@link SymbolMapWriter}s are first-class {@code _txn} symbol maps:
 * they live in the {@link TableWriter}'s {@code denseSymbolMapWriters} list and are freed there (see
 * {@code TableWriter.freeSymbolMapWriters}). This holder therefore <b>never</b> frees them and has
 * <b>no</b> {@code close()} -- it is a lookup facade only. Freeing here would double-free. On writer
 * teardown the writer simply drops its reference to this holder.
 * <p>
 * The dedicated dictionaries are keyed by <b>dimension index</b> (not dense slot): a dimension that
 * needs no dedicated dictionary ({@code IDENTITY}/{@code HASH}) has a {@code null} entry, so
 * {@link #dedicatedDictFor(int)} returns {@code null} for it.
 */
public class CompositeDictionaries {
    private final CellRegistry cellRegistry;
    // keyed by dimension index; null entry for a dimension that needs no dedicated dictionary
    private final ObjList<MapWriter> dedicatedDicts;

    public CompositeDictionaries(ObjList<MapWriter> dedicatedDicts, CellRegistry cellRegistry) {
        this.dedicatedDicts = dedicatedDicts;
        this.cellRegistry = cellRegistry;
    }

    /**
     * The {@link CellRegistry} wrapping the table-root {@code _cell} symbol map.
     */
    public CellRegistry cellRegistry() {
        return cellRegistry;
    }

    /**
     * The dedicated dictionary {@link MapWriter} for dimension {@code dimIndex}, or {@code null} if
     * that dimension reuses a source column's dictionary ({@code IDENTITY}) or needs none
     * ({@code HASH}).
     */
    public MapWriter dedicatedDictFor(int dimIndex) {
        return dimIndex >= 0 && dimIndex < dedicatedDicts.size() ? dedicatedDicts.getQuick(dimIndex) : null;
    }
}
