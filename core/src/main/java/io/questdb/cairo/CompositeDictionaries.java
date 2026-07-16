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
 * Non-owning holder for a composite table's interners: the per-dimension dedicated dictionaries (for
 * {@code TRUNCATE}/{@code EXPRESSION} dimensions) and the single {@link CellRegistry} over the
 * {@code _cell} symbol map. <b>Dual-mode</b>, mirroring {@link CellRegistry}: a write-side instance
 * wraps {@link MapWriter}s (built by {@code TableWriter}), a read-side instance wraps
 * {@link SymbolMapReader}s (built by {@code TableReader}) -- never both.
 * <p>
 * <b>Ownership.</b> On the write side, the underlying {@link SymbolMapWriter}s are first-class
 * {@code _txn} symbol maps: they live in the {@link TableWriter}'s {@code denseSymbolMapWriters} list
 * and are freed there (see {@code TableWriter.freeSymbolMapWriters}). On the read side, the underlying
 * {@link SymbolMapReaderImpl}s live in the {@code TableReader}'s {@code compositeInternerReaders} list
 * and are freed there (see {@code TableReader.freeSymbolMapReaders}). This holder therefore
 * <b>never</b> frees them and has <b>no</b> {@code close()} -- it is a lookup facade only. Freeing
 * here would double-free. On teardown the owner simply drops its reference to this holder.
 * <p>
 * The dedicated dictionaries/readers are keyed by <b>dimension index</b> (not dense slot): a
 * dimension that needs no dedicated dictionary ({@code IDENTITY}/{@code HASH}) has a {@code null}
 * entry, so {@link #dedicatedDictFor(int)}/{@link #dictReaderFor(int)} return {@code null} for it.
 */
public class CompositeDictionaries {
    private final CellRegistry cellRegistry;
    // write-side; keyed by dimension index; null entry for a dimension that needs no dedicated dictionary
    private final ObjList<MapWriter> dedicatedDicts;
    // read-side; keyed by dimension index; null entry for a dimension that needs no dedicated dictionary
    private final ObjList<SymbolMapReader> dedicatedDictReaders;

    public CompositeDictionaries(ObjList<MapWriter> dedicatedDicts, CellRegistry cellRegistry) {
        this.dedicatedDicts = dedicatedDicts;
        this.dedicatedDictReaders = null;
        this.cellRegistry = cellRegistry;
    }

    /**
     * Read-side constructor. Parameter order is deliberately {@code (CellRegistry, ObjList)} rather
     * than mirroring the write-side constructor's {@code (ObjList, CellRegistry)} order: since generics
     * are erased, {@code CompositeDictionaries(ObjList<SymbolMapReader>, CellRegistry)} would have the
     * same erasure as the write-side {@code CompositeDictionaries(ObjList<MapWriter>, CellRegistry)}
     * and fail to compile as a constructor name/erasure clash (unlike overloaded methods, constructors
     * cannot be disambiguated by generic type argument alone).
     */
    public CompositeDictionaries(CellRegistry readerCellRegistry, ObjList<SymbolMapReader> dedicatedDictReadersByDim) {
        this.dedicatedDicts = null;
        this.dedicatedDictReaders = dedicatedDictReadersByDim;
        this.cellRegistry = readerCellRegistry;
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
     * ({@code HASH}), or if this is a read-side instance.
     */
    public MapWriter dedicatedDictFor(int dimIndex) {
        return dedicatedDicts != null && dimIndex >= 0 && dimIndex < dedicatedDicts.size() ? dedicatedDicts.getQuick(dimIndex) : null;
    }

    /**
     * The dedicated dictionary {@link SymbolMapReader} for dimension {@code dimIndex}, or
     * {@code null} if that dimension needs no dedicated dictionary ({@code IDENTITY}/{@code HASH}),
     * or if this is a write-side instance.
     */
    public SymbolMapReader dictReaderFor(int dimIndex) {
        return dedicatedDictReaders != null && dimIndex >= 0 && dimIndex < dedicatedDictReaders.size() ? dedicatedDictReaders.getQuick(dimIndex) : null;
    }
}
