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

import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.charFileName;
import static io.questdb.cairo.TableUtils.offsetFileName;

public interface MapWriter extends SymbolCountProvider {
    static void createSymbolMapFiles(
            FilesFacade ff,
            MemoryMA mem,
            Path path,
            CharSequence columnName,
            long columnNameTxn,
            int symbolCapacity,
            boolean symbolCacheFlag
    ) {
        int plen = path.size();
        try {
            mem.smallFile(ff, offsetFileName(path.trimTo(plen), columnName, columnNameTxn), MemoryTag.MMAP_INDEX_WRITER);
            mem.jumpTo(0);
            mem.putInt(symbolCapacity);
            mem.putBool(symbolCacheFlag);
            mem.jumpTo(SymbolMapWriter.HEADER_SIZE);
            mem.sync(false);
            mem.close();

            if (!ff.touch(charFileName(path.trimTo(plen), columnName, columnNameTxn))) {
                throw CairoException.critical(ff.errno()).put("Cannot create ").put(path);
            }

            mem.smallFile(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn), MemoryTag.MMAP_INDEX_WRITER);
            BitmapIndexWriter.initKeyMemory(mem, TableUtils.MIN_INDEX_VALUE_BLOCK_SIZE);
            mem.sync(false);
            ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn));
        } finally {
            path.trimTo(plen);
            Misc.free(mem);
        }
    }

    /**
     * Column index in table writer metadata. This value is a pass-thru from table writer, and
     * it used by table writer to look-back the column name when needed.
     *
     * @return column index or -1 if this is a NullWriter (noop writer)
     */
    int getColumnIndex();

    boolean getNullFlag();

    int getSymbolCapacity();

    MemoryR getSymbolOffsetsMemory();

    MemoryR getSymbolValuesMemory();

    boolean isCached();

    int put(char c);

    int put(CharSequence symbol);

    int put(CharSequence symbol, SymbolValueCountCollector valueCountCollector);

    void rollback(int symbolCount);

    void setSymbolIndexInTxWriter(int symbolIndexInTxWriter);

    void sync(boolean async);

    /**
     * Phase 1 of the batched SYNC-mode flush (Linux only); see {@link io.questdb.cairo.vm.api.MemoryMA#syncFlushKick()}.
     * Default keeps current behavior (no-op kick + the fallback {@code sync(false)} in
     * {@link #syncFlushFinishIfExtended()} provides durability), so non-overriding writers are unchanged.
     */
    default void syncFlushKick() {
    }

    /**
     * Phase 2 of the batched SYNC-mode flush (Linux only); see {@link io.questdb.cairo.vm.api.MemoryMA#syncFlushDrain()}.
     * Default keeps current behavior: a no-op.
     */
    default void syncFlushDrain() {
    }

    /**
     * Phase 3 of the batched SYNC-mode flush; see {@link io.questdb.cairo.vm.api.MemoryMA#syncFlushFinishIfExtended()}.
     * Default keeps current behavior: a full {@code sync(false)} (the conservative fallback for writers that
     * did not push content to the device cache via the no-op default kick/drain, and for the non-Linux path).
     */
    default void syncFlushFinishIfExtended() {
        sync(false);
    }

    void truncate();

    void updateCacheFlag(boolean flag);

    void updateNullFlag(boolean flag);
}
