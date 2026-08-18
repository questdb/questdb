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
import io.questdb.cairo.sql.SymbolTable;
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
     * Reverse-looks-up dense key {@code key} on {@code writer}, returning its interned string value
     * -- the write-side mirror of {@link io.questdb.cairo.sql.SymbolTable#valueOf(int)}, which only
     * concrete reader types (e.g. {@code SymbolMapReaderImpl}) expose; {@code MapWriter} itself
     * declares no {@code valueOf}. A {@link SymbolMapWriter} already holds the exact backing data a
     * reverse lookup needs -- it appends new symbols to the very same offset/value memory
     * {@link #getSymbolOffsetsMemory()}/{@link #getSymbolValuesMemory()} already expose -- so this
     * reads it back directly instead of requiring a separate {@code SymbolMapReader} to be opened.
     * Mirrors {@code SymbolMapReaderImpl.uncachedValue(int)}'s exact mechanics.
     * <p>
     * Only meaningful for a {@code key} previously returned by {@link #put(CharSequence)} (or an
     * equivalent intern call) on this same {@code writer} -- e.g. composite-partitioning path
     * construction (Plan 4a Task 3), which reverse-looks-up an IDENTITY dimension's source-column
     * symbol key, a TRUNCATE dimension's dedicated-dictionary ordinal, or the {@code _cell}
     * registry's own cellKey, all of which are plain {@code MapWriter}s. Not meant for an arbitrary
     * out-of-range key or a {@code NullMapWriter} -- callers must already know {@code writer} is a
     * real, populated symbol map.
     * <p>
     * {@link SymbolTable#VALUE_IS_NULL} -- what {@link #put(CharSequence)} returns for a NULL
     * symbol, and therefore a perfectly ordinary key a caller can hold -- reverse-looks-up to
     * {@code null}, mirroring the read side's {@code SymbolMapReaderImpl#valueOf(int)} contract
     * for that one case (which already returns {@code null} for any key outside
     * {@code [0, symbolCount)}, VALUE_IS_NULL included). Without this guard the key falls through
     * to {@link SymbolMapWriter#keyToOffset(int)}, which turns {@code Integer.MIN_VALUE} into a
     * hugely NEGATIVE memory offset: an {@code assert} failure under {@code -ea}, and an unchecked
     * out-of-bounds native read (garbage value or SIGSEGV) without it. That was the root cause of
     * the composite-partitioning WAL-apply hang on a NULL IDENTITY dimension value -- see
     * {@code TableWriter#renderDimensionSegment}.
     * <p>
     * <b>This guard is NOT as broad as the read side's.</b> A review pass (composite-partitioning
     * NULL-fix follow-up) suggested widening it to the reader's full {@code key > -1 && key <
     * symbolCount} form, matching {@code SymbolMapReaderImpl#valueOf} bound-for-bound and making a
     * POSITIVE out-of-range key null too instead of silently reading garbage -- {@code -ea} or not.
     * That widened form was tried and reverted: {@code TableWriter#processPartitionRemoveCandidates0}
     * calls {@code CellRegistry#getTupleFromWriter}, which calls this method with a
     * {@code partitionRemoveCandidates}-queued cellKey that can be a currently-out-of-range ordinal
     * at render time (root cause not yet investigated) and, unlike every other caller here, does
     * NOT null-check the result before feeding it to {@code CompositeTupleCodec#decode} --
     * widening this guard turns that into an immediate {@code NullPointerException} that suspends
     * the table, empirically reproducible with as few as THREE distinct real (non-NULL, non-empty)
     * symbol values dispatched in one commit on a brand-new composite table -- not a NULL/empty-value
     * edge case at all, an everyday shape. So: for a POSITIVE out-of-range key, this method still
     * reads silent garbage rather than returning null -- exactly like an assertion-free build reads
     * a NULL IDENTITY key before this class's own {@code VALUE_IS_NULL} guard. Fixing that requires
     * fixing (or null-guarding) {@code getTupleFromWriter}'s caller first; tracked as a follow-up,
     * out of scope here.
     */
    static CharSequence valueOf(MapWriter writer, int key) {
        // DO NOT widen this guard to the reader's `key > -1 && key < symbolCount` shape. It looks
        // like an oversight and is not: CellRegistry#getTupleFromWriter reads back an ordinal this
        // same writer JUST interned, before the writer's committed symbol count covers it, and
        // relies on that read succeeding. Widening the guard was measured to turn this into a
        // TABLE-SUSPENDING failure on three ordinary distinct symbol values in a single commit, with
        // no NULL involved at all.
        if (key == SymbolTable.VALUE_IS_NULL) {
            return null;
        }
        return writer.getSymbolValuesMemory().getStrA(writer.getSymbolOffsetsMemory().getLong(SymbolMapWriter.keyToOffset(key)));
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

    /**
     * @return whether the value-to-key cache is allocated. Not the same question as
     * {@link #isCached()}, which reports what the column asked for: a column configured CACHE
     * keeps that flag after {@link SymbolMapWriter} has run the cache's key buffer out and
     * dropped it, because the drop is an internal fallback rather than a change to the column.
     * The two disagreeing is what tells a caller the column is missing a cache it is entitled
     * to. An implementation that holds no cache of its own answers whatever {@link #isCached()}
     * answers, so such a caller finds nothing to act on
     */
    boolean isCacheAllocated();

    boolean isCached();

    int put(char c);

    int put(CharSequence symbol);

    int put(CharSequence symbol, SymbolValueCountCollector valueCountCollector);

    void rollback(int symbolCount);

    void setSymbolIndexInTxWriter(int symbolIndexInTxWriter);

    void sync(boolean async);

    void truncate();

    void updateCacheFlag(boolean flag);

    void updateNullFlag(boolean flag);
}
