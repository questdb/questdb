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


package io.questdb.cairo.lv;

import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;

/**
 * A record flyweight that hands a vanilla key sink an LV-private symbol id where the
 * record behind it holds a raw one.
 * <p>
 * This is one of the two ways a {@code RecordSink} can emit a translated id, and the one
 * that costs the bytecode assembler nothing. A translated SYMBOL partition term classifies
 * as SYMBOL with <i>no</i> {@code writeSymbolAsString} bit, so the generated sink emits the
 * same {@code getInt} / {@code putInt} pair it already emits for an INT column; interposing
 * this record over the window input is what turns that {@code getInt} into a translation.
 * The alternative compiles the translator call into the sink itself - see
 * {@code RecordSinkFactory#getTranslatingInstance}, which the same binding vector drives.
 * <p>
 * <b>That alternative is the one live views will bind.</b> It is 0.6-0.9 ns per row cheaper
 * at the sink across every key shape - this record's extra dispatch, slot load and branch
 * put it within 2% of the resolved-string sink it was meant to replace - and it can suppress
 * the direct-column shortcut below in one place rather than at every site that compiles a
 * vanilla sink. This class stays because the comparison is owed again end to end, once a
 * live view can actually key through either: it is {@code LiveViewPartitionKeySinkBenchmark}'s
 * {@code RECORD} arm, and nothing in the refresh path uses it.
 *
 * <h2>The binding vector</h2>
 * {@code slotByColumn} is indexed by the delegate record's own column index and holds
 * either the {@link LiveViewSymbolIdTranslator} slot that column's source dictionary lives
 * in, or {@link #NOT_TRANSLATED}. It must cover every column the sink reads: a sink asking
 * for a column past its end is a binding vector built against the wrong metadata, and
 * failing on the spot is the point. Columns that are not SYMBOL, and SYMBOL columns whose
 * term stayed on the STRING path, carry {@link #NOT_TRANSLATED} and read straight through.
 *
 * <h2>Scope</h2>
 * Only {@link #getInt(int)} translates. Everything else delegates, including
 * {@link #getSymA(int)}: the string a raw id names does not change under translation, so a
 * consumer reading the symbol rather than the key reads the same value it always did.
 *
 * <h2>The sink this pairs with must report no direct column</h2>
 * A sink whose {@code getDirectColumnIndex()} is non-negative tells
 * {@code Unordered4Map.probeBatch} it may read that column out of page-frame memory and skip
 * the sink altogether - and skipping the sink skips this record with it, keying the raw WAL
 * id. {@code RecordSinkFactory} sets that index for any single-column filter, which is
 * exactly the narrow key shape this optimization aims at, so whoever compiles the vanilla
 * sink for a translated term has to suppress it.
 * {@code RecordSinkFactory#getTranslatingInstance} does so for the other mechanism.
 */
public final class LiveViewTranslatingRecord extends DelegatingRecord {
    /**
     * Slot value for a column the sink reads without translating.
     */
    public static final int NOT_TRANSLATED = -1;
    private final int[] slotByColumn;
    private LiveViewSymbolIdTranslator translator;

    /**
     * @param slotByColumn translator slot per delegate column index, {@link #NOT_TRANSLATED}
     *                     where the column reads through; copied, so the caller may reuse it
     */
    public LiveViewTranslatingRecord(@NotNull IntList slotByColumn) {
        this.slotByColumn = new int[slotByColumn.size()];
        for (int i = 0, n = slotByColumn.size(); i < n; i++) {
            this.slotByColumn[i] = slotByColumn.getQuick(i);
        }
    }

    @Override
    public int getInt(int col) {
        final int rawId = base.getInt(col);
        final int slot = slotByColumn[col];
        return slot == NOT_TRANSLATED ? rawId : translator.translate(slot, rawId);
    }

    /**
     * Binds the flyweight to the record a cursor is about to walk and to the translator
     * that cursor armed. Both change per cursor open, which is where the arming epoch lives,
     * so neither is worth caching across one.
     */
    public void of(@NotNull Record base, @NotNull LiveViewSymbolIdTranslator translator) {
        this.base = base;
        this.translator = translator;
    }
}
