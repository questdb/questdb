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

import io.questdb.cairo.sql.SymbolTable;

/**
 * Maps a raw symbol id a base-table scan hands a live view to the LV-private id the view
 * keys its partition maps and checkpoint roots by.
 * <p>
 * A WAL-local symbol id is stable for one transaction and no longer: the dirty band
 * restarts on every commit, so sibling transactions give the same raw id to different
 * strings, and the clean band names ids in the base table's dictionary rather than in a
 * namespace the view controls. Keying on either directly would produce a partition key
 * that means one thing on the cycle that wrote it and another on the cycle that reads it
 * back. Translation is what buys a fixed-width integer key that survives a WAL drain, an
 * applied-base replay, a checkpoint seal and a restore.
 *
 * <h2>Slots</h2>
 * A slot names one <i>source</i> column's dictionary - the distinct base SYMBOL column a
 * partition term traces to - not the term's position in a key. Two terms over the same
 * base column share a slot and therefore share an id namespace; two terms over different
 * base columns never do, even when both are the first component of their own key.
 *
 * <h2>What a translator owes its callers</h2>
 * <ul>
 *     <li>{@link SymbolTable#VALUE_IS_NULL} translates to itself. It is the only NULL
 *     encoding and is never interned.</li>
 *     <li>Every other returned id is non-negative. {@link SymbolTable#VALUE_NOT_FOUND} is
 *     a runtime cache sentinel and must never reach a partition key.</li>
 *     <li>A raw id the bound source cannot resolve, or a call against a slot no cursor
 *     armed for the current source, throws. It does not return a plausible id: a wrong
 *     partition key is in range for the dictionary, so nothing downstream would reject it
 *     and the error would surface only as wrong query results.</li>
 * </ul>
 * The arming and epoch discipline that makes the last point enforceable is the
 * implementation's; this interface is only the call the two emission mechanisms share.
 *
 * <h2>Who calls it</h2>
 * Two mechanisms emit a translated id, and a live view uses exactly one of them:
 * {@link LiveViewTranslatingRecord} interposes over the record a vanilla key sink reads,
 * and {@code RecordSinkFactory}'s translating-symbol mode compiles the call into the sink
 * itself.
 */
@FunctionalInterface
public interface LiveViewSymbolIdTranslator {

    /**
     * Translates {@code rawId} in the source bound to {@code slot} to this view's private
     * id for the same string.
     *
     * @param slot  the source column's dictionary slot
     * @param rawId the id the base scan produced, which may be a clean base-table id, a
     *              dirty WAL-local id, or {@link SymbolTable#VALUE_IS_NULL}
     * @return the LV-private id, or {@link SymbolTable#VALUE_IS_NULL} for NULL
     */
    int translate(int slot, int rawId);
}
