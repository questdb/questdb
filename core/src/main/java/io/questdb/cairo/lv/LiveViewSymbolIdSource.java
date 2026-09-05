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

/**
 * The one thing a cursor open hands {@link LiveViewSymbolIdRegistry#armFor}: how to
 * describe, for one bound slot, the symbol-id space the rows about to be read name their
 * values in.
 * <p>
 * A source exists because {@link LiveViewSymbolIdRegistry#translate} splits a raw id on
 * {@code rawId < cleanSymbolCount}, and that boundary moves with the source. Under a WAL
 * drain it is the transaction's own clean count and the band above it is that
 * transaction's dirty band - meaningful for exactly one transaction, because the WAL
 * writer restarts its local ids on every commit. Under an applied-base, seed or O3 replay
 * there is no dirty band at all and the boundary is the pinned reader's symbol count. A
 * slot carrying one source's boundary into the other's rows puts a legitimate base id
 * inside a stale dirty band, where it translates to a plausible id for the wrong string:
 * in range for the dictionary, so nothing downstream rejects it, and visible only as
 * wrong query results.
 * <p>
 * The registry drives the loop rather than the caller, so a source cannot arm some of the
 * slots and leave the rest on the previous cursor's boundary - {@code armFor} walks every
 * bound slot and refuses a source that does not answer for one. Arming is per cursor open,
 * not per transaction: a WAL drain opens a cursor per transaction, so the two coincide
 * there, while a replay arms once for the whole scan.
 *
 * @see LiveViewSymbolIdRegistry#armFor(LiveViewSymbolIdSource)
 */
@FunctionalInterface
public interface LiveViewSymbolIdSource {

    /**
     * Arms one slot by calling back into {@link LiveViewSymbolIdRegistry#armWal} or
     * {@link LiveViewSymbolIdRegistry#armStatic}. Both column indexes are supplied because
     * the two families name the same base column in different spaces: a WAL segment names
     * its columns by base-table writer index, and a pinned-reader page-frame cursor names
     * them by base-scan index.
     *
     * @param registry              the registry to arm the slot on
     * @param slot                  the slot being armed
     * @param baseScanColumnIndex   the slot's column index in the plan's base-scan metadata
     * @param baseWriterColumnIndex the slot's base-table writer index
     */
    void armSlot(LiveViewSymbolIdRegistry registry, int slot, int baseScanColumnIndex, int baseWriterColumnIndex);
}
