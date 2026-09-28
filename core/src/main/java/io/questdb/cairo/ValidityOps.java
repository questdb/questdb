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

import io.questdb.cairo.vm.api.MemoryA;

/**
 * The validity operations storage runs for a column as it writes, fills, copies and merges
 * values (F36, F37). Storage calls them per batch, never per row: after a NULL fill, at a column
 * top fill, and at the start of each out-of-order copy block. A column's operations follow from
 * its NULL policy through {@link #of(NullPolicy)}.
 * <p>
 * Every policy on this branch keeps NULL in the value (SENTINEL) or has none (NONE), so every
 * column answers {@link #NONE}, whose operations do nothing, and no column has validity memory:
 * storage passes 0 for every validity address. A policy that keeps NULLs in a validity bitmap
 * adds an arm to {@link #of(NullPolicy)}, which javac lists, and answers operations that write
 * the bits; the call sites stay as they are.
 */
public interface ValidityOps {
    ValidityOps NONE = new NoValidityOps();

    /**
     * The operations of a column with NULL policy {@code policy}.
     */
    static ValidityOps of(NullPolicy policy) {
        return switch (policy) {
            case SENTINEL, NONE -> ValidityOps.NONE;
        };
    }

    /**
     * Appends one NULL row's validity. Row appends choose their columns' operations at setup and
     * never call this per row for a column without validity memory (E7: a per-row call costs 6%
     * on in-order appends).
     */
    void appendNull(MemoryA validityMem);

    /**
     * Appends one valid row's validity; see {@link #appendNull(MemoryA)}.
     */
    void appendValid(MemoryA validityMem);

    /**
     * Copies the validity of {@code rowCount} rows from bit {@code srcBitOffset} of
     * {@code srcAddr} to bit {@code dstBitOffset} of {@code dstAddr}, as a copy block moves
     * their values.
     */
    void copy(long srcAddr, long srcBitOffset, long dstAddr, long dstBitOffset, long rowCount);

    /**
     * Sets the validity of {@code rowCount} rows from bit {@code bitOffset} of
     * {@code validityAddr}: valid when {@code isValid}, NULL otherwise.
     */
    void fill(long validityAddr, long bitOffset, long rowCount, boolean isValid);

    /**
     * The validity of an out-of-order merge block: entry {@code i} of the timestamp merge index
     * at {@code mergeIndexAddr} picks a partition row (validity at {@code srcAddr}, column top
     * {@code srcTop}) or an out-of-order row (validity at {@code srcO3Addr}), as the value merge
     * picks the values, and lands on bit {@code dstBitOffset + i} of {@code dstAddr}.
     */
    void merge(
            long mergeIndexAddr,
            long mergeCount,
            long srcAddr,
            long srcTop,
            long srcO3Addr,
            long dstAddr,
            long dstBitOffset
    );

    final class NoValidityOps implements ValidityOps {
        private NoValidityOps() {
        }

        @Override
        public void appendNull(MemoryA validityMem) {
        }

        @Override
        public void appendValid(MemoryA validityMem) {
        }

        @Override
        public void copy(long srcAddr, long srcBitOffset, long dstAddr, long dstBitOffset, long rowCount) {
        }

        @Override
        public void fill(long validityAddr, long bitOffset, long rowCount, boolean isValid) {
        }

        @Override
        public void merge(
                long mergeIndexAddr,
                long mergeCount,
                long srcAddr,
                long srcTop,
                long srcO3Addr,
                long dstAddr,
                long dstBitOffset
        ) {
        }
    }
}
