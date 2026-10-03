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

package io.questdb.cairo.sql;

import io.questdb.cairo.NullPolicy;
import io.questdb.std.DirectLongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The column vectors of one page frame, as every consumer of the frame's column data reads
 * them. Page frame memory hands column data out only through this descriptor.
 * <p>
 * Per column it answers:
 * <ul>
 *     <li>the data vector's address and size; a data address of 0 means the whole frame is
 *     NULL for the column (a column top, a missing column);</li>
 *     <li>the aux vector's address and size, for var-size columns;</li>
 *     <li>the column's NULL policy;</li>
 *     <li>the validity address, 0 when the frame has no validity bitmap for the column;</li>
 *     <li>the bit offset of the frame's first row within the first validity word;</li>
 *     <li>the NULL count, -1 when unknown.</li>
 * </ul>
 * No column has a validity bitmap yet: every column answers validity address 0, bit offset 0
 * and NULL count -1, and no consumer reads those three answers. A validity bitmap fills them in
 * later without changing a consumer's signature.
 * <p>
 * The descriptor is a view over flat per-column lists that its builder owns: the address cache
 * for native frames, the pool's decode buffers for Parquet and covered frames. The builder
 * points it at a frame with {@link #of}, once per frame; nothing is allocated per frame or per
 * row. The three validity lists may be null, when the builder carries no validity for the
 * frame; the column then answers validity address 0, bit offset 0 and NULL count -1.
 * <p>
 * A record owns its own descriptor and takes a frame's with {@link #copyFrom}, which copies
 * every field, so a record copy cannot leave a field behind. The record also keeps the lists
 * themselves, taken from its descriptor in one place, so that a per-row read costs no more than
 * it did before the descriptor existed.
 */
public final class ColumnVectorDescriptor implements Mutable {
    private DirectLongList auxAddresses;
    private DirectLongList auxSizes;
    private int columnCount;
    private int columnOffset;
    private DirectLongList dataAddresses;
    private DirectLongList dataSizes;
    private ObjList<NullPolicy> nullPolicies;
    private DirectLongList nullCounts;
    private DirectLongList validityAddresses;
    private DirectLongList validityBitOffsets;

    @Override
    public void clear() {
        auxAddresses = null;
        auxSizes = null;
        columnCount = 0;
        columnOffset = 0;
        dataAddresses = null;
        dataSizes = null;
        nullPolicies = null;
        nullCounts = null;
        validityAddresses = null;
        validityBitOffsets = null;
    }

    public void copyFrom(ColumnVectorDescriptor other) {
        auxAddresses = other.auxAddresses;
        auxSizes = other.auxSizes;
        columnCount = other.columnCount;
        columnOffset = other.columnOffset;
        dataAddresses = other.dataAddresses;
        dataSizes = other.dataSizes;
        nullPolicies = other.nullPolicies;
        nullCounts = other.nullCounts;
        validityAddresses = other.validityAddresses;
        validityBitOffsets = other.validityBitOffsets;
    }

    /**
     * The aux (index) vector address of a var-size column; 0 for a fixed-size column and for a
     * column top.
     */
    public long getAuxAddress(int columnIndex) {
        return auxAddresses.get(columnOffset + columnIndex);
    }

    public long getAuxSize(int columnIndex) {
        return auxSizes.get(columnOffset + columnIndex);
    }

    public int getColumnCount() {
        return columnCount;
    }

    /**
     * The position of this frame's first column in the flat lists. Column top counts that a
     * Parquet frame keeps next to the lists use the same position.
     */
    public int getColumnOffset() {
        return columnOffset;
    }

    /**
     * The data vector address, or 0 when the whole frame is NULL for the column.
     */
    public long getDataAddress(int columnIndex) {
        return dataAddresses.get(columnOffset + columnIndex);
    }

    public long getDataSize(int columnIndex) {
        return dataSizes.get(columnOffset + columnIndex);
    }

    /**
     * The NULL count of the column in this frame, -1 when unknown.
     */
    public long getNullCount(int columnIndex) {
        return nullCounts != null ? nullCounts.get(columnOffset + columnIndex) : -1;
    }

    /**
     * The column's NULL policy, from the query metadata's per-column answer.
     */
    public NullPolicy getNullPolicy(int columnIndex) {
        return nullPolicies.getQuick(columnIndex);
    }

    /**
     * The address of the validity word that holds the frame's first row, 0 when the frame has
     * no validity bitmap for the column.
     */
    public long getValidityAddress(int columnIndex) {
        return validityAddresses != null ? validityAddresses.get(columnOffset + columnIndex) : 0;
    }

    /**
     * The position of the frame's first row within the word at the validity address.
     */
    public long getValidityBitOffset(int columnIndex) {
        return validityBitOffsets != null ? validityBitOffsets.get(columnOffset + columnIndex) : 0;
    }

    // the lists behind the per-column answers, for a record's per-row reads (same package)
    DirectLongList getAuxAddresses() {
        return auxAddresses;
    }

    DirectLongList getAuxSizes() {
        return auxSizes;
    }

    DirectLongList getDataAddresses() {
        return dataAddresses;
    }

    DirectLongList getDataSizes() {
        return dataSizes;
    }

    /**
     * Points the descriptor at one frame's entries of its builder's lists.
     */
    public ColumnVectorDescriptor of(
            DirectLongList dataAddresses,
            DirectLongList dataSizes,
            DirectLongList auxAddresses,
            DirectLongList auxSizes,
            DirectLongList validityAddresses,
            DirectLongList validityBitOffsets,
            DirectLongList nullCounts,
            ObjList<NullPolicy> nullPolicies,
            int columnOffset,
            int columnCount
    ) {
        this.dataAddresses = dataAddresses;
        this.dataSizes = dataSizes;
        this.auxAddresses = auxAddresses;
        this.auxSizes = auxSizes;
        this.validityAddresses = validityAddresses;
        this.validityBitOffsets = validityBitOffsets;
        this.nullCounts = nullCounts;
        this.nullPolicies = nullPolicies;
        this.columnOffset = columnOffset;
        this.columnCount = columnCount;
        return this;
    }
}
