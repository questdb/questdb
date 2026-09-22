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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.CompressedOffsets;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Rows of a hash join build: the lookup structure holds keys, this holds the rows those keys
 * point at. {@link IntHashJoinBuild} keys it with an open-addressed INT table and
 * {@link MapHashJoinBuild} with a {@link io.questdb.cairo.map.Map}; both store the same
 * compressed offset of a chain head and share the row layout below.
 * <p>
 * A row is an eight-byte previous-match link (a byte offset plus eight, zero for a chain end)
 * followed by the build row's id, which a probe hands to its {@link HashJoinPayloadSource.Reader}
 * to read the payload columns where they live, as the light hash join does. A build that needs no
 * payload column stores the link alone. The heap is bounded by
 * {@link CompressedOffsets#MAX_ALIGNED8_HEAP_SIZE} before allocation or encoding, so every row
 * offset round trips through {@link CompressedOffsets#compressBiased8(long)}. Duplicate iteration
 * follows the links in reverse input order, as the light join's LongChain does.
 * <p>
 * The heap is owner-built and frozen for the execution. {@link #freeze()} publishes it and
 * hands out the generation that every probe asserts against, so that a probe of an expired
 * execution faults instead of reading a freed or re-filled row.
 */
final class HashJoinRowHeap implements Closeable {
    private static final int LINK_SIZE = Long.BYTES;
    private static final int ROW_ID_ROW_SIZE = LINK_SIZE + Long.BYTES;
    private final boolean hasRowId;
    private final long initialCapacity;
    private final HashJoinBuffer rows = new HashJoinBuffer(CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE);
    private final int rowSize;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long generation;
    private long nextHandleBase;
    private long rowBytes;

    /** A heap that stores row ids when the build has payload columns, and links alone otherwise. */
    HashJoinRowHeap(boolean hasPayload, long initialCapacity) {
        if (initialCapacity < 1 || initialCapacity > CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE) {
            throw new IllegalArgumentException("invalid hash join build capacity");
        }
        this.initialCapacity = initialCapacity;
        this.hasRowId = hasPayload;
        this.rowSize = getRowSize(hasPayload);
    }

    /**
     * Bytes of one row, so that a planner can bound a build's heap by its row count before
     * choosing to build. The payload's width does not matter: a row stores its id, not its columns.
     */
    static int getRowSize(boolean hasPayload) {
        return hasPayload ? ROW_ID_ROW_SIZE : LINK_SIZE;
    }

    /**
     * Appends one row and returns its byte offset, which the caller compresses into the key's
     * chain head. {@code link} is the row link of the match this row displaces: zero for a new
     * key, otherwise the previous head widened through
     * {@link CompressedOffsets#uncompressAligned8(int)}. A heap without row ids ignores the id.
     */
    long append(long rowId, long link) {
        final long offset = rowBytes;
        final long required = offset + rowSize;
        rows.ensure(required, initialCapacity);
        final long address = rows.address + offset;
        Unsafe.putLong(address, link);
        if (hasRowId) {
            Unsafe.putLong(address + LINK_SIZE, rowId);
        }
        rowBytes = required;
        return offset;
    }

    /** Releases the rows and expires every probe of this execution. */
    @Override
    public void close() {
        rows.close();
        rowBytes = 0;
        generation++;
        circuitBreaker = null;
    }

    /** Ends mutation and returns the generation that this execution's probes assert against. */
    long freeze() {
        return ++generation;
    }

    long getAddress() {
        return rows.address;
    }

    long getGeneration() {
        return generation;
    }

    long getRowCount() {
        return rowBytes / rowSize;
    }

    int getRowSize() {
        return rowSize;
    }

    /** Allocated native bytes, including unused capacity. */
    long getSizeInBytes() {
        return rows.capacity;
    }

    /** True when rows carry the id of their build row, which a payload reader positions at. */
    boolean hasRowId() {
        return hasRowId;
    }

    /** Takes the handle base of one execution, so that handles never repeat across executions. */
    long nextHandleBase() {
        if (nextHandleBase > Long.MAX_VALUE - rowBytes - 1) {
            throw CairoException.nonCritical().put("hash join handle capacity overflow");
        }
        final long handleBase = nextHandleBase;
        nextHandleBase += rowBytes + 1;
        return handleBase;
    }

    /** Binds the execution that charges and cancels this heap's allocations. */
    void of(@Nullable MemoryTracker memoryTracker, SqlExecutionCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        rows.of(memoryTracker, circuitBreaker);
    }

    /** Presizes the heap for a known row count, so that appends of that many rows do not grow it. */
    void reserve(long rowCount) {
        circuitBreaker.statefulThrowExceptionIfTrippedNoThrottle();
        if (rowCount > (CompressedOffsets.MAX_ALIGNED8_HEAP_SIZE - rowBytes) / rowSize) {
            throw CairoException.nonCritical().put("hash join build buffer overflow");
        }
        rows.ensure(rowBytes + rowCount * rowSize, initialCapacity);
    }

    /** The build row id of the row at this address; only a heap with row ids stores one. */
    static long getRowId(long rowAddress) {
        return Unsafe.getLong(rowAddress + LINK_SIZE);
    }
}
