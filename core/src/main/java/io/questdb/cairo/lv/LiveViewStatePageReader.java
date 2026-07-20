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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import org.jetbrains.annotations.NotNull;

/**
 * Strict bounded view of one immutable function-state page. Every primitive
 * read is checked against the page reference before the backing memory is
 * touched, so a malformed decoder cannot consume an adjacent partition, page,
 * block, or file trailer.
 */
public final class LiveViewStatePageReader {

    private long pageLength;
    private long pageStart;
    private MemoryR source;

    public boolean getBool(long offset) {
        boundsCheck(offset, Byte.BYTES);
        return source.getBool(pageStart + offset);
    }

    public byte getByte(long offset) {
        boundsCheck(offset, Byte.BYTES);
        return source.getByte(pageStart + offset);
    }

    public void getDecimal128(long offset, @NotNull Decimal128 sink) {
        boundsCheck(offset, Decimal128.BYTES);
        source.getDecimal128(pageStart + offset, sink);
    }

    public void getDecimal256(long offset, @NotNull Decimal256 sink) {
        boundsCheck(offset, Decimal256.BYTES);
        source.getDecimal256(pageStart + offset, sink);
    }

    public double getDouble(long offset) {
        boundsCheck(offset, Double.BYTES);
        return source.getDouble(pageStart + offset);
    }

    public int getInt(long offset) {
        boundsCheck(offset, Integer.BYTES);
        return source.getInt(pageStart + offset);
    }

    public long getLong(long offset) {
        boundsCheck(offset, Long.BYTES);
        return source.getLong(pageStart + offset);
    }

    public short getShort(long offset) {
        boundsCheck(offset, Short.BYTES);
        return source.getShort(pageStart + offset);
    }

    public boolean isOpen() {
        return source != null;
    }

    /** Opens an exact page slice after overflow-safe source bounds checks. */
    public LiveViewStatePageReader of(@NotNull MemoryR source, long pageStart, long pageLength) {
        if (pageStart < 0 || pageLength < 0 || pageStart > source.size() || pageLength > source.size() - pageStart) {
            throw invalid("state page reference out of bounds")
                    .put(" [offset=").put(pageStart)
                    .put(", length=").put(pageLength)
                    .put(", sourceSize=").put(source.size()).put(']');
        }
        this.source = source;
        this.pageStart = pageStart;
        this.pageLength = pageLength;
        return this;
    }

    public long size() {
        ensureOpen();
        return pageLength;
    }

    private static CairoException invalid(CharSequence message) {
        return CairoException.critical(0).put("live view checkpoint ").put(message);
    }

    private void boundsCheck(long offset, long bytes) {
        ensureOpen();
        if (offset < 0 || bytes < 0 || offset > pageLength || bytes > pageLength - offset) {
            throw invalid("state page read out of bounds")
                    .put(" [offset=").put(offset)
                    .put(", read=").put(bytes)
                    .put(", pageLength=").put(pageLength).put(']');
        }
    }

    private void ensureOpen() {
        if (source == null) {
            throw invalid("state page reader is not open");
        }
    }
}
