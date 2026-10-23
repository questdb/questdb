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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewCheckpointRingStateSource;
import io.questdb.std.LongList;
import org.jetbrains.annotations.NotNull;

/**
 * Injects a corrupt or hand-built persisted ring of one double per row behind a two-word
 * scalar straight into the restore path. It stands in for the chunk reader, not for a peer:
 * the rows it replays are ones a correct seal would never write, which is the point.
 */
final class LiveViewCheckpointStubDoubleRingSource implements LiveViewCheckpointRingStateSource {
    final LongList rows = new LongList();
    long frameSize;
    long scalarWord0;
    long scalarWord1;
    private long rowCount;

    @Override
    public void forEachRow(@NotNull RowConsumer consumer) {
        for (int i = 0, n = rows.size(); i < n; i += 2) {
            consumer.accept(rows.getQuick(i), rows.getQuick(i + 1));
        }
    }

    @Override
    public void forEachRow(@NotNull Decimal128RowConsumer consumer) {
        throw new AssertionError("a double ring replays one-word rows");
    }

    @Override
    public void forEachRow(@NotNull Decimal256RowConsumer consumer) {
        throw new AssertionError("a double ring replays one-word rows");
    }

    @Override
    public void forEachTimestamp(@NotNull TimestampConsumer consumer) {
        throw new AssertionError("a double ring replays one-word rows");
    }

    @Override
    public long getFrameSize() {
        return frameSize;
    }

    @Override
    public long getRowCount() {
        return rowCount;
    }

    @Override
    public long getScalarBits() {
        return scalarWord0;
    }

    @Override
    public long getScalarWord(int index) {
        return switch (index) {
            case 0 -> scalarWord0;
            case 1 -> scalarWord1;
            default -> throw new AssertionError("a double ring carries two scalar words, asked for " + index);
        };
    }

    @Override
    public int getScalarWordCount() {
        return 2;
    }

    // Resets the ring to no rows under the given declared row count, frame size and the
    // family's two scalar words - (sum, sumSq) for stddev, (sum, compensation) for ksum - which
    // the stub carries as raw bits.
    LiveViewCheckpointStubDoubleRingSource of(long rowCount, long frameSize, double scalar0, double scalar1) {
        this.rowCount = rowCount;
        this.frameSize = frameSize;
        this.scalarWord0 = Double.doubleToRawLongBits(scalar0);
        this.scalarWord1 = Double.doubleToRawLongBits(scalar1);
        rows.clear();
        return this;
    }

    LiveViewCheckpointStubDoubleRingSource row(long timestamp, double value) {
        rows.add(timestamp);
        rows.add(Double.doubleToRawLongBits(value));
        return this;
    }
}
