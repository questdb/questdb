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

import io.questdb.cairo.lv.LiveViewCheckpointRingStateSink;
import io.questdb.std.LongList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

/**
 * Records what freezeCheckpointRingState streams for a ring of one double per row behind a
 * two-word scalar, for comparison with the {@link LiveViewCheckpointStubDoubleRingSource} a
 * restore read.
 */
final class LiveViewCheckpointCapturingDoubleRingSink implements LiveViewCheckpointRingStateSink {
    private final LongList rows = new LongList();
    private long frameSize = -1;
    private long scalarWord0;
    private long scalarWord1;
    private int scalarWords;

    @Override
    public void putRow(long timestamp) {
        throw new AssertionError("a double ring streams one-word rows");
    }

    @Override
    public void putRow(long timestamp, long valueBits) {
        rows.add(timestamp);
        rows.add(valueBits);
    }

    @Override
    public void putRow(long timestamp, long hi, long lo) {
        throw new AssertionError("a double ring streams one-word rows");
    }

    @Override
    public void putRow(long timestamp, long hh, long hl, long lh, long ll) {
        throw new AssertionError("a double ring streams one-word rows");
    }

    @Override
    public void putScalarState(long scalarBits, long frameSize) {
        throw new AssertionError("a double ring carries a two-word scalar");
    }

    @Override
    public void putScalarState(long hi, long lo, long frameSize) {
        Assert.assertEquals("one scalar per partition", 0, scalarWords);
        Assert.assertEquals("the scalar precedes the rows", 0, rows.size());
        this.scalarWords = 2;
        this.scalarWord0 = hi;
        this.scalarWord1 = lo;
        this.frameSize = frameSize;
    }

    @Override
    public void putScalarState(long hh, long hl, long lh, long ll, long frameSize) {
        throw new AssertionError("a double ring carries a two-word scalar");
    }

    @Override
    public void putScalarState(long word0, long word1, long word2, long word3, long word4, long frameSize) {
        throw new AssertionError("a double ring carries a two-word scalar");
    }

    void assertCaptured(LiveViewCheckpointStubDoubleRingSource source) {
        Assert.assertEquals(2, scalarWords);
        Assert.assertEquals("scalar word 0", source.scalarWord0, scalarWord0);
        Assert.assertEquals("scalar word 1", source.scalarWord1, scalarWord1);
        Assert.assertEquals("frame size", source.frameSize, frameSize);
        TestUtils.assertEquals(source.rows, rows);
    }

    void clear() {
        rows.clear();
        frameSize = -1;
        scalarWord0 = 0;
        scalarWord1 = 0;
        scalarWords = 0;
    }
}
