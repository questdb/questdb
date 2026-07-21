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

package io.questdb.test.cairo.idx;

import io.questdb.PropertyKey;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Tests the adaptive posting-index encoder threshold (see
 * {@link io.questdb.cairo.CairoConfiguration#getPostingIndexAdaptiveDeltaAtOrAbove()}).
 * Default 2000: keys with at least that many row IDs skip the EF trial and
 * encode as DELTA directly. Below the threshold the encoder runs the
 * size-only EF-vs-DELTA race exactly as before.
 */
public class PostingIndexAdaptiveDeltaThresholdTest extends AbstractCairoTest {

    private static final int BUF_SIZE = 1 << 20; // 1 MiB scratch per buffer
    private long adaptiveDst;
    private long deltaDst;
    private long efDst;
    private PostingIndexUtils.EncodeContext encodeCtx;
    private long src;

    @Before
    public void setUpBuffers() {
        encodeCtx = new PostingIndexUtils.EncodeContext();
        adaptiveDst = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        efDst = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        deltaDst = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        src = Unsafe.malloc((long) 200_000 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
    }

    @After
    public void tearDownBuffers() {
        encodeCtx.close();
        Unsafe.free(adaptiveDst, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(efDst, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(deltaDst, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
        Unsafe.free(src, (long) 200_000 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
    }

    @Test
    public void testCairoConfigurationDefaultIs2000() {
        DefaultCairoConfiguration cfg = new DefaultCairoConfiguration(temp.getRoot().getAbsolutePath());
        Assert.assertEquals(2000, cfg.getPostingIndexAdaptiveDeltaAtOrAbove());
    }

    @Test
    public void testEncodeContextSetterClampsNonPositive() {
        encodeCtx.setAdaptiveDeltaAtOrAbove(0);
        // Below-threshold key gets size race (i.e. behaves like MAX_VALUE).
        // 100 row IDs is below any plausible production threshold and
        // (deliberately) below the BLOCK_CAPACITY-based DELTA fast path
        // boundary, so EF wins on size for dense data.
        int count = 100;
        fillDenseRowIds(count);
        encodeCtx.ensureCapacity(count);
        int adaptiveSize = PostingIndexUtils.encodeKeyNative(src, count, adaptiveDst, encodeCtx, PostingIndexUtils.ENCODING_ADAPTIVE);
        int efSize = PostingIndexUtils.encodeKeyNative(src, count, efDst, encodeCtx, PostingIndexUtils.ENCODING_EF);
        int deltaSize = PostingIndexUtils.encodeKeyNative(src, count, deltaDst, encodeCtx, PostingIndexUtils.ENCODING_DELTA);
        // Adaptive runs the size race
        Assert.assertEquals(Math.min(efSize, deltaSize), adaptiveSize);
    }

    @Test
    public void testEncoderForcesDeltaAtOrAboveThreshold() {
        encodeCtx.setAdaptiveDeltaAtOrAbove(500);
        int count = 500;
        fillDenseRowIds(count);
        encodeCtx.ensureCapacity(count);
        int adaptiveSize = PostingIndexUtils.encodeKeyNative(src, count, adaptiveDst, encodeCtx, PostingIndexUtils.ENCODING_ADAPTIVE);
        int deltaSize = PostingIndexUtils.encodeKeyNative(src, count, deltaDst, encodeCtx, PostingIndexUtils.ENCODING_DELTA);
        Assert.assertEquals("count==threshold must produce DELTA-encoded output", deltaSize, adaptiveSize);
        assertBytesEqual(adaptiveDst, deltaDst, deltaSize);
    }

    @Test
    public void testEncoderRunsSizeRaceBelowThreshold() {
        encodeCtx.setAdaptiveDeltaAtOrAbove(1000);
        int count = 500;
        fillDenseRowIds(count);
        encodeCtx.ensureCapacity(count);
        int adaptiveSize = PostingIndexUtils.encodeKeyNative(src, count, adaptiveDst, encodeCtx, PostingIndexUtils.ENCODING_ADAPTIVE);
        int efSize = PostingIndexUtils.encodeKeyNative(src, count, efDst, encodeCtx, PostingIndexUtils.ENCODING_EF);
        int deltaSize = PostingIndexUtils.encodeKeyNative(src, count, deltaDst, encodeCtx, PostingIndexUtils.ENCODING_DELTA);
        // Below threshold: adaptive picks the smaller of EF / DELTA
        Assert.assertEquals(Math.min(efSize, deltaSize), adaptiveSize);
    }

    @Test
    public void testNewContextDefaultsToMaxValue() {
        // EncodeContext built without a CairoConfiguration (e.g. tests) keeps
        // the prior pure-size-race behaviour at any count.
        PostingIndexUtils.EncodeContext freshCtx = new PostingIndexUtils.EncodeContext();
        try {
            int count = 5_000;
            fillDenseRowIds(count);
            freshCtx.ensureCapacity(count);
            int adaptiveSize = PostingIndexUtils.encodeKeyNative(src, count, adaptiveDst, freshCtx, PostingIndexUtils.ENCODING_ADAPTIVE);
            int efSize = PostingIndexUtils.encodeKeyNative(src, count, efDst, freshCtx, PostingIndexUtils.ENCODING_EF);
            int deltaSize = PostingIndexUtils.encodeKeyNative(src, count, deltaDst, freshCtx, PostingIndexUtils.ENCODING_DELTA);
            Assert.assertEquals("MAX_VALUE default must run pure size race", Math.min(efSize, deltaSize), adaptiveSize);
        } finally {
            freshCtx.close();
        }
    }

    @Test
    public void testWriterAppliesConfiguredThreshold() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_ADAPTIVE_DELTA_AT_OR_ABOVE, 137);
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(root)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "t", COLUMN_NAME_TXN_NONE)) {
                    Assert.assertEquals(137, writer.getAdaptiveDeltaAtOrAbove());
                }
            }
        });
    }

    @Test
    public void testWriterDefaultsTo2000() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(root)) {
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "t", COLUMN_NAME_TXN_NONE)) {
                    Assert.assertEquals(2000, writer.getAdaptiveDeltaAtOrAbove());
                }
            }
        });
    }

    private static void assertBytesEqual(long aAddr, long bAddr, int len) {
        for (int i = 0; i < len; i++) {
            byte a = Unsafe.getByte(aAddr + i);
            byte b = Unsafe.getByte(bAddr + i);
            if (a != b) {
                Assert.fail("byte mismatch at offset " + i + ": " + (a & 0xff) + " vs " + (b & 0xff));
            }
        }
    }

    /**
     * Generates {@code count} strictly increasing row IDs with a uniform-ish
     * delta pattern (small deltas in the BLOCK_CAPACITY range). Dense enough
     * for both EF and DELTA to encode it well so the size race is non-trivial.
     */
    private void fillDenseRowIds(int count) {
        long v = 0;
        for (int i = 0; i < count; i++) {
            // Mix of small deltas: 1, 2, 3, ... 7, 1, 2, ... — keeps deltas
            // dense and avoids accidentally producing a trivial layout.
            v += 1 + (i & 7);
            Unsafe.putLong(src + (long) i * Long.BYTES, v);
        }
    }
}
