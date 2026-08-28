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

import io.questdb.cairo.idx.BitpackUtils;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link BitpackUtils#packAllValues} is the inverse of three readers that were
 * already here, so it is asserted against all three rather than against a
 * hand-written byte pattern. A packer that disagrees with the layout does not
 * fail -- it produces values that decode as plausible garbage -- so a round trip
 * is the only assertion that catches it.
 */
public class BitpackUtilsPackTest extends AbstractCairoTest {

    @Test
    public void testPackRoundTripsThroughEveryUnpacker() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(0x5eed, 0x5eed);
            // Every width, including the byte-aligned ones the AVX2 path takes
            // and the awkward ones that straddle byte boundaries.
            for (int bitWidth = 1; bitWidth <= 64; bitWidth++) {
                final int count = 1 + rnd.nextInt(300);
                final long minValue = bitWidth > 40 ? 0 : rnd.nextLong(1000);
                final long span = bitWidth == 64 ? Long.MAX_VALUE : (1L << bitWidth) - 1;

                final long srcSize = (long) count * Long.BYTES;
                final long src = Unsafe.malloc(srcSize, MemoryTag.NATIVE_DEFAULT);
                final int packedSize = BitpackUtils.packedDataSize(count, bitWidth);
                final long packed = Unsafe.calloc(packedSize, MemoryTag.NATIVE_DEFAULT);
                final long dst = Unsafe.malloc(srcSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < count; i++) {
                        final long delta = span == Long.MAX_VALUE ? rnd.nextLong(Long.MAX_VALUE) : rnd.nextLong(span + 1);
                        Unsafe.getUnsafe().putLong(src + ((long) i << 3), minValue + delta);
                    }

                    BitpackUtils.packAllValues(src, count, bitWidth, minValue, packed);

                    // 1. bulk unpack
                    BitpackUtils.unpackAllValues(packed, count, bitWidth, minValue, dst);
                    for (int i = 0; i < count; i++) {
                        Assert.assertEquals(
                                "unpackAllValues mismatch [bitWidth=" + bitWidth + ", i=" + i + ']',
                                Unsafe.getUnsafe().getLong(src + ((long) i << 3)),
                                Unsafe.getUnsafe().getLong(dst + ((long) i << 3))
                        );
                    }

                    // 2. random access, which is what arm B actually needs
                    for (int probe = 0; probe < Math.min(count, 16); probe++) {
                        final int i = rnd.nextInt(count);
                        Assert.assertEquals(
                                "unpackValue mismatch [bitWidth=" + bitWidth + ", i=" + i + ']',
                                Unsafe.getUnsafe().getLong(src + ((long) i << 3)),
                                BitpackUtils.unpackValue(packed, i, bitWidth, minValue)
                        );
                    }

                    // 3. unpack from an arbitrary offset -- the AVX2 path
                    if (count > 1) {
                        final int start = rnd.nextInt(count - 1);
                        final int len = count - start;
                        BitpackUtils.unpackValuesFrom(packed, start, len, bitWidth, minValue, dst);
                        for (int i = 0; i < len; i++) {
                            Assert.assertEquals(
                                    "unpackValuesFrom mismatch [bitWidth=" + bitWidth + ", start=" + start + ", i=" + i + ']',
                                    Unsafe.getUnsafe().getLong(src + ((long) (start + i) << 3)),
                                    Unsafe.getUnsafe().getLong(dst + ((long) i << 3))
                            );
                        }
                    }
                } finally {
                    Unsafe.free(src, srcSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(packed, packedSize, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(dst, srcSize, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }
}
