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

package io.questdb.test.cairo;

import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.idx.SplitBlockBloomFilter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Reproduces the gen-lookup cache poisoning in {@link PostingIndexBwdReader}.
 * <p>
 * The backward reader caches, per key, the (gen, posInGen) entries a full walk
 * resolves, then replays them on the next lookup of the same key. The cache-add
 * guard during the build walk fires on {@code isEFMode}; the early-return paths
 * of {@code loadSparseGenByPrefixSum} reset {@code isFlatMode} but NOT
 * {@code isEFMode}. So when the walk visits an EF-encoded gen (sets isEFMode)
 * and then a lower gen that does not hold the key (its prefix sum reports the
 * key absent, but its SBBF false-positives so the gen is not skipped early),
 * the stale isEFMode makes the guard cache a bogus (lowerGen, stale posInGen)
 * entry. Replaying that entry reads the lower gen's wrong per-key posting list,
 * surfacing rows that do not belong to the key (or, when the stale ordinal is
 * out of range for the lower gen, an out-of-bounds read -> SIGSEGV).
 * <p>
 * This test exercises the assertable wrong-rows variant: the second (cache
 * replay) cursor must return exactly the rows the first (cache build) cursor
 * returned.
 */
public class PostingIndexBwdCachePoisonTest extends AbstractCairoTest {

    private static final int PRESENT_KEY_COUNT = 128;
    private static final int PRESENT_KEY_STEP = 16;

    @Test
    public void testReplayNotPoisonedByLeakedEfMode() throws Exception {
        assertMemoryLeak(() -> {
            // gen 0 (lower gen): a wide, gapped key set, none of which is the
            // query key. Find a query key in the gen's key range that the gen's
            // per-gen SBBF false-positives, so the reader does not skip gen 0 on
            // the bloom check and instead consults the prefix sum (which reports
            // the key absent). The SBBF is replicated with the same public API
            // the writer uses, so the false positive is exact and deterministic.
            final int minKey = 0;
            final int maxKey = (PRESENT_KEY_COUNT - 1) * PRESENT_KEY_STEP; // 2032
            final int queryKey = findBloomFalsePositiveKey(minKey, maxKey);
            Assert.assertTrue(
                    "could not find an SBBF false-positive key for the gen-0 key set",
                    queryKey > minKey && queryKey < maxKey && queryKey % PRESENT_KEY_STEP != 0
            );

            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();

                // Force Elias-Fano directly: the unit-test CairoConfiguration
                // ignores the row-id-encoding property (it returns the ADAPTIVE
                // default), so set the encoding on the writer. Mirror the
                // TestOnly convenience constructor's open + zero-txnAtSeal.
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, PostingIndexUtils.ENCODING_EF)) {
                    writer.of(path, "bp", COLUMN_NAME_TXN_NONE, true);
                    writer.setNextTxnAtSeal(0L);
                    // gen 0: present keys, two row ids each; queryKey absent.
                    // keyCount becomes maxKey + 1, so getCursor accepts queryKey.
                    for (int i = 0; i < PRESENT_KEY_COUNT; i++) {
                        int k = i * PRESENT_KEY_STEP;
                        writer.add(k, 1000 + 2L * i);
                        writer.add(k, 1000 + 2L * i + 1);
                    }
                    writer.commit();

                    // gen 1 (higher gen, visited first by the backward walk):
                    // ONLY queryKey, so its active-key ordinal (cached posInGen)
                    // is 0. EF-encoded -> sets isEFMode as the cursor walks it.
                    // Row ids above gen 0's keep per-key lists sorted.
                    writer.add(queryKey, 2000);
                    writer.add(queryKey, 2001);
                    writer.add(queryKey, 2002);
                    writer.commit();
                    // close() trims to the chain region but does NOT seal, so the
                    // reader sees two sparse gens.
                }

                try (PostingIndexBwdReader reader = new PostingIndexBwdReader(
                        configuration, path.trimTo(plen), "bp", COLUMN_NAME_TXN_NONE, -1, 0, null, null, 0)
                ) {
                    // Pass 1: cache build. queryKey lives only in gen 1.
                    LongList firstPass = drain(reader.getCursor(queryKey, 0, Long.MAX_VALUE));
                    Assert.assertEquals(
                            "first pass must return only gen-1 rows for the query key",
                            "[2002,2001,2000]",
                            toStr(firstPass)
                    );

                    // Pass 2: cache replay. Must return the same rows. With the
                    // isEFMode leak the cache holds a bogus (gen 0, 0) entry and
                    // the replay also emits gen 0's first key's posting list.
                    LongList secondPass = drain(reader.getCursor(queryKey, 0, Long.MAX_VALUE));
                    Assert.assertEquals(
                            "cache replay must return the same rows as the cache build",
                            toStr(firstPass),
                            toStr(secondPass)
                    );
                }
            }
        });
    }

    private static LongList drain(RowCursor cursor) {
        LongList out = new LongList();
        try {
            while (cursor.hasNext()) {
                out.add(cursor.next());
            }
        } finally {
            Misc.free(cursor);
        }
        return out;
    }

    private static int findBloomFalsePositiveKey(int minKey, int maxKey) {
        int size = SplitBlockBloomFilter.computeSize(PRESENT_KEY_COUNT, PostingIndexUtils.SPARSE_SBBF_DEFAULT_FPP);
        long addr = SplitBlockBloomFilter.allocate(size);
        try {
            Unsafe.setMemory(addr, size, (byte) 0);
            for (int i = 0; i < PRESENT_KEY_COUNT; i++) {
                SplitBlockBloomFilter.insert(addr, size, SplitBlockBloomFilter.hashKey(i * PRESENT_KEY_STEP));
            }
            for (int k = minKey + 1; k < maxKey; k++) {
                if (k % PRESENT_KEY_STEP == 0) {
                    continue; // a present key
                }
                if (SplitBlockBloomFilter.mightContain(addr, size, SplitBlockBloomFilter.hashKey(k))) {
                    return k;
                }
            }
        } finally {
            SplitBlockBloomFilter.free(addr, size);
        }
        return -1;
    }

    private static String toStr(LongList list) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0, n = list.size(); i < n; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(list.getQuick(i));
        }
        sb.append(']');
        return sb.toString();
    }
}
