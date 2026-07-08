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

import io.questdb.cairo.idx.PostingGenLookup;
import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Deterministic boundary coverage for the relaxed per-gen seal guard in
 * {@link PostingGenLookup#snapshotMetadata}. The assertion tolerates a single-txn WAL fast-lag dip
 * (a later gen's TXN_AT_SEAL landing exactly one txn below gen 0's) but must still trip on a larger
 * regression (a recycled / genuinely-too-old gen slot) or a negative seal. The multi-column reconcile
 * fuzz exercises the tolerated dip only probabilistically; these cases pin both arms directly.
 */
public class PostingGenLookupSealFloorTest extends AbstractCairoTest {

    private static final long ENTRY_OFFSET = 0;

    @Test
    public void testExactFloorDipPasses() throws Exception {
        // gen 1 sits exactly one txn below gen 0 - the documented fast-lag offset - so it is on the
        // floor and must be accepted, and its value captured verbatim.
        assertMemoryLeak(() -> {
            try (Path keyPath = new Path().of(configuration.getDbRoot()).concat("seal_pass.k");
                 MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);
                writeGenSeals(keyMem, 100L, 99L);

                final PostingGenLookup lookup = new PostingGenLookup();
                try {
                    lookup.snapshotMetadata(keyMem, 2, ENTRY_OFFSET);
                    lookup.commitSnapshot();
                    Assert.assertEquals(100L, lookup.getGenTxnAtSeal(0));
                    Assert.assertEquals(99L, lookup.getGenTxnAtSeal(1));
                } finally {
                    lookup.close();
                }
            }
        });
    }

    @Test
    public void testNegativeSealTrips() throws Exception {
        Assume.assumeTrue("assertion-only guard requires -ea", PostingGenLookup.class.desiredAssertionStatus());
        // A negative seal is never a valid table txn and must trip regardless of the floor.
        assertMemoryLeak(() -> {
            try (Path keyPath = new Path().of(configuration.getDbRoot()).concat("seal_negative.k");
                 MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);
                writeGenSeals(keyMem, 100L, -1L);

                final PostingGenLookup lookup = new PostingGenLookup();
                try {
                    lookup.snapshotMetadata(keyMem, 2, ENTRY_OFFSET);
                    Assert.fail("expected AssertionError for a negative gen seal");
                } catch (AssertionError expected) {
                    Assert.assertTrue(expected.getMessage().contains("posting gen seal below entry floor"));
                } finally {
                    lookup.close();
                }
            }
        });
    }

    @Test
    public void testRegressionBelowFloorTrips() throws Exception {
        Assume.assumeTrue("assertion-only guard requires -ea", PostingGenLookup.class.desiredAssertionStatus());
        // gen 1 sits two txns below gen 0 - deeper than the single-txn tolerance - so it must trip as a
        // recycled / too-old gen slot, the corruption the original strict guard protected against.
        assertMemoryLeak(() -> {
            try (Path keyPath = new Path().of(configuration.getDbRoot()).concat("seal_regress.k");
                 MemoryCMARW keyMem = Vm.getCMARWInstance()) {
                keyMem.smallFile(configuration.getFilesFacade(), keyPath.$(), MemoryTag.MMAP_DEFAULT);
                writeGenSeals(keyMem, 100L, 98L);

                final PostingGenLookup lookup = new PostingGenLookup();
                try {
                    lookup.snapshotMetadata(keyMem, 2, ENTRY_OFFSET);
                    Assert.fail("expected AssertionError for a >1-txn seal regression");
                } catch (AssertionError expected) {
                    Assert.assertTrue(expected.getMessage().contains("posting gen seal below entry floor"));
                } finally {
                    lookup.close();
                }
            }
        });
    }

    // Writes one V2 gen-dir entry per supplied seal at ENTRY_OFFSET, setting TXN_AT_SEAL (the field the
    // guard reads) and MAX_VALUE (the highest field, so the whole entry is mapped for snapshotMetadata's
    // per-gen reads); all other fields are left zero.
    private static void writeGenSeals(MemoryCMARW keyMem, long... seals) {
        for (int i = 0; i < seals.length; i++) {
            final long genOffset = PostingIndexChainEntry.resolveGenDirOffset(ENTRY_OFFSET, i);
            keyMem.putLong(genOffset + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, seals[i]);
            keyMem.putLong(genOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_VALUE, 0L);
        }
    }
}
