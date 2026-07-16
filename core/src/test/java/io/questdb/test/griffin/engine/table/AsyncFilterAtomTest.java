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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class AsyncFilterAtomTest extends AbstractCairoTest {
    private static final int WORKER_COUNT = 2;

    @Test
    public void testForeignOwnerReadsButDoesNotWriteOwnerStatistics() {
        final AsyncFilterAtom atom = newAtom();

        atom.updateSelectivityStats(-1, -1, true, 100, 100);
        atom.updateSelectivityStats(-1, -1, true, 100, 100);
        // Owner and foreign stealers alike consult the owner bucket, rather than defaulting to
        // the first-sample choice on every stolen frame.
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, -1, true, false));

        final AsyncFilterAtom freshAtom = newAtom();
        freshAtom.updateSelectivityStats(-1, -1, false, 100, 100);
        freshAtom.updateSelectivityStats(-1, -1, false, 100, 100);
        // Foreign stealers share no mutable state: the owner bucket is still unsampled.
        Assert.assertTrue(freshAtom.shouldUseLateMaterialization(-1, -1, true, false));
    }

    @Test
    public void testOutOfRangeWorkerIdDoesNotPolluteOwnerStatistics() {
        final AsyncFilterAtom atom = newAtom();

        // The reducing pool can hand out worker ids the atom was not sized for. A worker id of
        // exactly workerCount lands on the owner bucket unless it is folded into range.
        atom.updateSelectivityStats(-1, WORKER_COUNT, false, 100, 100);
        atom.updateSelectivityStats(-1, WORKER_COUNT, false, 100, 100);

        // The owner bucket must still be on its first-sample choice.
        Assert.assertTrue(atom.shouldUseLateMaterialization(-1, -1, true, false));
    }

    @Test
    public void testPerWorkerFiltersKeepOwnerStatisticsReachable() {
        final AsyncFilterAtom atom = newAtomWithPerWorkerFilters();

        // maybeAcquireFilter() returns -1 for the owner even when it hands out slots, so the owner
        // still reads the thread-safe stats. Every other thread holds a slot and reads its own
        // per-worker bucket.
        atom.updateSelectivityStats(-1, -1, true, 100, 100);
        atom.updateSelectivityStats(-1, -1, true, 100, 100);
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, -1, true, false));

        // A slot holder is unaffected by what the owner sampled.
        Assert.assertTrue(atom.shouldUseLateMaterialization(0, 0, true, false));
    }

    @Test
    public void testThreadSafeWorkerStatisticsAreIndependent() {
        final AsyncFilterAtom atom = newAtom();

        atom.updateSelectivityStats(-1, 0, false, 100, 100);
        atom.updateSelectivityStats(-1, 0, false, 100, 100);
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, 0, true, false));
        Assert.assertTrue(atom.shouldUseLateMaterialization(-1, 1, true, false));

        atom.updateSelectivityStats(-1, 1, false, 0, 100);
        atom.updateSelectivityStats(-1, 1, false, 0, 100);
        Assert.assertTrue(atom.shouldUseLateMaterialization(-1, 1, true, false));
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, 0, true, false));
    }

    @Test
    public void testWorkerIdBeyondWorkerCountStaysInBounds() {
        final AsyncFilterAtom atom = newAtom();

        // PageFrameReduceJob passes the shared reducing pool's carrier id, which is unrelated to
        // the atom's compile-time workerCount and can exceed it by any margin.
        atom.updateSelectivityStats(-1, WORKER_COUNT + 5, false, 0, 100);
        atom.updateSelectivityStats(-1, WORKER_COUNT + 5, false, 0, 100);
        Assert.assertTrue(atom.shouldUseLateMaterialization(-1, WORKER_COUNT + 5, true, false));

        atom.updateSelectivityStats(-1, WORKER_COUNT + 64, false, 100, 100);
        atom.updateSelectivityStats(-1, WORKER_COUNT + 64, false, 100, 100);
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, WORKER_COUNT + 64, true, false));
    }

    private AsyncFilterAtom newAtom() {
        return newAtom(null);
    }

    private AsyncFilterAtom newAtom(ObjList<Function> perWorkerFilters) {
        final IntHashSet filterColumns = new IntHashSet();
        filterColumns.add(0);
        final IntList columnTypes = new IntList();
        columnTypes.add(0);
        return new AsyncFilterAtom(
                configuration,
                BooleanConstant.TRUE,
                filterColumns,
                perWorkerFilters,
                columnTypes,
                false,
                WORKER_COUNT
        );
    }

    private AsyncFilterAtom newAtomWithPerWorkerFilters() {
        final ObjList<Function> perWorkerFilters = new ObjList<>();
        for (int i = 0; i < WORKER_COUNT; i++) {
            perWorkerFilters.add(BooleanConstant.TRUE);
        }
        return newAtom(perWorkerFilters);
    }
}
