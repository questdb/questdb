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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class AsyncFilterAtomTest extends AbstractCairoTest {
    private static final int SLOT_COUNT = 2;

    @Test
    public void testPerWorkerFiltersUseSlotStatistics() {
        final AsyncFilterAtom atom = newAtomWithPerWorkerFilters();

        // maybeAcquireFilter() returns -1 for the owner even when it hands out slots, so the owner
        // reads the shared stats. Every other thread holds a slot and reads its own.
        atom.getSelectivityStats(-1).update(100, 100);
        atom.getSelectivityStats(-1).update(100, 100);
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, true, false));

        // A slot holder is unaffected by what the owner sampled.
        Assert.assertTrue(atom.shouldUseLateMaterialization(0, true, false));
    }

    @Test
    public void testThreadSafeFilterSharesOneSelectivityEma() {
        final AsyncFilterAtom atom = newAtom();

        // A thread-safe filter builds no locks, so maybeAcquireFilter() hands -1 to the owner, to
        // every reducing worker and to every work-stealing thread alike: no caller on this path
        // has a slot to key statistics on.
        Assert.assertEquals(-1, atom.maybeAcquireFilter(0, false, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
        Assert.assertEquals(-1, atom.maybeAcquireFilter(-1, true, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));

        // They must all reduce against one EMA. Keying the statistics on the caller instead would
        // restart warm-up per worker, and a reducing pool spreads a short parquet scan so thinly
        // that no single bucket would ever reach MIN_SAMPLES -- forcing late materialization on
        // every frame of the scan, on every execution.
        Assert.assertSame(atom.getSelectivityStats(-1), atom.getSelectivityStats(0));
        Assert.assertSame(atom.getSelectivityStats(-1), atom.getSelectivityStats(7));

        // So MIN_SAMPLES frames settle the heuristic for the whole scan, whichever thread saw them.
        atom.getSelectivityStats(-1).update(100, 100);
        atom.getSelectivityStats(-1).update(100, 100);
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, true, false));
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
                false
        );
    }

    private AsyncFilterAtom newAtomWithPerWorkerFilters() {
        final ObjList<Function> perWorkerFilters = new ObjList<>();
        for (int i = 0; i < SLOT_COUNT; i++) {
            perWorkerFilters.add(BooleanConstant.TRUE);
        }
        return newAtom(perWorkerFilters);
    }
}
