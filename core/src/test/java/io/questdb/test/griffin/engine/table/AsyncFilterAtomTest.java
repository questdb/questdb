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

import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class AsyncFilterAtomTest extends AbstractCairoTest {

    @Test
    public void testForeignOwnerDoesNotShareOwnerStatistics() {
        final AsyncFilterAtom atom = newAtom();

        atom.updateSelectivityStats(-1, -1, true, 100, 100);
        atom.updateSelectivityStats(-1, -1, true, 100, 100);
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, -1, true, true, false));
        Assert.assertTrue(atom.shouldUseLateMaterialization(-1, -1, false, true, false));

        final AsyncFilterAtom freshAtom = newAtom();
        freshAtom.updateSelectivityStats(-1, -1, false, 100, 100);
        freshAtom.updateSelectivityStats(-1, -1, false, 100, 100);
        Assert.assertTrue(freshAtom.shouldUseLateMaterialization(-1, -1, true, true, false));
    }

    @Test
    public void testThreadSafeWorkerStatisticsAreIndependent() {
        final AsyncFilterAtom atom = newAtom();

        atom.updateSelectivityStats(-1, 0, false, 100, 100);
        atom.updateSelectivityStats(-1, 0, false, 100, 100);
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, 0, false, true, false));
        Assert.assertTrue(atom.shouldUseLateMaterialization(-1, 1, false, true, false));

        atom.updateSelectivityStats(-1, 1, false, 0, 100);
        atom.updateSelectivityStats(-1, 1, false, 0, 100);
        Assert.assertTrue(atom.shouldUseLateMaterialization(-1, 1, false, true, false));
        Assert.assertFalse(atom.shouldUseLateMaterialization(-1, 0, false, true, false));
    }

    private AsyncFilterAtom newAtom() {
        final IntHashSet filterColumns = new IntHashSet();
        filterColumns.add(0);
        final IntList columnTypes = new IntList();
        columnTypes.add(0);
        return new AsyncFilterAtom(
                configuration,
                BooleanConstant.TRUE,
                filterColumns,
                null,
                columnTypes,
                false,
                2
        );
    }
}
