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

package io.questdb.test.cairo.fuzz;

import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CompositeFuzzTest extends AbstractCairoTest {

    @Test
    public void testFixedSeedTwinEquality() throws Exception {
        assertMemoryLeak(() -> {
            CompositeFuzzRunner runner = CompositeFuzzRunner.of(engine, new Rnd(1234L, 5678L));
            runner.createTables("fuzz1");
            runner.applyGeneratedTransactions(200, 20);
            runner.assertTwinEqual();
        });
    }

    @Test
    public void testAxesVaryAcrossSeeds() throws Exception {
        assertMemoryLeak(() -> {
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (int i = 0; i < 12; i++) {
                CompositeFuzzRunner r = CompositeFuzzRunner.of(engine, new Rnd(i, i * 7L));
                r.createTables("axes" + i);
                seen.add(r.axes().toString());
            }
            org.junit.Assert.assertTrue("axes must vary across seeds, saw " + seen, seen.size() > 3);
        });
    }
}
