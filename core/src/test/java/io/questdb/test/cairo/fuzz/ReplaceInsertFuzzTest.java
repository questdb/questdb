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

package io.questdb.test.cairo.fuzz;

import io.questdb.std.Rnd;
import org.junit.Test;

public class ReplaceInsertFuzzTest extends AbstractFuzzTest {
    @Test
    public void testSimpleDataTransactionBigReplaceProb() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(
                0.01,
                0.2,
                0.1,
                0.01,
                0.02,
                0.02,
                0.08,
                0,
                1.0,
                0.01,
                0.1,
                0.01,
                0.01,
                0.8,
                0.9,
                0.05
        );
        setFuzzCounts(
                rnd.nextBoolean(), 1000, 5 + rnd.nextInt(100),
                20, 10, 200, rnd.nextInt(100), 1
        );
        runFuzz(rnd);
    }

    @Test
    public void testSimpleDataTransactionSmallReplaceProb() throws Exception {
        Rnd rnd = generateRandom(LOG);
        setFuzzProbabilities(
                0.01,
                0.2,
                0.1,
                0.01,
                0.15,
                0.05,
                0.08,
                0.15,
                1.0,
                0.01,
                0.1,
                0.01,
                0.01,
                0.8,
                0.05,
                0.05
        );
        setFuzzCounts(
                rnd.nextBoolean(), 1000, 5 + rnd.nextInt(100),
                20, 10, 200, rnd.nextInt(1000), 1
        );
        runFuzz(rnd);
    }
}
