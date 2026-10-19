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

package io.questdb.test.griffin;

import io.questdb.std.Rnd;

/**
 * Spreads a test's combination matrix over the runs of a suite parameterized by
 * {@link HashJoinBuildMode#parameters()}. Every run draws the same fixed sequence, so each
 * combination falls to exactly one run: the runs together check the whole matrix once, instead of
 * every run checking all of it, and each run checks combinations from every part of the matrix.
 * A test asks once per combination, in the same order in every run, and draws any random input
 * of a combination it skips all the same, so that every run generates the same matrix.
 */
final class HashJoinMatrixStripe {
    private static final int RUN_COUNT = HashJoinPayloadLayout.values().length * HashJoinBuildMode.values().length;
    private final Rnd rnd = new Rnd();
    private final int run;

    HashJoinMatrixStripe(HashJoinPayloadLayout payloadLayout, HashJoinBuildMode buildMode) {
        this.run = payloadLayout.ordinal() * HashJoinBuildMode.values().length + buildMode.ordinal();
    }

    /**
     * True when the next combination of the matrix falls to this run.
     */
    boolean isThisRun() {
        return rnd.nextInt(RUN_COUNT) == run;
    }
}
