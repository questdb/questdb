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

package io.questdb.cairo;

import io.questdb.std.Numbers;

public interface DatabaseCheckpointStatus {

    /**
     * Returns true when database is in "checkpoint" mode. Checkpoint mode is
     * entered when CHECKPOINT CREATE SQL is called and exited after
     * CHECKPOINT RELEASE is called.
     */
    default boolean isInProgress() {
        return startedAtTimestamp() != Numbers.LONG_NULL;
    }

    /**
     * Returns a non-negative number when the database is in "checkpoint" mode.
     * Checkpoint mode is entered when CHECKPOINT CREATE SQL is called
     * and exited after CHECKPOINT RELEASE is called.
     * The value is an epoch timestamp in micros for the time when CHECKPOINT CREATE was run.
     * If there is no checkpoint mode, returns {@link Numbers#LONG_NULL}.
     */
    long startedAtTimestamp();
}
