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

package io.questdb.mp;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface Job {
    RunStatus RUNNING_STATUS = () -> false;
    RunStatus TERMINATING_STATUS = () -> true;

    default void drain(int workerId) {
        while (true) {
            if (!run(workerId)) {
                return;
            }
        }
    }

    /**
     * Runs and returns true if it should be rescheduled ASAP.
     *
     * @param workerId  worker id
     * @param runStatus set to 1 when job is running, 2 when it is halting
     * @return true if job should be rescheduled ASAP
     */
    boolean run(int workerId, @NotNull RunStatus runStatus);

    /**
     * Runs and returns true if it should be rescheduled ASAP.
     *
     * @param workerId worker id
     * @return true if job should be rescheduled ASAP
     */
    default boolean run(int workerId) {
        return run(workerId, RUNNING_STATUS);
    }

    interface RunStatus {
        boolean isTerminating();
    }
}
