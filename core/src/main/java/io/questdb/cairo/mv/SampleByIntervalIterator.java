/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.mv;

/**
 * Allows iterating through SAMPLE BY buckets with the given step.
 * The goal is to split a potentially large time interval to be scanned by
 * the materialized view query into smaller intervals, thus minimizing
 * chances of out-of-memory kills.
 */
public interface SampleByIntervalIterator {

    /**
     * Returns maximum timestamp that belong to the iterated intervals.
     */
    long getMaxTimestamp();

    /**
     * Returns minimum timestamp that belong to the iterated intervals.
     */
    long getMinTimestamp();

    /**
     * Returns minimal number of SAMPLE BY buckets for in a single iteration.
     */
    int getStep();

    /**
     * High boundary for the current iteration's interval.
     * Meant to be used as an exclusive boundary when querying.
     */
    long getTimestampHi();

    /**
     * Low boundary for the current iteration's interval.
     * Meant to be used as an inclusive boundary when querying.
     */
    long getTimestampLo();

    /**
     * Iterates to the next interval.
     *
     * @return true if the iterator moved to the next interval; false if the iteration has ended
     */
    boolean next();

    /**
     * Reset the iterator for the given number of steps per iteration.
     *
     * @see #getStep()
     */
    void toTop(int step);
}
