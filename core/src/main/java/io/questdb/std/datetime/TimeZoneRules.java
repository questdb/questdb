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

package io.questdb.std.datetime;

public interface TimeZoneRules {

    /**
     * If the local epoch is a Daylight Saving Transition gap in forward time shift,
     * this method returns the offset of the timestamp to the beginning of the gap.
     */
    long getDstGapOffset(long localEpoch);

    long getLocalOffset(long localEpoch);

    long getLocalOffset(long localEpoch, int year);

    /**
     * Computes UTC epoch time for the next Daylight Saving Transition.
     *
     * @param utcEpoch arbitrary point in time, UTC epoch time
     * @return UTC epoch
     */
    long getNextDST(long utcEpoch);

    long getNextDST(long utcEpoch, int year);

    long getOffset(long utcEpoch);

    long getOffset(long utcEpoch, int year);

    boolean hasFixedOffset();
}
