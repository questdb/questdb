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

package io.questdb.std;

/**
 * Decides how much of its capacity a reusable structure keeps each time it clears: the most that any of its uses
 * needed in the current and the previous window of {@link #WINDOW_USES} uses, and never less than the floor the
 * structure was configured with. A use ends at each clear, whether or not the structure handed anything out, so a
 * demand that comes back at least once a window keeps its capacity, and a demand that does not come back stops being
 * kept within two windows.
 */
public final class RetainedCapacity {
    public static final int WINDOW_USES = 32;
    /**
     * The uses that bring a structure back to its floor whatever its earlier uses needed: the use in progress, then
     * two windows of uses that need nothing.
     */
    public static final int USES_TO_FLOOR = 2 * WINDOW_USES + 1;
    private final int floor;
    private int currentWindowPeak;
    private int previousWindowPeak;
    private int useCount;

    public RetainedCapacity(int floor) {
        this.floor = floor;
    }

    /**
     * Ends a use that needed {@code used} and returns the capacity the structure keeps.
     */
    public int endUse(int used) {
        currentWindowPeak = Math.max(currentWindowPeak, used);
        final int retained = Math.max(floor, Math.max(previousWindowPeak, currentWindowPeak));
        if (++useCount == WINDOW_USES) {
            previousWindowPeak = currentWindowPeak;
            currentWindowPeak = 0;
            useCount = 0;
        }
        return retained;
    }
}
