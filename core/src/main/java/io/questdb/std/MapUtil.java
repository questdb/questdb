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

package io.questdb.std;

public class MapUtil {
    /**
     * Used by hash-based collections as part of the logic for
     * compacting a probe sequence after an entry is deleted.
     * When a slot is freed, we examine the non-empty entries that follow it.
     * Some of them may have originally hashed to this slot but were displaced
     * because it was occupied. Once the slot becomes free, such entries
     * may need to be moved backward to preserve correct lookup semantics.
     *
     * @param elementPos the current position of the entry being evaluated
     * @param idealPos   the ideal position where this entry should be placed
     * @param gapPos     the position of the gap we are trying to fill
     * @return {@code true} if the entry should move backward to fill the gap
     */
    public static boolean shouldMoveToFillGap(long elementPos, long idealPos, long gapPos) {
        if (elementPos < idealPos) {
            // Entry displaced from its ideal position and wrapped around the table boundary.
            // Move if the ideal position is before the gap,
            // OR a gap is before the current position
            // [ ELEMENT, IDEAL_POS, GAP ] OR [ GAP, ELEMENT, IDEAL_POS ]
            return idealPos <= gapPos || gapPos <= elementPos;
        } else {
            // Move only if ideal, gap, and element are in linear order
            // [ IDEAL_POS, GAP, ELEMENT ]
            return idealPos <= gapPos && gapPos <= elementPos;
        }
    }
}
