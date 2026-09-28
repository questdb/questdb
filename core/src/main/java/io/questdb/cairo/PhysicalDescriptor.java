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

package io.questdb.cairo;

/**
 * What storage and record access need to know about a type's physical form, as closed sets its
 * type definition answers (F27, F39). Code that only moves values keys on these sets, never on
 * the tag, so a type that stores like an existing one adds no arm there (FR-011); a new value of
 * a set makes javac list every switch over it. None of them says anything about NULL: code that
 * decides NULL reads the column's NULL policy (FR-010, FR-031).
 */
public final class PhysicalDescriptor {

    private PhysicalDescriptor() {
    }

    /**
     * The data-movement tier: the width class of a fixed-size value, or {@link #VAR} for a
     * var-size layout, whose values live in a data vector addressed through an aux vector. Code
     * on this tier copies, shuffles, sizes and fills values; it never compares or sorts them.
     */
    public enum Movement {
        W1(0),
        W2(1),
        W4(2),
        W8(3),
        W16(4),
        W32(5),
        VAR(-1);

        private final int pow2Size;

        Movement(int pow2Size) {
            this.pow2Size = pow2Size;
        }

        /**
         * log2 of the value width in bytes; -1 for {@link #VAR}.
         */
        public int pow2Size() {
            return pow2Size;
        }

        /**
         * The value width in bytes; 0 for {@link #VAR}, whose data vector has no fixed stride.
         */
        public int size() {
            return pow2Size < 0 ? 0 : 1 << pow2Size;
        }
    }
}
