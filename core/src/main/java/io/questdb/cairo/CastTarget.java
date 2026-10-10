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
 * Whether the parser takes a type as the target of {@code cast(x as T)} and of the
 * {@code T 'literal'} form; {@link TypeDriver#isCastTarget(boolean)} answers from it.
 */
public enum CastTarget {
    /**
     * A target from a value and from {@code null}.
     */
    ALWAYS,
    /**
     * A target from {@code null} only (INTERVAL).
     */
    FROM_NULL_ONLY,
    /**
     * Never a target: LONG128, and the geohash and decimal widths, whose casts name their
     * pseudo types.
     */
    NEVER
}
