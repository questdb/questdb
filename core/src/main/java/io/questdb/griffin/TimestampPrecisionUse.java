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

package io.questdb.griffin;

/**
 * How a function's value depends on the precision of its TIMESTAMP argument, see
 * {@link FunctionFactory#getTimestampPrecisionUse()}.
 */
public enum TimestampPrecisionUse {
    /**
     * The value depends on the precision, as the argument's raw value or its text does.
     */
    DEPENDENT,
    /**
     * The value is the same at every precision, as a calendar field of the instant is.
     */
    INVARIANT,
    /**
     * The value is a timestamp of the same instant at every precision, typed at the argument's precision.
     */
    PRESERVING
}
