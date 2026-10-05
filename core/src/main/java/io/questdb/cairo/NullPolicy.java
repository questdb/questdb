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
 * How a column represents NULL. Code that holds a column reads the column's policy through a
 * per-column accessor such as {@link io.questdb.cairo.sql.RecordMetadata#getColumnNullPolicy(int)};
 * code that holds only a type reads {@link TypeDriver#getNullPolicy()}. Switch on it exhaustively
 * at setup, so javac lists every such site when a policy is added.
 */
public enum NullPolicy {
    /**
     * A reserved encoding in the data vector means NULL: a sentinel value for fixed-size
     * types, the length prefix or aux entry for var-size types.
     */
    SENTINEL,
    /**
     * The type has no NULL: every bit pattern is a value, so a written NULL becomes false or
     * 0, and column-top rows read as that value (BOOLEAN, BYTE, SHORT, CHAR).
     */
    NONE
}
