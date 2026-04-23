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

package io.questdb.test.griffin.fuzz.types;

/**
 * Broad category of a {@link FuzzColumnType}. Query and predicate generators
 * use it to decide whether a column is fit for a given slot (e.g., an ORDER
 * BY column, a GROUP BY key, a sum() argument, a join key).
 * <p>
 * The matrix is deliberately conservative: a kind reports a capability only
 * when all its concrete types support it. Corner cases that work for some
 * types but not others are left to the runtime error allowlist.
 */
public enum ColumnKind {
    NUMERIC,      // BYTE, SHORT, INT, LONG, FLOAT, DOUBLE
    DECIMAL,      // DECIMAL(p, s)
    TEMPORAL,     // DATE, TIMESTAMP
    BOOLEAN,
    CHAR,
    STRING_LIKE,  // STRING, VARCHAR, SYMBOL
    IDENTIFIER,   // UUID, IPv4, LONG256
    ARRAY;        // DOUBLE[], DOUBLE[][]

    public boolean isGroupable() {
        return this != ARRAY;
    }

    public boolean isJoinKey() {
        return this == NUMERIC || this == STRING_LIKE || this == CHAR
                || this == BOOLEAN || this == IDENTIFIER || this == TEMPORAL;
    }

    public boolean isOrderable() {
        return this == NUMERIC || this == DECIMAL || this == TEMPORAL
                || this == CHAR || this == STRING_LIKE;
    }

    public boolean isSummable() {
        return this == NUMERIC || this == DECIMAL;
    }
}
