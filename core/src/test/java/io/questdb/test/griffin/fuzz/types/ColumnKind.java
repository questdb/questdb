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

import io.questdb.std.Rnd;

/**
 * Broad category of a {@link FuzzColumnType}. Query and predicate generators
 * use it to decide whether a column is fit for a given slot (e.g., an ORDER
 * BY column, a GROUP BY key, a sum() argument, a join key).
 * <p>
 * The matrix is deliberately conservative: a kind reports a capability only
 * when all its concrete types support it. Corner cases that work for some
 * types but not others are left to the runtime error allowlist.
 * <p>
 * UUID, IPv4 and LONG256 each get their own kind rather than sharing one
 * IDENTIFIER kind. The generators pick both sides of a comparison from a
 * single kind, and the three types are mutually incomparable, so sharing a
 * kind produced {@code uuidCol < ipv4Literal} noise and - because the shared
 * kind then had to report itself unorderable - no ordering predicate over any
 * of them at all. That blind spot hid a JIT filter defect that crashed the
 * JVM on {@code uuidCol > uuidCol2} and another that returned wrong rows for
 * {@code ipv4Col < ipv4Col2}.
 */
public enum ColumnKind {
    NUMERIC,      // BYTE, SHORT, INT, LONG, FLOAT, DOUBLE
    DECIMAL,      // DECIMAL(p, s)
    TEMPORAL,     // DATE, TIMESTAMP
    BOOLEAN,
    CHAR,
    STRING_LIKE,  // STRING, VARCHAR, SYMBOL
    UUID,
    IPV4,
    LONG256,
    ARRAY;        // DOUBLE[], DOUBLE[][]

    private static final ColumnKind[] IDENTIFIER_KINDS = {UUID, IPV4, LONG256};

    /**
     * Draws one of the three identifier kinds uniformly. Kind pickers call it
     * so "an identifier" stays a single slot in their option list, keeping the
     * odds of an identifier group-by or sample-by key what they were while the
     * three shared one kind.
     */
    public static ColumnKind randomIdentifier(Rnd rnd) {
        return IDENTIFIER_KINDS[rnd.nextInt(IDENTIFIER_KINDS.length)];
    }

    public boolean isGroupable() {
        return this != ARRAY;
    }

    public boolean isIdentifier() {
        return this == UUID || this == IPV4 || this == LONG256;
    }

    public boolean isJoinKey() {
        return this == NUMERIC || this == STRING_LIKE || this == CHAR
                || this == BOOLEAN || isIdentifier() || this == TEMPORAL;
    }

    /**
     * LONG256 is absent on purpose: its Java {@code <} accepts only another
     * LONG256 column, and a LONG256 literal is a quoted hex string that the
     * comparison coerces to LONG, so every generated {@code long256Col <
     * '0x...'} would die with an implicit-cast error the oracle swallows as a
     * skip. UUID and IPv4 compare fine against both a column and a literal.
     */
    public boolean isOrderable() {
        return this == NUMERIC || this == DECIMAL || this == TEMPORAL
                || this == CHAR || this == STRING_LIKE
                || this == UUID || this == IPV4;
    }
}
