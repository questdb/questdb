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
 * Describes one column type the fuzzer can generate. Each implementation
 * lives in its own file so a new type can be added without touching the
 * rest of the fuzzer. Implementations are typically singletons; the two
 * parameterised families (DECIMAL, DOUBLE[]) use fresh instances per column
 * so precision/scale/dimensionality varies between columns.
 */
public interface FuzzColumnType {

    /**
     * SQL DDL fragment that follows the column name in CREATE TABLE, e.g.
     * {@code "INT"}, {@code "DECIMAL(18, 4)"}, {@code "DOUBLE[][]"}.
     */
    String getDdl();

    ColumnKind getKind();

    /**
     * Expression used in the data-loading {@code INSERT ... SELECT ...} to
     * produce a random value of this type, e.g. {@code "rnd_int()"},
     * {@code "rnd_double_array(2)"}, {@code "rnd_decimal(18, 4, 2)"}.
     */
    String getRndCall();

    /**
     * SQL literal of this type suitable for WHERE comparisons, casts, etc.
     * May return {@code "null"} with small probability. The result must
     * parse on its own in a SQL expression slot (no trailing punctuation).
     */
    String randomLiteral(Rnd rnd);
}
