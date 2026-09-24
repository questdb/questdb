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
 * The root of the per-type driver hierarchy: one driver instance per column type tag, holding
 * what the engine must know about that type. A driver is fetched once per column, batch or
 * query with {@link ColumnType#getTypeDriver(int)}, never per value.
 * <p>
 * Three driver concepts exist, and they are different things:
 * <ul>
 * <li>{@code TypeDriver}: per tag, this hierarchy. {@link FixedSizeTypeDriver} is the base of
 * the fixed-size leaves.</li>
 * <li>{@link ColumnTypeDriver}: the var-size storage API (aux and data vectors), which
 * extends this interface; STRING, BINARY, VARCHAR and ARRAY.</li>
 * <li>{@link TimestampDriver}: per timestamp <em>precision</em>, keyed by the encoded type,
 * not by tag. It is a separate facet, not a {@code TypeDriver}; the TIMESTAMP and DATE
 * drivers fetch it where a method needs temporal arithmetic.</li>
 * </ul>
 * Methods are added here only when a caller needs them. Pseudo tags (UNDEFINED, CURSOR,
 * VAR_ARG, RECORD, GEOHASH, DECIMAL, REGCLASS, REGPROCEDURE, ARRAY_STRING, PARAMETER, NULL)
 * have no driver.
 */
public interface TypeDriver {

    /**
     * The tag this driver serves. Exactly one driver instance exists per non-pseudo tag.
     */
    ColumnTypeTag getTag();

    /**
     * The tag's constant name, e.g. {@code GEOBYTE}; unlike {@link ColumnType#nameOf(int)}
     * this is defined for every tag.
     */
    default String getTypeName() {
        return getTag().name();
    }
}
