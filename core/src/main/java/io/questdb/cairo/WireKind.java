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

import org.jetbrains.annotations.Nullable;

/**
 * How a type's values travel on the result protocols: one kind per distinct byte form, so types
 * that write the same bytes under the same NULL test share a kind, and a protocol keeps one writer
 * per kind. A type driver returns its kind ({@link TypeDriver#getWireKind()}); the PostgreSQL wire
 * opcodes and size arithmetic, and the JSON, CSV and QWP writers switch exhaustively on it, so the
 * build lists every protocol that must write a new kind.
 * <p>
 * A kind covers the byte form and the NULL test together: a type with another type's bytes but no
 * reserved NULL value needs a kind of its own. Each existing type has its own writer, so each has
 * its own kind.
 */
public enum WireKind {
    BOOLEAN,
    BYTE,
    SHORT,
    CHAR,
    INT,
    LONG,
    DATE,
    TIMESTAMP,
    FLOAT,
    DOUBLE,
    STRING,
    SYMBOL,
    LONG256,
    GEOBYTE,
    GEOSHORT,
    GEOINT,
    GEOLONG,
    BINARY,
    UUID,
    // no protocol writes LONG128's values: each protocol's arm refuses it
    LONG128,
    IPV4,
    VARCHAR,
    ARRAY,
    INTERVAL,
    DECIMAL8,
    DECIMAL16,
    DECIMAL32,
    DECIMAL64,
    DECIMAL128,
    DECIMAL256,
    // type-registration: wire kind (see utils/type-probe/README.md)
    ;

    /**
     * The wire kind of a stored column type, or null for a pseudo type and for VARCHAR_SLICE, which
     * no protocol writes as a type of its own. Safe in per-row code: it reads a table filled from
     * the type drivers on first use and calls no driver.
     */
    public static @Nullable WireKind of(int columnType) {
        final short tag = ColumnType.tagOf(columnType);
        return tag >= 0 && tag <= ColumnType.MAX_TAG ? Kinds.BY_TAG[tag] : null;
    }

    private static final class Kinds {
        static final WireKind[] BY_TAG = new WireKind[ColumnType.MAX_TAG + 1];

        static {
            for (short tag = 0; tag <= ColumnType.MAX_TAG; tag++) {
                final TypeDriver driver = PhysicalDescriptor.storedTypeDriverOf(tag);
                BY_TAG[tag] = driver != null ? driver.getWireKind() : null;
            }
        }
    }
}
