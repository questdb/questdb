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

package io.questdb.cutlass.qwp.protocol;

import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_BOOLEAN;
import static io.questdb.cutlass.qwp.protocol.QwpConstants.TYPE_CHAR;

/**
 * Represents a column definition in a QWP v1 schema.
 * <p>
 * This class is immutable and safe for caching.
 */
public final class QwpColumnDef {
    private final String name;
    private final byte[] nameUtf8;
    private final byte typeCode;

    /**
     * Creates a column definition.
     *
     * @param name     the column name (UTF-8)
     * @param typeCode the QWP v1 type code (0x01-0x16)
     */
    public QwpColumnDef(String name, byte typeCode) {
        this.name = name;
        this.nameUtf8 = name.getBytes(StandardCharsets.UTF_8);
        this.typeCode = typeCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QwpColumnDef that = (QwpColumnDef) o;
        return typeCode == that.typeCode &&
                name.equals(that.name);
    }

    /**
     * Gets the fixed width in bytes for fixed-width types.
     *
     * @return width in bytes, or -1 for variable-width types
     */
    public int getFixedWidth() {
        return QwpConstants.getFixedTypeSize(typeCode);
    }

    /**
     * Gets the column name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the column name as pre-computed UTF-8 bytes.
     * <p>
     * Returns the internal array directly (no copy) for zero-allocation access.
     * Callers must not modify the returned array.
     *
     * @return UTF-8 encoded name bytes (do not modify)
     */
    public byte[] getNameUtf8() {
        return nameUtf8;
    }

    /**
     * Gets the base type code (without null bitmap flag).
     *
     * @return type code 0x01-0x0F
     */
    public byte getTypeCode() {
        return typeCode;
    }

    /**
     * Gets the type name for display purposes.
     */
    public String getTypeName() {
        return QwpConstants.getTypeName(typeCode);
    }

    /**
     * Gets the wire type code.
     *
     * @return type code as sent on wire
     */
    public byte getWireTypeCode() {
        return typeCode;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + typeCode;
        return result;
    }

    /**
     * Returns true if this is a fixed-width type.
     */
    public boolean isFixedWidth() {
        return QwpConstants.isFixedWidthType(typeCode);
    }

    @Override
    public String toString() {
        return name + ':' + getTypeName();
    }

    /**
     * Validates that this column definition has a valid type code.
     *
     * @throws QwpParseException if type code is invalid
     */
    public void validate() throws QwpParseException {
        // Valid type codes: TYPE_BOOLEAN (0x01) through TYPE_CHAR (0x16)
        // This includes all basic types, arrays, decimals, and char
        boolean valid = (typeCode >= TYPE_BOOLEAN && typeCode <= TYPE_CHAR);
        if (!valid) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_COLUMN_TYPE,
                    "invalid column type code: 0x" + Integer.toHexString(typeCode)
            );
        }
    }
}
