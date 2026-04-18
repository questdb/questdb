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

package io.questdb.cutlass.qwp.codec;

import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

/**
 * Emits QWP schema sections for egress result batches.
 * <p>
 * See {@code docs/QWP_SPECIFICATION.md} §9 for the schema format.
 */
public final class QwpEgressSchemaWriter {

    private QwpEgressSchemaWriter() {
    }

    /**
     * Schema mode byte for schema reference.
     */
    public static final byte SCHEMA_MODE_REFERENCE = 0x01;

    /**
     * Writes a schema reference (mode 0x01 + schema_id varint).
     *
     * @return address just past the reference
     */
    public static long writeReference(long bufAddr, long schemaId) {
        Unsafe.getUnsafe().putByte(bufAddr, SCHEMA_MODE_REFERENCE);
        return QwpVarint.encode(bufAddr + 1, schemaId);
    }

    /**
     * Writes a full-mode schema (mode 0x00 + schema_id + per-column definitions).
     * Column name is UTF-8 encoded; length is varint-prefixed.
     *
     * @return address just past the schema block
     */
    public static long writeFull(long bufAddr, long schemaId, ObjList<QwpEgressColumnDef> columns) {
        Unsafe.getUnsafe().putByte(bufAddr, QwpConstants.SCHEMA_MODE_FULL);
        long p = QwpVarint.encode(bufAddr + 1, schemaId);
        for (int i = 0, n = columns.size(); i < n; i++) {
            QwpEgressColumnDef col = columns.getQuick(i);
            byte[] nameBytes = col.getName() == null ? new byte[0] : col.getName().getBytes(StandardCharsets.UTF_8);
            p = QwpVarint.encode(p, nameBytes.length);
            for (byte b : nameBytes) {
                Unsafe.getUnsafe().putByte(p++, b);
            }
            Unsafe.getUnsafe().putByte(p++, col.getWireType());
        }
        return p;
    }

    /**
     * Worst-case serialized size of a full-mode schema. Used to ensure the wire buffer
     * has space before encoding.
     */
    public static int worstCaseFullSize(ObjList<QwpEgressColumnDef> columns) {
        int total = 1 /* mode */ + QwpVarint.MAX_VARINT_BYTES /* schema id */;
        for (int i = 0, n = columns.size(); i < n; i++) {
            QwpEgressColumnDef col = columns.getQuick(i);
            int nameLen = col.getName() == null ? 0 : col.getName().getBytes(StandardCharsets.UTF_8).length;
            total += QwpVarint.MAX_VARINT_BYTES + nameLen + 1 /* type */;
        }
        return total;
    }
}
