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

package io.questdb.cutlass.qwp.server;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

final class QwpSchemaControl {
    static final int KIND_DESCRIBE = 1;
    static final int KIND_SCHEMA = 2;
    static final int MAX_MESSAGE_SIZE = 1024 * 1024;
    static final int RESULT_KNOWN = 0;
    static final int RESULT_MISSING = 1;
    static final int RESULT_DENIED = 2;
    static final int RESULT_UNAVAILABLE = 3;
    static final int RESULT_TOO_LARGE = 4;

    private QwpSchemaControl() {
    }

    static EncodedFeedback encodeFeedback(
            CairoEngine engine,
            SecurityContext securityContext,
            LowerCaseCharSequenceObjHashMap<String> tableNames,
            int maxSuffixSize
    ) {
        if (tableNames.size() == 0) {
            return EncodedFeedback.NONE;
        }
        if (maxSuffixSize < Short.BYTES || tableNames.size() >= 0xffff) {
            return EncodedFeedback.INVALIDATE_ALL;
        }
        ObjList<String> names = new ObjList<>(tableNames.size());
        tableNames.forEach((key, value) -> names.add(value));
        ObjList<byte[]> encodedNames = new ObjList<>(names.size());
        ObjList<byte[]> schemas = new ObjList<>(names.size());
        long size = Short.BYTES;
        for (int i = 0, n = names.size(); i < n; i++) {
            String tableName = names.getQuick(i);
            final byte[] name;
            try {
                name = encodeUtf8(tableName);
            } catch (CharacterCodingException e) {
                return EncodedFeedback.INVALIDATE_ALL;
            }
            byte[] schema = snapshot(engine, securityContext, tableName, 0, MAX_MESSAGE_SIZE);
            int result = schema[QwpConstants.HEADER_SIZE + 9] & 0xff;
            if (result == RESULT_DENIED || result == RESULT_UNAVAILABLE || result == RESULT_TOO_LARGE) {
                return EncodedFeedback.INVALIDATE_ALL;
            }
            int schemaPayloadLength = schema.length - QwpConstants.HEADER_SIZE;
            size += Short.BYTES + name.length + Integer.BYTES + schemaPayloadLength;
            if (size > maxSuffixSize || size > MAX_MESSAGE_SIZE) {
                return EncodedFeedback.INVALIDATE_ALL;
            }
            encodedNames.add(name);
            schemas.add(schema);
        }
        ByteBuffer out = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
        out.putShort((short) names.size());
        for (int i = 0, n = names.size(); i < n; i++) {
            byte[] name = encodedNames.getQuick(i);
            byte[] schema = schemas.getQuick(i);
            int schemaPayloadLength = schema.length - QwpConstants.HEADER_SIZE;
            out.putShort((short) name.length).put(name).putInt(schemaPayloadLength)
                    .put(schema, QwpConstants.HEADER_SIZE, schemaPayloadLength);
        }
        return new EncodedFeedback(QwpConstants.SCHEMA_FEEDBACK_MODE_UPDATES, out.array());
    }

    static byte[] describe(CairoEngine engine, SecurityContext securityContext, long address, int length, int maxMessageSize) {
        if (length < QwpConstants.HEADER_SIZE + 11 || Unsafe.getInt(address) != QwpConstants.MAGIC_MESSAGE
                || Unsafe.getByte(address + QwpConstants.HEADER_OFFSET_VERSION) != QwpConstants.VERSION
                || Unsafe.getByte(address + QwpConstants.HEADER_OFFSET_FLAGS) != QwpConstants.FLAG_CONTROL
                || Unsafe.getShort(address + QwpConstants.HEADER_OFFSET_TABLE_COUNT) != 0) {
            return null;
        }
        int payloadLength = Unsafe.getInt(address + QwpConstants.HEADER_OFFSET_PAYLOAD_LENGTH);
        if (payloadLength != length - QwpConstants.HEADER_SIZE || Unsafe.getByte(address + QwpConstants.HEADER_SIZE) != KIND_DESCRIBE) {
            return null;
        }
        long requestId = Unsafe.getLong(address + QwpConstants.HEADER_SIZE + 1);
        int nameLength = Unsafe.getShort(address + QwpConstants.HEADER_SIZE + 9) & 0xffff;
        if (requestId <= 0 || nameLength == 0 || nameLength > 381 || payloadLength != 11 + nameLength) {
            return null;
        }
        byte[] utf8 = new byte[nameLength];
        long nameAddress = address + QwpConstants.HEADER_SIZE + 11;
        for (int i = 0; i < nameLength; i++) {
            utf8[i] = Unsafe.getByte(nameAddress + i);
        }
        final String tableName;
        try {
            tableName = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(utf8)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
        if (tableName.length() > 127 || !TableUtils.isValidTableName(tableName, engine.getConfiguration().getMaxFileNameLength())) {
            return null;
        }

        return snapshot(engine, securityContext, tableName, requestId, maxMessageSize);
    }

    static byte[] snapshot(CairoEngine engine, SecurityContext securityContext, String tableName, long requestId, int maxMessageSize) {
        if (engine.isReadOnlyMode()) {
            return result(requestId, RESULT_UNAVAILABLE);
        }

        TableToken token = engine.getTableTokenIfExists(tableName);
        if (token == null) {
            return result(requestId, RESULT_MISSING);
        }
        try {
            securityContext.authorizeInsert(token);
        } catch (CairoException e) {
            return result(requestId, e.isAuthorizationError() ? RESULT_DENIED : RESULT_UNAVAILABLE);
        }
        try (TableRecordMetadata metadata = engine.getLegacyMetadata(token)) {
            int activeCount = 0;
            int timestampIndex = metadata.getTimestampIndex();
            int compactTimestampIndex = -1;
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (metadata.getColumnType(i) > 0) {
                    if (i == timestampIndex) {
                        compactTimestampIndex = activeCount;
                    }
                    if (++activeCount > QwpConstants.MAX_COLUMNS_PER_TABLE) {
                        return result(requestId, RESULT_TOO_LARGE);
                    }
                }
            }
            long bytes = QwpConstants.HEADER_SIZE + 26L;
            byte[][] encodedNames = new byte[activeCount][];
            int activeIndex = 0;
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (metadata.getColumnType(i) > 0) {
                    String columnName = metadata.getColumnName(i);
                    if (columnName.length() > 127) {
                        return result(requestId, RESULT_TOO_LARGE);
                    }
                    byte[] name;
                    try {
                        name = encodeUtf8(columnName);
                    } catch (CharacterCodingException e) {
                        return result(requestId, RESULT_UNAVAILABLE);
                    }
                    if (name.length > 381) {
                        return result(requestId, RESULT_TOO_LARGE);
                    }
                    encodedNames[activeIndex++] = name;
                    bytes += 2 + name.length + 4 + 2;
                    if (bytes > Math.min(MAX_MESSAGE_SIZE, maxMessageSize)) {
                        return result(requestId, RESULT_TOO_LARGE);
                    }
                }
            }
            ByteBuffer out = header((int) bytes, (int) bytes - QwpConstants.HEADER_SIZE);
            out.put((byte) KIND_SCHEMA).putLong(requestId).put((byte) RESULT_KNOWN);
            out.putInt(metadata.getTableId()).putLong(metadata.getMetadataVersion());
            out.putShort((short) compactTimestampIndex).putShort((short) activeCount);
            activeIndex = 0;
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                int type = metadata.getColumnType(i);
                if (type > 0) {
                    byte[] name = encodedNames[activeIndex++];
                    out.putShort((short) name.length).put(name).putInt(type).putShort((short) 0);
                }
            }
            return out.array();
        } catch (CairoException e) {
            return result(requestId, e.isAuthorizationError() ? RESULT_DENIED : RESULT_UNAVAILABLE);
        }
    }

    private static ByteBuffer header(int totalLength, int payloadLength) {
        ByteBuffer out = ByteBuffer.allocate(totalLength).order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(QwpConstants.MAGIC_MESSAGE).put(QwpConstants.VERSION).put(QwpConstants.FLAG_CONTROL)
                .putShort((short) 0).putInt(payloadLength);
        return out;
    }

    private static byte[] encodeUtf8(String value) throws CharacterCodingException {
        ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(java.nio.CharBuffer.wrap(value));
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        return bytes;
    }

    private static byte[] result(long requestId, int result) {
        ByteBuffer out = header(QwpConstants.HEADER_SIZE + 10, 10);
        out.put((byte) KIND_SCHEMA).putLong(requestId).put((byte) result);
        return out.array();
    }

    static byte[] tooLarge(long requestId) {
        return result(requestId, RESULT_TOO_LARGE);
    }

    static final class EncodedFeedback {
        static final EncodedFeedback INVALIDATE_ALL = new EncodedFeedback(
                QwpConstants.SCHEMA_FEEDBACK_MODE_INVALIDATE_ALL, null);
        static final EncodedFeedback NONE = new EncodedFeedback((byte) 0, null);
        final byte mode;
        final byte[] suffix;

        private EncodedFeedback(byte mode, byte[] suffix) {
            this.mode = mode;
            this.suffix = suffix;
        }
    }
}
