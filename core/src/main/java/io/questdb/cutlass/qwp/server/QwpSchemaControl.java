/*******************************************************************************
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
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Encodes QWP schema control messages straight into native memory.
 * <p>
 * Every method writes at the address it is given and never allocates. A
 * DESCRIBE reply and each feedback entry share one payload encoder,
 * {@link #writeSchemaPayload}, so the two message kinds cannot drift apart.
 * <p>
 * SCHEMA payload layout (little-endian): {@code kind:u8, requestId:i64,
 * result:u8}, then for {@link #RESULT_KNOWN} only: {@code tableId:i32,
 * metadataVersion:i64, timestampIndex:i16, columnCount:u16} and per column
 * {@code nameLength:u16, name (UTF-8), columnType:i32, params:u16}.
 */
final class QwpSchemaControl {
    static final int KIND_DESCRIBE = 1;
    static final int KIND_SCHEMA = 2;
    static final int MAX_MESSAGE_SIZE = 1024 * 1024;
    static final int RESULT_DENIED = 2;
    static final int RESULT_KNOWN = 0;
    static final int RESULT_MISSING = 1;
    /**
     * Size of a SCHEMA payload that carries only a result code:
     * kind, requestId and result.
     */
    static final int RESULT_PAYLOAD_SIZE = 1 + Long.BYTES + 1;
    static final int RESULT_TOO_LARGE = 4;
    static final int RESULT_UNAVAILABLE = 3;
    private static final int COLUMN_ENTRY_OVERHEAD = Short.BYTES + Integer.BYTES + Short.BYTES;
    // kind, requestId and nameLength precede the table name in a DESCRIBE payload
    private static final int DESCRIBE_PAYLOAD_PREFIX = 1 + Long.BYTES + Short.BYTES;
    // feedback entry: nameLength and schemaPayloadLength surround the table name
    private static final int FEEDBACK_ENTRY_OVERHEAD = Short.BYTES + Integer.BYTES;
    // tableId, metadataVersion, timestampIndex and columnCount follow the result
    private static final int KNOWN_PAYLOAD_PREFIX = RESULT_PAYLOAD_SIZE + Integer.BYTES + Long.BYTES + Short.BYTES + Short.BYTES;
    private static final int MAX_NAME_CHARS = 127;
    // a 127-char UTF-16 name encodes to at most three bytes per char
    private static final int MAX_NAME_BYTES = MAX_NAME_CHARS * 3;

    private QwpSchemaControl() {
    }

    /**
     * Validates a DESCRIBE request and writes the framed SCHEMA reply.
     *
     * @param request       address of the received QWP control frame
     * @param requestLength length of the received frame in bytes
     * @param nameSink      scratch sink for the decoded table name
     * @param response      address to write the reply frame to
     * @param responseLimit bytes available at {@code response}; must be at least
     *                      {@link QwpConstants#HEADER_SIZE} plus {@link #RESULT_PAYLOAD_SIZE}
     * @return reply frame length, or -1 when the request is malformed
     */
    static int describe(
            CairoEngine engine,
            SecurityContext securityContext,
            long request,
            int requestLength,
            StringSink nameSink,
            long response,
            int responseLimit
    ) {
        assert responseLimit >= QwpConstants.HEADER_SIZE + RESULT_PAYLOAD_SIZE;
        if (requestLength < QwpConstants.HEADER_SIZE + DESCRIBE_PAYLOAD_PREFIX
                || Unsafe.getInt(request) != QwpConstants.MAGIC_MESSAGE
                || Unsafe.getByte(request + QwpConstants.HEADER_OFFSET_VERSION) != QwpConstants.VERSION
                || Unsafe.getByte(request + QwpConstants.HEADER_OFFSET_FLAGS) != QwpConstants.FLAG_CONTROL
                || Unsafe.getShort(request + QwpConstants.HEADER_OFFSET_TABLE_COUNT) != 0) {
            return -1;
        }
        int payloadLength = Unsafe.getInt(request + QwpConstants.HEADER_OFFSET_PAYLOAD_LENGTH);
        long payload = request + QwpConstants.HEADER_SIZE;
        if (payloadLength != requestLength - QwpConstants.HEADER_SIZE || Unsafe.getByte(payload) != KIND_DESCRIBE) {
            return -1;
        }
        long requestId = Unsafe.getLong(payload + 1);
        int nameLength = Unsafe.getShort(payload + 1 + Long.BYTES) & 0xffff;
        if (requestId <= 0 || nameLength == 0 || nameLength > MAX_NAME_BYTES || payloadLength != DESCRIBE_PAYLOAD_PREFIX + nameLength) {
            return -1;
        }
        long nameLo = payload + DESCRIBE_PAYLOAD_PREFIX;
        nameSink.clear();
        if (!Utf8s.utf8ToUtf16(nameLo, nameLo + nameLength, nameSink)
                || nameSink.length() > MAX_NAME_CHARS
                || !TableUtils.isValidTableName(nameSink, engine.getConfiguration().getMaxFileNameLength())) {
            return -1;
        }

        long packed;
        for (int retries = 0; ; retries++) {
            try {
                packed = writeSchemaPayload(
                        engine,
                        securityContext,
                        nameSink,
                        requestId,
                        response + QwpConstants.HEADER_SIZE,
                        responseLimit - QwpConstants.HEADER_SIZE
                );
                break;
            } catch (TableReferenceOutOfDateException e) {
                if (retries >= engine.getConfiguration().getMaxSqlRecompileAttempts()) {
                    packed = writeResult(response + QwpConstants.HEADER_SIZE, requestId, RESULT_UNAVAILABLE);
                    break;
                }
                // Resolve and authorize the replacement token before acquiring its metadata.
            }
        }
        int replyPayloadLength = Numbers.decodeLowInt(packed);
        writeFrameHeader(response, replyPayloadLength);
        return QwpConstants.HEADER_SIZE + replyPayloadLength;
    }

    /**
     * Writes the schema feedback suffix for the given table names.
     * <p>
     * The suffix is {@code count:u16} followed by one entry per table:
     * {@code nameLength:u16, name (UTF-8), schemaPayloadLength:u32} and the
     * SCHEMA payload with request id zero. Bytes written past a failed
     * attempt are garbage the caller must not send.
     *
     * @return suffix length when the response must carry
     * {@link QwpConstants#SCHEMA_FEEDBACK_MODE_UPDATES}; 0 when there is
     * nothing to report; -1 when the caller must send
     * {@link QwpConstants#SCHEMA_FEEDBACK_MODE_INVALIDATE_ALL} instead
     */
    static int encodeFeedback(
            CairoEngine engine,
            SecurityContext securityContext,
            LowerCaseCharSequenceObjHashMap<String> tableNames,
            long address,
            int limit
    ) {
        int count = tableNames.size();
        if (count == 0) {
            return 0;
        }
        if (limit < Short.BYTES || count >= 0xffff) {
            return -1;
        }
        long p = address;
        long hi = address + limit;
        Unsafe.putShort(p, (short) count);
        p += Short.BYTES;
        ObjList<CharSequence> names = tableNames.keys();
        for (int i = 0; i < count; i++) {
            CharSequence tableName = names.getQuick(i);
            int nameBytes = Utf8s.utf8Bytes(tableName);
            if (hi - p < FEEDBACK_ENTRY_OVERHEAD + nameBytes + RESULT_PAYLOAD_SIZE) {
                return -1;
            }
            Unsafe.putShort(p, (short) nameBytes);
            p += Short.BYTES;
            Utf8s.strCpyUtf8(tableName, p, nameBytes);
            p += nameBytes;
            long schemaLengthAddress = p;
            p += Integer.BYTES;
            long packed;
            try {
                packed = writeSchemaPayload(engine, securityContext, tableName, 0, p, (int) (hi - p));
            } catch (TableReferenceOutOfDateException e) {
                return -1;
            }
            int result = Numbers.decodeHighInt(packed);
            if (result == RESULT_DENIED || result == RESULT_UNAVAILABLE) {
                return -1;
            }
            int schemaLength = Numbers.decodeLowInt(packed);
            Unsafe.putInt(schemaLengthAddress, schemaLength);
            p += schemaLength;
        }
        return (int) (p - address);
    }

    private static void writeFrameHeader(long address, int payloadLength) {
        Unsafe.putInt(address + QwpConstants.HEADER_OFFSET_MAGIC, QwpConstants.MAGIC_MESSAGE);
        Unsafe.putByte(address + QwpConstants.HEADER_OFFSET_VERSION, QwpConstants.VERSION);
        Unsafe.putByte(address + QwpConstants.HEADER_OFFSET_FLAGS, QwpConstants.FLAG_CONTROL);
        Unsafe.putShort(address + QwpConstants.HEADER_OFFSET_TABLE_COUNT, (short) 0);
        Unsafe.putInt(address + QwpConstants.HEADER_OFFSET_PAYLOAD_LENGTH, payloadLength);
    }

    private static long writeResult(long address, long requestId, int result) {
        Unsafe.putByte(address, (byte) KIND_SCHEMA);
        Unsafe.putLong(address + 1, requestId);
        Unsafe.putByte(address + 1 + Long.BYTES, (byte) result);
        return Numbers.encodeLowHighInts(RESULT_PAYLOAD_SIZE, result);
    }

    /**
     * Writes one SCHEMA payload, without the QWP frame header, at {@code address}.
     * A result-only payload is always written, so {@code limit} must be at
     * least {@link #RESULT_PAYLOAD_SIZE}.
     *
     * @return payload length in the low int and the {@code RESULT_*} code in the high int
     */
    private static long writeSchemaPayload(
            CairoEngine engine,
            SecurityContext securityContext,
            CharSequence tableName,
            long requestId,
            long address,
            int limit
    ) {
        assert limit >= RESULT_PAYLOAD_SIZE;
        if (engine.isReadOnlyMode()) {
            return writeResult(address, requestId, RESULT_UNAVAILABLE);
        }
        TableToken token = engine.getTableTokenIfExists(tableName);
        if (token == null) {
            return writeResult(address, requestId, RESULT_MISSING);
        }
        try {
            securityContext.authorizeInsert(token);
        } catch (CairoException e) {
            return writeResult(address, requestId, e.isAuthorizationError() ? RESULT_DENIED : RESULT_UNAVAILABLE);
        }
        try (TableRecordMetadata metadata = engine.getLegacyMetadata(token)) {
            long hi = address + limit;
            long p = address + KNOWN_PAYLOAD_PREFIX;
            if (p > hi) {
                return writeResult(address, requestId, RESULT_TOO_LARGE);
            }
            int timestampIndex = metadata.getTimestampIndex();
            int compactTimestampIndex = -1;
            int activeCount = 0;
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                int type = metadata.getColumnType(i);
                if (type <= 0) {
                    continue;
                }
                if (i == timestampIndex) {
                    compactTimestampIndex = activeCount;
                }
                if (++activeCount > QwpConstants.MAX_COLUMNS_PER_TABLE) {
                    return writeResult(address, requestId, RESULT_TOO_LARGE);
                }
                CharSequence columnName = metadata.getColumnName(i);
                if (columnName.length() > MAX_NAME_CHARS) {
                    return writeResult(address, requestId, RESULT_TOO_LARGE);
                }
                int nameBytes = Utf8s.utf8Bytes(columnName);
                if (hi - p < COLUMN_ENTRY_OVERHEAD + nameBytes) {
                    return writeResult(address, requestId, RESULT_TOO_LARGE);
                }
                Unsafe.putShort(p, (short) nameBytes);
                p += Short.BYTES;
                Utf8s.strCpyUtf8(columnName, p, nameBytes);
                p += nameBytes;
                Unsafe.putInt(p, type);
                p += Integer.BYTES;
                Unsafe.putShort(p, (short) 0);
                p += Short.BYTES;
            }
            writeResult(address, requestId, RESULT_KNOWN);
            long q = address + RESULT_PAYLOAD_SIZE;
            Unsafe.putInt(q, metadata.getTableId());
            q += Integer.BYTES;
            Unsafe.putLong(q, metadata.getMetadataVersion());
            q += Long.BYTES;
            Unsafe.putShort(q, (short) compactTimestampIndex);
            q += Short.BYTES;
            Unsafe.putShort(q, (short) activeCount);
            return Numbers.encodeLowHighInts((int) (p - address), RESULT_KNOWN);
        } catch (CairoException e) {
            return writeResult(address, requestId, e.isAuthorizationError() ? RESULT_DENIED : RESULT_UNAVAILABLE);
        }
    }
}
