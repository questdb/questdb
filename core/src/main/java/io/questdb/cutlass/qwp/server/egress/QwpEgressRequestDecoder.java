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

package io.questdb.cutlass.qwp.server.egress;

import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

/**
 * Stateful decoder for inbound QWP egress messages.
 * <p>
 * Reusable across requests on a single connection. After {@link #decodeQueryRequest},
 * the public fields hold the parsed request and bind parameters have been pushed
 * into the supplied {@link BindVariableService}.
 * <p>
 * Phase-1 scope: QUERY_REQUEST, CANCEL, CREDIT. ARRAY bind parameters are not
 * yet supported and trigger a parse error.
 */
public class QwpEgressRequestDecoder {

    public final StringSink sql = new StringSink();
    public long initialCredit;
    public long requestId;

    /**
     * Decodes a CANCEL frame body. Caller has already verified msg_kind == 0x14
     * via {@link #peekMsgKind(long)}.
     *
     * @return the request_id of the query to cancel
     */
    public long decodeCancel(long payload, int payloadLen) throws QwpParseException {
        if (payloadLen < 9) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("CANCEL frame too short: ").put(payloadLen);
        }
        return Unsafe.getUnsafe().getLong(payload + 1);
    }

    /**
     * Decodes a CREDIT frame body. Returns the additional bytes to grant; the
     * caller pulls request_id via {@link Unsafe} or holds it externally.
     */
    public long decodeCredit(long payload, int payloadLen) throws QwpParseException {
        if (payloadLen < 10) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("CREDIT frame too short: ").put(payloadLen);
        }
        long limit = payload + payloadLen;
        return QwpVarint.decode(payload + 9, limit);
    }

    /**
     * Decodes a QUERY_REQUEST payload starting at {@code payload}, of length
     * {@code payloadLen}. The first byte (msg_kind) must already be QUERY_REQUEST.
     * <p>
     * Populates {@link #requestId}, {@link #sql}, {@link #initialCredit}, and
     * pushes bind parameters into {@code bindVars}.
     */
    public void decodeQueryRequest(long payload, int payloadLen, BindVariableService bindVars)
            throws QwpParseException, SqlException {
        long limit = payload + payloadLen;
        long p = payload + 1; // skip msg_kind
        if (p + 8 > limit) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("QUERY_REQUEST: header truncated");
        }
        requestId = Unsafe.getUnsafe().getLong(p);
        p += 8;

        long sqlLen = QwpVarint.decode(p, limit);
        p = advanceVarint(p, sqlLen);
        if (p + sqlLen > limit) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("QUERY_REQUEST: SQL truncated");
        }
        sql.clear();
        Utf8s.utf8ToUtf16(p, p + sqlLen, sql);
        p += sqlLen;

        initialCredit = QwpVarint.decode(p, limit);
        p = advanceVarint(p, initialCredit);

        long bindCount = QwpVarint.decode(p, limit);
        p = advanceVarint(p, bindCount);
        if (bindCount < 0 || bindCount > QwpConstants.MAX_COLUMNS_PER_TABLE) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("QUERY_REQUEST: bind_count out of range: ").put(bindCount);
        }

        bindVars.clear();
        for (int i = 0; i < (int) bindCount; i++) {
            p = decodeBind(p, limit, i, bindVars);
        }
    }

    /**
     * Returns the {@link QwpEgressMsgKind} byte at the start of the payload.
     */
    public byte peekMsgKind(long payload) {
        return Unsafe.getUnsafe().getByte(payload);
    }

    /**
     * Resets reusable state. Bind variable service is cleared in {@link #decodeQueryRequest}.
     */
    public void reset() {
        sql.clear();
        requestId = 0;
        initialCredit = 0;
    }

    private static long advanceVarint(long p, long decodedValue) {
        return p + QwpVarint.encodedLength(decodedValue);
    }

    /**
     * Reads the null flag byte and (if present) the 1-byte single-row bitmap.
     * Returns true if the bind value is NULL.
     */
    private static boolean readNullFlag(long[] cursor, long limit) throws QwpParseException {
        long p = cursor[0];
        if (p >= limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated null flag");
        byte flag = Unsafe.getUnsafe().getByte(p++);
        if (flag == 0) {
            cursor[0] = p;
            return false;
        }
        if (p >= limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated null bitmap");
        byte bitmap = Unsafe.getUnsafe().getByte(p++);
        cursor[0] = p;
        return (bitmap & 0x01) != 0;
    }

    private long decodeBind(long start, long limit, int index, BindVariableService bindVars)
            throws QwpParseException, SqlException {
        if (start >= limit) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind ").put(index).put(": truncated type code");
        }
        byte type = Unsafe.getUnsafe().getByte(start);
        long[] cursor = {start + 1};
        boolean isNull = readNullFlag(cursor, limit);
        long p = cursor[0];

        switch (type) {
            case QwpConstants.TYPE_BOOLEAN -> {
                if (isNull) {
                    bindVars.setBoolean(index);
                } else {
                    if (p >= limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated BOOLEAN");
                    byte bits = Unsafe.getUnsafe().getByte(p++);
                    bindVars.setBoolean(index, (bits & 0x01) != 0);
                }
            }
            case QwpConstants.TYPE_BYTE -> {
                if (isNull) {
                    bindVars.setByte(index);
                } else {
                    if (p >= limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated BYTE");
                    bindVars.setByte(index, Unsafe.getUnsafe().getByte(p++));
                }
            }
            case QwpConstants.TYPE_SHORT -> {
                if (isNull) {
                    bindVars.setShort(index);
                } else {
                    if (p + 2 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated SHORT");
                    bindVars.setShort(index, Unsafe.getUnsafe().getShort(p));
                    p += 2;
                }
            }
            case QwpConstants.TYPE_CHAR -> {
                if (isNull) {
                    bindVars.setChar(index);
                } else {
                    if (p + 2 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated CHAR");
                    bindVars.setChar(index, (char) Unsafe.getUnsafe().getShort(p));
                    p += 2;
                }
            }
            case QwpConstants.TYPE_INT -> {
                if (isNull) {
                    bindVars.setInt(index);
                } else {
                    if (p + 4 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated INT");
                    bindVars.setInt(index, Unsafe.getUnsafe().getInt(p));
                    p += 4;
                }
            }
            case QwpConstants.TYPE_LONG -> {
                if (isNull) {
                    bindVars.setLong(index);
                } else {
                    if (p + 8 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated LONG");
                    bindVars.setLong(index, Unsafe.getUnsafe().getLong(p));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_DATE -> {
                if (isNull) {
                    bindVars.setDate(index);
                } else {
                    if (p + 8 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DATE");
                    bindVars.setDate(index, Unsafe.getUnsafe().getLong(p));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_TIMESTAMP, QwpConstants.TYPE_TIMESTAMP_NANOS -> {
                if (isNull) {
                    bindVars.setTimestamp(index);
                } else {
                    if (p + 8 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated TIMESTAMP");
                    bindVars.setTimestamp(index, Unsafe.getUnsafe().getLong(p));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_FLOAT -> {
                if (isNull) {
                    bindVars.setFloat(index);
                } else {
                    if (p + 4 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated FLOAT");
                    bindVars.setFloat(index, Float.intBitsToFloat(Unsafe.getUnsafe().getInt(p)));
                    p += 4;
                }
            }
            case QwpConstants.TYPE_DOUBLE -> {
                if (isNull) {
                    bindVars.setDouble(index);
                } else {
                    if (p + 8 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DOUBLE");
                    bindVars.setDouble(index, Double.longBitsToDouble(Unsafe.getUnsafe().getLong(p)));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_STRING, QwpConstants.TYPE_SYMBOL -> {
                if (isNull) {
                    bindVars.setStr(index);
                } else {
                    // (N+1) x uint32 offsets where N=1 → 2 offsets = 8 bytes
                    if (p + 8 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated STRING offsets");
                    int strLen = Unsafe.getUnsafe().getInt(p + 4); // offset[1] - offset[0] (offset[0] = 0)
                    p += 8;
                    if (p + strLen > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated STRING bytes");
                    StringSink scratch = scratchSink();
                    scratch.clear();
                    Utf8s.utf8ToUtf16(p, p + strLen, scratch);
                    bindVars.setStr(index, scratch.toString());
                    p += strLen;
                }
            }
            case QwpConstants.TYPE_VARCHAR -> {
                if (isNull) {
                    bindVars.setVarchar(index);
                } else {
                    if (p + 8 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated VARCHAR offsets");
                    int strLen = Unsafe.getUnsafe().getInt(p + 4);
                    p += 8;
                    if (p + strLen > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated VARCHAR bytes");
                    // BindVariableService.setVarchar wants Utf8Sequence; build a copy.
                    bindVars.setVarchar(index, new io.questdb.std.str.Utf8String(copyBytes(p, strLen), false));
                    p += strLen;
                }
            }
            case QwpConstants.TYPE_UUID -> {
                if (isNull) {
                    bindVars.setUuid(index, Numbers.LONG_NULL, Numbers.LONG_NULL);
                } else {
                    if (p + 16 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated UUID");
                    long lo = Unsafe.getUnsafe().getLong(p);
                    long hi = Unsafe.getUnsafe().getLong(p + 8);
                    bindVars.setUuid(index, lo, hi);
                    p += 16;
                }
            }
            case QwpConstants.TYPE_LONG256 -> {
                if (isNull) {
                    bindVars.setLong256(index);
                } else {
                    if (p + 32 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated LONG256");
                    long l0 = Unsafe.getUnsafe().getLong(p);
                    long l1 = Unsafe.getUnsafe().getLong(p + 8);
                    long l2 = Unsafe.getUnsafe().getLong(p + 16);
                    long l3 = Unsafe.getUnsafe().getLong(p + 24);
                    bindVars.setLong256(index, l0, l1, l2, l3);
                    p += 32;
                }
            }
            case QwpConstants.TYPE_GEOHASH -> {
                long precisionBits = QwpVarint.decode(p, limit);
                p = advanceVarint(p, precisionBits);
                int bytesPerValue = (int) ((precisionBits + 7) >>> 3);
                int geoType = io.questdb.cairo.ColumnType.getGeoHashTypeWithBits((int) precisionBits);
                if (isNull) {
                    bindVars.setGeoHash(index, geoType);
                } else {
                    if (p + bytesPerValue > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated GEOHASH");
                    long bits = 0;
                    for (int b = 0; b < bytesPerValue; b++) {
                        bits |= ((long) (Unsafe.getUnsafe().getByte(p + b) & 0xFF)) << (b * 8);
                    }
                    bindVars.setGeoHash(index, bits, geoType);
                    p += bytesPerValue;
                }
            }
            case QwpConstants.TYPE_DECIMAL64 -> {
                if (p >= limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL64 scale");
                int scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
                if (isNull) {
                    bindVars.setDecimal(index, 0, 0, 0, Numbers.LONG_NULL,
                            io.questdb.cairo.ColumnType.getDecimalType(18, scale));
                } else {
                    if (p + 8 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL64 value");
                    long ll = Unsafe.getUnsafe().getLong(p);
                    bindVars.setDecimal(index, 0, 0, 0, ll,
                            io.questdb.cairo.ColumnType.getDecimalType(18, scale));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_DECIMAL128 -> {
                if (p >= limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL128 scale");
                int scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
                int decimalType = io.questdb.cairo.ColumnType.getDecimalType(38, scale);
                if (isNull) {
                    bindVars.setDecimal(index, 0, 0, Numbers.LONG_NULL, Numbers.LONG_NULL, decimalType);
                } else {
                    if (p + 16 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL128 value");
                    long lo = Unsafe.getUnsafe().getLong(p);
                    long hi = Unsafe.getUnsafe().getLong(p + 8);
                    bindVars.setDecimal(index, 0, 0, hi, lo, decimalType);
                    p += 16;
                }
            }
            case QwpConstants.TYPE_DECIMAL256 -> {
                if (p >= limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL256 scale");
                int scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
                int decimalType = io.questdb.cairo.ColumnType.getDecimalType(77, scale);
                if (isNull) {
                    bindVars.setDecimal(index,
                            Numbers.LONG_NULL, Numbers.LONG_NULL,
                            Numbers.LONG_NULL, Numbers.LONG_NULL, decimalType);
                } else {
                    if (p + 32 > limit) throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL256 value");
                    long ll = Unsafe.getUnsafe().getLong(p);
                    long lh = Unsafe.getUnsafe().getLong(p + 8);
                    long hl = Unsafe.getUnsafe().getLong(p + 16);
                    long hh = Unsafe.getUnsafe().getLong(p + 24);
                    bindVars.setDecimal(index, hh, hl, lh, ll, decimalType);
                    p += 32;
                }
            }
            case QwpConstants.TYPE_DOUBLE_ARRAY, QwpConstants.TYPE_LONG_ARRAY ->
                    throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA)
                            .put("bind ").put(index).put(": ARRAY bind parameters not yet supported in Phase 1 egress");
            default ->
                    throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA)
                            .put("bind ").put(index).put(": unsupported wire type 0x").put(Integer.toHexString(type & 0xFF));
        }
        return p;
    }

    private byte[] copyBytes(long src, int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = Unsafe.getUnsafe().getByte(src + i);
        }
        return out;
    }

    private final StringSink scratchSink = new StringSink();

    private StringSink scratchSink() {
        return scratchSink;
    }
}
