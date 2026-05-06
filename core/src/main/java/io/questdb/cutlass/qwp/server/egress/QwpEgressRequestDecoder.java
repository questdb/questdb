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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.griffin.SqlException;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
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

    /**
     * Reusable composite select-cache key built from the current request's
     * bind-variable types and SQL text. Filled by
     * {@link #buildSelectCacheKey(BindVariableService)} and consumed by the
     * caller for {@code selectCache.poll} and, indirectly, for
     * {@code selectCache.put} via the SQL-text slot on
     * {@code QwpEgressProcessorState}.
     */
    public final StringSink selectCacheKey = new StringSink();
    public final StringSink sql = new StringSink();
    /**
     * Reusable sink passed to {@link BindVariableService#setStr}. The
     * implementation copies the value out, so we can safely reuse this
     * flyweight across binds.
     */
    private final StringSink stringBindScratch = new StringSink();
    /**
     * Reusable view passed to {@link BindVariableService#setVarchar}. The
     * implementation copies the value out, so we can safely reuse this
     * flyweight across binds.
     */
    private final DirectUtf8String varcharBindView = new DirectUtf8String();
    /**
     * Reusable scratch for {@link QwpVarint#decode(long, long, QwpVarint.DecodeResult)}.
     * Holding it as a field avoids a per-varint allocation and lets the canonical-byte-count
     * trap from {@code QwpVarint.encodedLength} be replaced with the actual bytes-consumed
     * count returned by the decoder.
     */
    private final QwpVarint.DecodeResult varintScratch = new QwpVarint.DecodeResult();
    public long initialCredit;
    public long requestId;
    /**
     * Reusable scratch for the parsed null flag that {@link #readNullFlag} writes
     * into. Holding it as a field removes the {@code boolean[1]} allocation per bind.
     */
    private boolean bindIsNull;

    /**
     * Builds the select-cache key for the just-decoded request. Returns the SQL
     * text unchanged when there are no binds (preserving the existing
     * SQL-as-cache-key shape for bindless queries). When binds are present,
     * prefixes the SQL text with a bracketed list of ColumnType ints so
     * factories compiled against different bind signatures occupy different
     * cache slots. Mirrors pgwire's {@code TypesAndSelect} contract.
     *
     * <p>Call AFTER {@link #decodeQueryRequest} has pushed the bind types into
     * the service; the returned CharSequence is valid until the next call to
     * this method on the same decoder instance.
     */
    public CharSequence buildSelectCacheKey(BindVariableService bindVars) {
        int bindCount = bindVars.getIndexedVariableCount();
        if (bindCount == 0) {
            return sql;
        }
        selectCacheKey.clear();
        selectCacheKey.put('[');
        for (int i = 0; i < bindCount; i++) {
            if (i > 0) {
                selectCacheKey.put(',');
            }
            selectCacheKey.put(bindVars.getFunction(i).getType());
        }
        selectCacheKey.put(']').put(sql);
        return selectCacheKey;
    }

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
        return Unsafe.getLong(payload + 1);
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
        long credit = QwpVarint.decode(payload + 9, limit);
        // A varint whose decoded long has the sign bit set would otherwise flow
        // into flow-control accounting as a negative budget. Reject explicitly
        // so credit bookkeeping never underflows.
        if (credit < 0) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA)
                    .put("CREDIT: additional_bytes must be non-negative: ").put(credit);
        }
        return credit;
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
        requestId = Unsafe.getLong(p);
        p += 8;

        QwpVarint.decode(p, limit, varintScratch);
        long sqlLen = varintScratch.value;
        p += varintScratch.bytesRead;
        // Reject negative or oversized sql_len explicitly: a hostile varint can encode a 64-bit
        // signed value with the sign bit set. Without this guard, "p + sqlLen > limit" wraps for
        // negative sqlLen, the bounds check passes silently, and Utf8s.utf8ToUtf16 runs with
        // end < start. Cap by remaining frame bytes (limit - p) which is a small positive long.
        if (sqlLen < 0 || sqlLen > (limit - p)) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("QUERY_REQUEST: sql_len out of range: ").put(sqlLen);
        }
        sql.clear();
        Utf8s.utf8ToUtf16(p, p + sqlLen, sql);
        p += sqlLen;

        QwpVarint.decode(p, limit, varintScratch);
        initialCredit = varintScratch.value;
        p += varintScratch.bytesRead;
        if (initialCredit < 0) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("QUERY_REQUEST: initial_credit must be non-negative: ").put(initialCredit);
        }

        QwpVarint.decode(p, limit, varintScratch);
        long bindCount = varintScratch.value;
        p += varintScratch.bytesRead;
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
        return Unsafe.getByte(payload);
    }

    /**
     * Resets reusable state. Bind variable service is cleared in {@link #decodeQueryRequest}.
     */
    public void reset() {
        sql.clear();
        selectCacheKey.clear();
        requestId = 0;
        initialCredit = 0;
    }

    private long decodeBind(long start, long limit, int index, BindVariableService bindVars)
            throws QwpParseException, SqlException {
        if (start >= limit) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind ").put(index).put(": truncated type code");
        }
        byte type = Unsafe.getByte(start);
        long p = readNullFlag(start + 1, limit);
        boolean isNull = bindIsNull;

        switch (type) {
            case QwpConstants.TYPE_BOOLEAN -> {
                if (isNull) {
                    bindVars.setBoolean(index);
                } else {
                    if (p >= limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated BOOLEAN");
                    byte bits = Unsafe.getByte(p++);
                    bindVars.setBoolean(index, (bits & 0x01) != 0);
                }
            }
            case QwpConstants.TYPE_BYTE -> {
                if (isNull) {
                    bindVars.setByte(index);
                } else {
                    if (p >= limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated BYTE");
                    bindVars.setByte(index, Unsafe.getByte(p++));
                }
            }
            case QwpConstants.TYPE_SHORT -> {
                if (isNull) {
                    bindVars.setShort(index);
                } else {
                    if (p + 2 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated SHORT");
                    bindVars.setShort(index, Unsafe.getShort(p));
                    p += 2;
                }
            }
            case QwpConstants.TYPE_CHAR -> {
                if (isNull) {
                    bindVars.setChar(index);
                } else {
                    if (p + 2 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated CHAR");
                    bindVars.setChar(index, (char) Unsafe.getShort(p));
                    p += 2;
                }
            }
            case QwpConstants.TYPE_INT -> {
                if (isNull) {
                    bindVars.setInt(index);
                } else {
                    if (p + 4 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated INT");
                    bindVars.setInt(index, Unsafe.getInt(p));
                    p += 4;
                }
            }
            case QwpConstants.TYPE_LONG -> {
                if (isNull) {
                    bindVars.setLong(index);
                } else {
                    if (p + 8 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated LONG");
                    bindVars.setLong(index, Unsafe.getLong(p));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_DATE -> {
                if (isNull) {
                    bindVars.setDate(index);
                } else {
                    if (p + 8 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DATE");
                    bindVars.setDate(index, Unsafe.getLong(p));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_TIMESTAMP -> {
                if (isNull) {
                    bindVars.setTimestamp(index);
                } else {
                    if (p + 8 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated TIMESTAMP");
                    bindVars.setTimestamp(index, Unsafe.getLong(p));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_TIMESTAMP_NANOS -> {
                // TIMESTAMP (micros) and TIMESTAMP_NANOS are distinct QuestDB types with different
                // unit-of-time semantics. Routing both through setTimestamp() would tag the bind as
                // TIMESTAMP_MICRO and downstream coercion (NanosTimestampDriver.from) would
                // multiply the nanos value by 1000 when the placeholder column is TIMESTAMP_NANO.
                if (isNull) {
                    bindVars.setTimestampNano(index);
                } else {
                    if (p + 8 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated TIMESTAMP_NANOS");
                    bindVars.setTimestampNano(index, Unsafe.getLong(p));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_FLOAT -> {
                if (isNull) {
                    bindVars.setFloat(index);
                } else {
                    if (p + 4 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated FLOAT");
                    bindVars.setFloat(index, Float.intBitsToFloat(Unsafe.getInt(p)));
                    p += 4;
                }
            }
            case QwpConstants.TYPE_DOUBLE -> {
                if (isNull) {
                    bindVars.setDouble(index);
                } else {
                    if (p + 8 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DOUBLE");
                    bindVars.setDouble(index, Double.longBitsToDouble(Unsafe.getLong(p)));
                    p += 8;
                }
            }
            // SYMBOL bind: single UTF-8 value, same (N+1) x uint32 offsets layout as VARCHAR
            // but decoded into a String bind so the planner sees a CharSequence-typed placeholder.
            case QwpConstants.TYPE_SYMBOL -> {
                if (isNull) {
                    bindVars.setStr(index);
                } else {
                    // (N+1) x uint32 offsets where N=1 -> 2 offsets = 8 bytes.
                    // Spec: offset[0] must be 0 and offset[1] must be >= 0 (treated as uint32
                    // but Java reads signed int, so values >= 2^31 would come through as negative
                    // and must be rejected).
                    if (p + 8 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated SYMBOL offsets");
                    int offset0 = Unsafe.getInt(p);
                    int strLen = Unsafe.getInt(p + 4);
                    if (offset0 != 0 || strLen < 0)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY)
                                .put("bind: SYMBOL offsets invalid [offset0=").put(offset0).put(", strLen=").put(strLen).put("]");
                    p += 8;
                    if (p + strLen > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated SYMBOL bytes");
                    // Reuse stringBindScratch -- StrBindVariable.setValue copies the CharSequence
                    // into its own utf16Sink, so the scratch can be freely reused for the next bind.
                    stringBindScratch.clear();
                    Utf8s.utf8ToUtf16(p, p + strLen, stringBindScratch);
                    bindVars.setStr(index, stringBindScratch);
                    p += strLen;
                }
            }
            case QwpConstants.TYPE_VARCHAR -> {
                if (isNull) {
                    bindVars.setVarchar(index);
                } else {
                    if (p + 8 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated VARCHAR offsets");
                    int offset0 = Unsafe.getInt(p);
                    int strLen = Unsafe.getInt(p + 4);
                    if (offset0 != 0 || strLen < 0)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INVALID_OFFSET_ARRAY)
                                .put("bind: VARCHAR offsets invalid [offset0=").put(offset0).put(", strLen=").put(strLen).put("]");
                    p += 8;
                    if (p + strLen > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated VARCHAR bytes");
                    // Reuse varcharBindView -- StrBindVariable.setValue(Utf8Sequence) copies into
                    // its own utf8Sink, so the view can be re-pointed for the next bind.
                    bindVars.setVarchar(index, varcharBindView.of(p, p + strLen));
                    p += strLen;
                }
            }
            case QwpConstants.TYPE_UUID -> {
                if (isNull) {
                    bindVars.setUuid(index, Numbers.LONG_NULL, Numbers.LONG_NULL);
                } else {
                    if (p + 16 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated UUID");
                    long lo = Unsafe.getLong(p);
                    long hi = Unsafe.getLong(p + 8);
                    bindVars.setUuid(index, lo, hi);
                    p += 16;
                }
            }
            case QwpConstants.TYPE_LONG256 -> {
                if (isNull) {
                    bindVars.setLong256(index);
                } else {
                    if (p + 32 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated LONG256");
                    long l0 = Unsafe.getLong(p);
                    long l1 = Unsafe.getLong(p + 8);
                    long l2 = Unsafe.getLong(p + 16);
                    long l3 = Unsafe.getLong(p + 24);
                    bindVars.setLong256(index, l0, l1, l2, l3);
                    p += 32;
                }
            }
            case QwpConstants.TYPE_GEOHASH -> {
                QwpVarint.decode(p, limit, varintScratch);
                long precisionBits = varintScratch.value;
                p += varintScratch.bytesRead;
                // ColumnType.getGeoHashTypeWithBits guards bits with `assert` only, and
                // pow2SizeOfBits indexes a 61-entry array without a runtime bound. With -ea off
                // an out-of-range precisionBits produces a malformed GeoHash column type or
                // AIOOBE. Validate explicitly against the documented 1..60 range.
                if (precisionBits < 1 || precisionBits > ColumnType.GEOLONG_MAX_BITS) {
                    throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA)
                            .put("bind ").put(index).put(": GEOHASH precision_bits out of range (1..")
                            .put(ColumnType.GEOLONG_MAX_BITS).put("): ").put(precisionBits);
                }
                int bytesPerValue = (int) ((precisionBits + 7) >>> 3);
                int geoType = ColumnType.getGeoHashTypeWithBits((int) precisionBits);
                if (isNull) {
                    bindVars.setGeoHash(index, geoType);
                } else {
                    if (p + bytesPerValue > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated GEOHASH");
                    long bits = 0;
                    for (int b = 0; b < bytesPerValue; b++) {
                        bits |= ((long) (Unsafe.getByte(p + b) & 0xFF)) << (b * 8);
                    }
                    bindVars.setGeoHash(index, bits, geoType);
                    p += bytesPerValue;
                }
            }
            case QwpConstants.TYPE_DECIMAL64 -> {
                if (p >= limit)
                    throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL64 scale");
                int scale = Unsafe.getByte(p++) & 0xFF;
                validateDecimalScale(index, scale);
                if (isNull) {
                    bindVars.setDecimal(index, 0, 0, 0, Decimals.DECIMAL64_NULL,
                            ColumnType.getDecimalType(18, scale));
                } else {
                    if (p + 8 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL64 value");
                    long ll = Unsafe.getLong(p);
                    bindVars.setDecimal(index, 0, 0, 0, ll,
                            ColumnType.getDecimalType(18, scale));
                    p += 8;
                }
            }
            case QwpConstants.TYPE_DECIMAL128 -> {
                if (p >= limit)
                    throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL128 scale");
                int scale = Unsafe.getByte(p++) & 0xFF;
                validateDecimalScale(index, scale);
                int decimalType = ColumnType.getDecimalType(38, scale);
                if (isNull) {
                    // Canonical DECIMAL128 NULL is (HI=Long.MIN_VALUE, LO=0). The setDecimal
                    // parameter order is (hh, hl, lh, ll) where lh holds the HI half and ll the LO.
                    bindVars.setDecimal(index, 0, 0, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, decimalType);
                } else {
                    if (p + 16 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL128 value");
                    long lo = Unsafe.getLong(p);
                    long hi = Unsafe.getLong(p + 8);
                    bindVars.setDecimal(index, 0, 0, hi, lo, decimalType);
                    p += 16;
                }
            }
            case QwpConstants.TYPE_DECIMAL256 -> {
                if (p >= limit)
                    throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL256 scale");
                int scale = Unsafe.getByte(p++) & 0xFF;
                validateDecimalScale(index, scale);
                // DECIMAL256 stores values that fit in 76 digits of precision
                // (see Decimals.MAX_PRECISION). getDecimalType asserts on > MAX_PRECISION.
                int decimalType = ColumnType.getDecimalType(Decimals.MAX_PRECISION, scale);
                if (isNull) {
                    // Canonical DECIMAL256 NULL is (HH=Long.MIN_VALUE, HL=0, LH=0, LL=0).
                    // Writing LONG_NULL in all four slots produces a different 256-bit value
                    // that Decimals.isNull*() will NOT recognise as NULL.
                    bindVars.setDecimal(index,
                            Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                            Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL, decimalType);
                } else {
                    if (p + 32 > limit)
                        throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated DECIMAL256 value");
                    long ll = Unsafe.getLong(p);
                    long lh = Unsafe.getLong(p + 8);
                    long hl = Unsafe.getLong(p + 16);
                    long hh = Unsafe.getLong(p + 24);
                    bindVars.setDecimal(index, hh, hl, lh, ll, decimalType);
                    p += 32;
                }
            }
            case QwpConstants.TYPE_DOUBLE_ARRAY, QwpConstants.TYPE_LONG_ARRAY ->
                    throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA)
                            .put("bind ").put(index).put(": ARRAY bind parameters not yet supported in Phase 1 egress");
            default -> throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA)
                    .put("bind ").put(index).put(": unsupported wire type 0x").put(Integer.toHexString(type & 0xFF));
        }
        return p;
    }

    /**
     * Reads the null flag byte and (if present) the 1-byte single-row bitmap.
     * Stores the parsed null status in {@link #bindIsNull} and returns the
     * position just past the null section. Zero allocation.
     */
    private long readNullFlag(long start, long limit) throws QwpParseException {
        if (start >= limit) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated null flag");
        }
        byte flag = Unsafe.getByte(start);
        long p = start + 1;
        if (flag == 0) {
            bindIsNull = false;
            return p;
        }
        if (p >= limit) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA).put("bind: truncated null bitmap");
        }
        byte bitmap = Unsafe.getByte(p);
        bindIsNull = (bitmap & 0x01) != 0;
        return p + 1;
    }

    // ColumnType.getDecimalType asserts the range but does not enforce it at runtime.
    // Reject out-of-range scales here so a client-supplied byte never reaches the
    // assertion, which would otherwise surface as an uncategorised AssertionError.
    private static void validateDecimalScale(int bindIndex, int scale) throws QwpParseException {
        if (scale > Decimals.MAX_SCALE) {
            throw QwpParseException.instance(QwpParseException.ErrorCode.INSUFFICIENT_DATA)
                    .put("bind ").put(bindIndex).put(": DECIMAL scale out of range (0..")
                    .put(Decimals.MAX_SCALE).put("): ").put(scale);
        }
    }

}
