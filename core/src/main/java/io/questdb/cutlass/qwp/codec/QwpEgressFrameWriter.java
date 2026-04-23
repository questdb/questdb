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
import io.questdb.std.Unsafe;

/**
 * Static helpers for serialising outbound QWP egress frames directly into a
 * native buffer.
 * <p>
 * Layout written into a caller-owned native buffer:
 * <pre>
 *   [WebSocket frame header (reserved, patched last)]
 *   [QWP message header (12 bytes)]
 *   [msg_kind (1 byte) | request_id (8 bytes)]
 *   [batch_seq (varint)]                                 -- RESULT_BATCH only
 *   [table block body or control-message body]
 * </pre>
 * <p>
 * The caller reserves {@link #WS_HEADER_RESERVATION} bytes at the start of the
 * buffer so the WebSocket frame header can be written in-place once the payload
 * length is known, without any copy.
 */
public final class QwpEgressFrameWriter {

    /**
     * Reservation for the outbound WebSocket frame header. Enough for an
     * unmasked 64-bit extended-length binary frame (2-byte base header +
     * 8-byte extended length).
     */
    public static final int WS_HEADER_RESERVATION = 10;

    private QwpEgressFrameWriter() {
    }

    /**
     * Writes the fixed 12-byte QWP message header at {@code bufAddr}.
     *
     * @return offset just past the header (= bufAddr + 12)
     */
    public static long writeMessageHeader(long bufAddr, byte version, byte flags, int tableCount, int payloadLength) {
        Unsafe.getUnsafe().putInt(bufAddr + QwpConstants.HEADER_OFFSET_MAGIC, QwpConstants.MAGIC_MESSAGE);
        Unsafe.getUnsafe().putByte(bufAddr + QwpConstants.HEADER_OFFSET_VERSION, version);
        Unsafe.getUnsafe().putByte(bufAddr + QwpConstants.HEADER_OFFSET_FLAGS, flags);
        Unsafe.getUnsafe().putShort(bufAddr + QwpConstants.HEADER_OFFSET_TABLE_COUNT, (short) tableCount);
        Unsafe.getUnsafe().putInt(bufAddr + QwpConstants.HEADER_OFFSET_PAYLOAD_LENGTH, payloadLength);
        return bufAddr + QwpConstants.HEADER_SIZE;
    }

    /**
     * Writes the body of a {@code CACHE_RESET} frame: msg_kind + reset_mask.
     * {@code resetMask} is the bitwise OR of
     * {@link QwpEgressMsgKind#RESET_MASK_DICT} and
     * {@link QwpEgressMsgKind#RESET_MASK_SCHEMAS}.
     *
     * @return address just past the body
     */
    public static long writeCacheReset(long bufAddr, byte resetMask) {
        Unsafe.getUnsafe().putByte(bufAddr, QwpEgressMsgKind.CACHE_RESET);
        Unsafe.getUnsafe().putByte(bufAddr + 1, resetMask);
        return bufAddr + 2;
    }

    /**
     * Writes the body of an {@code EXEC_DONE} frame: msg_kind + request_id +
     * op_type (CompiledQuery.TYPE_*) + rows_affected (varint).
     *
     * @return address just past the body
     */
    public static long writeExecDone(long bufAddr, long requestId, short opType, long rowsAffected) {
        Unsafe.getUnsafe().putByte(bufAddr, QwpEgressMsgKind.EXEC_DONE);
        Unsafe.getUnsafe().putLong(bufAddr + 1, requestId);
        Unsafe.getUnsafe().putByte(bufAddr + 9, (byte) opType);
        return QwpVarint.encode(bufAddr + 10, rowsAffected);
    }

    /**
     * Patches the payload_length field of an already-written message header.
     */
    public static void patchPayloadLength(long msgHeaderAddr, int payloadLength) {
        Unsafe.getUnsafe().putInt(msgHeaderAddr + QwpConstants.HEADER_OFFSET_PAYLOAD_LENGTH, payloadLength);
    }

    /**
     * Writes the egress RESULT_BATCH prelude (msg_kind + request_id + batch_seq).
     *
     * @return address just past the prelude
     */
    public static long writeResultBatchPrelude(long bufAddr, long requestId, long batchSeq) {
        Unsafe.getUnsafe().putByte(bufAddr, QwpEgressMsgKind.RESULT_BATCH);
        Unsafe.getUnsafe().putLong(bufAddr + 1, requestId);
        return QwpVarint.encode(bufAddr + 9, batchSeq);
    }

    /**
     * Writes a RESULT_END frame body: msg_kind + request_id + final_seq + total_rows.
     *
     * @return address just past the body
     */
    public static long writeResultEnd(long bufAddr, long requestId, long finalSeq, long totalRows) {
        Unsafe.getUnsafe().putByte(bufAddr, QwpEgressMsgKind.RESULT_END);
        Unsafe.getUnsafe().putLong(bufAddr + 1, requestId);
        long p = QwpVarint.encode(bufAddr + 9, finalSeq);
        return QwpVarint.encode(p, totalRows);
    }

    /**
     * Writes a QUERY_ERROR frame body from a {@link CharSequence}, encoding UTF-8
     * directly into {@code bufAddr} without the intermediate {@code String} +
     * {@code byte[]} allocation. Truncates if the encoded UTF-8 exceeds {@code msgCapBytes}
     * (caller chooses; spec caps the wire field at 65535 since msg_len is u16).
     *
     * @return address just past the body
     */
    public static long writeQueryError(long bufAddr, long requestId, byte status, CharSequence msg, int msgCapBytes) {
        Unsafe.getUnsafe().putByte(bufAddr, QwpEgressMsgKind.QUERY_ERROR);
        Unsafe.getUnsafe().putLong(bufAddr + 1, requestId);
        Unsafe.getUnsafe().putByte(bufAddr + 9, status);
        long bytesStart = bufAddr + 12;
        int written = 0;
        if (msg != null) {
            int charLen = msg.length();
            int cap = Math.min(msgCapBytes, 0xFFFF);
            for (int i = 0; i < charLen && written < cap; i++) {
                char c = msg.charAt(i);
                if (c < 0x80) {
                    if (written + 1 > cap) break;
                    Unsafe.getUnsafe().putByte(bytesStart + written, (byte) c);
                    written++;
                } else if (c < 0x800) {
                    if (written + 2 > cap) break;
                    Unsafe.getUnsafe().putByte(bytesStart + written, (byte) (0xC0 | (c >> 6)));
                    Unsafe.getUnsafe().putByte(bytesStart + written + 1, (byte) (0x80 | (c & 0x3F)));
                    written += 2;
                } else if (Character.isHighSurrogate(c) && i + 1 < charLen
                        && Character.isLowSurrogate(msg.charAt(i + 1))) {
                    if (written + 4 > cap) break;
                    int cp = Character.toCodePoint(c, msg.charAt(i + 1));
                    i++;
                    Unsafe.getUnsafe().putByte(bytesStart + written, (byte) (0xF0 | (cp >> 18)));
                    Unsafe.getUnsafe().putByte(bytesStart + written + 1, (byte) (0x80 | ((cp >> 12) & 0x3F)));
                    Unsafe.getUnsafe().putByte(bytesStart + written + 2, (byte) (0x80 | ((cp >> 6) & 0x3F)));
                    Unsafe.getUnsafe().putByte(bytesStart + written + 3, (byte) (0x80 | (cp & 0x3F)));
                    written += 4;
                } else {
                    if (written + 3 > cap) break;
                    Unsafe.getUnsafe().putByte(bytesStart + written, (byte) (0xE0 | (c >> 12)));
                    Unsafe.getUnsafe().putByte(bytesStart + written + 1, (byte) (0x80 | ((c >> 6) & 0x3F)));
                    Unsafe.getUnsafe().putByte(bytesStart + written + 2, (byte) (0x80 | (c & 0x3F)));
                    written += 3;
                }
            }
        }
        Unsafe.getUnsafe().putShort(bufAddr + 10, (short) written);
        return bytesStart + written;
    }

}
