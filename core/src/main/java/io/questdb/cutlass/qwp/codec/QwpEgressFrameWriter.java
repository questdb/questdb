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
import io.questdb.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.std.Unsafe;

/**
 * Static helpers for serialising outbound QWP egress frames directly into a
 * native buffer.
 * <p>
 * Layout written into a caller-owned native buffer:
 * <pre>
 *   [WebSocket frame header (reserved, patched last)]
 *   [QWP message header (12 bytes)]
 *   [msg_kind (1 byte) | request_id (8 bytes) | batch_seq (varint)]   -- for RESULT_BATCH only
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
     * Writes a QUERY_ERROR frame body: msg_kind + request_id + status + msg_len (u16) + msg_bytes (UTF-8).
     * Truncates the message to 64 KiB - 1 to fit msg_len.
     *
     * @return address just past the body
     */
    public static long writeQueryError(long bufAddr, long requestId, byte status, byte[] msgBytes) {
        Unsafe.getUnsafe().putByte(bufAddr, QwpEgressMsgKind.QUERY_ERROR);
        Unsafe.getUnsafe().putLong(bufAddr + 1, requestId);
        Unsafe.getUnsafe().putByte(bufAddr + 9, status);
        int msgLen = Math.min(msgBytes.length, 0xFFFF);
        Unsafe.getUnsafe().putShort(bufAddr + 10, (short) msgLen);
        for (int i = 0; i < msgLen; i++) {
            Unsafe.getUnsafe().putByte(bufAddr + 12 + i, msgBytes[i]);
        }
        return bufAddr + 12 + msgLen;
    }

    /**
     * Writes a WebSocket BINARY frame header into the reserved space in front of
     * the QWP message. Returns the address of the actual frame start (may be
     * higher than the original buffer start if fewer than 10 header bytes were
     * needed).
     *
     * @param wsHeaderReservationAddr address of the reserved region start (= caller's buffer start)
     * @param qwpMessageAddr          address where the QWP message header starts
     *                                (= wsHeaderReservationAddr + WS_HEADER_RESERVATION)
     * @param qwpPayloadLen           total QWP bytes including the 12-byte message header
     * @return address of the WS frame start (offset into the caller's buffer)
     */
    public static long wrapInBinaryFrame(long wsHeaderReservationAddr, long qwpMessageAddr, int qwpPayloadLen) {
        int wsHeaderSize = WebSocketFrameWriter.headerSize(qwpPayloadLen, false);
        long frameStart = qwpMessageAddr - wsHeaderSize;
        WebSocketFrameWriter.writeBinaryFrameHeader(frameStart, qwpPayloadLen);
        return frameStart;
    }
}
