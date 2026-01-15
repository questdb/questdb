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

package io.questdb.test.cutlass.http.websocket;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;

/**
 * Base class for WebSocket unit tests.
 * Provides memory management utilities for direct buffer testing.
 */
public abstract class AbstractWebSocketTest extends AbstractTest {

    protected long allocateBuffer(int size) {
        return Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
    }

    protected void freeBuffer(long address, int size) {
        Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
    }

    protected void writeBytes(long address, byte... bytes) {
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(address + i, bytes[i]);
        }
    }

    protected byte[] readBytes(long address, int length) {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = Unsafe.getUnsafe().getByte(address + i);
        }
        return result;
    }

    protected void writeByte(long address, int offset, int value) {
        Unsafe.getUnsafe().putByte(address + offset, (byte) value);
    }

    protected byte readByte(long address, int offset) {
        return Unsafe.getUnsafe().getByte(address + offset);
    }

    protected void writeShort(long address, int offset, short value) {
        Unsafe.getUnsafe().putShort(address + offset, value);
    }

    protected short readShort(long address, int offset) {
        return Unsafe.getUnsafe().getShort(address + offset);
    }

    protected void writeLong(long address, int offset, long value) {
        Unsafe.getUnsafe().putLong(address + offset, value);
    }

    protected long readLong(long address, int offset) {
        return Unsafe.getUnsafe().getLong(address + offset);
    }

    /**
     * Helper method to create a masked WebSocket frame.
     *
     * @param opcode  the WebSocket opcode
     * @param payload the payload data
     * @param fin     whether this is the final frame
     * @param maskKey the 4-byte mask key
     * @return the complete frame as a byte array
     */
    protected byte[] createMaskedFrame(int opcode, byte[] payload, boolean fin, byte[] maskKey) {
        int payloadLen = payload.length;
        int headerLen;

        if (payloadLen <= 125) {
            headerLen = 2 + 4; // 2 byte header + 4 byte mask
        } else if (payloadLen <= 65535) {
            headerLen = 4 + 4; // 2 byte header + 2 byte extended length + 4 byte mask
        } else {
            headerLen = 10 + 4; // 2 byte header + 8 byte extended length + 4 byte mask
        }

        byte[] frame = new byte[headerLen + payloadLen];
        int offset = 0;

        // First byte: FIN + opcode
        frame[offset++] = (byte) ((fin ? 0x80 : 0x00) | (opcode & 0x0F));

        // Second byte: MASK bit + payload length
        if (payloadLen <= 125) {
            frame[offset++] = (byte) (0x80 | payloadLen);
        } else if (payloadLen <= 65535) {
            frame[offset++] = (byte) (0x80 | 126);
            frame[offset++] = (byte) ((payloadLen >> 8) & 0xFF);
            frame[offset++] = (byte) (payloadLen & 0xFF);
        } else {
            frame[offset++] = (byte) (0x80 | 127);
            for (int i = 7; i >= 0; i--) {
                frame[offset++] = (byte) ((payloadLen >> (i * 8)) & 0xFF);
            }
        }

        // Mask key
        System.arraycopy(maskKey, 0, frame, offset, 4);
        offset += 4;

        // Masked payload
        for (int i = 0; i < payloadLen; i++) {
            frame[offset + i] = (byte) (payload[i] ^ maskKey[i % 4]);
        }

        return frame;
    }

    /**
     * Helper method to create a masked WebSocket frame with default mask key.
     */
    protected byte[] createMaskedFrame(int opcode, byte[] payload, boolean fin) {
        return createMaskedFrame(opcode, payload, fin, new byte[]{0x12, 0x34, 0x56, 0x78});
    }

    /**
     * Helper method to create a masked WebSocket frame with FIN=true.
     */
    protected byte[] createMaskedFrame(int opcode, byte[] payload) {
        return createMaskedFrame(opcode, payload, true);
    }
}
