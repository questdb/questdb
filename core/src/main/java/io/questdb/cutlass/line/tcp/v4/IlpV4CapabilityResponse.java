/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.line.tcp.v4;

import io.questdb.std.Unsafe;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Parser and encoder for ILP v4 capability response messages.
 * <p>
 * CAPABILITY_RESPONSE (8 bytes):
 * <pre>
 * Offset  Size    Field
 * 0       4       Magic: "ILP!" (0x21504C49 little-endian)
 * 4       1       Negotiated version: uint8
 * 5       1       Reserved: uint8
 * 6       2       Flags: uint16 (server capabilities)
 * </pre>
 * <p>
 * If the server does not support ILP v4, it responds with "ILP0" magic
 * indicating fallback to text protocol.
 */
public class IlpV4CapabilityResponse {

    private int magic;
    private byte negotiatedVersion;
    private byte reserved;
    private int flags;

    /**
     * Parses a capability response from direct memory.
     *
     * @param address the memory address
     * @param length  available bytes
     * @throws IlpV4ParseException if response is invalid
     */
    public void parse(long address, int length) throws IlpV4ParseException {
        if (length < CAPABILITY_RESPONSE_SIZE) {
            throw IlpV4ParseException.headerTooShort();
        }

        this.magic = Unsafe.getUnsafe().getInt(address);
        this.negotiatedVersion = Unsafe.getUnsafe().getByte(address + 4);
        this.reserved = Unsafe.getUnsafe().getByte(address + 5);
        this.flags = Unsafe.getUnsafe().getShort(address + 6) & 0xFFFF;

        // No validation of magic here - caller should check isFallback()
    }

    /**
     * Parses a capability response from a byte array.
     *
     * @param buf    the buffer
     * @param offset starting offset
     * @param length available bytes
     * @throws IlpV4ParseException if response is invalid
     */
    public void parse(byte[] buf, int offset, int length) throws IlpV4ParseException {
        if (length < CAPABILITY_RESPONSE_SIZE) {
            throw IlpV4ParseException.headerTooShort();
        }

        this.magic = (buf[offset] & 0xFF) |
                ((buf[offset + 1] & 0xFF) << 8) |
                ((buf[offset + 2] & 0xFF) << 16) |
                ((buf[offset + 3] & 0xFF) << 24);

        this.negotiatedVersion = buf[offset + 4];
        this.reserved = buf[offset + 5];

        this.flags = (buf[offset + 6] & 0xFF) |
                ((buf[offset + 7] & 0xFF) << 8);
    }

    /**
     * Encodes a capability response to direct memory.
     *
     * @param address           destination address
     * @param negotiatedVersion the negotiated protocol version
     * @param flags             the negotiated capability flags
     * @return address after the encoded response
     */
    public static long encode(long address, byte negotiatedVersion, int flags) {
        Unsafe.getUnsafe().putInt(address, MAGIC_CAPABILITY_RESPONSE);
        Unsafe.getUnsafe().putByte(address + 4, negotiatedVersion);
        Unsafe.getUnsafe().putByte(address + 5, (byte) 0); // reserved
        Unsafe.getUnsafe().putShort(address + 6, (short) flags);
        return address + CAPABILITY_RESPONSE_SIZE;
    }

    /**
     * Encodes a capability response to a byte array.
     *
     * @param buf               destination buffer
     * @param offset            starting offset
     * @param negotiatedVersion the negotiated protocol version
     * @param flags             the negotiated capability flags
     * @return offset after the encoded response
     */
    public static int encode(byte[] buf, int offset, byte negotiatedVersion, int flags) {
        buf[offset] = 'I';
        buf[offset + 1] = 'L';
        buf[offset + 2] = 'P';
        buf[offset + 3] = '!';
        buf[offset + 4] = negotiatedVersion;
        buf[offset + 5] = 0; // reserved
        buf[offset + 6] = (byte) (flags & 0xFF);
        buf[offset + 7] = (byte) ((flags >> 8) & 0xFF);
        return offset + CAPABILITY_RESPONSE_SIZE;
    }

    /**
     * Encodes a fallback response (server doesn't support ILP v4).
     *
     * @param address destination address
     * @return address after the encoded response
     */
    public static long encodeFallback(long address) {
        Unsafe.getUnsafe().putInt(address, MAGIC_FALLBACK);
        Unsafe.getUnsafe().putInt(address + 4, 0); // rest is zeros
        return address + CAPABILITY_RESPONSE_SIZE;
    }

    /**
     * Encodes a fallback response to a byte array.
     *
     * @param buf    destination buffer
     * @param offset starting offset
     * @return offset after the encoded response
     */
    public static int encodeFallback(byte[] buf, int offset) {
        buf[offset] = 'I';
        buf[offset + 1] = 'L';
        buf[offset + 2] = 'P';
        buf[offset + 3] = '0';
        buf[offset + 4] = 0;
        buf[offset + 5] = 0;
        buf[offset + 6] = 0;
        buf[offset + 7] = 0;
        return offset + CAPABILITY_RESPONSE_SIZE;
    }

    // ==================== Getters ====================

    public int getMagic() {
        return magic;
    }

    /**
     * Returns true if this is a successful ILP v4 capability response.
     */
    public boolean isSuccess() {
        return magic == MAGIC_CAPABILITY_RESPONSE;
    }

    /**
     * Returns true if the server responded with fallback (doesn't support v4).
     */
    public boolean isFallback() {
        return magic == MAGIC_FALLBACK;
    }

    public byte getNegotiatedVersion() {
        return negotiatedVersion;
    }

    public int getFlags() {
        return flags;
    }

    public boolean supportsLZ4() {
        return (flags & FLAG_LZ4) != 0;
    }

    public boolean supportsZstd() {
        return (flags & FLAG_ZSTD) != 0;
    }

    public boolean supportsGorilla() {
        return (flags & FLAG_GORILLA) != 0;
    }

    public void reset() {
        magic = 0;
        negotiatedVersion = 0;
        reserved = 0;
        flags = 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IlpV4CapabilityResponse{");
        if (isFallback()) {
            sb.append("FALLBACK");
        } else {
            sb.append("negotiatedVersion=").append(negotiatedVersion);
            sb.append(", flags=0x").append(Integer.toHexString(flags));
            if (supportsLZ4()) sb.append("[LZ4]");
            if (supportsZstd()) sb.append("[Zstd]");
            if (supportsGorilla()) sb.append("[Gorilla]");
        }
        sb.append('}');
        return sb.toString();
    }
}
