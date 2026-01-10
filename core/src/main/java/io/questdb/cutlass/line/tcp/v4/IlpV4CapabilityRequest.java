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
 * Parser and encoder for ILP v4 capability request messages.
 * <p>
 * CAPABILITY_REQUEST (8 bytes):
 * <pre>
 * Offset  Size    Field
 * 0       4       Magic: "ILP?" (0x3F504C49 little-endian)
 * 4       1       Min version supported: uint8
 * 5       1       Max version supported: uint8
 * 6       2       Flags: uint16
 *                   bit 0: supports LZ4
 *                   bit 1: supports Zstd
 *                   bit 2: supports Gorilla timestamps
 *                   bits 3-15: reserved
 * </pre>
 */
public class IlpV4CapabilityRequest {

    private int magic;
    private byte minVersion;
    private byte maxVersion;
    private int flags;

    /**
     * Parses a capability request from direct memory.
     *
     * @param address the memory address
     * @param length  available bytes
     * @throws IlpV4ParseException if request is invalid
     */
    public void parse(long address, int length) throws IlpV4ParseException {
        if (length < CAPABILITY_REQUEST_SIZE) {
            throw IlpV4ParseException.headerTooShort();
        }

        this.magic = Unsafe.getUnsafe().getInt(address);
        this.minVersion = Unsafe.getUnsafe().getByte(address + 4);
        this.maxVersion = Unsafe.getUnsafe().getByte(address + 5);
        this.flags = Unsafe.getUnsafe().getShort(address + 6) & 0xFFFF;

        validate();
    }

    /**
     * Parses a capability request from a byte array.
     *
     * @param buf    the buffer
     * @param offset starting offset
     * @param length available bytes
     * @throws IlpV4ParseException if request is invalid
     */
    public void parse(byte[] buf, int offset, int length) throws IlpV4ParseException {
        if (length < CAPABILITY_REQUEST_SIZE) {
            throw IlpV4ParseException.headerTooShort();
        }

        this.magic = (buf[offset] & 0xFF) |
                ((buf[offset + 1] & 0xFF) << 8) |
                ((buf[offset + 2] & 0xFF) << 16) |
                ((buf[offset + 3] & 0xFF) << 24);

        this.minVersion = buf[offset + 4];
        this.maxVersion = buf[offset + 5];

        this.flags = (buf[offset + 6] & 0xFF) |
                ((buf[offset + 7] & 0xFF) << 8);

        validate();
    }

    private void validate() throws IlpV4ParseException {
        if (magic != MAGIC_CAPABILITY_REQUEST) {
            throw IlpV4ParseException.invalidMagic();
        }

        if (minVersion > maxVersion) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INVALID_MAGIC,
                    "invalid capability request: minVersion > maxVersion"
            );
        }
    }

    /**
     * Encodes a capability request to direct memory.
     *
     * @param address    destination address
     * @param minVersion minimum version supported
     * @param maxVersion maximum version supported
     * @param flags      capability flags
     * @return address after the encoded request
     */
    public static long encode(long address, byte minVersion, byte maxVersion, int flags) {
        Unsafe.getUnsafe().putInt(address, MAGIC_CAPABILITY_REQUEST);
        Unsafe.getUnsafe().putByte(address + 4, minVersion);
        Unsafe.getUnsafe().putByte(address + 5, maxVersion);
        Unsafe.getUnsafe().putShort(address + 6, (short) flags);
        return address + CAPABILITY_REQUEST_SIZE;
    }

    /**
     * Encodes a capability request to a byte array.
     *
     * @param buf        destination buffer
     * @param offset     starting offset
     * @param minVersion minimum version supported
     * @param maxVersion maximum version supported
     * @param flags      capability flags
     * @return offset after the encoded request
     */
    public static int encode(byte[] buf, int offset, byte minVersion, byte maxVersion, int flags) {
        buf[offset] = 'I';
        buf[offset + 1] = 'L';
        buf[offset + 2] = 'P';
        buf[offset + 3] = '?';
        buf[offset + 4] = minVersion;
        buf[offset + 5] = maxVersion;
        buf[offset + 6] = (byte) (flags & 0xFF);
        buf[offset + 7] = (byte) ((flags >> 8) & 0xFF);
        return offset + CAPABILITY_REQUEST_SIZE;
    }

    // ==================== Getters ====================

    public int getMagic() {
        return magic;
    }

    public byte getMinVersion() {
        return minVersion;
    }

    public byte getMaxVersion() {
        return maxVersion;
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
        minVersion = 0;
        maxVersion = 0;
        flags = 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IlpV4CapabilityRequest{");
        sb.append("minVersion=").append(minVersion);
        sb.append(", maxVersion=").append(maxVersion);
        sb.append(", flags=0x").append(Integer.toHexString(flags));
        if (supportsLZ4()) sb.append("[LZ4]");
        if (supportsZstd()) sb.append("[Zstd]");
        if (supportsGorilla()) sb.append("[Gorilla]");
        sb.append('}');
        return sb.toString();
    }
}
