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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.Unsafe;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Mutable, reusable QWP v1 message header parser.
 * <p>
 * The message header is 12 bytes:
 * <pre>
 * Offset  Size    Field
 * 0       4       Magic: "QWP1"
 * 4       1       Version: uint8
 * 5       1       Flags: uint8
 * 6       2       Table count: uint16 (little-endian)
 * 8       4       Payload length: uint32 (little-endian)
 * </pre>
 * <p>
 * This class is designed for zero-allocation parsing. Create once and reuse
 * by calling {@link #parse(long, int)} or {@link #parse(byte[], int, int)}.
 */
public class QwpMessageHeader {

    private byte flags;
    private int magic;
    // Configuration limits
    private long maxPayloadLength = DEFAULT_MAX_BATCH_SIZE;
    private long payloadLength;
    private int tableCount;
    private byte version;

    /**
     * Creates a new header parser with default limits.
     */
    public QwpMessageHeader() {
    }

    /**
     * Checks if the given 4 bytes match the QWP v1 message magic.
     *
     * @param magic the magic integer to check
     * @return true if it's a QWP v1 message
     */
    public static boolean isMessageMagic(int magic) {
        return magic == MAGIC_MESSAGE;
    }

    /**
     * Reads the magic integer from direct memory.
     *
     * @param address memory address
     * @return magic integer (little-endian)
     */
    public static int readMagic(long address) {
        return Unsafe.getInt(address);
    }

    /**
     * Reads the magic integer from a byte array.
     *
     * @param buf    buffer
     * @param offset starting offset
     * @return magic integer (little-endian)
     */
    public static int readMagic(byte[] buf, int offset) {
        return (buf[offset] & 0xFF) |
                ((buf[offset + 1] & 0xFF) << 8) |
                ((buf[offset + 2] & 0xFF) << 16) |
                ((buf[offset + 3] & 0xFF) << 24);
    }

    /**
     * Returns the flags byte.
     *
     * @return flags
     */
    public byte getFlags() {
        return flags;
    }

    /**
     * Returns the magic bytes as an integer.
     *
     * @return magic integer
     */
    public int getMagic() {
        return magic;
    }

    /**
     * Returns the payload length in bytes.
     *
     * @return payload length
     */
    public long getPayloadLength() {
        return payloadLength;
    }

    /**
     * Returns the number of tables in this batch.
     *
     * @return table count (0-65535)
     */
    public int getTableCount() {
        return tableCount;
    }

    /**
     * Returns the total message length (header + payload).
     *
     * @return total message size in bytes
     */
    public long getTotalLength() {
        return HEADER_SIZE + payloadLength;
    }

    /**
     * Returns the protocol version.
     *
     * @return version number
     */
    public byte getVersion() {
        return version;
    }

    /**
     * Returns true if delta symbol dictionary encoding is enabled.
     *
     * @return true if delta symbol dictionary mode
     */
    public boolean isDeltaSymbolDictEnabled() {
        return (flags & FLAG_DELTA_SYMBOL_DICT) != 0;
    }

    /**
     * Returns true if Gorilla timestamp encoding is enabled.
     *
     * @return true if Gorilla timestamps
     */
    public boolean isGorillaEnabled() {
        return (flags & FLAG_GORILLA) != 0;
    }

    /**
     * Parses a header from direct memory.
     *
     * @param address the memory address containing the header
     * @param length  available bytes (must be >= 12)
     * @throws QwpParseException if header is invalid
     */
    public void parse(long address, int length) throws QwpParseException {
        if (length < HEADER_SIZE) {
            throw QwpParseException.headerTooShort();
        }

        // Read all fields (little-endian)
        this.magic = Unsafe.getInt(address + HEADER_OFFSET_MAGIC);
        this.version = Unsafe.getByte(address + HEADER_OFFSET_VERSION);
        this.flags = Unsafe.getByte(address + HEADER_OFFSET_FLAGS);
        this.tableCount = Unsafe.getShort(address + HEADER_OFFSET_TABLE_COUNT) & 0xFFFF;
        this.payloadLength = Unsafe.getInt(address + HEADER_OFFSET_PAYLOAD_LENGTH) & 0xFFFFFFFFL;

        validate();
    }

    /**
     * Parses a header from a byte array.
     *
     * @param buf    the buffer containing the header
     * @param offset starting offset
     * @param length available bytes (must be >= 12)
     * @throws QwpParseException if header is invalid
     */
    public void parse(byte[] buf, int offset, int length) throws QwpParseException {
        if (length < HEADER_SIZE) {
            throw QwpParseException.headerTooShort();
        }

        // Read magic (little-endian)
        this.magic = (buf[offset] & 0xFF) |
                ((buf[offset + 1] & 0xFF) << 8) |
                ((buf[offset + 2] & 0xFF) << 16) |
                ((buf[offset + 3] & 0xFF) << 24);

        this.version = buf[offset + HEADER_OFFSET_VERSION];
        this.flags = buf[offset + HEADER_OFFSET_FLAGS];

        // Read table count (little-endian uint16)
        this.tableCount = (buf[offset + HEADER_OFFSET_TABLE_COUNT] & 0xFF) |
                ((buf[offset + HEADER_OFFSET_TABLE_COUNT + 1] & 0xFF) << 8);

        // Read payload length (little-endian uint32)
        this.payloadLength = (buf[offset + HEADER_OFFSET_PAYLOAD_LENGTH] & 0xFFL) |
                ((buf[offset + HEADER_OFFSET_PAYLOAD_LENGTH + 1] & 0xFFL) << 8) |
                ((buf[offset + HEADER_OFFSET_PAYLOAD_LENGTH + 2] & 0xFFL) << 16) |
                ((buf[offset + HEADER_OFFSET_PAYLOAD_LENGTH + 3] & 0xFFL) << 24);

        validate();
    }

    /**
     * Resets the header for reuse.
     */
    public void reset() {
        magic = 0;
        version = 0;
        flags = 0;
        tableCount = 0;
        payloadLength = 0;
    }

    /**
     * Sets the maximum allowed payload length.
     *
     * @param maxPayloadLength max payload in bytes
     */
    public void setMaxPayloadLength(long maxPayloadLength) {
        this.maxPayloadLength = maxPayloadLength;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QwpMessageHeader{");
        sb.append("magic=").append(magicToString(magic));
        sb.append(", version=").append(version);
        sb.append(", flags=0x").append(Integer.toHexString(flags & 0xFF));
        if (isGorillaEnabled()) {
            sb.append("[Gorilla]");
        }
        sb.append(", tableCount=").append(tableCount);
        sb.append(", payloadLength=").append(payloadLength);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Converts a magic integer to a string representation.
     *
     * @param magic the magic integer
     * @return string like "QWP1"
     */
    private static String magicToString(int magic) {
        char[] chars = new char[4];
        chars[0] = (char) (magic & 0xFF);
        chars[1] = (char) ((magic >> 8) & 0xFF);
        chars[2] = (char) ((magic >> 16) & 0xFF);
        chars[3] = (char) ((magic >> 24) & 0xFF);
        return new String(chars);
    }

    /**
     * Validates the parsed header values.
     *
     * @throws QwpParseException if validation fails
     */
    private void validate() throws QwpParseException {
        // Validate magic bytes
        if (magic != MAGIC_MESSAGE) {
            throw QwpParseException.invalidMagic();
        }

        // Validate version. Ingest pins to v1 (the v2 bump is egress-only) so
        // a v2 message arriving on the ingest path is rejected at the wire
        // level rather than silently accepted with no v2-specific handling.
        if (version < VERSION_1 || version > MAX_SUPPORTED_INGEST_VERSION) {
            throw QwpParseException.unsupportedVersion();
        }

        // Validate payload length
        if (payloadLength > maxPayloadLength) {
            throw QwpParseException.payloadTooLarge();
        }
    }
}
