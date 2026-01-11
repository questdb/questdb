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

package io.questdb.cutlass.http.ilpv4;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Represents a decoded ILP v4 message containing multiple table blocks.
 * <p>
 * This class provides both indexed and iterator access to the decoded table blocks.
 */
public class IlpV4DecodedMessage implements Iterable<IlpV4DecodedTableBlock> {

    private final IlpV4DecodedTableBlock[] tableBlocks;
    private final byte flags;
    private final long payloadLength;

    public IlpV4DecodedMessage(IlpV4DecodedTableBlock[] tableBlocks, byte flags, long payloadLength) {
        this.tableBlocks = tableBlocks;
        this.flags = flags;
        this.payloadLength = payloadLength;
    }

    /**
     * Gets the number of table blocks in this message.
     *
     * @return table count
     */
    public int getTableCount() {
        return tableBlocks.length;
    }

    /**
     * Gets a table block by index.
     *
     * @param index table index (0-based)
     * @return the decoded table block
     * @throws ArrayIndexOutOfBoundsException if index is out of range
     */
    public IlpV4DecodedTableBlock getTableBlock(int index) {
        return tableBlocks[index];
    }

    /**
     * Gets a table block by table name.
     *
     * @param tableName the table name to find
     * @return the first table block with that name, or null if not found
     */
    public IlpV4DecodedTableBlock getTableBlock(String tableName) {
        for (IlpV4DecodedTableBlock block : tableBlocks) {
            if (block.getTableName().equals(tableName)) {
                return block;
            }
        }
        return null;
    }

    /**
     * Gets all table blocks as an array.
     *
     * @return array of table blocks
     */
    public IlpV4DecodedTableBlock[] getTableBlocks() {
        return tableBlocks;
    }

    /**
     * Returns true if Gorilla timestamp encoding was used.
     *
     * @return true if Gorilla enabled
     */
    public boolean isGorillaEnabled() {
        return (flags & IlpV4Constants.FLAG_GORILLA) != 0;
    }

    /**
     * Returns true if LZ4 compression was used.
     *
     * @return true if LZ4 compressed
     */
    public boolean isLZ4Compressed() {
        return (flags & IlpV4Constants.FLAG_LZ4) != 0;
    }

    /**
     * Returns true if Zstd compression was used.
     *
     * @return true if Zstd compressed
     */
    public boolean isZstdCompressed() {
        return (flags & IlpV4Constants.FLAG_ZSTD) != 0;
    }

    /**
     * Returns true if any compression was used.
     *
     * @return true if compressed
     */
    public boolean isCompressed() {
        return (flags & IlpV4Constants.FLAG_COMPRESSION_MASK) != 0;
    }

    /**
     * Gets the flags byte from the message header.
     *
     * @return flags
     */
    public byte getFlags() {
        return flags;
    }

    /**
     * Gets the original payload length.
     *
     * @return payload length in bytes
     */
    public long getPayloadLength() {
        return payloadLength;
    }

    /**
     * Gets the total row count across all table blocks.
     *
     * @return total rows
     */
    public long getTotalRowCount() {
        long total = 0;
        for (IlpV4DecodedTableBlock block : tableBlocks) {
            total += block.getRowCount();
        }
        return total;
    }

    /**
     * Returns true if this message contains no table blocks.
     *
     * @return true if empty
     */
    public boolean isEmpty() {
        return tableBlocks.length == 0;
    }

    @Override
    public Iterator<IlpV4DecodedTableBlock> iterator() {
        return new TableBlockIterator();
    }

    private class TableBlockIterator implements Iterator<IlpV4DecodedTableBlock> {
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < tableBlocks.length;
        }

        @Override
        public IlpV4DecodedTableBlock next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return tableBlocks[index++];
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IlpV4DecodedMessage{");
        sb.append("tableCount=").append(tableBlocks.length);
        sb.append(", flags=0x").append(Integer.toHexString(flags & 0xFF));
        if (isGorillaEnabled()) {
            sb.append("[Gorilla]");
        }
        if (isLZ4Compressed()) {
            sb.append("[LZ4]");
        }
        if (isZstdCompressed()) {
            sb.append("[Zstd]");
        }
        sb.append(", payloadLength=").append(payloadLength);
        sb.append(", tables=[");
        for (int i = 0; i < tableBlocks.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(tableBlocks[i].getTableName());
            sb.append("(").append(tableBlocks[i].getRowCount()).append(" rows)");
        }
        sb.append("]}");
        return sb.toString();
    }
}
