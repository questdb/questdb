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

package io.questdb.cairo.file;

import io.questdb.std.Zip;

public class BlockFileUtils {
    /**
     * +---------------------------------------------------------------------+
     * |                        Block Length (4 bytes)                       |
     * +---------------------------------------------------------------------+
     * |                        Type (4 bytes)                               |
     * +-------------+---------------+---------------------------------------+
     * |                  Depends on Type ...                                |
     * +---------------------------------------------------------------------+
     */
    public static final int BLOCK_LENGTH_OFFSET = 0;
    public static final int BLOCK_TYPE_OFFSET = BLOCK_LENGTH_OFFSET + Integer.BYTES;
    public static final int BLOCK_HEADER_SIZE = BLOCK_TYPE_OFFSET + Integer.BYTES;
    /**
     * +---------------------------------------------------------------+
     * |                Version (8 bytes)                              |
     * +---------------------------------------------------------------+
     * |            Region A Offset + Size (16 bytes)                  |
     * +---------------------------------------------------------------+
     * |            Region B Offset + Size (16 bytes)                  |
     * +---------------------------------------------------------------+
     * |                       ...                                     |
     * +---------------------------------------------------------------+
     */
    public static final int HEADER_VERSION_OFFSET = 0;
    public static final int HEADER_REGION_A_OFFSET = HEADER_VERSION_OFFSET + Long.BYTES;
    public static final int HEADER_REGION_A_LENGTH = HEADER_REGION_A_OFFSET + Long.BYTES;
    public static final int HEADER_REGION_B_OFFSET = HEADER_REGION_A_LENGTH + Long.BYTES;
    public static final int HEADER_REGION_B_LENGTH = HEADER_REGION_B_OFFSET + Long.BYTES;
    public static final int HEADER_SIZE = HEADER_REGION_B_LENGTH + Long.BYTES;
    /**
     * +---------------------------------------------------------------+
     * |                   Checksum (4 bytes)                          |
     * +---------------------------------------------------------------+
     * |                  Block Count (4 bytes)                        |
     * +---------------------------------------------------------------+
     * |                  Blocks                                       |
     * +---------------------------------------------------------------+
     */
    public static final int REGION_CHECKSUM_OFFSET = 0;
    public static final int REGION_BLOCK_COUNT_OFFSET = REGION_CHECKSUM_OFFSET + Integer.BYTES;
    public static final int REGION_HEADER_SIZE = REGION_BLOCK_COUNT_OFFSET + Integer.BYTES;

    public static int checksum(long checksumAddress, long checksumSize) {
        int checksum = 0;
        while (checksumSize > 0) {
            int chunkSize = (int) Math.min(Integer.MAX_VALUE, checksumSize);
            checksum = Zip.crc32(checksum, checksumAddress, chunkSize);
            checksumAddress += chunkSize;
            checksumSize -= chunkSize;
        }
        return checksum;
    }

    public static long getRegionLengthOffset(final long version) {
        return isRegionA(version) ? HEADER_REGION_A_LENGTH : HEADER_REGION_B_LENGTH;
    }

    public static long getRegionOffsetOffset(final long version) {
        return isRegionA(version) ? HEADER_REGION_A_OFFSET : HEADER_REGION_B_OFFSET;
    }

    public static boolean isRegionA(final long version) {
        return (version & 1) != 0; // 0 - empty file, 1 - region A, 2 - region B, ...
    }
}
