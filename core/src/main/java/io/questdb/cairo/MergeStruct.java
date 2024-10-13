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

package io.questdb.cairo;

public final class MergeStruct {
    public static final int STAGE_MERGE = 1;
    public static final int STAGE_PREFIX = 0;
    public static final int STAGE_SUFFIX = 2;
    final static int MERGE_STRUCT_ENTRY_SIZE = 32;
    private final static int SECONDARY_COL_OFFSET = 16;

    public static int getFirstColumnOffset(int columnIndex) {
        return columnIndex * MERGE_STRUCT_ENTRY_SIZE;
    }

    public static int getSecondColumnOffset(int columnIndex) {
        return getFirstColumnOffset(columnIndex) + SECONDARY_COL_OFFSET;
    }

    static long getDestAddressFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 4];
    }

    static long getDestAddressSizeFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 5];
    }

    static long getDestAppendOffsetFromOffsetStage(long[] mergeStruct, int offset, int stage) {
        return mergeStruct[offset + 10 + stage];
    }

    static long getDestIndexKeyFdFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 7];
    }

    static long getDestIndexStartOffsetFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 9];
    }

    static long getDestIndexValueFdFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 8];
    }

    static long getSrcAddressFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 1];
    }

    static long getSrcAddressSizeFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 2];
    }

    static long getSrcFdFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset];
    }

    static long getSrcFixedAddress(long[] mergeStruct, int columnIndex) {
        return getSrcAddressFromOffset(mergeStruct, getFirstColumnOffset(columnIndex));
    }

    static void setDestAddressFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 4] = value;
    }

    static void setDestAddressSizeFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 5] = value;
    }

    static void setDestAppendOffsetFromOffset0(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 10] = value;
    }

    static void setDestAppendOffsetFromOffset1(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 11] = value;
    }

    static void setDestAppendOffsetFromOffset2(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 12] = value;
    }

    static void setDestFdFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 3] = value;
    }

    static void setDestIndexKeyFdFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 7] = value;
    }

    static void setDestIndexStartOffsetFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 9] = value;
    }

    static void setDestIndexValueFdFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 8] = value;
    }

    static void setSrcAddressFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 1] = value;
    }

    static void setSrcAddressSizeFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 2] = value;
    }

    static void setSrcFdFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset] = value;
    }
}
