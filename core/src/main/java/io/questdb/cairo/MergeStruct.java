/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
    final static int MERGE_STRUCT_ENTRY_SIZE = 32;
    private final static int SECONDARY_COL_OFFSET = 16;

    public static int getFirstColumnOffset(int columnIndex) {
        return columnIndex * MERGE_STRUCT_ENTRY_SIZE;
    }

    public static int getSecondColumnOffset(int columnIndex) {
        return getFirstColumnOffset(columnIndex) + SECONDARY_COL_OFFSET;
    }

    static void setSrcFixedFd(long[] mergeStruct, int columnIndex, long value) {
        setSrcFdFromOffset(mergeStruct, getFirstColumnOffset(columnIndex), value);
    }

    static void setSrcFdFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset] = value;
    }

    static long getSrcFixedFd(long[] mergeStruct, int columnIndex) {
        return getSrcFdFromOffset(mergeStruct, getFirstColumnOffset(columnIndex));
    }

    static long getSrcFdFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset];
    }

    static void setSrcFixedAddress(long[] mergeStruct, int columnIndex, long value) {
        setSrcAddressFromOffset(mergeStruct, getFirstColumnOffset(columnIndex), value);
    }

    static void setSrcAddressFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 1] = value;
    }

    static long getSrcFixedAddress(long[] mergeStruct, int columnIndex) {
        return getSrcAddressFromOffset(mergeStruct, getFirstColumnOffset(columnIndex));
    }

    static long getSrcAddressFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 1];
    }

    static void setSrcFixedAddressSize(long[] mergeStruct, int columnIndex, long value) {
        setSrcAddressSizeFromOffset(mergeStruct, getFirstColumnOffset(columnIndex), value);
    }

    static long getSrcFixedAddressSize(long[] mergeStruct, int columnIndex) {
        return getSrcAddressSizeFromOffset(mergeStruct, getFirstColumnOffset(columnIndex));
    }

    static long getSrcAddressSizeFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 2];
    }

    static void setSrcVarFd(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[getSecondColumnOffset(columnIndex)] = value;
    }

    static void setSrcVarAddress(long[] mergeStruct, int columnIndex, long value) {
        setSrcAddressFromOffset(mergeStruct, getSecondColumnOffset(columnIndex), value);
    }

    static long getSrcVarAddress(long[] mergeStruct, int columnIndex) {
        return getSrcAddressFromOffset(mergeStruct, getSecondColumnOffset(columnIndex));
    }

    static void setSrcVarAddressSize(long[] mergeStruct, int columnIndex, long value) {
        setSrcAddressSizeFromOffset(mergeStruct, getSecondColumnOffset(columnIndex), value);
    }

    static void setSrcAddressSizeFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 2] = value;
    }

    static long getSrcVarAddressSize(long[] mergeStruct, int columnIndex) {
        return mergeStruct[getSecondColumnOffset(columnIndex) + 2];
    }

    static void setDestFixedFd(long[] mergeStruct, int columnIndex, long value) {
        setDestFdFromOffset(mergeStruct, getFirstColumnOffset(columnIndex), value);
    }

    static void setDestFdFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 3] = value;
    }

    static void setDestFixedAddress(long[] mergeStruct, int columnIndex, long value) {
        setDestAddressFromOffset(mergeStruct, getFirstColumnOffset(columnIndex), value);
    }

    static void setDestAddressFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 4] = value;
    }

    static long getDestFixedAddress(long[] mergeStruct, int columnIndex) {
        return getDestAddressFromOffset(mergeStruct, getFirstColumnOffset(columnIndex));
    }

    static long getDestAddressFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 4];
    }

    static void setDestFixedAddressSize(long[] mergeStruct, int columnIndex, long value) {
        setDestAddressSizeFromOffset(mergeStruct, getFirstColumnOffset(columnIndex), value);
    }

    static void setDestAddressSizeFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 5] = value;
    }

    static void setDestFixedAppendOffset(long[] mergeStruct, int columnIndex, long value) {
        setDestAppendOffsetFromOffset(mergeStruct, getFirstColumnOffset(columnIndex), value);
    }

    static void setDestAppendOffsetFromOffset(long[] mergeStruct, int offset, long value) {
        mergeStruct[offset + 6] = value;
    }

    static long getDestFixedAppendOffset(long[] mergeStruct, int columnIndex) {
        return mergeStruct[getFirstColumnOffset(columnIndex) + 6];
    }

    static void setDestVarFd(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[getSecondColumnOffset(columnIndex) + 3] = value;
    }

    static void setDestVarAddress(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[getSecondColumnOffset(columnIndex) + 4] = value;
    }

    static long getDestVarAddress(long[] mergeStruct, int columnIndex) {
        return mergeStruct[getSecondColumnOffset(columnIndex) + 4];
    }

    static void setDestVarAddressSize(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[getSecondColumnOffset(columnIndex) + 5] = value;
    }

    static long getDestVarAddressSize(long[] mergeStruct, int columnIndex) {
        return getDestAddressSizeFromOffset(mergeStruct, getSecondColumnOffset(columnIndex));
    }

    static long getDestAddressSizeFromOffset(long[] mergeStruct, int offset) {
        return mergeStruct[offset + 5];
    }

    static void setDestVarAppendOffset(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[getSecondColumnOffset(columnIndex) + 6] = value;
    }

    static void setIndexKeyFd(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[getFirstColumnOffset(columnIndex) + 7] = value;
    }

    static long getIndexKeyFd(long[] mergeStruct, int columnIndex) {
        return mergeStruct[getFirstColumnOffset(columnIndex) + 7];
    }

    static long getIndexValueFd(long[] mergeStruct, int columnIndex) {
        return mergeStruct[getSecondColumnOffset(columnIndex) + 7];
    }

    static void setIndexValueFd(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[getSecondColumnOffset(columnIndex) + 7] = value;
    }

    static long getDestVarAppendOffset(long[] mergeStruct, int columnIndex) {
        return mergeStruct[getSecondColumnOffset(columnIndex) + 6];
    }
}
