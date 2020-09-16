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
    final static int MERGE_STRUCT_ENTRY_SIZE = 16;

    static void mergeStructSetSrcFixedFd(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE] = value;
    }

    static void setSrcFixedAddress(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 1] = value;
    }

    static long getSrcFixedAddress(long[] mergeStruct, int columnIndex) {
        return mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 1];
    }

    static void setSrcFixedAddressSize(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 2] = value;
    }

    static long getSrcFixedAddressSize(long[] mergeStruct, int columnIndex) {
        return mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 2];
    }

    static void setSrcVarFd(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8] = value;
    }

    static void setSrcVarAddress(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 1] = value;
    }

    static long getSrcVarAddress(long[] mergeStruct, int columnIndex) {
        return mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 1];
    }

    static void setSrcVarAddressSize(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 2] = value;
    }

    static long getSrcVarAddressSize(long[] mergeStruct, int columnIndex) {
        return mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 2];
    }

    static void setDestFixedFd(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 3] = value;
    }

    static void setDestFixedAddress(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 4] = value;
    }

    static long getDestFixedAddress(long[] mergeStruct, int columnIndex) {
        return mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 4];
    }

    static void setDestFixedAddressSize(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 5] = value;
    }

    static void setDestFixedAppendOffset(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 6] = value;
    }

    static long getDestFixedAppendOffset(long[] mergeStruct, int columnIndex) {
        return mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 6];
    }

    static void setDestVarFd(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 3] = value;
    }

    static void getDestVarAddress(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 4] = value;
    }

    static long getDestVarAddress(long[] mergeStruct, int columnIndex) {
        return mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 4];
    }

    static void setDestVarAddressSize(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 5] = value;
    }

    static void setDestVarAppendOffset(long[] mergeStruct, int columnIndex, long value) {
        mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 6] = value;
    }

    static long getDestVarAppendOffset(long[] mergeStruct, int columnIndex) {
        return mergeStruct[columnIndex * MERGE_STRUCT_ENTRY_SIZE + 8 + 6];
    }
}
