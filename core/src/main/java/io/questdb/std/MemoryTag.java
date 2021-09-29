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

package io.questdb.std;

public final class MemoryTag {
    public static final int MMAP_DEFAULT = 0;
    public static final int NATIVE_DEFAULT = 1;
    public static final int MMAP_O3 = 2;
    public static final int NATIVE_O3 = 3;
    public static final int NATIVE_RECORD_CHAIN = 4;
    public static final int MMAP_TABLE_WRITER = 5;
    public static final int NATIVE_TREE_CHAIN = 6;
    public static final int MMAP_TABLE_READER = 7;
    public static final int NATIVE_COMPACT_MAP = 8;
    public static final int NATIVE_FAST_MAP = 9;
    public static final int NATIVE_LONG_LIST = 10;
    public static final int NATIVE_HTTP_CONN = 11;
    public static final int NATIVE_PGW_CONN = 12;
    public static final int SIZE = NATIVE_PGW_CONN + 1;

    private static final IntObjHashMap<String> tagNameMap = new IntObjHashMap<>();

    public static String nameOf(int tag) {
        final int index = tagNameMap.keyIndex(tag);
        if (index > -1) {
            return "Unknown";
        }
        return tagNameMap.valueAtQuick(index);
    }

    static {
        tagNameMap.put(MMAP_DEFAULT, "MMAP_DEFAULT");
        tagNameMap.put(NATIVE_DEFAULT, "NATIVE_DEFAULT");
        tagNameMap.put(MMAP_O3, "MMAP_O3");
        tagNameMap.put(NATIVE_O3, "NATIVE_O3");
        tagNameMap.put(NATIVE_RECORD_CHAIN, "NATIVE_RECORD_CHAIN");
        tagNameMap.put(MMAP_TABLE_WRITER, "MMAP_TABLE_WRITER");
        tagNameMap.put(NATIVE_TREE_CHAIN, "NATIVE_TREE_CHAIN");
        tagNameMap.put(MMAP_TABLE_READER, "MMAP_TABLE_READER");
        tagNameMap.put(NATIVE_COMPACT_MAP, "NATIVE_COMPACT_MAP");
        tagNameMap.put(NATIVE_FAST_MAP, "NATIVE_FAST_MAP");
        tagNameMap.put(NATIVE_LONG_LIST, "NATIVE_LONG_LIST");
        tagNameMap.put(NATIVE_HTTP_CONN, "NATIVE_HTTP_CONN");
        tagNameMap.put(NATIVE_PGW_CONN, "NATIVE_PGW_CONN");
    }
}
