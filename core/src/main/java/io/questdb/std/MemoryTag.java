/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
    public static final int NATIVE_FAST_MAP_LONG_LIST = 10;
    public static final int NATIVE_HTTP_CONN = 11;
    public static final int NATIVE_PGW_CONN = 12;
    public static final int MMAP_INDEX_READER = 13;
    public static final int MMAP_INDEX_WRITER = 14;
    public static final int MMAP_INDEX_SLIDER = 15;
    public static final int MMAP_BLOCK_WRITER = 16;
    public static final int NATIVE_REPL = 17;
    public static final int NATIVE_SAMPLE_BY_LONG_LIST = 18;
    public static final int NATIVE_LATEST_BY_LONG_LIST = 19;
    public static final int NATIVE_JIT_LONG_LIST = 20;
    public static final int NATIVE_LONG_LIST = 21;
    public static final int NATIVE_JIT = 22;
    public static final int SIZE = NATIVE_JIT + 1;
    private static final ObjList<String> tagNameMap = new ObjList<>(SIZE);

    public static String nameOf(int tag) {
        return tagNameMap.getQuick(tag);
    }

    static {
        tagNameMap.extendAndSet(MMAP_DEFAULT, "MMAP_DEFAULT");
        tagNameMap.extendAndSet(NATIVE_DEFAULT, "NATIVE_DEFAULT");
        tagNameMap.extendAndSet(MMAP_O3, "MMAP_O3");
        tagNameMap.extendAndSet(NATIVE_O3, "NATIVE_O3");
        tagNameMap.extendAndSet(NATIVE_RECORD_CHAIN, "NATIVE_RECORD_CHAIN");
        tagNameMap.extendAndSet(MMAP_TABLE_WRITER, "MMAP_TABLE_WRITER");
        tagNameMap.extendAndSet(NATIVE_TREE_CHAIN, "NATIVE_TREE_CHAIN");
        tagNameMap.extendAndSet(MMAP_TABLE_READER, "MMAP_TABLE_READER");
        tagNameMap.extendAndSet(NATIVE_COMPACT_MAP, "NATIVE_COMPACT_MAP");
        tagNameMap.extendAndSet(NATIVE_FAST_MAP, "NATIVE_FAST_MAP");
        tagNameMap.extendAndSet(NATIVE_FAST_MAP_LONG_LIST, "NATIVE_FAST_MAP_LONG_LIST");
        tagNameMap.extendAndSet(NATIVE_HTTP_CONN, "NATIVE_HTTP_CONN");
        tagNameMap.extendAndSet(NATIVE_PGW_CONN, "NATIVE_PGW_CONN");
        tagNameMap.extendAndSet(MMAP_INDEX_READER, "MMAP_INDEX_READER");
        tagNameMap.extendAndSet(MMAP_INDEX_WRITER, "MMAP_INDEX_WRITER");
        tagNameMap.extendAndSet(MMAP_INDEX_SLIDER, "MMAP_INDEX_SLIDER");
        tagNameMap.extendAndSet(MMAP_BLOCK_WRITER, "MMAP_BLOCK_WRITER");
        tagNameMap.extendAndSet(NATIVE_REPL, "NATIVE_REPL");
        tagNameMap.extendAndSet(NATIVE_SAMPLE_BY_LONG_LIST, "NATIVE_SAMPLE_BY_LONG_LIST");
        tagNameMap.extendAndSet(NATIVE_LATEST_BY_LONG_LIST, "NATIVE_LATEST_BY_LONG_LIST");
        tagNameMap.extendAndSet(NATIVE_JIT_LONG_LIST, "NATIVE_JIT_LONG_LIST");
        tagNameMap.extendAndSet(NATIVE_LONG_LIST, "NATIVE_LONG_LIST");
        tagNameMap.extendAndSet(NATIVE_JIT, "NATIVE_JIT");
    }
}
