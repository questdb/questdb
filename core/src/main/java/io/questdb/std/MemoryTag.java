/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
    public static final int MMAP_BLOCK_WRITER = 16;
    public static final int MMAP_DEFAULT = 0;
    public static final int MMAP_IMPORT = 33;
    public static final int MMAP_INDEX_READER = 13;
    public static final int MMAP_INDEX_SLIDER = 15;
    public static final int MMAP_INDEX_WRITER = 14;
    public static final int MMAP_O3 = 2;
    public static final int MMAP_PARALLEL_IMPORT = 39;
    public static final int MMAP_SEQUENCER_METADATA = 38;
    public static final int MMAP_TABLE_READER = 7;
    public static final int MMAP_TABLE_WAL_READER = 36;
    public static final int MMAP_TABLE_WAL_WRITER = 37;
    public static final int MMAP_TABLE_WRITER = 5;
    public static final int MMAP_TX_LOG = 53;
    public static final int MMAP_TX_LOG_CURSOR = 52;
    public static final int MMAP_UPDATE = 27;
    public static final int NATIVE_CB1 = 28;
    public static final int NATIVE_CB2 = 29;
    public static final int NATIVE_CB3 = 30;
    public static final int NATIVE_CB4 = 31;
    public static final int NATIVE_CB5 = 32;
    public static final int NATIVE_COMPACT_MAP = 8;
    public static final int NATIVE_DEFAULT = 1;
    public static final int NATIVE_DIRECT_BYTE_SINK = 54;
    public static final int NATIVE_CIRCULAR_BUFFER = 56;
    public static final int NATIVE_DIRECT_CHAR_SINK = 51;
    public static final int NATIVE_DIRECT_UTF8_SINK = 55;
    public static final int NATIVE_FAST_MAP = 9;
    public static final int NATIVE_FAST_MAP_LONG_LIST = 10;
    public static final int NATIVE_FUNC_RSS = 50;
    public static final int NATIVE_HTTP_CONN = 11;
    public static final int NATIVE_ILP_RSS = 46;
    public static final int NATIVE_IMPORT = 34;
    public static final int NATIVE_IO_DISPATCHER_RSS = 49;
    public static final int NATIVE_JIT = 22;
    public static final int NATIVE_JIT_LONG_LIST = 20;
    public static final int NATIVE_JOIN_MAP = 41;
    public static final int NATIVE_LATEST_BY_LONG_LIST = 19;
    public static final int NATIVE_LOGGER = 43;
    public static final int NATIVE_LONG_LIST = 21;
    public static final int NATIVE_MIG = 44;
    public static final int NATIVE_MIG_MMAP = 45;
    public static final int NATIVE_O3 = 3;
    public static final int NATIVE_OFFLOAD = 23;
    public static final int NATIVE_PARALLEL_IMPORT = 40;
    public static final int NATIVE_PATH = 24;
    public static final int NATIVE_PGW_CONN = 12;
    public static final int NATIVE_RECORD_CHAIN = 4;
    public static final int NATIVE_REPL = 17;
    public static final int NATIVE_ROSTI = 35;
    public static final int NATIVE_SAMPLE_BY_LONG_LIST = 18;
    public static final int NATIVE_SQL_COMPILER = 42;
    public static final int NATIVE_TABLE_READER = 25;
    public static final int NATIVE_TABLE_WRITER = 26;
    public static final int NATIVE_TEXT_PARSER_RSS = 48;
    public static final int NATIVE_TLS_RSS = 47;
    public static final int NATIVE_TREE_CHAIN = 6;
    public static final int SIZE = NATIVE_CIRCULAR_BUFFER + 1;
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
        tagNameMap.extendAndSet(NATIVE_OFFLOAD, "NATIVE_OFFLOAD");
        tagNameMap.extendAndSet(MMAP_UPDATE, "MMAP_UPDATE");
        tagNameMap.extendAndSet(NATIVE_PATH, "NATIVE_PATH");
        tagNameMap.extendAndSet(NATIVE_TABLE_READER, "NATIVE_TABLE_READER");
        tagNameMap.extendAndSet(NATIVE_TABLE_WRITER, "NATIVE_TABLE_WRITER");
        tagNameMap.extendAndSet(NATIVE_CB1, "NATIVE_CB1");
        tagNameMap.extendAndSet(NATIVE_CB2, "NATIVE_CB2");
        tagNameMap.extendAndSet(NATIVE_CB3, "NATIVE_CB3");
        tagNameMap.extendAndSet(NATIVE_CB4, "NATIVE_CB4");
        tagNameMap.extendAndSet(NATIVE_CB5, "NATIVE_CB5");
        tagNameMap.extendAndSet(MMAP_IMPORT, "MMAP_IMPORT");
        tagNameMap.extendAndSet(NATIVE_IMPORT, "NATIVE_IMPORT");
        tagNameMap.extendAndSet(NATIVE_ROSTI, "NATIVE_ROSTI");
        tagNameMap.extendAndSet(MMAP_TABLE_WAL_READER, "MMAP_TABLE_WAL_READER");
        tagNameMap.extendAndSet(MMAP_TABLE_WAL_WRITER, "MMAP_TABLE_WAL_WRITER");
        tagNameMap.extendAndSet(MMAP_SEQUENCER_METADATA, "MMAP_SEQUENCER_METADATA");
        tagNameMap.extendAndSet(MMAP_PARALLEL_IMPORT, "MMAP_PARALLEL_IMPORT");
        tagNameMap.extendAndSet(NATIVE_PARALLEL_IMPORT, "NATIVE_PARALLEL_IMPORT");
        tagNameMap.extendAndSet(NATIVE_JOIN_MAP, "NATIVE_JOIN_MAP");
        tagNameMap.extendAndSet(NATIVE_LOGGER, "NATIVE_LOGGER");
        tagNameMap.extendAndSet(NATIVE_MIG, "NATIVE_MIG");
        tagNameMap.extendAndSet(NATIVE_MIG_MMAP, "NATIVE_MIG_MMAP");
        tagNameMap.extendAndSet(NATIVE_ILP_RSS, "NATIVE_ILP_RSS");
        tagNameMap.extendAndSet(NATIVE_TLS_RSS, "NATIVE_TLS_RSS");
        tagNameMap.extendAndSet(NATIVE_TEXT_PARSER_RSS, "NATIVE_TEXT_PARSER_RSS");
        tagNameMap.extendAndSet(NATIVE_IO_DISPATCHER_RSS, "NATIVE_IO_DISPATCHER_RSS");
        tagNameMap.extendAndSet(NATIVE_FUNC_RSS, "NATIVE_FUNC_RSS");
        tagNameMap.extendAndSet(NATIVE_DIRECT_CHAR_SINK, "NATIVE_DIRECT_CHAR_SINK");
        tagNameMap.extendAndSet(NATIVE_DIRECT_UTF8_SINK, "NATIVE_DIRECT_UTF8_SINK");
        tagNameMap.extendAndSet(NATIVE_DIRECT_BYTE_SINK, "NATIVE_DIRECT_BYTE_SINK");
        tagNameMap.extendAndSet(MMAP_TX_LOG_CURSOR, "MMAP_TX_LOG_CURSOR");
        tagNameMap.extendAndSet(MMAP_TX_LOG, "MMAP_TX_LOG");
        tagNameMap.extendAndSet(NATIVE_SQL_COMPILER, "NATIVE_SQL_COMPILER");
        tagNameMap.extendAndSet(NATIVE_CIRCULAR_BUFFER, "NATIVE_CIRCULAR_BUFFER");
    }
}
