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

package io.questdb.cairo.wal;

public class WalUtils {
    public static final String CONVERT_FILE_NAME = "_convert";
    public static final int CREATE_TABLE_WALID = -3;
    public static final int DROP_TABLE_STRUCTURE_VERSION = -2;
    public static final int DROP_TABLE_WALID = -2;
    public static final String EVENT_FILE_NAME = "_event";
    public static final String EVENT_INDEX_FILE_NAME = "_event.i";
    public static final CharSequence INITIAL_META_FILE_NAME = "_meta.0";
    public static final int METADATA_WALID = -1;
    public static final int SEG_MIN_ID = 0;
    public static final int SEG_NONE_ID = Integer.MAX_VALUE >> 2;
    public static final int SEG_MAX_ID = SEG_NONE_ID - 1;
    public static final String SEQ_DIR = "txn_seq";
    public static final String SEQ_DIR_DEPRECATED = "seq";
    public static final long SEQ_META_OFFSET_WAL_LENGTH = 0;
    public static final long SEQ_META_OFFSET_WAL_VERSION = SEQ_META_OFFSET_WAL_LENGTH + Integer.BYTES;
    public static final long SEQ_META_OFFSET_STRUCTURE_VERSION = SEQ_META_OFFSET_WAL_VERSION + Integer.BYTES;
    public static final long SEQ_META_OFFSET_COLUMN_COUNT = SEQ_META_OFFSET_STRUCTURE_VERSION + Long.BYTES;
    public static final long SEQ_META_OFFSET_TIMESTAMP_INDEX = SEQ_META_OFFSET_COLUMN_COUNT + Integer.BYTES;
    public static final long SEQ_META_TABLE_ID = SEQ_META_OFFSET_TIMESTAMP_INDEX + Integer.BYTES;
    public static final long SEQ_META_SUSPENDED = SEQ_META_TABLE_ID + Integer.BYTES;
    public static final long SEQ_META_OFFSET_COLUMNS = SEQ_META_SUSPENDED + Byte.BYTES;
    public static final String TABLE_REGISTRY_NAME_FILE = "tables.d";
    public static final String TXNLOG_FILE_NAME = "_txnlog";
    public static final String TXNLOG_FILE_NAME_META_INX = "_txnlog.meta.i";
    public static final String TXNLOG_FILE_NAME_META_VAR = "_txnlog.meta.d";
    public static final int WALE_HEADER_SIZE = Integer.BYTES + Integer.BYTES;
    public static final long WALE_MAX_TXN_OFFSET_32 = 0L;
    public static final int WAL_FORMAT_OFFSET_32 = Integer.BYTES;
    public static final int WAL_FORMAT_VERSION = 0;
    public static final String WAL_INDEX_FILE_NAME = "_wal_index.d";
    public static final String WAL_NAME_BASE = "wal";
}
