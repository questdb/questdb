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


package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;

/**
 * A row cursor factory whose rows come from an ordered list of keys, each scanned on its own,
 * such as the per-symbol index cursors of an {@code IN (...)} list. Besides the usual per-frame
 * cursor, which interleaves all keys within one page frame, it hands out a cursor for one key
 * over one frame, so that {@link KeyMajorPageFrameRecordCursor} can emit the rows key by key
 * across all page frames of the scan.
 */
public interface KeyedRowCursorFactory extends RowCursorFactory {

    /**
     * Returns the rows of the key at {@code keyIndex} within the given page frame, in the
     * factory's index direction. The frame passed in may be a snapshot that supports only
     * {@link PageFrame#getIndexReader(int, int)}, {@link PageFrame#getPartitionLo()},
     * {@link PageFrame#getPartitionHi()}, {@link PageFrame#getPartitionIndex()} and
     * {@link PageFrame#getFormat()}.
     */
    RowCursor getCursor(int keyIndex, PageFrame pageFrame, PageFrameMemory pageFrameMemory);

    /**
     * Index column scanned by every key, the column the frame snapshot serves index readers for.
     */
    int getIndexColumnIndex();

    /**
     * Index direction of every key, see {@link io.questdb.cairo.idx.IndexReader#DIR_FORWARD}.
     */
    int getIndexDirection();

    /**
     * Index key (see {@link io.questdb.cairo.TableUtils#toIndexKey(int)}) of the key at
     * {@code keyIndex}, or -1 when it is resolved only inside the row cursor. Lets
     * {@link KeyMajorPageFrameRecordCursor} check the index before it decodes a Parquet frame that
     * may hold no rows of the key.
     */
    int getIndexKey(int keyIndex);

    /**
     * Number of keys, valid once {@link #prepareCursor} has run for the current execution.
     */
    int getKeyCount();
}
