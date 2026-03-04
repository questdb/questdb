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

package io.questdb.cairo.sql;

import io.questdb.std.Rows;

/**
 * A {@link PageFrameMemoryRecord} whose {@link #getRowId()} includes the
 * {@link TimeFrameCursor#TIME_FRAME_ROW_ID_MARKER} bits. Use this record
 * type in time frame cursor implementations so that callers that store
 * and later replay row IDs get correctly marked values.
 */
public class TimeFrameMemoryRecord extends PageFrameMemoryRecord {

    public TimeFrameMemoryRecord(byte letter) {
        super(letter);
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(frameIndex, rowIndex) | TimeFrameCursor.TIME_FRAME_ROW_ID_MARKER;
    }
}
