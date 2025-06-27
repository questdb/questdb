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

package io.questdb.cairo.sql.async;

import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.join.JoinRecord;

public class PageFrameProjectionRecord extends JoinRecord {
    private PageFrameMemoryRecord pageFrameMemoryRecord;

    public PageFrameProjectionRecord(VirtualRecord record) {
        super(record.getColumnCount());
        // page frame record is arriving from jobs and it is not known
        // at the construction phase
        of(record, null);
    }

    public PageFrameProjectionRecord of(PageFrameMemoryRecord baseRecord) {
        this.pageFrameMemoryRecord = baseRecord;
        of(this, baseRecord);
        return this;
    }

    public void setRowIndex(long r) {
        this.pageFrameMemoryRecord.setRowIndex(r);
    }
}
