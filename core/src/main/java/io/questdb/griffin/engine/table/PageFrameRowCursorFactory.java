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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.griffin.PlanSink;

public class PageFrameRowCursorFactory implements RowCursorFactory {
    private final int baseOrder;
    private PageFrameBwdRowCursor bwdCursor;
    private PageFrameFwdRowCursor fwdCursor;

    public PageFrameRowCursorFactory(int order) {
        this.baseOrder = order;
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        if (baseOrder == PartitionFrameCursorFactory.ORDER_ASC || baseOrder == PartitionFrameCursorFactory.ORDER_ANY) {
            if (fwdCursor == null) {
                fwdCursor = new PageFrameFwdRowCursor();
            }
            return fwdCursor.of(pageFrame);
        }

        if (bwdCursor == null) {
            bwdCursor = new PageFrameBwdRowCursor();
        }
        return bwdCursor.of(pageFrame);
    }

    @Override
    public boolean isEntity() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        int order = sink.getOrder();
        if (order == PartitionFrameCursorFactory.ORDER_ANY || order < 0) {
            order = baseOrder;
        }
        if (order == PartitionFrameCursorFactory.ORDER_DESC) {
            sink.type("Row backward scan");
        } else {
            sink.type("Row forward scan");
        }
    }
}
