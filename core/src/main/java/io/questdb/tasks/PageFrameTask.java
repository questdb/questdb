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

package io.questdb.tasks;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.std.DirectLongList;

public class PageFrameTask {
    private DirectLongList rows;
    private PageFrame pageFrame;
    private Function filter;
    private long producerId;

    public Function getFilter() {
        return filter;
    }

    public long getProducerId() {
        return producerId;
    }

    public PageFrame getPageFrame() {
        return pageFrame;
    }

    public DirectLongList getRows() {
        return rows;
    }

    public void of(
            long producerId,
            PageFrame pageFrame,
            Function filter,
            DirectLongList rows
    ) {
        this.producerId = producerId;
        this.pageFrame = pageFrame;
        this.filter = filter;
        this.rows = rows;
    }

    public DirectLongList takeRows() {
        DirectLongList dll = rows;
        rows = null;
        return dll;
    }
}
