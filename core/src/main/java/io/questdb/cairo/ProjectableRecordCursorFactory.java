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

package io.questdb.cairo;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;

public abstract class ProjectableRecordCursorFactory implements RecordCursorFactory {
    // Same guard, and for the same reason, as AbstractRecordCursorFactory.closed: ownership chains close
    // the same factory from more than one owner on error paths (a failing constructor closes the base
    // factory it adopted, and the generator's catch then frees its own reference to it), while _close()
    // implementations free adopted functions and native resources that must not be freed twice. Leaving
    // subclasses to be idempotent by accident -- because their _close() happens to detach every field it
    // frees -- is not a contract any of them declares. close() sets the flag BEFORE _close() runs, so a
    // throwing _close() cannot let a second owner re-enter and double-free what the first attempt did
    // release; the flip side is that _close() runs at most once, so an implementation owning several
    // resources must attempt them all in one pass.
    private boolean closed;
    private final RecordMetadata metadata;
    private RecordMetadata queryProjectMetadata;

    public ProjectableRecordCursorFactory(RecordMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public final void close() {
        if (!closed) {
            closed = true;
            _close();
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        if (queryProjectMetadata != null) {
            return queryProjectMetadata;
        }
        return metadata;
    }

    public void setQueryProjectedMetadata(RecordMetadata metadata) {
        this.queryProjectMetadata = metadata;
    }

    protected void _close() {
        // nothing to do
    }
}