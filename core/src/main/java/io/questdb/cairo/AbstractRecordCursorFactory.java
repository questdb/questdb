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

/**
 * Abstract base class for record cursor factory implementations.
 */
public abstract class AbstractRecordCursorFactory implements RecordCursorFactory {
    /**
     * The record metadata.
     */
    private final RecordMetadata metadata;
    // Guards _close() against repeated invocation: factory ownership chains close the same
    // instance from more than one owner on error paths (a failing factory constructor closes
    // its adopted base factory, and the generator catch then frees its own reference to it),
    // and _close() implementations free adopted functions and native resources that must not
    // be freed twice. close() deliberately sets the flag before _close() runs: were it set
    // after, a throwing _close() would let a second owner re-enter and double-free whatever
    // the first attempt did release. The flip side is that _close() runs at most once, so
    // implementations owning several resources must keep their cleanup best-effort (attempt
    // every close, rethrow the first failure with later ones suppressed) rather than stop at
    // the first throwing close. Sorted after the final field per the final-then-mutable field
    // grouping.
    private boolean closed;

    /**
     * Constructs a new record cursor factory.
     *
     * @param metadata the record metadata
     */
    public AbstractRecordCursorFactory(RecordMetadata metadata) {
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
        return metadata;
    }

    /**
     * Closes resources held by this factory. Subclasses should override to release resources.
     */
    protected void _close() {
        // nothing to do
    }
}
