/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.index.experimental;

public class FilteredCursor extends AbstractFilteredCursor {

    private Cursor delegate;
    private CursorFilter filter;

    public FilteredCursor() {
    }

    public void configure(Cursor delegate, CursorFilter filter) {
        this.delegate = delegate;
        this.filter = filter;
        reset();
    }

    @Override
    protected long getNext() {
        while (delegate.hasNext()) {
            long value = delegate.next();
            if (filter.accept(value)) {
                return value;
            }
        }
        return -2;
    }
}
