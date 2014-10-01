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

package com.nfsdb.journal.lang.cst.impl.join;

import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.lang.cst.DataItem;
import com.nfsdb.journal.lang.cst.JoinedSource;

public class InnerSkipJoin extends AbstractImmutableIterator<DataItem> implements JoinedSource {

    private final JoinedSource delegate;
    private DataItem data;

    public InnerSkipJoin(JoinedSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        DataItem data;

        while (delegate.hasNext()) {
            if ((data = delegate.next()).slave != null) {
                this.data = data;
                return true;
            }
        }

        return false;
    }

    @Override
    public DataItem next() {
        return data;
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
