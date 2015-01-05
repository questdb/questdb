/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.lang.cst.impl.qry.GenericRecordSource;
import com.nfsdb.lang.cst.impl.qry.Record;
import com.nfsdb.lang.cst.impl.qry.RecordMetadata;
import com.nfsdb.lang.cst.impl.qry.RecordSource;

import java.util.NoSuchElementException;

public class InnerSkipJoin extends AbstractImmutableIterator<Record> implements GenericRecordSource {

    private final RecordSource<? extends Record> delegate;
    private Record data;

    public InnerSkipJoin(RecordSource<? extends Record> delegate) {
        this.delegate = delegate;
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public boolean hasNext() {
        Record data;

        while (delegate.hasNext()) {
            if ((data = delegate.next()).getSlave() != null) {
                this.data = data;
                return true;
            }
        }
        this.data = null;
        return false;
    }

    @Override
    public Record next() {
        if (data == null) {
            throw new NoSuchElementException();
        }
        return data;
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
