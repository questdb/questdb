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
import com.nfsdb.lang.cst.GenericRecordSource;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.lang.cst.impl.qry.SplitRecord;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
public class InnerSkipJoin extends AbstractImmutableIterator<Record> implements GenericRandomAccessRecordSource {

    private final RecordSource<? extends SplitRecord> delegate;
    private Record data;

    public InnerSkipJoin(RecordSource<? extends SplitRecord> delegate) {
        this.delegate = delegate;
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public Record getByRowId(long rowId) {
        return null;
    }

    @Override
    public boolean hasNext() {
        SplitRecord data;

        while (delegate.hasNext()) {
            if ((data = delegate.next()).hasB()) {
                this.data = data;
                return true;
            }
        }
        this.data = null;
        return false;
    }

    @Override
    public Record next() {
        return data;
    }

    @Override
    public void reset() {
        delegate.reset();
    }
}
