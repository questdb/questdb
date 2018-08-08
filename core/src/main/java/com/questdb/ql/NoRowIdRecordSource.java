/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql;

import com.questdb.ql.ops.AbstractRecordSource;
import com.questdb.ql.ops.Parameter;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Misc;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.factory.ReaderFactory;

public class NoRowIdRecordSource extends AbstractRecordSource {
    private RecordSource delegate;

    @Override
    public void close() {
        Misc.free(delegate);
    }

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        return delegate.prepareCursor(factory, cancellationHandler);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    @Override
    public Parameter getParam(CharSequence name) {
        return delegate.getParam(name);
    }

    @Override
    public void setParameterMap(CharSequenceObjHashMap<Parameter> map) {
        delegate.setParameterMap(map);
    }

    @Override
    public Record getRecord() {
        return delegate.getRecord();
    }

    @Override
    public Record newRecord() {
        return delegate.newRecord();
    }

    public NoRowIdRecordSource of(RecordSource delegate) {
        this.delegate = delegate;
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("NoRowIdRecordSource").put(',');
        sink.putQuoted("src").put(':').put(delegate);
        sink.put('}');
    }
}
