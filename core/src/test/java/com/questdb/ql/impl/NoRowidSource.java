/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.impl;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.ql.ops.Parameter;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.CharSink;

public class NoRowidSource implements RecordSource {
    private RecordSource delegate;

    @Override
    public RecordMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public Parameter getParam(CharSequence name) {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        return delegate.prepareCursor(factory, cancellationHandler);
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        return delegate.prepareCursor(factory);
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public void setParameterMap(CharSequenceObjHashMap<Parameter> map) {
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    public NoRowidSource of(RecordSource delegate) {
        this.delegate = delegate;
        return this;
    }

    @Override
    public void toSink(CharSink sink) {
        delegate.toSink(sink);
    }
}
