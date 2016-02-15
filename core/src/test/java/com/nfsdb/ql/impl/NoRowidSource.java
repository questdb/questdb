/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.impl;

import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.ops.Parameter;
import com.nfsdb.std.CharSequenceObjHashMap;

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
}
