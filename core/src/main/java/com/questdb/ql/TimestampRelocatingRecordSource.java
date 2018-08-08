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
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.factory.ReaderFactory;

public class TimestampRelocatingRecordSource extends AbstractRecordSource implements RecordMetadata {
    private final RecordSource delegate;
    private final RecordMetadata metadata;
    private final int timestampIndex;

    public TimestampRelocatingRecordSource(RecordSource delegate, int timestampIndex) {
        this.delegate = delegate;
        this.metadata = delegate.getMetadata();
        this.timestampIndex = timestampIndex;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public RecordMetadata getMetadata() {
        return this;
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        return delegate.prepareCursor(factory, cancellationHandler);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return delegate.supportsRowIdAccess();
    }

    @Override
    public String getAlias() {
        return metadata.getAlias();
    }

    @Override
    public void setAlias(String alias) {
        metadata.setAlias(alias);
    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return metadata.getColumn(name);
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        return metadata.getColumnIndex(name);
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        return metadata.getColumnIndexQuiet(name);
    }

    @Override
    public String getColumnName(int index) {
        return metadata.getColumnName(index);
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return metadata.getColumnQuick(index);
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public Record getRecord() {
        return delegate.getRecord();
    }

    @Override
    public Record newRecord() {
        return delegate.newRecord();
    }

    @Override
    public void toSink(CharSink sink) {

    }
}
