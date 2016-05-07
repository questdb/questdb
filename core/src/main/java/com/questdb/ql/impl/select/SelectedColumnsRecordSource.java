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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.questdb.ql.impl.select;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.CharSink;
import com.questdb.std.ObjList;

public class SelectedColumnsRecordSource extends AbstractCombinedRecordSource {
    private final RecordSource recordSource;
    private final RecordMetadata metadata;
    private final SelectedColumnsRecord record;
    private final SelectedColumnsStorageFacade storageFacade;
    private RecordCursor recordCursor;

    public SelectedColumnsRecordSource(RecordSource recordSource, ObjList<CharSequence> names, CharSequenceHashSet aliases) {
        this.recordSource = recordSource;
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names, aliases);
        this.record = new SelectedColumnsRecord(dm, names);
        this.storageFacade = new SelectedColumnsStorageFacade(dm, metadata, names);
    }

    public SelectedColumnsRecordSource(RecordSource recordSource, ObjList<CharSequence> names) {
        this.recordSource = recordSource;
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new SelectedColumnsMetadata(dm, names);
        this.record = new SelectedColumnsRecord(dm, names);
        this.storageFacade = new SelectedColumnsStorageFacade(dm, metadata, names);
    }

    @Override
    public Record getByRowId(long rowId) {
        return record.of(recordCursor.getByRowId(rowId));
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory);
        this.storageFacade.of(recordCursor.getStorageFacade());
        return this;
    }

    @Override
    public void reset() {
        recordSource.reset();
    }

    @Override
    public boolean supportsRowIdAccess() {
        return recordSource.supportsRowIdAccess();
    }

    @Override
    public boolean hasNext() {
        return recordCursor.hasNext();
    }

    @Override
    public Record next() {
        return record.of(recordCursor.next());
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("SelectedColumnsRecordSource").put(',');
        sink.putQuoted("src").put(':').put(recordSource);
        sink.put('}');
    }
}
