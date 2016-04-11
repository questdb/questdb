/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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

package com.nfsdb.ql.impl.select;

import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.AbstractCombinedRecordSource;
import com.nfsdb.std.CharSequenceHashSet;
import com.nfsdb.std.ObjList;

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
    public String toString() {
        return "SelectedColumnsRecordSource{" +
                "recordSource=" + recordSource +
                ", metadata=" + metadata +
                '}';
    }
}
