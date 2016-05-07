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

package com.questdb.ql.impl;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.*;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSink;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JournalSource extends AbstractCombinedRecordSource {
    private final PartitionSource partitionSource;
    private final RowSource rowSource;
    private final JournalRecord rec;
    private final JournalMetadata metadata;
    private PartitionCursor partitionCursor;
    private RowCursor cursor;

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public JournalSource(PartitionSource partitionSource, RowSource rowSource) {
        this.metadata = partitionSource.getMetadata();
        this.rec = new JournalRecord(this.metadata);
        this.partitionSource = partitionSource;
        rowSource.configure(partitionSource.getMetadata());
        this.rowSource = rowSource;
    }

    @Override
    public JournalRecord getByRowId(long rowId) {
        rec.rowid = rowId;
        return rec;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return partitionCursor.getStorageFacade();
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.partitionCursor = partitionSource.prepareCursor(factory);
        this.rowSource.prepare(partitionCursor.getStorageFacade());
        return this;
    }

    @Override
    public void reset() {
        // cursor type reset
        if (partitionCursor != null) {
            partitionCursor.reset();
        }
        rowSource.reset();
        cursor = null;
    }

    @Override
    public boolean supportsRowIdAccess() {
        return true;
    }

    @Override
    public boolean hasNext() {
        return (cursor != null && cursor.hasNext()) || nextSlice();
    }

    @Override
    public JournalRecord next() {
        rec.rowid = cursor.next();
        return rec;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("JournalSource").put(',');
        sink.putQuoted("psrc").put(':').put(partitionSource).put(',');
        sink.putQuoted("rsrc").put(':').put(rowSource);
        sink.put('}');
    }

    @SuppressWarnings("unchecked")
    private boolean nextSlice() {
        while (partitionCursor.hasNext()) {
            PartitionSlice slice = partitionCursor.next();
            cursor = rowSource.prepareCursor(slice);

            if (cursor == null) {
                return false;
            }

            if (cursor.hasNext()) {
                rec.partition = slice.partition;
                return true;
            }
        }

        return false;
    }
}
