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

import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.ql.*;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.CharSink;

public class FilteredRowSource extends AbstractRowSource {

    private final RowSource delegate;
    private final VirtualColumn filter;
    private RowCursor underlying;
    private JournalRecord rec;

    public FilteredRowSource(RowSource delegate, VirtualColumn filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.delegate.configure(metadata);
        this.rec = new JournalRecord(metadata);
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        this.underlying = delegate.prepareCursor(slice);
        this.rec.partition = slice.partition;
        return this;
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public boolean hasNext() {
        while (underlying.hasNext()) {
            rec.rowid = underlying.next();
            if (filter.getBool(rec)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long next() {
        return rec.rowid;
    }

    @Override
    public void prepare(StorageFacade facade, CancellationHandler cancellationHandler) {
        delegate.prepare(facade, cancellationHandler);
        filter.prepare(facade);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("FilteredRowSource").put(',');
        sink.putQuoted("rsrc").put(':').put(delegate);
        sink.put('}');
    }

    @Override
    public String toString() {
        return "FilteredRowSource{}";
    }

}
