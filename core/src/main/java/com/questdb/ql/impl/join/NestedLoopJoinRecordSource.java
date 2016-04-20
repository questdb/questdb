/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.ql.impl.join;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.ops.AbstractCombinedRecordSource;

public class NestedLoopJoinRecordSource extends AbstractCombinedRecordSource {
    private final RecordSource masterSource;
    private final RecordSource slaveSource;
    private final SplitRecordMetadata metadata;
    private final SplitRecord record;
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;
    private boolean nextSlave = false;

    public NestedLoopJoinRecordSource(RecordSource masterSource, RecordSource slaveSource) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;

        RecordMetadata mm = masterSource.getMetadata();
        this.metadata = new SplitRecordMetadata(mm, slaveSource.getMetadata());
        this.record = new SplitRecord(metadata, mm.getColumnCount());
    }

    @Override
    public SplitRecord getByRowId(long rowId) {
        return null;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        masterCursor = masterSource.prepareCursor(factory);
        slaveCursor = slaveSource.prepareCursor(factory);
        return this;
    }

    @Override
    public void reset() {
        // cursor reset
        masterSource.reset();
        slaveSource.reset();
        nextSlave = false;
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    @Override
    public boolean hasNext() {
        return nextSlave || masterCursor.hasNext();
    }

    @Override
    public SplitRecord next() {
        if (!nextSlave) {
            record.setA(masterCursor.next());
            slaveSource.reset();
        }

        if (nextSlave || slaveCursor.hasNext()) {
            record.setB(slaveCursor.next());
            nextSlave = slaveCursor.hasNext();
        } else {
            record.setB(null);
            nextSlave = false;
        }
        return record;
    }
}
