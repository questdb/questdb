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

import com.questdb.std.str.CharSink;
import com.questdb.std.str.FileNameExtractorCharSequence;
import com.questdb.store.Partition;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

public class NoOpJournalPartitionSource implements PartitionSource, PartitionCursor, com.questdb.std.ImmutableIterator<PartitionSlice> {

    private final JournalMetadata metadata;

    public NoOpJournalPartitionSource(JournalMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public JournalMetadata getMetadata() {
        return metadata;
    }

    @Override
    public PartitionCursor prepareCursor(ReaderFactory readerFactory) {
        return this;
    }

    @Override
    public Partition getPartition(int index) {
        return null;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public void releaseCursor() {
    }

    @Override
    public void toTop() {
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public PartitionSlice next() {
        return null;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("NoOpJournalPartitionSource").put(',');
        sink.putQuoted("journal").put(':').putQuoted(FileNameExtractorCharSequence.get(metadata.getName()));
        sink.put('}');
    }
}
