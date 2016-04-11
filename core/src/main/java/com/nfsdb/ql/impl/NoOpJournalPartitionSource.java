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

package com.nfsdb.ql.impl;

import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionCursor;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.PartitionSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.std.AbstractImmutableIterator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class NoOpJournalPartitionSource extends AbstractImmutableIterator<PartitionSlice> implements PartitionSource, PartitionCursor {

    private final JournalMetadata metadata;

    public NoOpJournalPartitionSource(JournalMetadata metadata) {
        this.metadata = metadata;
        reset();
    }

    @Override
    public JournalMetadata getMetadata() {
        return metadata;
    }

    @Override
    public PartitionCursor prepareCursor(JournalReaderFactory readerFactory) throws JournalException {
        return this;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public final void reset() {
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
    @Override
    public PartitionSlice next() {
        return null;
    }

    @Override
    public String toString() {
        return "NoOpJournalPartitionSource{" +
                "metadata=\n" + metadata +
                '}';
    }
}
