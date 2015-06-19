/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.query;

import com.nfsdb.Journal;
import com.nfsdb.collections.LongList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.utils.Rows;

public class OrderedResultSet<T> extends ResultSet<T> {
    public OrderedResultSet(Journal<T> journal, LongList rowIDs) {
        super(journal, rowIDs);
    }

    public long getMaxTimestamp() throws JournalException {
        if (size() == 0) {
            return 0;
        }
        long rowID = getRowID(size() - 1);
        int timestampColumnIndex = getJournal().getMetadata().getTimestampIndex();
        return getJournal().getPartition(Rows.toPartitionIndex(rowID), true).getLong(Rows.toLocalRowID(rowID), timestampColumnIndex);
    }
}
