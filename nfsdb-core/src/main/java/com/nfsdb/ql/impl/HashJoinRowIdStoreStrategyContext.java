/*
 * Copyright (c) 2014-2015. NFSdb.
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

import com.nfsdb.ql.RandomAccessRecordSource;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;

public class HashJoinRowIdStoreStrategyContext implements HashJoinStoreStrategyContext {
    private final RandomAccessRecordSource<? extends Record> slaveSource;
    private final RowIdHolderRecord rowIdRecord = new RowIdHolderRecord();

    public HashJoinRowIdStoreStrategyContext(RandomAccessRecordSource<? extends Record> slaveSource) {
        this.slaveSource = slaveSource;
    }

    @Override
    public Record getHashTableRecord(Record r) {
        return rowIdRecord.init(r.getRowId());
    }

    @Override
    public RecordMetadata getHashRecordMetadata() {
        return rowIdRecord.getMetadata();
    }

    @Override
    public Record getNextSlave(Record hashTableRecord) {
        return slaveSource.getByRowId(hashTableRecord.getLong(0));
    }
}
