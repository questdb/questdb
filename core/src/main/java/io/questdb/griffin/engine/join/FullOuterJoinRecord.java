/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;

public class FullOuterJoinRecord extends JoinRecord {
    protected Record masterNullRecord;
    protected Record slaveNullRecord;
    private Record flappingMaster;
    private Record flappingSlave;

    public FullOuterJoinRecord(int split, Record masterNullRecord, Record slaveNullRecord) {
        super(split);
        this.masterNullRecord = masterNullRecord;
        this.slaveNullRecord = slaveNullRecord;
    }

    public void of(Record master, Record slave) {
        super.of(master, slave);
        this.flappingMaster = master;
        this.flappingSlave = slave;
    }

    void hasMaster(boolean value) {
        if (value) {
            master = flappingMaster;
        } else {
            master = masterNullRecord;
        }
    }

    boolean hasMaster() {
        return master != masterNullRecord;
    }

    void hasSlave(boolean value) {
        if (value) {
            slave = flappingSlave;
        } else {
            slave = slaveNullRecord;
        }
    }
}
