/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

public class OuterJoinRecord extends JoinRecord {
    private final Record nullRecord;
    private Record flappingSlave;

    public OuterJoinRecord(int split, Record nullRecord) {
        super(split);
        this.nullRecord = nullRecord;
    }

    void hasSlave(boolean value) {
        if (value) {
            if (flappingSlave != slave) {
                slave = flappingSlave;
            }
        } else {
            if (slave != nullRecord) {
                slave = nullRecord;
            }
        }
    }

    void of(Record master, Record slave) {
        super.of(master, slave);
        this.flappingSlave = slave;
    }
}
