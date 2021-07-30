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

package io.questdb.cairo;

import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;

public class TableReplayModel implements Mutable, Sinkable {

    static final String[] ACTION_NAMES = {
            "whole",
            "append"
    };

    private final LongList partitionActions = new LongList();
    private int tableOperation = 0;
    private long dataVersion;

    public void addPartitionAction(long action, long timestamp, long startRow, long rowCount) {
        partitionActions.add(action);
        partitionActions.add(timestamp);
        partitionActions.add(startRow);
        partitionActions.add(rowCount);
    }

    @Override
    public void clear() {
        partitionActions.clear();
        tableOperation = 0;
    }

    public int getTableOperation() {
        return tableOperation;
    }

    public void setTableOperation(int tableOperation) {
        this.tableOperation = tableOperation;
    }

    public long getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(long dataVersion) {
        this.dataVersion = dataVersion;
    }

    @Override
    public void toSink(CharSink sink) {

        switch (tableOperation) {
            case 0:
                sink.put("keep");
                break;
            case 1:
                sink.put("truncate");
                break;
        }
        sink.put(" dataVersion=").put(dataVersion).put('\n');
        for (int i = 0, n = partitionActions.size(); i < n; i += 4) {
            sink
                    .put(ACTION_NAMES[(int) partitionActions.getQuick(i)]).put(' ')
                    .put("ts=").putISODate(partitionActions.getQuick(i + 1)).put(' ')
                    .put("startRow=").put(partitionActions.getQuick(i + 2)).put(' ')
                    .put("rowCount=").put(partitionActions.getQuick(i + 3))
                    .put('\n');
        }
    }
}
