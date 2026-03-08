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

package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;
import io.questdb.mp.ValueHolder;
import io.questdb.std.Numbers;

public class MatViewRefreshTask implements ValueHolder<MatViewRefreshTask> {
    public static final int FULL_REFRESH = 1;
    public static final int INCREMENTAL_REFRESH = 0;
    public static final int INVALIDATE = 3;
    public static final int RANGE_REFRESH = 2;
    public static final int UNDEFINED = -1;
    public static final int UPDATE_REFRESH_INTERVALS = 4;
    public TableToken baseTableToken;
    public String invalidationReason;
    public TableToken matViewToken;
    public int operation = UNDEFINED;
    public long rangeFrom = Numbers.LONG_NULL;
    public long rangeTo = Numbers.LONG_NULL;
    public long refreshTriggerTimestamp = Numbers.LONG_NULL;

    public static String getRefreshOperationName(int operation) {
        switch (operation) {
            case INCREMENTAL_REFRESH:
                return "incremental_refresh";
            case FULL_REFRESH:
                return "full_refresh";
            case RANGE_REFRESH:
                return "range_refresh";
            case INVALIDATE:
                return "invalidate";
            default:
                return "unknown";
        }
    }

    public static boolean isRefreshOperation(int operation) {
        return operation == INCREMENTAL_REFRESH || operation == RANGE_REFRESH || operation == FULL_REFRESH;
    }

    @Override
    public void clear() {
        operation = UNDEFINED;
        baseTableToken = null;
        matViewToken = null;
        invalidationReason = null;
        refreshTriggerTimestamp = Numbers.LONG_NULL;
        rangeFrom = Numbers.LONG_NULL;
        rangeTo = Numbers.LONG_NULL;
    }

    @Override
    public void copyTo(MatViewRefreshTask anotherHolder) {
        anotherHolder.operation = operation;
        anotherHolder.baseTableToken = baseTableToken;
        anotherHolder.matViewToken = matViewToken;
        anotherHolder.invalidationReason = invalidationReason;
        anotherHolder.refreshTriggerTimestamp = refreshTriggerTimestamp;
        anotherHolder.rangeFrom = rangeFrom;
        anotherHolder.rangeTo = rangeTo;
    }

    public boolean isBaseTableTask() {
        return matViewToken == null;
    }
}
