/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

public class MatViewRefreshTask implements ValueHolder<MatViewRefreshTask> {
    public static final int FULL_REFRESH = 1;
    public static final int INCREMENTAL_REFRESH = 0;
    public static final int INVALIDATE = 2;
    public static final int UNDEFINED = -1;
    public TableToken baseTableToken;
    public String invalidationReason;
    public TableToken matViewToken;
    public int operation = UNDEFINED;
    public long refreshTriggeredTimestamp = -1;

    @Override
    public void clear() {
        operation = UNDEFINED;
        baseTableToken = null;
        matViewToken = null;
        invalidationReason = null;
        refreshTriggeredTimestamp = -1;
    }

    @Override
    public void copyTo(MatViewRefreshTask anotherHolder) {
        anotherHolder.operation = operation;
        anotherHolder.baseTableToken = baseTableToken;
        anotherHolder.matViewToken = matViewToken;
        anotherHolder.invalidationReason = invalidationReason;
        anotherHolder.refreshTriggeredTimestamp = refreshTriggeredTimestamp;
    }

    public boolean isBaseTableTask() {
        return matViewToken == null;
    }
}
