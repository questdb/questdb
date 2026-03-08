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
import io.questdb.std.ObjectFactory;

public class MatViewTimerTask implements ValueHolder<MatViewTimerTask> {
    public static final int ADD = 0;
    public static final ObjectFactory<MatViewTimerTask> ITEM_FACTORY = MatViewTimerTask::new;
    public static final int REMOVE = 1;
    public static final int UPDATE = 2;
    private TableToken matViewToken;
    private int operation = -1;

    @Override
    public void clear() {
        matViewToken = null;
        operation = -1;
    }

    @Override
    public void copyTo(MatViewTimerTask dest) {
        dest.matViewToken = matViewToken;
        dest.operation = operation;
    }

    public TableToken getMatViewToken() {
        return matViewToken;
    }

    public int getOperation() {
        return operation;
    }

    public MatViewTimerTask ofAdd(TableToken matViewToken) {
        this.matViewToken = matViewToken;
        this.operation = ADD;
        return this;
    }

    public MatViewTimerTask ofRemove(TableToken matViewToken) {
        this.matViewToken = matViewToken;
        this.operation = REMOVE;
        return this;
    }

    public MatViewTimerTask ofUpdate(TableToken matViewToken) {
        this.matViewToken = matViewToken;
        this.operation = UPDATE;
        return this;
    }
}
