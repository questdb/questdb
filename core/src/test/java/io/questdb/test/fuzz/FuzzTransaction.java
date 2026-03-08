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

package io.questdb.test.fuzz;

import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

public class FuzzTransaction implements QuietCloseable {
    public ObjList<FuzzTransactionOperation> operationList = new ObjList<>();
    public boolean reopenTable;
    public boolean rollback;
    public int structureVersion;
    public boolean waitAllDone;
    public int waitBarrierVersion;
    private LongList noCommitIntervals;
    private long replaceHiTs;
    private long replaceLoTs;

    public LongList getNoCommitIntervals() {
        return noCommitIntervals;
    }

    public long getReplaceHiTs() {
        return replaceHiTs;
    }

    public long getReplaceLoTs() {
        return replaceLoTs;
    }

    public boolean hasReplaceRange() {
        return replaceHiTs > replaceLoTs;
    }

    public void setNoCommitIntervals(LongList excludedIntervals) {
        this.noCommitIntervals = excludedIntervals;
    }

    public void setReplaceRange(long replaceLoTs, long replaceHiTs) {
        this.replaceLoTs = replaceLoTs;
        this.replaceHiTs = replaceHiTs;
        waitAllDone = true;
    }

    @Override
    public void close() {
        Misc.freeObjListAndClear(operationList);
    }
}
