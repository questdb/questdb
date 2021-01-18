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

package io.questdb.std;

public abstract class AbstractLockable {
    private static final long TARGET_SEQUENCE_OFFSET;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    // to "lock" the entry thread must successfully CAS targetSequence form "srcSequence" value
    // to "srcSequence+1". Executing thread must not be changing value of "srcSequence"
    private int tgtSequence;
    private int srcSequence;

    public boolean tryLock() {
        return Unsafe.cas(this, TARGET_SEQUENCE_OFFSET, srcSequence, srcSequence + 1);
    }

    protected void of(int initialSequence) {
        this.tgtSequence = initialSequence;
        this.srcSequence = initialSequence;
    }

    static {
        TARGET_SEQUENCE_OFFSET = Unsafe.getFieldOffset(AbstractLockable.class, "tgtSequence");
    }
}
