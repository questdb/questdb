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

package io.questdb.mp;

//single consumer or producer sequence
abstract class AbstractSSequence extends AbstractSequence implements Sequence {

    AbstractSSequence(WaitStrategy waitStrategy) {
        super(waitStrategy);
    }

    AbstractSSequence() {
        this(NullWaitStrategy.INSTANCE);
    }

    @Override
    public Barrier getBarrier() {
        return barrier;
    }

    @Override
    public long nextBully() {
        long cursor;
        while ((cursor = next()) < 0) {
            bully();
        }
        return cursor;
    }

    @Override
    public Barrier root() {
        return barrier != OpenBarrier.INSTANCE ? barrier.root() : this;
    }

    @Override
    public void setBarrier(Barrier barrier) {
        this.barrier = barrier;
    }

    @Override
    public Barrier then(Barrier barrier) {
        barrier.setBarrier(this);
        return barrier;
    }

    @Override
    public long waitForNext() {
        long r;
        WaitStrategy waitStrategy = getWaitStrategy();
        while ((r = next()) < 0) {
            if (r == -2) {
                continue;
            }
            waitStrategy.await();
        }
        return r;
    }

    private void bully() {
        barrier.getWaitStrategy().signal();
    }
}
