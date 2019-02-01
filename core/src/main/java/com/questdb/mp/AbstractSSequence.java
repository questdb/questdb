/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.mp;

abstract class AbstractSSequence extends AbstractSequence implements Sequence {

    AbstractSSequence(WaitStrategy waitStrategy) {
        super(waitStrategy);
    }

    AbstractSSequence() {
        this(NullWaitStrategy.INSTANCE);
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
    public long waitForNext() {
        long r;
        WaitStrategy waitStrategy = getWaitStrategy();
        while ((r = next()) < 0) {
            waitStrategy.await();
        }
        return r;
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

    private void bully() {
        barrier.getWaitStrategy().signal();
    }
}
