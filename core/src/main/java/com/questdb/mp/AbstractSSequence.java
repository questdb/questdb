/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

abstract class AbstractSSequence implements Sequence {
    private final WaitStrategy waitStrategy;
    Barrier barrier = OpenBarrier.INSTANCE;

    AbstractSSequence(WaitStrategy waitStrategy) {
        this.waitStrategy = waitStrategy;
    }

    AbstractSSequence() {
        this(null);
    }

    @Override
    public void alert() {
        if (waitStrategy != null) {
            waitStrategy.alert();
        }
    }

    @Override
    public void followedBy(Barrier barrier) {
        this.barrier = barrier;
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
        return waitStrategy == null ? next() : waitForNext0();
    }

    @Override
    public void signal() {
        if (waitStrategy != null) {
            waitStrategy.signal();
        }
    }

    private void bully() {
        this.barrier.signal();
    }

    private long waitForNext0() {
        long r;
        while ((r = next()) < 0) {
            waitStrategy.await();
        }
        return r;
    }
}
