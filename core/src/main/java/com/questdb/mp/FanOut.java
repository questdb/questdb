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

import com.questdb.std.ObjList;
import com.questdb.std.Unsafe;

public class FanOut implements Barrier {
    private static final long HOLDER = Unsafe.getFieldOffset(FanOut.class, "holder");
    private final Holder holder;
    private Barrier barrier;

    public FanOut(Barrier... barriers) {
        Holder h = new Holder();
        for (int i = 0; i < barriers.length; i++) {
            Barrier sq = barriers[i];
            h.barriers.add(sq);
            if (sq.getWaitStrategy().acceptSignal()) {
                h.waitStrategies.add(sq.getWaitStrategy());
            }
        }
        h.setupWaitStrategy();
        holder = h;
    }

    public static FanOut to(Barrier barrier) {
        return new FanOut().and(barrier);
    }

    public <T extends Barrier> T addAndGet(T sequence) {
        and(sequence);
        return sequence;
    }

    public FanOut and(Barrier barrier) {
        Holder _new;
        do {
            Holder h = this.holder;
            // read barrier to make sure "holder" read doesn't fall below this

            if (h.barriers.indexOf(barrier) > -1) {
                return this;
            }
            if (this.barrier != null) {
                barrier.root().setBarrier(this.barrier);
            }
            _new = new Holder();
            _new.barriers.addAll(h.barriers);
            _new.barriers.add(barrier);
            _new.waitStrategies.addAll(h.waitStrategies);

            if (barrier.getWaitStrategy().acceptSignal()) {
                _new.waitStrategies.add(barrier.getWaitStrategy());
            }
            _new.setupWaitStrategy();

        } while (!Unsafe.getUnsafe().compareAndSwapObject(this, HOLDER, holder, _new));

        return this;
    }

    // this is firebug bug, the code does not write to array elements
    // it has to take a copy of this.barriers as this reference can change while
    // loop is in flight
    @Override
    public long availableIndex(final long lo) {
        long l = Long.MAX_VALUE;
        ObjList<Barrier> sequences = holder.barriers;
        for (int i = 0, n = sequences.size(); i < n; i++) {
            l = Math.min(l, sequences.getQuick(i).availableIndex(lo));
        }
        return l;
    }

    @Override
    public WaitStrategy getWaitStrategy() {
        return holder.waitStrategy;
    }

    @Override
    public Barrier root() {
        return barrier != null ? barrier.root() : this;
    }

    public void setBarrier(Barrier barrier) {
        this.barrier = barrier;
        ObjList<Barrier> barriers = holder.barriers;
        for (int i = 0, n = barriers.size(); i < n; i++) {
            barriers.getQuick(i).root().setBarrier(barrier);
        }
    }

    @Override
    public Barrier then(Barrier barrier) {
        barrier.setBarrier(this);
        return barrier;
    }

    public void remove(Barrier barrier) {
        Holder _new;
        do {
            Holder h = this.holder;
            // read barrier to make sure "holder" read doesn't fall below this

            if (h.barriers.indexOf(barrier) == -1) {
                return;
            }
            _new = new Holder();
            for (int i = 0, n = h.barriers.size(); i < n; i++) {
                Barrier sq = h.barriers.getQuick(i);
                if (sq != barrier) {
                    _new.barriers.add(sq);
                }
            }

            WaitStrategy that = barrier.getWaitStrategy();
            if (that.acceptSignal()) {
                for (int i = 0, n = h.waitStrategies.size(); i < n; i++) {
                    WaitStrategy ws = h.waitStrategies.getQuick(i);
                    if (ws != that) {
                        _new.waitStrategies.add(ws);
                    }
                }
            } else {
                _new.waitStrategies.addAll(h.waitStrategies);
            }
            _new.setupWaitStrategy();

        } while (!Unsafe.getUnsafe().compareAndSwapObject(this, HOLDER, holder, _new));
    }

    private static class Holder {

        private final ObjList<Barrier> barriers = new ObjList<>();
        private final ObjList<WaitStrategy> waitStrategies = new ObjList<>();
        private final FanOutWaitStrategy fanOutWaitStrategy = new FanOutWaitStrategy();
        private WaitStrategy waitStrategy;

        private void setupWaitStrategy() {
            if (waitStrategies.size() > 0) {
                waitStrategy = fanOutWaitStrategy;
            } else {
                waitStrategy = NullWaitStrategy.INSTANCE;
            }
        }

        private class FanOutWaitStrategy implements WaitStrategy {
            @Override
            public boolean acceptSignal() {
                for (int i = 0, n = waitStrategies.size(); i < n; i++) {
                    if (waitStrategies.getQuick(i).acceptSignal()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void alert() {
                for (int i = 0, n = waitStrategies.size(); i < n; i++) {
                    waitStrategies.getQuick(i).alert();
                }
            }

            @Override
            public void await() {
                for (int i = 0, n = waitStrategies.size(); i < n; i++) {
                    waitStrategies.getQuick(i).await();
                }
            }

            @Override
            public void signal() {
                for (int i = 0, n = waitStrategies.size(); i < n; i++) {
                    waitStrategies.getQuick(i).signal();
                }
            }
        }
    }
}
