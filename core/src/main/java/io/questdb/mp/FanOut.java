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

import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

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

    public FanOut and(Barrier barrier) {
        Holder _new;
        Barrier root = null;

        final long current = this.barrier != null ? this.barrier.current() : -1;
        Unsafe.getUnsafe().loadFence();

        do {
            Holder h = this.holder;
            // read barrier to make sure "holder" read doesn't fall below this

            if (h.barriers.indexOf(barrier) > -1) {
                return this;
            }

            if (root == null && this.barrier != null) {
                root = barrier.root();
                root.setBarrier(this.barrier);
                root.setCurrent(current);
                Unsafe.getUnsafe().storeFence();
            }
            _new = new Holder();
            _new.barriers.addAll(h.barriers);
            _new.barriers.add(barrier);
            _new.waitStrategies.addAll(h.waitStrategies);

            if (barrier.getWaitStrategy().acceptSignal()) {
                _new.waitStrategies.add(barrier.getWaitStrategy());
            }
            _new.setupWaitStrategy();
            if (Unsafe.getUnsafe().compareAndSwapObject(this, HOLDER, h, _new)) {
                // catch up with others
                if (root != null && this.barrier != null) {
                    root.setCurrent(this.barrier.current());
                }
                break;
            }
        } while (true);

        Unsafe.getUnsafe().storeFence();
        return this;
    }

    // this is firebug bug, the code does not write to array elements
    // it has to take a copy of this.barriers as this reference can change while
    // loop is in flight
    @Override
    public long availableIndex(final long lo) {
        long l = barrier.availableIndex(lo);
        ObjList<Barrier> barriers = holder.barriers;
        for (int i = 0, n = barriers.size(); i < n; i++) {
            l = Math.min(l, barriers.getQuick(i).availableIndex(lo));
        }
        return l;
    }

    @Override
    public long current() {
        return barrier.current();
    }

    @Override
    public Barrier getBarrier() {
        return barrier;
    }

    @Override
    public WaitStrategy getWaitStrategy() {
        return holder.waitStrategy;
    }

    public void remove(SCSequence barrier) {
        Unsafe.getUnsafe().storeFence();

        Holder _new;
        do {
            Holder h = this.holder;
            // read barrier to make sure "holder" read doesn't fall below this

            if (h.barriers.indexOf(barrier) == -1) {
                break;
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
            if (Unsafe.getUnsafe().compareAndSwapObject(this, HOLDER, h, _new)) {
                break;
            }
        } while (true);
        barrier.reset();
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
    public void setCurrent(long value) {
        ObjList<Barrier> barriers = holder.barriers;
        for (int i = 0, n = barriers.size(); i < n; i++) {
            barriers.getQuick(i).setCurrent(value);
        }
    }

    @Override
    public Barrier then(Barrier barrier) {
        barrier.setBarrier(this);
        return barrier;
    }

    private static class Holder {

        private final ObjList<Barrier> barriers = new ObjList<>();
        private final FanOutWaitStrategy fanOutWaitStrategy = new FanOutWaitStrategy();
        private final ObjList<WaitStrategy> waitStrategies = new ObjList<>();
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
