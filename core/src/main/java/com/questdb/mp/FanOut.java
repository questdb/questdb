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

import com.questdb.ex.FatalError;
import com.questdb.misc.Unsafe;
import com.questdb.std.ObjList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class FanOut implements Barrier {
    private static final long HOLDER;
    private Holder holder;
    private volatile boolean barrier;

    public FanOut(Sequence... sequences) {
        Holder h = new Holder();
        for (int i = 0; i < sequences.length; i++) {
            Sequence sq = sequences[i];
            h.sequences.add(sq);
            if (sq.hasWaitStrategy()) {
                h.waitStrategies.add(sq.getWaitStrategy());
            }
        }
        h.setupWaitStrategy();
        holder = h;
    }

    public void add(Sequence sequence) {
        Holder _new;
        do {
            Holder h = this.holder;
            // read barrier to make sure "holder" read doesn't fall below this
            boolean b = barrier;

            if (h.sequences.indexOf(sequence) > -1) {
                return;
            }
            _new = new Holder();
            _new.sequences.addAll(h.sequences);
            _new.sequences.add(sequence);
            _new.waitStrategies.addAll(h.waitStrategies);

            if (sequence.hasWaitStrategy()) {
                _new.waitStrategies.add(sequence.getWaitStrategy());
            }
            _new.setupWaitStrategy();
        } while (!Unsafe.getUnsafe().compareAndSwapObject(this, HOLDER, holder, _new));
    }

    @SuppressFBWarnings("CVAA_CONTRAVARIANT_ELEMENT_ASSIGNMENT")
    // this is firebug bug, the code does not write to array elements
    // it has to take a copy of this.barriers as this reference can change while
    // loop is in flight
    @Override
    public long availableIndex(final long lo) {
        long l = Long.MAX_VALUE;
        ObjList<Sequence> sequences = holder.sequences;
        for (int i = 0, n = sequences.size(); i < n; i++) {
            l = Math.min(l, sequences.getQuick(i).availableIndex(lo));
        }
        return l;
    }

    @Override
    public WaitStrategy getWaitStrategy() {
        return holder.waitStrategy;
    }

    public void followedBy(Barrier barrier) {
        ObjList<Sequence> sequences = holder.sequences;
        for (int i = 0, n = sequences.size(); i < n; i++) {
            sequences.getQuick(i).followedBy(barrier);
        }
    }

    public void remove(Sequence sequence) {
        Holder _new;
        do {
            Holder h = this.holder;
            // read barrier to make sure "holder" read doesn't fall below this
            boolean b = barrier;

            if (h.sequences.indexOf(sequence) == -1) {
                return;
            }
            _new = new Holder();
            for (int i = 0, n = h.sequences.size(); i < n; i++) {
                Sequence sq = h.sequences.getQuick(i);
                if (sq != sequence) {
                    _new.sequences.add(sq);
                }
            }

            if (sequence.hasWaitStrategy()) {
                WaitStrategy that = sequence.getWaitStrategy();
                for (int i = 0, n = h.waitStrategies.size(); i < n; i++) {
                    WaitStrategy ws = h.waitStrategies.getQuick(i);
                    if (ws != that) {
                        _new.waitStrategies.add(ws);
                    }
                }
            }
            _new.setupWaitStrategy();

        } while (!Unsafe.getUnsafe().compareAndSwapObject(this, HOLDER, holder, _new));
    }

    private static class Holder {

        private ObjList<Sequence> sequences = new ObjList<>();
        private ObjList<WaitStrategy> waitStrategies = new ObjList<>();
        private WaitStrategy waitStrategy;
        private FanOutWaitStrategy fanOutWaitStrategy = new FanOutWaitStrategy();

        private void setupWaitStrategy() {
            if (waitStrategies.size() > 0) {
                waitStrategy = fanOutWaitStrategy;
            } else {
                waitStrategy = NullWaitStrategy.INSTANCE;
            }
        }

        private class FanOutWaitStrategy implements WaitStrategy {
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


    static {
        try {
            HOLDER = Unsafe.getUnsafe().objectFieldOffset(FanOut.class.getDeclaredField("holder"));
        } catch (NoSuchFieldException e) {
            throw new FatalError("Internal error", e);
        }
    }
}
