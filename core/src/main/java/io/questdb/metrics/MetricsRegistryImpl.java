/*+*****************************************************************************
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

package io.questdb.metrics;

import io.questdb.std.str.BorrowableUtf8Sink;
import org.jetbrains.annotations.NotNull;

/**
 * Holds the set of scrape {@link Target}s and renders them into Prometheus format.
 * <p>
 * The target set is mutated by the lifecycle/role-switch thread (addTarget / removeTarget /
 * the newXxx factory methods) while the /metrics HTTP thread iterates it via
 * scrapeIntoPrometheus. These run concurrently, so the registry keeps the targets in a
 * copy-on-write snapshot: every mutation builds a brand new array under {@link #lock} and
 * publishes it through the volatile {@link #targets} reference; the scrape reads that
 * reference once and iterates the captured snapshot without holding any registry lock.
 * <p>
 * Because the scrape never holds the registry lock while calling into a target, it can hold
 * at most one lock at a time on that path (a target's own close-vs-scrape monitor, if any).
 * No thread acquires the registry lock and a target lock together, so the two cannot deadlock.
 */
public class MetricsRegistryImpl implements MetricsRegistry {
    private static final Target[] EMPTY = new Target[0];
    private final Object lock = new Object();
    private volatile Target[] targets = EMPTY;

    @Override
    public void addTarget(Target target) {
        synchronized (lock) {
            final Target[] current = targets;
            final Target[] next = new Target[current.length + 1];
            System.arraycopy(current, 0, next, 0, current.length);
            next[current.length] = target;
            targets = next;
        }
    }

    @Override
    public AtomicLongGauge newAtomicLongGauge(CharSequence name) {
        AtomicLongGauge gauge = new AtomicLongGaugeImpl(name);
        addTarget(gauge);
        return gauge;
    }

    @Override
    public Counter newCounter(CharSequence name) {
        Counter counter = new CounterImpl(name);
        addTarget(counter);
        return counter;
    }

    @Override
    public CounterWithOneLabel newCounter(CharSequence name, CharSequence labelName0, CharSequence[] labelValues0) {
        CounterWithOneLabel counter = new CounterWithOneLabelImpl(name, labelName0, labelValues0);
        addTarget(counter);
        return counter;
    }

    @Override
    public CounterWithTwoLabels newCounter(
            CharSequence name,
            CharSequence labelName0, CharSequence[] labelValues0,
            CharSequence labelName1, CharSequence[] labelValues1
    ) {
        CounterWithTwoLabels counter = new CounterWithTwoLabelsImpl(name, labelName0, labelValues0, labelName1, labelValues1);
        addTarget(counter);
        return counter;
    }

    @Override
    public DoubleGauge newDoubleGauge(CharSequence name) {
        DoubleGaugeImpl gauge = new DoubleGaugeImpl(name);
        addTarget(gauge);
        return gauge;
    }

    @Override
    public LongGauge newLongGauge(CharSequence name) {
        LongGauge gauge = new LongGaugeImpl(name);
        addTarget(gauge);
        return gauge;
    }

    @Override
    public LongGauge newLongGauge(int memoryTag) {
        LongGauge gauge = new MemoryTagLongGauge(memoryTag);
        addTarget(gauge);
        return gauge;
    }

    @Override
    public LongGauge newVirtualGauge(CharSequence _name, VirtualLongGauge.StatProvider provider) {
        VirtualLongGauge gauge = new VirtualLongGauge(_name, provider);
        addTarget(gauge);
        return gauge;
    }

    @Override
    public void removeTarget(Target target) {
        synchronized (lock) {
            final Target[] current = targets;
            int index = -1;
            for (int i = 0, n = current.length; i < n; i++) {
                if (current[i] == target) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return;
            }
            if (current.length == 1) {
                targets = EMPTY;
                return;
            }
            final Target[] next = new Target[current.length - 1];
            System.arraycopy(current, 0, next, 0, index);
            System.arraycopy(current, index + 1, next, index, current.length - index - 1);
            targets = next;
        }
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        // Read the snapshot once, then iterate it without holding any registry lock. The
        // array referenced here is never mutated in place: mutations publish a fresh array.
        final Target[] snapshot = targets;
        for (int i = 0, n = snapshot.length; i < n; i++) {
            snapshot[i].scrapeIntoPrometheus(sink);
        }
    }
}
