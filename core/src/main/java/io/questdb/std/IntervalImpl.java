/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class IntervalImpl implements Interval, Sinkable {
    public static IntervalImpl EMPTY = new IntervalImpl();

    CharSequence interval = null;
    CharSequence modifier = null;
    long repetition = -1;
    CharSequence timestamp = null;

    public IntervalImpl() {
    }

    public void clear() {
        interval = null;
        modifier = null;
        repetition = -1;
        timestamp = null;
    }

    @Override
    public CharSequence getInterval() {
        return interval;
    }

    @Override
    public CharSequence getModifier() {
        return modifier;
    }

    @Override
    public long getRepetition() {
        return repetition;
    }

    @Override
    public CharSequence getTimestamp() {
        return timestamp;
    }

    public void of(CharSequence interval, CharSequence modifier, long repetition, CharSequence timestamp) {
        this.interval = interval;
        this.modifier = modifier;
        this.repetition = repetition;
        this.timestamp = timestamp;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.put(timestamp);
        if (modifier != null) {
            sink.put(';');
            sink.put(modifier);
        }
        if (interval != null) {
            sink.put(';');
            sink.put(interval);
        }
        if (repetition != -1) {
            sink.put(';');
            sink.put(repetition);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp);
        if (modifier != null) {
            sb.append(';');
            sb.append(modifier);
        }
        if (interval != null) {
            sb.append(';');
            sb.append(interval);
        }
        if (repetition != -1) {
            sb.append(';');
            sb.append(repetition);
        }
        return sb.toString();
    }
}
