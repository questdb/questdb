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

package io.questdb.griffin.plan.logical;

import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

/**
 * An equality between columns of two join inputs, or of one input, that a join key or a filter implements: each side's
 * input, column id, name and SQL position, and the inputs whose ON clause states it, -1 for WHERE.
 */
public final class JoinEquality implements Mutable {
    public static final ObjectFactory<JoinEquality> FACTORY = JoinEquality::new;
    private final IntList owners = new IntList();
    private int leftColumnId;
    private CharSequence leftName;
    private int leftPosition;
    private int leftSource;
    private int rightColumnId;
    private CharSequence rightName;
    private int rightPosition;
    private int rightSource;

    public void addOwners(IntList owners) {
        for (int i = 0, n = owners.size(); i < n; i++) {
            final int owner = owners.getQuick(i);
            if (this.owners.indexOf(owner, 0, this.owners.size()) < 0) {
                this.owners.add(owner);
            }
        }
    }

    @Override
    public void clear() {
        owners.clear();
        leftColumnId = rightColumnId = -1;
        leftName = rightName = null;
        leftPosition = rightPosition = -1;
        leftSource = rightSource = -1;
    }

    public int getLeftColumnId() {
        return leftColumnId;
    }

    public CharSequence getLeftName() {
        return leftName;
    }

    public int getLeftPosition() {
        return leftPosition;
    }

    public int getLeftSource() {
        return leftSource;
    }

    /**
     * The input on the other side of the equality from {@code source}, one of its two inputs.
     */
    public int getOtherSource(int source) {
        return leftSource == source ? rightSource : leftSource;
    }

    public IntList getOwners() {
        return owners;
    }

    public int getRightColumnId() {
        return rightColumnId;
    }

    public CharSequence getRightName() {
        return rightName;
    }

    public int getRightPosition() {
        return rightPosition;
    }

    public int getRightSource() {
        return rightSource;
    }

    public JoinEquality of(int leftSource, int leftColumnId, CharSequence leftName, int leftPosition,
                           int rightSource, int rightColumnId, CharSequence rightName, int rightPosition) {
        this.leftSource = leftSource;
        this.leftColumnId = leftColumnId;
        this.leftName = leftName;
        this.leftPosition = leftPosition;
        this.rightSource = rightSource;
        this.rightColumnId = rightColumnId;
        this.rightName = rightName;
        this.rightPosition = rightPosition;
        return this;
    }

    public void reverse() {
        final int source = leftSource;
        final int id = leftColumnId;
        final CharSequence name = leftName;
        final int position = leftPosition;
        leftSource = rightSource;
        leftColumnId = rightColumnId;
        leftName = rightName;
        leftPosition = rightPosition;
        rightSource = source;
        rightColumnId = id;
        rightName = name;
        rightPosition = position;
    }
}
