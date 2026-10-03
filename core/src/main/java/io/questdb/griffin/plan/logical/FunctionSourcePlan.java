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
import io.questdb.std.ObjectFactory;

/**
 * A prepared table-function relation; executable ownership belongs to the compiler.
 */
public final class FunctionSourcePlan extends LogicalPlan {
    public static final ObjectFactory<FunctionSourcePlan> FACTORY = FunctionSourcePlan::new;
    private final OutputSchema recordSchema = new OutputSchema();
    private final IntList sourceColumnIndexes = new IntList();
    private boolean isProjectable;
    private boolean isSequenceStable;
    private CharSequence recordName;

    @Override
    public void clear() {
        super.clear();
        recordSchema.clear();
        sourceColumnIndexes.clear();
        isProjectable = false;
        isSequenceStable = false;
        recordName = null;
    }

    /**
     * The name of the single RECORD column that carries each source row, or null for a plain source.
     */
    public CharSequence getRecordName() {
        return recordName;
    }

    public OutputSchema getRecordSchema() {
        return recordSchema;
    }

    public IntList getSourceColumnIndexes() {
        return sourceColumnIndexes;
    }

    @Override
    public LogicalPlan inputAt(int index) {
        throw new IndexOutOfBoundsException("function source has no input: " + index);
    }

    @Override
    public int inputCount() {
        return 0;
    }

    /**
     * True when the source reads only the columns its output keeps, so pruning saves work.
     */
    public boolean isProjectable() {
        return isProjectable;
    }

    /**
     * True when every evaluation within one execution yields the same rows in the same order, as the
     * table function declares.
     */
    public boolean isSequenceStable() {
        return isSequenceStable;
    }

    public FunctionSourcePlan of(int position) {
        setPosition(position);
        return this;
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        throw new IndexOutOfBoundsException("function source has no input: " + index);
    }

    public void setProjectable(boolean isProjectable) {
        this.isProjectable = isProjectable;
    }

    public void setSequenceStable(boolean isSequenceStable) {
        this.isSequenceStable = isSequenceStable;
    }

    public void setRecordName(CharSequence recordName) {
        this.recordName = recordName;
    }
}
