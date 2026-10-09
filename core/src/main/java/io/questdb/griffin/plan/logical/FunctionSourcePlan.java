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
    private boolean hasExternalDataSource;
    private boolean isDeterministic;
    private boolean isLongSequence;
    private boolean isPageFrameSupported;
    private boolean isProjectable;
    private boolean isRandomAccess;
    private CharSequence recordName;
    private PhysicalProperties.ScanDirection scanDirection = PhysicalProperties.ScanDirection.UNKNOWN;

    @Override
    public void clear() {
        super.clear();
        recordSchema.clear();
        sourceColumnIndexes.clear();
        hasExternalDataSource = false;
        isDeterministic = false;
        isLongSequence = false;
        isPageFrameSupported = false;
        isProjectable = false;
        isRandomAccess = false;
        recordName = null;
        scanDirection = PhysicalProperties.ScanDirection.UNKNOWN;
    }

    /**
     * Copies the physical properties of the source factory, which the table function builds once it is bound.
     */
    public void copyPhysicalProperties(FunctionSourcePlan source) {
        isLongSequence = source.isLongSequence;
        isPageFrameSupported = source.isPageFrameSupported;
        isRandomAccess = source.isRandomAccess;
        scanDirection = source.scanDirection;
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

    /**
     * The order the source factory emits its rows in.
     */
    public PhysicalProperties.ScanDirection getScanDirection() {
        return scanDirection;
    }

    public IntList getSourceColumnIndexes() {
        return sourceColumnIndexes;
    }

    /**
     * True when the table function reads a data source outside the database.
     */
    public boolean hasExternalDataSource() {
        return hasExternalDataSource;
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
     * True when every cursor the source opens within one execution yields the same rows in the same order.
     */
    public boolean isDeterministic() {
        return isDeterministic;
    }

    /**
     * True when the source factory reads long_sequence().
     */
    public boolean isLongSequence() {
        return isLongSequence;
    }

    /**
     * True when the source factory supports page-frame cursors.
     */
    public boolean isPageFrameSupported() {
        return isPageFrameSupported;
    }

    /**
     * True when the source reads only the columns its output keeps, so pruning saves work.
     */
    public boolean isProjectable() {
        return isProjectable;
    }

    /**
     * True when the cursor of the source factory supports random access.
     */
    public boolean isRandomAccess() {
        return isRandomAccess;
    }

    public FunctionSourcePlan of(int position) {
        setPosition(position);
        return this;
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        throw new IndexOutOfBoundsException("function source has no input: " + index);
    }

    public void setDeterministic(boolean isDeterministic) {
        this.isDeterministic = isDeterministic;
    }

    public void setExternalDataSource(boolean hasExternalDataSource) {
        this.hasExternalDataSource = hasExternalDataSource;
    }

    /**
     * Records the physical properties of the source factory.
     */
    public void setPhysicalProperties(boolean isRandomAccess, boolean isPageFrameSupported, PhysicalProperties.ScanDirection scanDirection,
                                      boolean isLongSequence) {
        this.isRandomAccess = isRandomAccess;
        this.isPageFrameSupported = isPageFrameSupported;
        this.scanDirection = scanDirection;
        this.isLongSequence = isLongSequence;
    }

    public void setProjectable(boolean isProjectable) {
        this.isProjectable = isProjectable;
    }

    public void setRecordName(CharSequence recordName) {
        this.recordName = recordName;
    }
}
