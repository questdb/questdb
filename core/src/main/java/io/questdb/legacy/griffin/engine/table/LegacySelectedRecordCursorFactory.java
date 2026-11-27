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

package io.questdb.legacy.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.jit.CompiledFilter;
import io.questdb.legacy.cairo.sql.LegacyRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * The column selection the legacy planner generates: the shared {@link SelectedRecordCursorFactory}, which code outside
 * the planner recognises, with the contract of {@link LegacyRecordCursorFactory} that the legacy planner reads.
 */
public class LegacySelectedRecordCursorFactory extends SelectedRecordCursorFactory implements LegacyRecordCursorFactory {

    public LegacySelectedRecordCursorFactory(RecordMetadata metadata, IntList columnCrossIndex, RecordCursorFactory base) {
        super(metadata, columnCrossIndex, base);
    }

    @Override
    public boolean canPeelForTopK() {
        return true;
    }

    // to be used in combination with compiled filter
    @Override
    public @Nullable ObjList<Function> getBindVarFunctions() {
        return LegacyRecordCursorFactory.getBindVarFunctions(getBaseFactory());
    }

    // to be used in combination with compiled filter
    @Override
    public @Nullable MemoryCARW getBindVarMemory() {
        return LegacyRecordCursorFactory.getBindVarMemory(getBaseFactory());
    }

    @Override
    public CompiledFilter getCompiledFilter() {
        return LegacyRecordCursorFactory.getCompiledFilter(getBaseFactory());
    }

    @Override
    public void halfClose() {
        LegacyRecordCursorFactory.halfClose(getBaseFactory());
    }

    @Override
    public boolean isLegacyStableWithinExecution() {
        return getBaseFactory().isLegacyStableWithinExecution();
    }

    @Override
    public RecordCursorFactory rewrapOverTopK(RecordCursorFactory topK, RecordMetadata orderedMetadata) {
        final RecordCursorFactory base = getBaseFactory();
        // Shares columnCrossIndex with the orphaned wrapper. Per the
        // RecordCursorFactory.rewrapOverTopK contract, the caller must not close the orphan;
        // its state has transferred here. Same precedent as the AsOf and LatestBy peels.
        return new LegacySelectedRecordCursorFactory(orderedMetadata, getColumnCrossIndex(),
                LegacyRecordCursorFactory.rewrapOverTopK(base, topK, base.getMetadata()));
    }

    @Override
    public int translateOrderByColumnToBase(int projectedIndex) {
        final IntList columnCrossIndex = getColumnCrossIndex();
        if (projectedIndex < 0 || projectedIndex >= columnCrossIndex.size()) {
            return -1;
        }
        return LegacyRecordCursorFactory.translateOrderByColumnToBase(getBaseFactory(), columnCrossIndex.getQuick(projectedIndex));
    }
}
