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
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.legacy.cairo.sql.LegacyRecordCursorFactory;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * The projection the legacy planner generates: the shared {@link VirtualRecordCursorFactory}, which code outside the
 * planner recognises, with the contract of {@link LegacyRecordCursorFactory} that the legacy planner reads.
 */
public class LegacyVirtualRecordCursorFactory extends VirtualRecordCursorFactory implements LegacyRecordCursorFactory {

    public LegacyVirtualRecordCursorFactory(
            @NotNull RecordMetadata virtualMetadata,
            @NotNull PriorityMetadata priorityMetadata,
            @NotNull ObjList<Function> functions,
            @NotNull RecordCursorFactory base,
            int virtualColumnReservedSlots
    ) {
        super(virtualMetadata, priorityMetadata, functions, base, virtualColumnReservedSlots);
    }

    @Override
    public boolean canPeelForTopK() {
        return true;
    }

    @Override
    public boolean isStableWithinExecution() {
        final ObjList<Function> functions = getFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).isStableWithinExecution()) {
                return false;
            }
        }
        return LegacyRecordCursorFactory.isStableWithinExecution(getBaseFactory());
    }

    @Override
    public RecordCursorFactory rewrapOverTopK(RecordCursorFactory topK, RecordMetadata orderedMetadata) {
        final RecordCursorFactory base = getBaseFactory();
        final PriorityMetadata priorityMetadata = getPriorityMetadata();
        // Shares functions and priorityMetadata with the orphaned wrapper. Per the
        // RecordCursorFactory.rewrapOverTopK contract, the caller must not close the orphan;
        // its state has transferred here. Same precedent as the AsOf and LatestBy peels.
        return new LegacyVirtualRecordCursorFactory(
                orderedMetadata,
                priorityMetadata,
                getFunctions(),
                LegacyRecordCursorFactory.rewrapOverTopK(base, topK, base.getMetadata()),
                priorityMetadata.getVirtualColumnReservedSlots()
        );
    }

    @Override
    public int translateOrderByColumnToBase(int projectedIndex) {
        final ObjList<Function> functions = getFunctions();
        if (projectedIndex < 0 || projectedIndex >= functions.size()) {
            return -1;
        }
        ColumnFunction columnFn = ColumnFunction.unwrap(functions.getQuick(projectedIndex));
        if (columnFn == null) {
            return -1;
        }
        int baseIdx = getPriorityMetadata().getBaseColumnIndex(columnFn.getColumnIndex());
        if (baseIdx < 0) {
            return -1;
        }
        return LegacyRecordCursorFactory.translateOrderByColumnToBase(getBaseFactory(), baseIdx);
    }
}
