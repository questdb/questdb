/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \_   _  ___  ___| |_|  _ \| __ )
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

package io.questdb.griffin.codegen;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.join.MaterializedRecordCursorFactory;
import io.questdb.griffin.plan.logical.RowCopy;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;

/**
 * Builds the copy of the rows of a factory that the generator puts between the factory and a consumer that reads its
 * rows again or by row id, see {@link RowCopy} and {@link MaterializedRecordCursorFactory}.
 */
final class RowCopies {

    private RowCopies() {
    }

    /**
     * Copies every column of the rows of {@code base} as {@code copy} does. Consumes the base on entry, including on
     * failure.
     */
    static RecordCursorFactory copy(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            EntityColumnFilter columnFilter,
            RecordCursorFactory base,
            RowCopy copy
    ) {
        final RecordSink sink;
        try {
            columnFilter.of(base.getMetadata().getColumnCount());
            sink = RecordSinkFactory.getInstance(configuration, asm, base.getMetadata(), columnFilter);
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return new MaterializedRecordCursorFactory(configuration, base, sink, copy.isStrict(), copy.isJoinBudgeted(),
                copy.isCopiedInBatches());
    }
}
