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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.RecordToRowCopierUtils;
import io.questdb.std.BytecodeAssembler;

import java.lang.reflect.Method;

/**
 * Helpers that tell which record-to-row copier implementation the compiler builds.
 */
public final class RecordToRowCopierTestUtils {

    private RecordToRowCopierTestUtils() {
    }

    /**
     * Counts the chunk sub-methods of a copier. The single-method and the chunked copiers are generated under
     * the same class name, but only the chunked one splits the column copies into {@code copy0..copyN}, so the
     * count is 0 for the single-method copier and for {@code LoopingRecordToRowCopier}.
     */
    public static int countChunkMethods(RecordToRowCopier copier) {
        int count = 0;
        for (Method method : copier.getClass().getDeclaredMethods()) {
            if (method.getName().matches("copy\\d+")) {
                count++;
            }
        }
        return count;
    }

    /**
     * Generates the copier that {@code INSERT INTO dst SELECT * FROM src} compiles to: the same metadata,
     * column filter and configuration.
     */
    public static RecordToRowCopier generateInsertSelectCopier(TableReader src, TableReader dst, CairoConfiguration configuration) {
        final EntityColumnFilter columnFilter = new EntityColumnFilter();
        columnFilter.of(dst.getMetadata().getColumnCount());
        return RecordToRowCopierUtils.generateCopier(
                new BytecodeAssembler(),
                src.getMetadata(),
                dst.getMetadata(),
                columnFilter,
                configuration
        );
    }
}
