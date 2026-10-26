/*******************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.RecordSinkTemplate;
import org.jetbrains.annotations.NotNull;

/**
 * The copier classes of the master key check of one HORIZON JOIN slave whose key positions share
 * a slave column, generated once for all workers, see {@link HorizonJoinSharedKeyCheck}. Each
 * {@link HorizonJoinTimeFrameHelper} creates its own check from the template.
 */
public final class HorizonJoinSharedKeyCheckTemplate {
    private final CairoConfiguration configuration;
    private final RecordSinkTemplate earliestSinkTemplate;
    private final RecordSinkTemplate laterSinkTemplate;

    /**
     * @param earliestSinkTemplate copies, for every key position whose slave column occurs at an
     *                             earlier position, the master column of the earliest such position
     * @param laterSinkTemplate    copies, for the same key positions, the master column of the
     *                             position itself
     */
    public HorizonJoinSharedKeyCheckTemplate(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordSinkTemplate earliestSinkTemplate,
            @NotNull RecordSinkTemplate laterSinkTemplate
    ) {
        this.configuration = configuration;
        this.earliestSinkTemplate = earliestSinkTemplate;
        this.laterSinkTemplate = laterSinkTemplate;
    }

    HorizonJoinSharedKeyCheck newInstance() {
        return new HorizonJoinSharedKeyCheck(configuration, earliestSinkTemplate.newInstance(), laterSinkTemplate.newInstance());
    }
}
