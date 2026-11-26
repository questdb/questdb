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


package io.questdb.griffin;

import io.questdb.std.QuietCloseable;

/**
 * The query planner master shipped before the logical-plan planner, kept as a fallback until the new one has run
 * stably for several releases.
 * {@link SqlCompilerImpl} hands it every statement it compiles while
 * {@link SqlExecutionContext#isLegacyPlannerEnabled()} holds. The engine creates it on first use, through
 * {@link SqlCompilerFactory#newLegacyPlanner(io.questdb.cairo.CairoEngine)}, and closes it.
 */
public interface LegacyPlanner extends QuietCloseable {

    /**
     * Leases a compiler of the legacy planner; closing the compiler returns it.
     */
    LegacySqlCompiler getSqlCompiler();
}
