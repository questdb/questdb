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

package io.questdb.test.griffin.fuzz;

/**
 * The query shape {@link QueryGenerator} picked, carried on the {@link GeneratedQuery} so the
 * driver can report and assert per-shape statistics.
 * <p>
 * A generator whose SQL drifts out of step with the engine's rules (a LATEST ON PARTITION BY key
 * type the code generator rejects, say) still leaves the run green: every query it emits raises an
 * expected error and is counted as skipped. QueryFuzzTest holds each shape to a minimum accepted
 * rate, which needs to know which generator produced the query.
 */
public enum QueryShape {
    GROUP_BY,
    HORIZON_JOIN,
    LATEST_ON,
    POSTING,
    SAMPLE_BY,
    SIMPLE,
    TEMPORAL_JOIN,
    WINDOW,
    WINDOW_JOIN
}
