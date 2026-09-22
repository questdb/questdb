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

package io.questdb.test.griffin.fuzz;

/**
 * A kind of fault the query fuzzer can inject into a single query run. Each is
 * armed at a random trigger point, the query is executed once, and the runner
 * asserts the factory frees its resources on the error path and that the query
 * recovers once the fault is removed.
 * <ul>
 *   <li>{@link #FILE} &mdash; one filesystem operation fails once, via
 *       {@code FailureFileFacade}.</li>
 *   <li>{@link #FUNCTION} &mdash; a SQL function emitted into the query throws
 *       once mid-evaluation.</li>
 *   <li>{@link #MALLOC} &mdash; a native allocation fails once, via the RSS
 *       memory limit (the same out-of-memory path production hits).</li>
 * </ul>
 */
public enum FaultType {
    FILE, FUNCTION, MALLOC
}
