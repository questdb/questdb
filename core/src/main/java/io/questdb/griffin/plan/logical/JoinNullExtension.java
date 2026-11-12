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

/**
 * How an outer step decides which of the rows it preserves it NULL-extends.
 */
public enum JoinNullExtension {
    /**
     * A preserved row whose key partners all fail the ON residual: the keys state the ON clause's equalities.
     */
    BY_KEYS,
    /**
     * A preserved row whose keys all hold the values the NULL record of the other input reads for them, NULL, or
     * false or 0 for a type with no NULL sentinel, and whom every row of the other input fails the ON residual; the
     * step drops every other preserved row without a key partner that passes the ON residual. The keys are
     * equalities a later INNER join implies, which hold for every row that join keeps, and the ON residual holds
     * the whole ON clause: the later join drops every other row the ON clause pairs or NULL-extends, so every row
     * the step emits holds its keys.
     */
    BY_ON_RESIDUAL
}
