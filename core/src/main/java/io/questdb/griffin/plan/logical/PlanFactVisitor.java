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
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

/**
 * Receives the facts of a plan object, see {@link PlanFactSource#visitFacts}, each under the name of the attribute that
 * holds it. A column id is one the object defines or requests, not one it reads; a reference names another plan object
 * of the same plan.
 */
public interface PlanFactVisitor {

    void columnId(String fact, int columnId);

    void columnIds(String fact, IntList columnIds);

    void fact(String fact, boolean value);

    void fact(String fact, long value);

    /**
     * A value compared by identity, or by content when it is text.
     */
    void fact(String fact, Object value);

    void facts(String fact, IntList values);

    void facts(String fact, LongList values);

    /**
     * Values compared as {@link #fact(String, Object)} compares one.
     */
    void facts(String fact, ObjList<?> values);

    void output(String fact, OutputSchema output);

    void reference(String fact, Object planObject);
}
