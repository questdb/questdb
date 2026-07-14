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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.async.PageFrameReducer;

/**
 * A {@link PageFrameReducer} paired with the name of the reduce phase it implements.
 * <p>
 * A window join picks its reducer from a chain of compile-time flags the query's output never
 * reveals. Each reducer owns its own slot acquire/release pair, so a slot-leak test has to pin which
 * one a query routed to: a test that pinned only the factory class would follow the optimizer
 * silently onto a neighbouring reducer, still breach, still release, and cover the wrong method.
 * <p>
 * The name lives next to the method reference rather than in a lookup the routing chain would have
 * to be kept in step with, so the two cannot drift, and the reducer itself stays off the factory's
 * public surface - a test asks for the name, not the function.
 */
final class NamedPageFrameReducer {
    private final String name;
    private final PageFrameReducer reducer;

    NamedPageFrameReducer(String name, PageFrameReducer reducer) {
        this.name = name;
        this.reducer = reducer;
    }

    String getName() {
        return name;
    }

    PageFrameReducer getReducer() {
        return reducer;
    }
}
