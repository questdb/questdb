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

package io.questdb.metrics;

import io.questdb.mp.ValueHolder;
import io.questdb.std.ObjectFactory;

public class QueryTrace implements ValueHolder<QueryTrace> {
    public static final ObjectFactory<QueryTrace> ITEM_FACTORY = QueryTrace::new;

    public long executionNanos;
    // Elapsed nanos from cursor open to the first row; -1 when the query
    // produced no rows. -1 (not 0) is the sentinel because 0 is a legitimate
    // measurement under a coarse test clock.
    public long firstRowNanos = -1;
    public boolean isJit;
    public String principal;
    public String queryText;
    public long timestamp;
    public long waitNanos;

    @Override
    public void clear() {
        executionNanos = 0;
        firstRowNanos = -1;
        isJit = false;
        principal = null;
        queryText = null;
        timestamp = 0;
        waitNanos = 0;
    }

    @Override
    public void copyTo(QueryTrace dest) {
        dest.executionNanos = executionNanos;
        dest.firstRowNanos = firstRowNanos;
        dest.isJit = isJit;
        dest.principal = principal;
        dest.queryText = queryText;
        dest.timestamp = timestamp;
        dest.waitNanos = waitNanos;
    }
}
