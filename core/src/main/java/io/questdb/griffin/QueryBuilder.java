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

package io.questdb.griffin;

import io.questdb.cairo.TableToken;
import io.questdb.griffin.engine.ops.CreateTableOperationFuture;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;

public final class QueryBuilder implements Mutable {
    private final SqlCompiler compiler;
    private final StringSink sink = new StringSink();

    QueryBuilder(SqlCompiler compiler) {
        this.compiler = compiler;
    }

    public QueryBuilder $(CharSequence value) {
        sink.put(value);
        return this;
    }

    public QueryBuilder $(char value) {
        sink.put(value);
        return this;
    }

    public QueryBuilder $(int value) {
        sink.put(value);
        return this;
    }

    @Override
    public void clear() {
        sink.clear();
    }

    public CompiledQuery compile(SqlExecutionContext executionContext) throws SqlException {
        return compiler.compile(sink, executionContext);
    }

    public TableToken createTable(SqlExecutionContext executionContext) throws SqlException {
        try (
                final Operation op = compiler.compile(sink, executionContext).getOperation();
                final CreateTableOperationFuture fut = (CreateTableOperationFuture) op.execute(executionContext, null)
        ) {
            fut.await();
            return fut.getTableToken();
        }
    }

    @Override
    public String toString() {
        return sink.toString();
    }
}
