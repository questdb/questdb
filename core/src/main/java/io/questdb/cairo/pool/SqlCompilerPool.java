/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.pool;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.*;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Rnd;

public final class SqlCompilerPool extends AbstractMultiTenantPool<SqlCompilerPool.C> {
    // The table tokens below are fake, only needed to satisfy the contract of the base class.
    // These tokens do not represent real tables, so they should never be used in that context.
    // Should be used only as a key for pool entries, and should stay internal to the pool.
    // It also should be kept in mind that some details of these fake tokens can make it into
    // logs, such as the directory names, and they do not (and really should not) exist on disk.
    private static final TableToken[] TOKENS = {
            new TableToken("blue", "/compilers/blue/", 0, false, false, false),
            new TableToken("red", "/compilers/red/", 0, false, false, false),
            new TableToken("green", "/compilers/green/", 0, false, false, false)
    };
    private final CairoEngine engine;
    private final Rnd rnd = new Rnd();

    public SqlCompilerPool(CairoEngine engine) {
        // Passing zero as TTL, because SqlCompiler instances are expected to be returned to the pool immediately
        // after usage. The `releaseInactive()` method is also overridden to return with hardcoded 'false' for the
        // same reason. It is not meant to be called.
        super(engine.getConfiguration(), (engine.getConfiguration().getSqlCompilerPoolCapacity() / ENTRY_SIZE) + 1, 0L);
        this.engine = engine;
    }

    public C get() {
        return super.get(getRandToken());
    }

    @Override
    public boolean releaseInactive() {
        // noop, should not be used
        return false;
    }

    private TableToken getRandToken() {
        return TOKENS[rnd.nextPositiveInt() % TOKENS.length];
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_SQL_COMPILER;
    }

    @Override
    protected C newTenant(TableToken tableToken, Entry<C> entry, int index) {
        return new C(
                engine.getSqlCompilerFactory().getInstance(engine),
                this,
                tableToken,
                entry,
                index
        );
    }

    public static class C implements SqlCompiler, PoolTenant<C> {
        private final SqlCompiler delegate;
        private final int index;
        private Entry<C> entry;
        private AbstractMultiTenantPool<C> pool;
        private TableToken tableToken;

        public C(
                SqlCompiler delegate,
                AbstractMultiTenantPool<C> pool,
                TableToken tableToken,
                Entry<C> entry,
                int index
        ) {
            this.delegate = delegate;
            this.pool = pool;
            this.tableToken = tableToken;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public void close() {
            // revert any debug flags
            setFullFatJoins(false);
            final AbstractMultiTenantPool<C> pool = this.pool;
            if (pool != null && entry != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            delegate.close();
        }

        @Override
        public CompiledQuery compile(CharSequence s, SqlExecutionContext ctx) throws SqlException {
            return delegate.compile(s, ctx);
        }

        @Override
        public void compileBatch(CharSequence queryText, SqlExecutionContext sqlExecutionContext, BatchCallback batchCallback) throws Exception {
            delegate.compileBatch(queryText, sqlExecutionContext, batchCallback);
        }

        @Override
        public Entry<C> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public TableToken getTableToken() {
            return tableToken;
        }

        @Override
        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public QueryBuilder query() {
            return delegate.query();
        }

        @Override
        public void refresh() {
            clear();
        }

        @Override
        public void setEnableJitNullChecks(boolean value) {
            delegate.setEnableJitNullChecks(value);
        }

        @Override
        public void setFullFatJoins(boolean fullFatJoins) {
            delegate.setFullFatJoins(fullFatJoins);
        }

        @Override
        public ExecutionModel testCompileModel(CharSequence query, SqlExecutionContext executionContext) throws SqlException {
            return delegate.testCompileModel(query, executionContext);
        }

        @Override
        public ExpressionNode testParseExpression(CharSequence expression, QueryModel model) throws SqlException {
            return delegate.testParseExpression(expression, model);
        }

        @Override
        public void testParseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException {
            delegate.testParseExpression(expression, listener);
        }

        @Override
        public void updateTableToken(TableToken tableToken) {
            this.tableToken = tableToken;
        }
    }
}
