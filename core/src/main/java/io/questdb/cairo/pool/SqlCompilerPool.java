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

package io.questdb.cairo.pool;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.BatchCallback;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.ExpressionParserListener;
import io.questdb.griffin.QueryBuilder;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.InsertModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Rnd;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class SqlCompilerPool extends AbstractMultiTenantPool<SqlCompilerPool.C> {
    // The table tokens below are fake, only needed to satisfy the contract of the base class.
    // These tokens do not represent real tables, so they should never be used in that context.
    // Should be used only as a key for pool entries, and should stay internal to the pool.
    // It also should be kept in mind that some details of these fake tokens can make it into
    // logs, such as the directory names, and they do not (and really should not) exist on disk.
    private static final TableToken[] TOKENS = {
            new TableToken("blue", "/compilers/blue/", null, 0, false, false, false),
            new TableToken("red", "/compilers/red/", null, 0, false, false, false),
            new TableToken("green", "/compilers/green/", null, 0, false, false, false)
    };
    private final CairoEngine engine;
    private final Rnd rnd = new Rnd();

    public SqlCompilerPool(CairoEngine engine) {
        // Passing zero as TTL, because SqlCompiler instances are expected to be returned to the pool immediately
        // after usage. The `releaseInactive()` method is also overridden to return with hardcoded 'false' for the
        // same reason. It is not meant to be called.
        super(engine.getConfiguration(), (engine.getConfiguration().getSqlCompilerPoolCapacity() / engine.getConfiguration().getPoolSegmentSize()) + 1, 0L);
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
    protected C newTenant(TableToken tableToken, Entry<C> rootEntry, Entry<C> entry, int index, @Nullable ResourcePoolSupervisor<C> supervisor) {
        return new C(
                engine.getSqlCompilerFactory().getInstance(engine),
                this,
                tableToken,
                rootEntry,
                entry,
                index
        );
    }

    public static class C implements SqlCompiler, PoolTenant<C> {
        private final SqlCompiler delegate;
        private final int index;
        private final Entry<C> rootEntry;
        private Entry<C> entry;
        private AbstractMultiTenantPool<C> pool;
        private TableToken tableToken;

        public C(
                SqlCompiler delegate,
                AbstractMultiTenantPool<C> pool,
                TableToken tableToken,
                Entry<C> rootEntry,
                Entry<C> entry,
                int index
        ) {
            this.delegate = delegate;
            this.pool = pool;
            this.tableToken = tableToken;
            this.rootEntry = rootEntry;
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
        public CompiledQuery compile(CharSequence sqlText, SqlExecutionContext ctx) throws SqlException {
            return delegate.compile(sqlText, ctx);
        }

        @Override
        public void compileBatch(CharSequence batchText, SqlExecutionContext sqlExecutionContext, BatchCallback batchCallback) throws Exception {
            delegate.compileBatch(batchText, sqlExecutionContext, batchCallback);
        }

        @Override
        public void execute(Operation op, SqlExecutionContext executionContext) throws SqlException {
            delegate.execute(op, executionContext);
        }

        @Override
        public ExecutionModel generateExecutionModel(CharSequence sqlText, SqlExecutionContext executionContext) throws SqlException {
            return delegate.generateExecutionModel(sqlText, executionContext);
        }

        @Override
        public RecordCursorFactory generateSelectWithRetries(
                QueryModel queryModel,
                @Nullable InsertModel insertModel,
                SqlExecutionContext executionContext,
                boolean generateProgressLogger
        ) throws SqlException {
            return delegate.generateSelectWithRetries(queryModel, insertModel, executionContext, generateProgressLogger);
        }

        @Override
        public BytecodeAssembler getAsm() {
            return delegate.getAsm();
        }

        public SqlCompiler getDelegate() {
            return delegate;
        }

        @Override
        public CairoEngine getEngine() {
            return delegate.getEngine();
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
        public Entry<C> getRootEntry() {
            return rootEntry;
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
        public void refresh(ResourcePoolSupervisor<C> supervisor) {
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
        public ExpressionNode testParseExpression(CharSequence expression, QueryModel model) throws SqlException {
            return delegate.testParseExpression(expression, model);
        }

        @Override
        public void testParseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException {
            delegate.testParseExpression(expression, listener);
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sink.put("SqlCompilerPool.C{index=").put(index).put(", tableToken=").put(tableToken).put('}');
        }

        @Override
        public void updateTableToken(TableToken tableToken) {
            this.tableToken = tableToken;
        }
    }
}
