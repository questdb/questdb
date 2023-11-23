package io.questdb.cairo.pool;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.*;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Rnd;

public final class SqlCompilerPool extends AbstractMultiTenantPool<SqlCompilerPool.C> {
    // todo: are you sure there are no side effects here? for example the dirName? at very least it's being logged
    //  by the pool and that's confusing
    //  note: we should not use too many colours otherwise the pool might create more compiler instances
    //  then with no pooling at all. This is because we have to have a separate compiler for each colour.
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
    protected C newTenant(TableToken tableName, Entry<C> entry, int index) {
        return new C(
                engine.getSqlCompilerFactory().getInstance(engine),
                this,
                tableName,
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
