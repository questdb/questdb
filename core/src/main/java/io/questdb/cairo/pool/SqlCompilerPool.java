package io.questdb.cairo.pool;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.*;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.std.Rnd;

public final class SqlCompilerPool extends AbstractMultiTenantPool<SqlCompilerPool.C> {
    private static final long INACTIVE_COMPILER_TTL_MILLIS = 5 * 60 * 1000L; // 5 minutes
    // todo: consider making the constants configurable
    private static final int MAX_POOL_SEGMENTS = 64;
    // todo: are you sure there are no side effects here? for example the dirName? at very least it's being logged
    // by the pool and that's confusing
    // note: we should not use too many colours otherwise the pool might create more compiler instances
    // then with no pooling at all. This is because we have to have a separate compiler for each colour.
    private static final TableToken[] TOKENS = {
            new TableToken("blue", "/compilers/blue/", 0, false),
            new TableToken("red", "/compilers/red/", 0, false),
            new TableToken("green", "/compilers/green/", 0, false)
    };
    private final CairoEngine engine;
    private final Rnd rnd = new Rnd();

    public SqlCompilerPool(CairoEngine engine) {
        super(engine.getConfiguration(), MAX_POOL_SEGMENTS, INACTIVE_COMPILER_TTL_MILLIS);
        this.engine = engine;
    }

    public C get() {
        return super.get(getRandToken());
    }

    private TableToken getRandToken() {
        return TOKENS[rnd.nextPositiveInt() % TOKENS.length];
    }

    @Override
    protected byte getListenerSrc() {
        // todo: what is this?
        return 0;
    }

    @Override
    protected C newTenant(TableToken tableName, Entry<C> entry, int index) {
        // todo: fix this method-train.
        // why it looks like this: there is a life-cycle issue: when the SqlCompilerPool is constructed
        // the configuration is not yet initialized thus getFactoryProvider returns null
        // see PropServerConfiguration#init(CairoEngine engine, FunctionFactoryCache functionFactoryCache, FreeOnExit freeOnExit)
        SqlCompilerImpl delegate = engine.getSqlCompilerFactory().getInstance(engine);
        return new C(delegate, this, tableName, entry, index);
    }

    static class C implements PoolTenant, SqlCompiler {
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
        public void compileBatch(CharSequence queryText, SqlExecutionContext sqlExecutionContext, BatchCallback batchCallback) throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException, SqlException {
            delegate.compileBatch(queryText, sqlExecutionContext, batchCallback);
        }

        @SuppressWarnings("unchecked")
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
            // todo: is this correct? should we be doing this?
            // and if so then perhaps we should do this while returning the compiler to the pool?
            this.clear();
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
        public void updateTableToken(TableToken tableToken) {
            this.tableToken = tableToken;
        }
    }
}
