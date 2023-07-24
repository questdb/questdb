package io.questdb.cairo.pool;

import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.*;
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
    private static final TableToken TOKEN_BLUE = new TableToken("blue", "/compilers/blue/", 0, false);
    private static final TableToken TOKEN_GREEN = new TableToken("green", "/compilers/green/", 0, false);
    private static final TableToken TOKEN_RED = new TableToken("red", "/compilers/red/", 0, false);
    private static final TableToken[] TOKENS = {TOKEN_RED, TOKEN_BLUE, TOKEN_GREEN};
    private final CairoEngine engine;
    private final FactoryProvider factoryProvider;
    private final Rnd rnd = new Rnd();
    private final DatabaseSnapshotAgent snapshotAgent;

    public SqlCompilerPool(CairoEngine engine, FactoryProvider factoryProvider, DatabaseSnapshotAgent snapshotAgent) {
        super(engine.getConfiguration(), MAX_POOL_SEGMENTS, INACTIVE_COMPILER_TTL_MILLIS);
        this.engine = engine;
        this.factoryProvider = factoryProvider;
        this.snapshotAgent = snapshotAgent;
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
        SqlCompilerImpl delegate = factoryProvider.getSqlCompilerFactory().getInstance(engine, engine.getFunctionFactoryCache(), snapshotAgent);
        return new C(delegate, this, tableName, entry, index);
    }

    public static class C implements PoolTenant, SqlCompiler {

        private final SqlCompiler delegate;
        private final int index;
        private Entry<C> entry;
        private AbstractMultiTenantPool<C> pool;
        private TableToken tableToken;

        public C(SqlCompiler delegate,
                 AbstractMultiTenantPool<C> pool,
                 TableToken tableToken,
                 Entry<C> entry,
                 int index) {
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
        public void updateTableToken(TableToken tableToken) {
            this.tableToken = tableToken;
        }
    }
}
