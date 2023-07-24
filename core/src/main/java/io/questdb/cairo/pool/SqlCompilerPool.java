package io.questdb.cairo.pool;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlCompiler;

public final class SqlCompilerPool extends AbstractMultiTenantPool<SqlCompilerPool.C> {
    private static final long INACTIVE_COMPILER_TTL_MILLIS = 5 * 60 * 1000L; // 5 minutes
    // todo: consider making the constants configurable
    private static final int MAX_POOL_SEGMENTS = 64;

    // todo: are you sure there are no side effects here? for example the dirName? at very least it's being logged
    // by the pool and that's confusing
    private static final TableToken RED_TOKEN = new TableToken("red", "/compilers/red/", 0, false);
    private final CairoEngine engine;
    private final DatabaseSnapshotAgent snapshotAgent;

    public SqlCompilerPool(CairoEngine engine, DatabaseSnapshotAgent snapshotAgent) {
        super(engine.getConfiguration(), MAX_POOL_SEGMENTS, INACTIVE_COMPILER_TTL_MILLIS);
        this.engine = engine;
        this.snapshotAgent = snapshotAgent;
    }

    public C get() {
        return super.get(getRandToken());
    }

    private static TableToken getRandToken() {
        // todo: implement actual token selection
        // maybe random, maybe round robin, maybe something else
        return RED_TOKEN;
    }

    @Override
    protected byte getListenerSrc() {
        return 0;
    }

    @Override
    protected C newTenant(TableToken tableName, Entry<C> entry, int index) {
        return new C(engine, engine.getFunctionFactoryCache(), snapshotAgent, this, tableName, entry, index);
    }

    public static class C extends SqlCompiler implements PoolTenant {

        private final int index;
        private Entry<C> entry;
        private AbstractMultiTenantPool<C> pool;
        private TableToken tableToken;

        public C(CairoEngine engine,
                 FunctionFactoryCache functionFactoryCache,
                 DatabaseSnapshotAgent snapshotAgent,
                 AbstractMultiTenantPool<C> pool,
                 TableToken tableToken,
                 Entry<C> entry,
                 int index) {
            super(engine, functionFactoryCache, snapshotAgent);
            this.pool = pool;
            this.tableToken = tableToken;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            final AbstractMultiTenantPool<C> pool = this.pool;
            if (pool != null && entry != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            super.close();
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
