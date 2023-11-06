package io.questdb.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;

public abstract class AbstractAnonymousMultiTenantPool<O extends QuietCloseable & Mutable> extends AbstractMultiTenantPool<AbstractAnonymousMultiTenantPool.Wrapper<O>> {
    // todo: are you sure there are no side effects here? for example the dirName? at very least it is being
    //  logged by the pool and that is confusing
    //  note: we should not use too many colours otherwise the pool might create more compiler instances
    //  then with no pooling at all. This is because we have to have a separate compiler for each colour.
    private static final TableToken[] TOKENS = {
            new TableToken("blue", "/compilers/blue/", 0, false, false),
            new TableToken("red", "/compilers/red/", 0, false, false),
            new TableToken("green", "/compilers/green/", 0, false, false)
    };

    private final Rnd rnd = new Rnd();

    protected AbstractAnonymousMultiTenantPool(CairoConfiguration configuration, int maxSegments) {
        super(configuration, maxSegments, 0L);
    }

    public boolean releaseInactive() {
        // noop, not used for anonymous pools
        return false;
    }

    private TableToken getRandToken() {
        return TOKENS[rnd.nextPositiveInt() % TOKENS.length];
    }

    protected Wrapper<O> get() {
        return super.get(getRandToken());
    }

    protected static class Wrapper<O extends QuietCloseable & Mutable> implements PoolTenant<Wrapper<O>>, Mutable {
        protected final O delegate;
        private final int index;
        private Entry<Wrapper<O>> entry;
        private AbstractMultiTenantPool<Wrapper<O>> pool;
        private TableToken tableToken;

        protected Wrapper(
                O delegate,
                AbstractMultiTenantPool<Wrapper<O>> pool,
                TableToken tableToken,
                Entry<Wrapper<O>> entry,
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
            final AbstractMultiTenantPool<Wrapper<O>> pool = this.pool;
            if (pool != null && entry != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            delegate.close();
        }

        @Override
        public Entry<Wrapper<O>> getEntry() {
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
            clear();
        }

        @Override
        public void updateTableToken(TableToken tableToken) {
            this.tableToken = tableToken;
        }
    }
}
