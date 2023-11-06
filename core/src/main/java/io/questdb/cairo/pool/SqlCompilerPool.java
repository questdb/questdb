package io.questdb.cairo.pool;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.*;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;

public final class SqlCompilerPool extends AbstractAnonymousMultiTenantPool<SqlCompiler> {
    private final CairoEngine engine;

    public SqlCompilerPool(CairoEngine engine) {
        super(engine.getConfiguration(), engine.getConfiguration().getSqlCompilerPoolMaxSegments());
        this.engine = engine;
    }

    @Override
    public C get() {
        return (C) super.get();
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_SQL_COMPILER;
    }

    @Override
    protected C newTenant(TableToken tableName, Entry<Wrapper<SqlCompiler>> entry, int index) {
        return new C(
                engine.getSqlCompilerFactory().getInstance(engine),
                this,
                tableName,
                entry,
                index
        );
    }

    public static class C extends Wrapper<SqlCompiler> implements SqlCompiler {
        private C(
                SqlCompiler delegate, AbstractMultiTenantPool<Wrapper<SqlCompiler>> pool,
                TableToken tableToken, Entry<Wrapper<SqlCompiler>> entry, int index
        ) {
            super(delegate, pool, tableToken, entry, index);
        }

        @Override
        public void close() {
            // revert any debug flags
            setFullFatJoins(false);
            super.close();
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
        public QueryBuilder query() {
            return delegate.query();
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
    }
}
