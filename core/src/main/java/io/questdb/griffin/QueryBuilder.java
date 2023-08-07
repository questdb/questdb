package io.questdb.griffin;

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

    @Override
    public String toString() {
        return sink.toString();
    }
}
