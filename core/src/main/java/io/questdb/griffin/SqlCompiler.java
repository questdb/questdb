package io.questdb.griffin;

import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;

public interface SqlCompiler extends QuietCloseable, Mutable {

    CompiledQuery compile(CharSequence s, SqlExecutionContext ctx) throws SqlException;

    void compileBatch(CharSequence queryText, SqlExecutionContext sqlExecutionContext, BatchCallback batchCallback) throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException, SqlException;

    // todo: consider moving QueryBuilder from SqlCompilerImpl elsewhere (SqlCompiler?)
    SqlCompilerImpl.QueryBuilder query();
}
