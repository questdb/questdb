package io.questdb.griffin;

import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.QueryPausedException;
import io.questdb.std.Mutable;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

public interface SqlCompiler extends QuietCloseable, Mutable {

    CompiledQuery compile(CharSequence s, SqlExecutionContext ctx) throws SqlException;

    void compileBatch(CharSequence queryText, SqlExecutionContext sqlExecutionContext, BatchCallback batchCallback) throws PeerIsSlowToReadException, PeerDisconnectedException, QueryPausedException, SqlException;

    QueryBuilder query();

    @TestOnly
    ExecutionModel testCompileModel(CharSequence query, SqlExecutionContext executionContext) throws SqlException;

    @TestOnly
    void setFullFatJoins(boolean fullFatJoins);

    @TestOnly
    ExpressionNode testParseExpression(CharSequence expression, QueryModel model) throws SqlException;

    @TestOnly
    void setEnableJitNullChecks(boolean value);
}
