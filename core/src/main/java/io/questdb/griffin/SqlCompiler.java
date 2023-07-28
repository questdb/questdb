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
    void setEnableJitNullChecks(boolean value);

    @TestOnly
    void setFullFatJoins(boolean fullFatJoins);

    @TestOnly
    ExecutionModel testCompileModel(CharSequence query, SqlExecutionContext executionContext) throws SqlException;

    @TestOnly
    ExpressionNode testParseExpression(CharSequence expression, QueryModel model) throws SqlException;

    @TestOnly
    void testParseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException;
}
