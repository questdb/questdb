package io.questdb.griffin;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.model.CreateTableModel;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.GenericLexer;
import io.questdb.std.ObjectPool;

public interface SqlParserCallback {
    default RecordCursorFactory generateShowSqlFactory(QueryModel model) throws SqlException {
        assert false;
        return null;
    }

    default int parseShowSql(
            GenericLexer lexer,
            QueryModel model,
            CharSequence tok,
            ObjectPool<ExpressionNode> expressionNodePool
    ) throws SqlException {
        return -1;
    }

    default ExecutionModel unknownCreateTableSuffix(
            GenericLexer lexer,
            SqlExecutionContext executionContext,
            CreateTableModel model,
            CharSequence tok
    ) throws SqlException {
        throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
    }
}
