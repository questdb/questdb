package io.questdb.griffin;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.GenericLexer;
import io.questdb.std.ObjectPool;

public interface SqlParserCallback {
    default int parseShowSql(
            GenericLexer lexer,
            QueryModel model,
            CharSequence tok,
            ObjectPool<ExpressionNode> expressionNodePool
    ) throws SqlException {
        return -1;
    }

    default RecordCursorFactory generateShowSqlFactory(QueryModel model) throws SqlException {
        assert false;
        return null;
    }
}
