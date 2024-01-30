package io.questdb.griffin;

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.model.CreateTableModel;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.GenericLexer;
import io.questdb.std.ObjectPool;
import org.jetbrains.annotations.Nullable;

public interface SqlParserCallback {

    default ExecutionModel createTableSuffix(
            GenericLexer lexer,
            SecurityContext securityContext,
            CreateTableModel model,
            @Nullable CharSequence tok
    ) throws SqlException {
        if (tok != null) {
            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
        }
        return model;
    }

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
}
