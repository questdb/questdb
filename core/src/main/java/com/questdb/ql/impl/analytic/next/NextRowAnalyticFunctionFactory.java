package com.questdb.ql.impl.analytic.next;

import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.ql.impl.analytic.AnalyticFunctionFactory;
import com.questdb.ql.impl.analytic.AnalyticUtils;
import com.questdb.ql.model.AnalyticColumn;
import com.questdb.ql.model.ExprNode;
import com.questdb.ql.parser.QueryError;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;

public class NextRowAnalyticFunctionFactory implements AnalyticFunctionFactory {
    @Override
    public AnalyticFunction newInstance(ServerConfiguration configuration, RecordMetadata metadata, AnalyticColumn column) throws ParserException {
        ExprNode ast = column.getAst();
        if (ast.paramCount > 1) {
            throw QueryError.$(ast.position, "Too many arguments");
        }

        if (ast.paramCount < 1) {
            throw QueryError.$(ast.position, "Column name expected");
        }

        if (metadata.getColumnIndexQuiet(ast.rhs.token) == -1) {
            throw QueryError.invalidColumn(ast.rhs.position, ast.rhs.token);
        }

        ObjList<ExprNode> pby = column.getPartitionBy();
        int n = pby.size();

        if (n > 0) {
            ObjHashSet<String> partitionBy = AnalyticUtils.HASH_SET.get();
            partitionBy.clear();

            for (int i = 0; i < n; i++) {
                ExprNode node = pby.getQuick(i);
                if (node.type != ExprNode.NodeType.LITERAL) {
                    throw QueryError.$(node.position, "Column name expected");
                }

                if (metadata.getColumnIndexQuiet(node.token) == -1) {
                    throw QueryError.invalidColumn(node.position, node.token);
                }

                partitionBy.add(node.token);
            }

            return new NextRowAnalyticFunction(configuration.getDbAnalyticFuncPage(), metadata, partitionBy, ast.rhs.token);
        } else {
            return new NextRowNonPartAnalyticFunction(configuration.getDbAnalyticFuncPage(), metadata, ast.rhs.token);
        }
    }
}
