/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.impl.analytic.prev;

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
import com.questdb.store.ColumnType;

public class PrevValueAnalyticFunctionFactory implements AnalyticFunctionFactory {
    @Override
    public AnalyticFunction newInstance(ServerConfiguration configuration, RecordMetadata metadata, AnalyticColumn column) throws ParserException {
        ExprNode ast = column.getAst();
        if (ast.paramCount > 1) {
            throw QueryError.$(ast.position, "Too many arguments");
        }

        if (ast.paramCount < 1) {
            throw QueryError.$(ast.position, "Column name expected");
        }

        int valueIndex;
        if ((valueIndex = metadata.getColumnIndexQuiet(ast.rhs.token)) == -1) {
            throw QueryError.invalidColumn(ast.rhs.position, ast.rhs.token);
        }

        boolean valueIsString = metadata.getColumnQuick(valueIndex).getType() == ColumnType.STRING;

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

            if (valueIsString) {
                return new PrevStrAnalyticFunction(configuration.getDbAnalyticFuncPage(), metadata, partitionBy, ast.rhs.token, column.getAlias());
            }

            return new PrevValueAnalyticFunction(configuration.getDbAnalyticFuncPage(), metadata, partitionBy, ast.rhs.token, column.getAlias());
        } else {
            return new PrevValueNonPartAnalyticFunction(metadata, ast.rhs.token, column.getAlias());
        }
    }
}
