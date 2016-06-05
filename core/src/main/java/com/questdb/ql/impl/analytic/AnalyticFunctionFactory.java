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

package com.questdb.ql.impl.analytic;

import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Chars;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.model.AnalyticColumn;
import com.questdb.ql.model.ExprNode;
import com.questdb.ql.parser.QueryError;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;

public class AnalyticFunctionFactory {
    private static final ThreadLocal<ObjHashSet<String>> HASH_SET = new ThreadLocal<ObjHashSet<String>>() {
        @Override
        protected ObjHashSet<String> initialValue() {
            return new ObjHashSet<>();
        }
    };

    public static AnalyticFunction newInstance(ServerConfiguration configuration, RecordMetadata metadata, AnalyticColumn column) throws ParserException {
        ExprNode ast = column.getAst();

        if (ast.type == ExprNode.NodeType.FUNCTION) {

            if (Chars.equals(ast.token, "next")) {
                if (ast.paramCount > 1) {
                    throw QueryError.$(ast.position, "Too many arguments");
                }

                if (ast.paramCount < 1) {
                    throw QueryError.$(ast.position, "Column name expected");
                }

                ObjList<ExprNode> pby = column.getPartitionBy();
                int n = pby.size();

                if (n > 0) {
                    ObjHashSet<String> partitionBy = HASH_SET.get();
                    partitionBy.clear();

                    for (int i = 0; i < n; i++) {
                        ExprNode node = pby.getQuick(i);
                        if (node.type != ExprNode.NodeType.LITERAL) {
                            throw QueryError.$(node.position, "Column name expected");
                        }
                        partitionBy.add(node.token);
                    }

                    return new NextRowAnalyticFunction(configuration.getDbAnalyticFuncPage(), metadata, partitionBy, ast.rhs.token);
                } else {
                    return new NextRowNonPartAnalyticFunction(configuration.getDbAnalyticFuncPage(), metadata, ast.rhs.token);
                }
            }
        }

        throw QueryError.$(ast.position, "Unknown function");

    }
}
