/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.griffin;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.model.ExecutionModel;
import com.questdb.griffin.model.QueryColumn;
import com.questdb.griffin.model.QueryModel;
import com.questdb.std.GenericLexer;
import com.questdb.std.ObjectPool;

public class SqlCompiler {
    private final SqlOptimiser optimiser;
    private final SqlParser parser;
    private final ObjectPool<SqlNode> sqlNodePool;
    private final CharacterStore characterStore;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final GenericLexer lexer;
    private final SqlCodeGenerator codeGenerator;

    public SqlCompiler(CairoEngine engine, CairoConfiguration configuration) {
        //todo: apply configuration to all storage parameters
        sqlNodePool = new ObjectPool<>(SqlNode.FACTORY, 128);
        queryColumnPool = new ObjectPool<>(QueryColumn.FACTORY, 64);
        queryModelPool = new ObjectPool<>(QueryModel.FACTORY, 16);
        characterStore = new CharacterStore();
        lexer = new GenericLexer();
        configureLexer(lexer);
        codeGenerator = new SqlCodeGenerator(engine, configuration);

        final PostOrderTreeTraversalAlgo postOrderTreeTraversalAlgo = new PostOrderTreeTraversalAlgo();
        optimiser = new SqlOptimiser(
                engine,
                characterStore, sqlNodePool,
                queryColumnPool, queryModelPool, postOrderTreeTraversalAlgo
        );

        parser = new SqlParser(
                configuration,
                optimiser,
                characterStore,
                sqlNodePool,
                queryColumnPool,
                queryModelPool,
                postOrderTreeTraversalAlgo
        );
    }

    public static void configureLexer(GenericLexer lexer) {
        lexer.defineSymbol("(");
        lexer.defineSymbol(")");
        lexer.defineSymbol(",");
        lexer.defineSymbol("/*");
        lexer.defineSymbol("*/");
        lexer.defineSymbol("--");
        for (int i = 0, k = OperatorExpression.operators.size(); i < k; i++) {
            OperatorExpression op = OperatorExpression.operators.getQuick(i);
            if (op.symbol) {
                lexer.defineSymbol(op.token);
            }
        }
    }

    public RecordCursorFactory compile(CharSequence query) throws SqlException {
        return generate(compileExecutionModel(query));
    }

    private void clear() {
        sqlNodePool.clear();
        characterStore.clear();
        queryColumnPool.clear();
        queryModelPool.clear();
        optimiser.clear();
        parser.clear();
    }

    ExecutionModel compileExecutionModel(GenericLexer lexer) throws SqlException {
        ExecutionModel model = parser.parse(lexer);
        if (model instanceof QueryModel) {
            return optimiser.optimise((QueryModel) model);
        }
        return model;
    }

    ExecutionModel compileExecutionModel(CharSequence query) throws SqlException {
        clear();
        lexer.of(query);
        return compileExecutionModel(lexer);
    }

    RecordCursorFactory generate(ExecutionModel executionModel) throws SqlException {
        return codeGenerator.generate(executionModel);
    }

    // this exposed for testing only
    SqlNode parseExpression(CharSequence expression) throws SqlException {
        clear();
        lexer.of(expression);
        return parser.expr(lexer);
    }

    // test only
    void parseExpression(CharSequence expression, ExpressionParserListener listener) throws SqlException {
        clear();
        lexer.of(expression);
        parser.expr(lexer, listener);
    }
}
