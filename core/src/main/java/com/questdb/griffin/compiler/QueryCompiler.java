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

package com.questdb.griffin.compiler;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.lexer.ParserException;
import com.questdb.griffin.lexer.SqlLexerOptimiser;
import com.questdb.griffin.lexer.model.ParsedModel;
import com.questdb.griffin.lexer.model.QueryModel;

public class QueryCompiler {
    private final SqlLexerOptimiser queryParser;

    public QueryCompiler(CairoEngine engine, CairoConfiguration configuration) {
        this.queryParser = new SqlLexerOptimiser(engine, configuration);
    }

    public RecordCursorFactory compileQuery(CharSequence query) throws ParserException {
        return compile(queryParser.parse(query));
    }

    private void clearState() {
        // todo: clear
    }

    private RecordCursorFactory compile(ParsedModel model) {
        if (model.getModelType() == ParsedModel.QUERY) {
            clearState();
            final QueryModel qm = (QueryModel) model;
//            qm.setParameterMap(EMPTY_PARAMS);
//            RecordCursorFactory factory = compile(qm);
//            rs.setParameterMap(EMPTY_PARAMS);
            return null;
        }
        throw new IllegalArgumentException("QueryModel expected");
    }

    private RecordCursorFactory compile(QueryModel model) {
//        optimiseSubQueries(model, factory);
//        createOrderHash(model);
//        return compileNoOptimise(model, factory);
        return null;
    }
}
