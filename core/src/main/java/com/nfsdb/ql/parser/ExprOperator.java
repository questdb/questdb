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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.std.CharSequenceObjHashMap;
import com.nfsdb.std.ObjList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
final class ExprOperator {

    static final ObjList<ExprOperator> operators = new ObjList<ExprOperator>() {{
        add(new ExprOperator("^", 2, false, OperatorType.BINARY));
        add(new ExprOperator("*", 3, true, OperatorType.BINARY));
        add(new ExprOperator("/", 3, true, OperatorType.BINARY));
        add(new ExprOperator("+", 4, true, OperatorType.BINARY));
        add(new ExprOperator("-", 4, true, OperatorType.BINARY));
        add(new ExprOperator("<", 6, true, OperatorType.BINARY));
        add(new ExprOperator("<=", 6, true, OperatorType.BINARY));
        add(new ExprOperator(">", 6, true, OperatorType.BINARY));
        add(new ExprOperator(">=", 6, true, OperatorType.BINARY));
        add(new ExprOperator("=", 7, true, OperatorType.BINARY));
        add(new ExprOperator("~", 7, true, OperatorType.BINARY));
        add(new ExprOperator("!=", 7, true, OperatorType.BINARY));
        add(new ExprOperator("in", 7, true, OperatorType.SET, false));
        add(new ExprOperator("and", 11, true, OperatorType.BINARY, false));
        add(new ExprOperator("or", 11, true, OperatorType.BINARY, false));
    }};
    static final CharSequenceObjHashMap<ExprOperator> opMap = new CharSequenceObjHashMap<ExprOperator>() {{
        for (int i = 0, k = operators.size(); i < k; i++) {
            ExprOperator op = operators.getQuick(i);
            put(op.token, op);
        }
    }};
    final String token;
    final int precedence;
    final boolean leftAssociative;
    final OperatorType type;
    final boolean symbol;

    private ExprOperator(String token, int precedence, boolean leftAssociative, OperatorType type, boolean symbol) {
        this.token = token;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = symbol;
    }

    private ExprOperator(String token, int precedence, boolean leftAssociative, OperatorType type) {
        this.token = token;
        this.precedence = precedence;
        this.leftAssociative = leftAssociative;
        this.type = type;
        this.symbol = true;
    }

    enum OperatorType {
        UNARY, BINARY, SET
    }
}
