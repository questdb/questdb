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

import com.questdb.griffin.common.ExprNode;
import com.questdb.griffin.common.PostOrderTreeTraversalAlgo;
import com.questdb.griffin.parser.ParserException;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.Chars;
import com.questdb.store.factory.configuration.ColumnName;

class LiteralMatcher implements PostOrderTreeTraversalAlgo.Visitor {
    private final PostOrderTreeTraversalAlgo algo;
    private CharSequenceIntHashMap names;
    private String alias;
    private boolean match;

    LiteralMatcher(PostOrderTreeTraversalAlgo algo) {
        this.algo = algo;
    }

    @Override
    public void visit(ExprNode node) throws ParserException {
        if (node.type == ExprNode.LITERAL && match) {
            int f = names.get(node.token);

            if (f == 0) {
                return;
            }

            if (f > 0) {
                throw ParserException.ambiguousColumn(node.position);
            }

            if (alias == null) {
                match = false;
                return;
            }

            ColumnName columnName = ColumnName.singleton(node.token);

            if (columnName.alias() == null) {
                match = false;
                return;
            }

            if (Chars.equals(columnName.alias(), alias) && (f = names.get(columnName.name())) > -1) {
                if (f > 0) {
                    throw ParserException.ambiguousColumn(node.position);
                }
                node.token = columnName.name().toString();
                return;
            }
            match = false;
        }
    }

    boolean matches(ExprNode node, CharSequenceIntHashMap names, String alias) throws ParserException {
        this.match = true;
        this.names = names;
        this.alias = alias;
        algo.traverse(node, this);
        return match;
    }
}
