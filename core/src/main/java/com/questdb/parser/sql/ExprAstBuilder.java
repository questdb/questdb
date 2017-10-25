/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.parser.sql;

import com.questdb.parser.sql.model.ExprNode;

import java.util.ArrayDeque;
import java.util.Deque;

final class ExprAstBuilder implements ExprListener {

    private final Deque<ExprNode> stack = new ArrayDeque<>();

    @Override
    public void onNode(ExprNode node) {
        switch (node.paramCount) {
            case 0:
                break;
            case 1:
                node.rhs = stack.poll();
                break;
            case 2:
                node.rhs = stack.poll();
                node.lhs = stack.poll();
                break;
            default:
                for (int i = 0; i < node.paramCount; i++) {
                    node.args.add(stack.poll());
                }
                break;
        }
        stack.push(node);
    }

    public ExprNode poll() {
        return stack.poll();
    }

    public void reset() {
        stack.clear();
    }

    public int size() {
        return stack.size();
    }
}
