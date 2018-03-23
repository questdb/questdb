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

package com.questdb.griffin.lexer;

import com.questdb.griffin.common.ExprNode;
import com.questdb.std.IntStack;

import java.util.ArrayDeque;

final class PostOrderTreeTraversalAlgo {
    private final ArrayDeque<ExprNode> stack = new ArrayDeque<>();
    private final IntStack indexStack = new IntStack();

    void traverse(ExprNode node, Visitor visitor) throws ParserException {

        // post-order iterative tree traversal
        // see http://en.wikipedia.org/wiki/Tree_traversal

        stack.clear();
        indexStack.clear();

        ExprNode lastVisited = null;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                stack.push(node);
                indexStack.push(0);
                node = node.rhs;
            } else {
                ExprNode peek = stack.peek();
                if (peek.paramCount < 3) {
                    if (peek.lhs != null && lastVisited != peek.lhs) {
                        node = peek.lhs;
                    } else {
                        visitor.visit(peek);
                        lastVisited = stack.poll();
                        indexStack.pop();
                    }
                } else {
                    int index = indexStack.peek();
                    if (index < peek.paramCount) {
                        node = peek.args.getQuick(index);
                        indexStack.update(index + 1);
                    } else {
                        visitor.visit(peek);
                        lastVisited = stack.poll();
                        indexStack.pop();
                    }
                }
            }
        }
    }

    @FunctionalInterface
    public interface Visitor {
        void visit(ExprNode node) throws ParserException;
    }
}
