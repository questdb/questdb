/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.ex.ParserException;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.std.IntStack;

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

    public interface Visitor {
        void visit(ExprNode node) throws ParserException;
    }
}
