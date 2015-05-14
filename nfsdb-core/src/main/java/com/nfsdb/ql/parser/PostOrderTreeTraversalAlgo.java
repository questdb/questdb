/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.ql.parser;

import com.nfsdb.collections.IntStack;
import com.nfsdb.ql.model.ExprNode;

import java.util.ArrayDeque;

public class PostOrderTreeTraversalAlgo {
    private final ArrayDeque<ExprNode> stack = new ArrayDeque<>();
    private final IntStack indexStack = new IntStack();

    public void traverse(ExprNode node, Visitor visitor) throws ParserException {

        // post-order iterative tree traversal
        // see http://en.wikipedia.org/wiki/Tree_traversal

        stack.clear();
        indexStack.clear();

        ExprNode lastVisited = null;

        while (!stack.isEmpty() || node != null) {
            if (node != null) {
                stack.addFirst(node);
                indexStack.push(0);
                node = node.lhs;
            } else {
                ExprNode peek = stack.peek();
                if (peek.paramCount < 3) {
                    if (peek.rhs != null && lastVisited != peek.rhs) {
                        node = peek.rhs;
                    } else {
                        visitor.visit(peek);
                        lastVisited = stack.pollFirst();
                        indexStack.pop();
                    }
                } else {
                    int index = indexStack.peek();
                    if (index < peek.paramCount) {
                        node = peek.args.get(index);
                        indexStack.update(index + 1);
                    } else {
                        visitor.visit(peek);
                        lastVisited = stack.pollFirst();
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
