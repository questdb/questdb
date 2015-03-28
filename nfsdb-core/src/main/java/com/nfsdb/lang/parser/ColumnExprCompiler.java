/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.lang.parser;

import com.nfsdb.lang.ast.ExprNode;
import com.nfsdb.lang.cst.impl.virt.VirtualColumn;

import java.util.ArrayDeque;
import java.util.Deque;

public class ColumnExprCompiler {

    public VirtualColumn compile(ExprNode node) {
        // iterative post-order tree traversal
        Deque<ExprNode> parentStack = new ArrayDeque<>();
        Deque<VirtualColumn> exprStack = new ArrayDeque<>();

        ExprNode last = null;

        while (!parentStack.isEmpty() || node != null) {
            if (node != null) {
                parentStack.push(node);
                node = node.lhs;
            } else {
                ExprNode peek = parentStack.peek();
                switch (peek.paramCount) {
                    case 0:
                    case 1:
                        compile0(peek, exprStack);
                        break;
                    case 2:
                        if (last != peek.rhs) {
                            node = node.rhs;
                        } else {
                            compile0(peek, exprStack);
                        }
                        break;
                    default:

                }
                if (peek.rhs != null && last != peek.rhs) {
                    node = node.rhs;
                } else {
                    compile0(peek, exprStack);
                    last = parentStack.pop();
                }
            }
        }
        return exprStack.pop();
    }

    private void compile0(ExprNode node, Deque<VirtualColumn> exprStack) {

    }
}
