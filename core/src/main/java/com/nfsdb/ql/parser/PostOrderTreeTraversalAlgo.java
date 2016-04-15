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
