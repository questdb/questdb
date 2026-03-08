/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.IntStack;
import io.questdb.std.Mutable;

import java.util.ArrayDeque;

public final class PostOrderTreeTraversalAlgo implements Mutable {
    private static final int CYCLICAL_AST_LIMIT = 1_000_000;
    private final IntStack backupDepth = new IntStack();
    private final IntStack indexStack = new IntStack();
    private final IntStack indexStackBackup = new IntStack();
    private final ArrayDeque<ExpressionNode> stack = new ArrayDeque<>();
    private final ArrayDeque<ExpressionNode> stackBackup = new ArrayDeque<>();

    public void backup() {
        int size = stack.size();
        backupDepth.push(size);
        for (int i = 0; i < size; i++) {
            stackBackup.push(stack.pop());
            indexStackBackup.push(indexStack.pop());
        }
    }

    @Override
    public void clear() {
        backupDepth.clear();
        indexStack.clear();
        indexStackBackup.clear();
        stack.clear();
        stackBackup.clear();
    }

    public void restore() {
        stack.clear();
        indexStack.clear();
        if (backupDepth.size() > 0) {
            int size = backupDepth.pop();
            for (int i = 0; i < size; i++) {
                stack.push(stackBackup.pop());
                indexStack.push(indexStackBackup.pop());
            }
        }
    }

    public void traverse(ExpressionNode node, Visitor visitor) throws SqlException {
        backup();
        try {
            // post-order iterative tree traversal
            // see http://en.wikipedia.org/wiki/Tree_traversal

            stack.clear();
            // indexStack keeps the index of the node we are currently visiting.
            // When paramCount < 3, we are relying on rhs (index 0) and lhs (index 1) to store the node parameters.
            indexStack.clear();

            int limit = CYCLICAL_AST_LIMIT;
            while (!stack.isEmpty() || node != null) {
                if (limit-- <= 0) {
                    throw CairoException.nonCritical().put("detected a recursive expression AST");
                }
                if (node != null) {
                    if (!visitor.descend(node)) {
                        node = null;
                        continue;
                    }
                    stack.push(node);
                    indexStack.push(0);
                    node = node.rhs;
                } else {
                    ExpressionNode peek = stack.peek();
                    assert peek != null;
                    if (peek.paramCount < 3) {
                        final int index = indexStack.peek();
                        if (index == 0) {
                            indexStack.update(1);
                            node = peek.lhs;
                        } else {
                            visitor.visit(peek);
                            stack.poll();
                            indexStack.pop();
                        }
                    } else {
                        int index = indexStack.peek();
                        if (index < peek.paramCount) {
                            node = peek.args.getQuick(index);
                            indexStack.update(index + 1);
                        } else {
                            visitor.visit(peek);
                            stack.poll();
                            indexStack.pop();
                        }
                    }
                }
            }
        } finally {
            restore();
        }
    }

    public interface Visitor {

        /**
         * Called on each node in the top-down, left-right descent order. When
         * this method returns false, the algorithm does not visit the current
         * node, nor its children nodes.
         * <p>
         * Example. For the tree like
         * <pre>
         *     A
         *   /  \
         *  B   C
         * the visit order will be:
         * C -&gt; B -&gt; A
         * while the descent order will be:
         * A -&gt; B -&gt; C
         * </pre>
         *
         * @param node tree node to validate or abort the descent
         * @return true to allow descent and false otherwise
         * @throws SqlException to allow error reporting from descend validator
         */
        default boolean descend(ExpressionNode node) throws SqlException {
            return true;
        }

        void visit(ExpressionNode node) throws SqlException;
    }
}
