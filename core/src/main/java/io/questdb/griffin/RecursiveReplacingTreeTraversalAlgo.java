/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.griffin.model.ExpressionNode;

public final class RecursiveReplacingTreeTraversalAlgo {

    public ExpressionNode traverse(ExpressionNode node, ReplacingVisitor visitor) throws SqlException {
        if (node == null) {
            return null;
        }

        switch (node.paramCount) {
            case 0:
                break;
            case 1:
                node.rhs = traverse(node.rhs, visitor);
                break;
            case 2:
                node.lhs = traverse(node.lhs, visitor);
                node.rhs = traverse(node.rhs, visitor);
                break;
            default:
                for (int i = 0; i < node.paramCount; i++) {
                    ExpressionNode arg = node.args.get(i);
                    node.args.set(i, traverse(arg, visitor));
                }
                break;
        }

        return visitor.visit(node);
    }

    public interface ReplacingVisitor {

        ExpressionNode visit(ExpressionNode node) throws SqlException;
    }
}
