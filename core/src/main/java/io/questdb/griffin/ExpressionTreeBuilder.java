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

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;

import java.util.ArrayDeque;
import java.util.Deque;

final class ExpressionTreeBuilder implements ExpressionParserListener {

    private final Deque<ExpressionNode> argStack = new ArrayDeque<>();
    private final Deque<QueryModel> modelStack = new ArrayDeque<>();
    private QueryModel model;

    @Override
    public void onNode(ExpressionNode node) throws SqlException {

        if (node.type == ExpressionNode.QUERY && node.queryModel == null) {
            // this is a validation request
            if (model == null) {
                throw SqlException.$(node.position, "query is not allowed here");
            }
            return;
        }

        if (node.queryModel != null) {
            model.addExpressionModel(node);
        }

        switch (node.paramCount) {
            case 0:
                break;
            case 1:
                node.rhs = argStack.poll();
                break;
            case 2:
                node.rhs = argStack.poll();
                node.lhs = argStack.poll();
                break;
            default:
                for (int i = 0; i < node.paramCount; i++) {
                    node.args.add(argStack.poll());
                }
                break;
        }
        argStack.push(node);
    }

    ExpressionNode poll() {
        return argStack.poll();
    }

    void popModel() {
        this.model = modelStack.poll();
    }

    void pushModel(QueryModel model) {
        if (this.model != null) {
            modelStack.push(this.model);
        }
        this.model = model;
    }

    void reset() {
        argStack.clear();
        modelStack.clear();
    }

    int size() {
        return argStack.size();
    }
}
