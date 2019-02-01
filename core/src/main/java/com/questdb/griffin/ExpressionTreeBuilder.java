/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin;

import com.questdb.griffin.model.ExpressionNode;
import com.questdb.griffin.model.QueryModel;

import java.util.ArrayDeque;
import java.util.Deque;

final class ExpressionTreeBuilder implements ExpressionParserListener {

    private final Deque<ExpressionNode> stack = new ArrayDeque<>();
    private final Deque<QueryModel> modelStack = new ArrayDeque<>();
    private QueryModel model;

    @Override
    public void onNode(ExpressionNode node) {

        if (node.queryModel != null) {
            model.addExpressionModel(node);
        }

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

    ExpressionNode poll() {
        return stack.poll();
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
        stack.clear();
        modelStack.clear();
    }

    int size() {
        return stack.size();
    }
}
