/*+*****************************************************************************
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

package io.questdb.griffin.plan.logical;

import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * The slots a node hands to {@link PlanExpressionVisitor}: an absent expression and a negative column id are not
 * reads and are not visited.
 */
final class PlanReads {

    private PlanReads() {
    }

    static int columnId(int columnId, int position, PlanExpressionVisitor visitor) {
        return columnId < 0 ? columnId : visitor.visitColumnId(columnId, position);
    }

    static void columnIds(IntList columnIds, IntList positions, PlanExpressionVisitor visitor) {
        for (int i = 0, n = columnIds.size(); i < n; i++) {
            columnIds.setQuick(i, columnId(columnIds.getQuick(i), positions != null && i < positions.size() ? positions.getQuick(i) : -1, visitor));
        }
    }

    static BoundExpression expression(BoundExpression expression, PlanExpressionVisitor visitor) {
        return expression == null ? null : visitor.visitExpression(expression);
    }

    static void expressions(ObjList<BoundExpression> expressions, PlanExpressionVisitor visitor) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            expressions.setQuick(i, expression(expressions.getQuick(i), visitor));
        }
    }

    static void functions(ObjList<FunctionExpression> functions, PlanExpressionVisitor visitor) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.setQuick(i, (FunctionExpression) visitor.visitExpression(functions.getQuick(i)));
        }
    }
}
