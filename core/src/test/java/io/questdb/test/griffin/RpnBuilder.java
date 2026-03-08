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

package io.questdb.test.griffin;

import io.questdb.griffin.ExpressionParserListener;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class RpnBuilder implements ExpressionParserListener {
    private final StringSink sink = new StringSink();

    @Override
    public void onNode(ExpressionNode node) {
        if (node.queryModel != null) {
            sink.put('(').put(node.queryModel).put(')').put(' ');
        } else {
            sink.put(node.token);
            // Serialize window context if present
            if (node.windowExpression != null) {
                serializeWindowContext(node.windowExpression);
            }
            sink.put(' ');
        }
    }

    public void reset() {
        sink.clear();
    }

    public final CharSequence rpn() {
        if (sink.charAt(sink.length() - 1) == ' ') {
            sink.clear(sink.length() - 1);
        }
        return sink;
    }

    private void serializeExpressionToRpn(ExpressionNode node) {
        if (node == null) {
            return;
        }
        // Match PostOrderTreeTraversalAlgo's behavior:
        // paramCount < 3: uses lhs/rhs
        // paramCount >= 3: uses args
        if (node.paramCount < 3) {
            // Use lhs/rhs for operands
            if (node.lhs != null) {
                serializeExpressionToRpn(node.lhs);
                sink.put(' ');
            }
            if (node.rhs != null) {
                serializeExpressionToRpn(node.rhs);
                sink.put(' ');
            }
            sink.put(node.token);
        } else {
            // Use args for 3+ parameters
            ObjList<ExpressionNode> args = node.args;
            for (int i = 0, n = args.size(); i < n; i++) {
                if (i > 0) sink.put(' ');
                serializeExpressionToRpn(args.getQuick(i));
            }
            if (args.size() > 0) sink.put(' ');
            sink.put(node.token);
        }
    }

    private void serializeFrameBound(ExpressionNode expr, int kind, char timeUnit, boolean isLower) {
        if (expr != null) {
            serializeExpressionToRpn(expr);
            if (timeUnit != 0) {
                sink.put(' ');
                switch (timeUnit) {
                    case WindowExpression.TIME_UNIT_HOUR -> sink.put("hour");
                    case WindowExpression.TIME_UNIT_MINUTE -> sink.put("minute");
                    case WindowExpression.TIME_UNIT_SECOND -> sink.put("second");
                    case WindowExpression.TIME_UNIT_MILLISECOND -> sink.put("millisecond");
                    case WindowExpression.TIME_UNIT_MICROSECOND -> sink.put("microsecond");
                    case WindowExpression.TIME_UNIT_NANOSECOND -> sink.put("nanosecond");
                    case WindowExpression.TIME_UNIT_DAY -> sink.put("day");
                    default -> sink.put(timeUnit);
                }
            }
            sink.put(' ');
        }

        switch (kind) {
            case WindowExpression.PRECEDING -> {
                if (expr == null && isLower) {
                    sink.put("unbounded ");
                }
                sink.put("preceding");
            }
            case WindowExpression.FOLLOWING -> {
                if (expr == null && !isLower) {
                    sink.put("unbounded ");
                }
                sink.put("following");
            }
            case WindowExpression.CURRENT -> sink.put("current row");
        }
    }

    private void serializeWindowContext(WindowExpression wc) {
        if (wc.isIgnoreNulls()) {
            sink.put(" ignore nulls");
        }
        sink.put(" over (");

        boolean needSpace = false;

        // Partition by
        ObjList<ExpressionNode> partitionBy = wc.getPartitionBy();
        if (partitionBy.size() > 0) {
            sink.put("partition by ");
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                if (i > 0) sink.put(", ");
                serializeExpressionToRpn(partitionBy.getQuick(i));
            }
            needSpace = true;
        }

        // Order by
        ObjList<ExpressionNode> orderBy = wc.getOrderBy();
        if (orderBy.size() > 0) {
            if (needSpace) sink.put(' ');
            sink.put("order by ");
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                if (i > 0) sink.put(", ");
                serializeExpressionToRpn(orderBy.getQuick(i));
                int dir = wc.getOrderByDirection().getQuick(i);
                if (dir == QueryModel.ORDER_DIRECTION_DESCENDING) {
                    sink.put(" desc");
                }
            }
            needSpace = true;
        }

        // Frame specification (only if non-default)
        if (wc.isNonDefaultFrame()) {
            if (needSpace) sink.put(' ');

            // Frame mode
            switch (wc.getFramingMode()) {
                case WindowExpression.FRAMING_ROWS:
                    sink.put("rows ");
                    break;
                case WindowExpression.FRAMING_RANGE:
                    sink.put("range ");
                    break;
                case WindowExpression.FRAMING_GROUPS:
                    sink.put("groups ");
                    break;
            }

            sink.put("between ");
            serializeFrameBound(wc.getRowsLoExpr(), wc.getRowsLoKind(), wc.getRowsLoExprTimeUnit(), true);
            sink.put(" and ");
            serializeFrameBound(wc.getRowsHiExpr(), wc.getRowsHiKind(), wc.getRowsHiExprTimeUnit(), false);

            // Exclusion
            if (wc.getExclusionKind() != WindowExpression.EXCLUDE_NO_OTHERS) {
                sink.put(" exclude ");
                switch (wc.getExclusionKind()) {
                    case WindowExpression.EXCLUDE_CURRENT_ROW:
                        sink.put("current row");
                        break;
                    case WindowExpression.EXCLUDE_GROUP:
                        sink.put("group");
                        break;
                    case WindowExpression.EXCLUDE_TIES:
                        sink.put("ties");
                        break;
                }
            }
        }

        sink.put(')');
    }
}
