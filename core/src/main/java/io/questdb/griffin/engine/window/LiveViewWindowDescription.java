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

package io.questdb.griffin.engine.window;

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * The window syntax a live-view checkpoint descriptor is derived from, captured while the
 * parsed window is available so the checkpoint identity survives its disposal.
 */
public final class LiveViewWindowDescription {
    private final String canonicalWindowName;
    private final int exclusionKind;
    private final int framingMode;
    private final boolean isAnchorReset;
    private final boolean isAnchored;
    private final boolean isIgnoreNulls;
    private final int orderByCount;
    private final int orderByDirection;
    private final CharSequence orderByName;
    private final int orderByPosition;
    private final String orderSignature;
    private final ObjList<CharSequence> partitionColumns;
    private final String partitionSignature;
    private final int position;
    private final long rowsHi;
    private final int rowsHiExprPos;
    private final char rowsHiExprTimeUnit;
    private final int rowsHiKind;
    private final long rowsLo;
    private final int rowsLoExprPos;
    private final char rowsLoExprTimeUnit;
    private final int rowsLoKind;

    private LiveViewWindowDescription(WindowExpression window) {
        final ObjList<ExpressionNode> partitionBy = window.getPartitionBy();
        final ObjList<ExpressionNode> orderBy = window.getOrderBy();
        canonicalWindowName = window.getResolvedWindowName() == null ? "" : Chars.toLowerCaseAscii(window.getResolvedWindowName());
        exclusionKind = window.getExclusionKind();
        framingMode = window.getFramingMode();
        isAnchored = window.getAnchorKind() != WindowExpression.ANCHOR_KIND_NONE || window.isResolvedWindowAnchored();
        isAnchorReset = window.getRowsLoKind() == WindowExpression.PRECEDING && window.getRowsLoExpr() == null
                && window.getRowsHiKind() == WindowExpression.CURRENT && window.getRowsHiExpr() == null;
        isIgnoreNulls = window.isIgnoreNulls();
        orderByCount = orderBy.size();
        orderByDirection = orderByCount > 0 ? window.getOrderByDirection().getQuick(0) : 0;
        orderByName = orderByCount > 0 ? Chars.toString(orderBy.getQuick(0).token) : null;
        orderByPosition = orderByCount > 0 ? orderBy.getQuick(0).position : -1;
        partitionColumns = new ObjList<>(partitionBy.size());
        for (int i = 0, n = partitionBy.size(); i < n; i++) {
            final ExpressionNode node = partitionBy.getQuick(i);
            partitionColumns.add(node.type == ExpressionNode.LITERAL ? Chars.toString(node.token) : null);
        }
        orderSignature = signature(orderBy, window.getOrderByDirection());
        partitionSignature = signature(partitionBy, null);
        position = window.getAst() == null ? 0 : window.getAst().position;
        rowsHi = window.getRowsHi();
        rowsHiExprPos = window.getRowsHiExprPos();
        rowsHiExprTimeUnit = window.getRowsHiExprTimeUnit();
        rowsHiKind = window.getRowsHiKind();
        rowsLo = window.getRowsLo();
        rowsLoExprPos = window.getRowsLoExprPos();
        rowsLoExprTimeUnit = window.getRowsLoExprTimeUnit();
        rowsLoKind = window.getRowsLoKind();
    }

    public static LiveViewWindowDescription of(WindowExpression window) {
        return new LiveViewWindowDescription(window);
    }

    public String getCanonicalWindowName() {
        return canonicalWindowName;
    }

    public int getExclusionKind() {
        return exclusionKind;
    }

    public int getFramingMode() {
        return framingMode;
    }

    public int getOrderByCount() {
        return orderByCount;
    }

    /**
     * The direction of the first ORDER BY term.
     */
    public int getOrderByDirection() {
        return orderByDirection;
    }

    /**
     * The first ORDER BY term as written.
     */
    public CharSequence getOrderByName() {
        return orderByName;
    }

    public int getOrderByPosition() {
        return orderByPosition;
    }

    public String getOrderSignature() {
        return orderSignature;
    }

    public int getPartitionByCount() {
        return partitionColumns.size();
    }

    /**
     * The PARTITION BY term's column name, or null when the term is an expression.
     */
    public CharSequence getPartitionColumn(int index) {
        return partitionColumns.getQuick(index);
    }

    public String getPartitionSignature() {
        return partitionSignature;
    }

    public int getPosition() {
        return position;
    }

    public long getRowsHi() {
        return rowsHi;
    }

    public int getRowsHiExprPos() {
        return rowsHiExprPos;
    }

    public char getRowsHiExprTimeUnit() {
        return rowsHiExprTimeUnit;
    }

    public int getRowsHiKind() {
        return rowsHiKind;
    }

    public long getRowsLo() {
        return rowsLo;
    }

    public int getRowsLoExprPos() {
        return rowsLoExprPos;
    }

    public char getRowsLoExprTimeUnit() {
        return rowsLoExprTimeUnit;
    }

    public int getRowsLoKind() {
        return rowsLoKind;
    }

    /**
     * Whether the frame is literally UNBOUNDED PRECEDING ... CURRENT ROW, the frame an anchor resets.
     */
    public boolean isAnchorReset() {
        return isAnchorReset;
    }

    /**
     * Whether an ANCHOR owns the window, directly or through a named window definition.
     */
    public boolean isAnchored() {
        return isAnchored;
    }

    public boolean isIgnoreNulls() {
        return isIgnoreNulls;
    }

    private static String signature(ObjList<ExpressionNode> expressions, IntList directions) {
        final StringSink sink = new StringSink();
        sink.put(expressions.size()).putAscii(':');
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final StringSink expressionSink = new StringSink();
            expressions.getQuick(i).toSink(expressionSink);
            sink.put(expressionSink.length()).putAscii(':').put(expressionSink);
            if (directions != null) {
                sink.putAscii(':').put(directions.getQuick(i));
            }
            sink.putAscii(';');
        }
        return sink.toString();
    }
}
