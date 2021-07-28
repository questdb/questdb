/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.str.StringSink;

import java.util.ArrayDeque;
import java.util.Deque;

public class RpnBuilder implements ExpressionParserListener {
    private final Deque<ExpressionNode> stack = new ArrayDeque<>();
    private final StringSink sink = new StringSink();

    @Override
    public void onNode(ExpressionNode node) {
        if (node.queryModel != null) {
            sink.put('(').put(node.queryModel).put(')');
            return;
        }
        switch (node.type) {
            case ExpressionNode.GEOHASH_TYPE_SIZE:
                stack.push(node);
                break;

            case ExpressionNode.GEOHASH_TYPE:
                sink.put(ColumnType.nameOf(ColumnType.GEOHASH)).put('(').put(stack.poll().token).put(")");
                break;

            default:
                sink.put(node.token);
        }
    }

    public void reset() {
        sink.clear();
    }

    public final CharSequence rpn() {
        return sink;
    }
}
