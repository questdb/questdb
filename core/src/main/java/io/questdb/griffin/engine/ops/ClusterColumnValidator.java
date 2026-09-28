/*******************************************************************************
 *     ___   _   _           _   ____  ____
 *    / _ \ | | | | ___  ___| |_|  _ \| __ )
 *   | | | || | | |/ _ \/ __| __| | | |  _ \
 *   | |_| || |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\___/ \___||___/\__|____/|____/
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

package io.questdb.griffin.engine.ops;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.ObjList;

import java.util.function.Function;
import java.util.function.IntUnaryOperator;

final class ClusterColumnValidator {

    private ClusterColumnValidator() {
    }

    /**
     * Validates the v1 clustering grammar and returns the one physical cluster
     * column index, or {@code -1} when no clustering was declared.
     * <p>
     * The designated timestamp is an implicit final sort key. SQL may spell it
     * as {@code ORDER BY sym, ts}, but it is deliberately not persisted as a
     * second cluster column.
     */
    static int validate(
            ObjList<ExpressionNode> clusterExprs,
            Function<CharSequence, Integer> columnIndexResolver,
            IntUnaryOperator columnTypeResolver,
            int timestampIndex
    ) throws SqlException {
        final int count = clusterExprs.size();
        if (count == 0) {
            return -1;
        }
        if (count > 2) {
            throw SqlException.$(
                    clusterExprs.getQuick(2).position,
                    "clustering supports one SYMBOL column followed by the designated timestamp"
            );
        }

        final ExpressionNode clusterExpr = clusterExprs.getQuick(0);
        final int clusterIndex = columnIndexResolver.apply(clusterExpr.token);
        if (clusterIndex < 0) {
            throw SqlException.invalidColumn(clusterExpr.position, clusterExpr.token);
        }
        if (!ColumnType.isSymbol(columnTypeResolver.applyAsInt(clusterIndex))) {
            throw SqlException.$(clusterExpr.position, "cluster column must be a SYMBOL column [column=")
                    .put(clusterExpr.token).put(']');
        }

        if (count == 2) {
            final ExpressionNode timestampExpr = clusterExprs.getQuick(1);
            final int explicitTimestampIndex = columnIndexResolver.apply(timestampExpr.token);
            if (explicitTimestampIndex < 0) {
                throw SqlException.invalidColumn(timestampExpr.position, timestampExpr.token);
            }
            if (timestampIndex < 0 || explicitTimestampIndex != timestampIndex) {
                throw SqlException.$(
                        timestampExpr.position,
                        "second cluster column must be the designated timestamp"
                );
            }
        }
        return clusterIndex;
    }
}
