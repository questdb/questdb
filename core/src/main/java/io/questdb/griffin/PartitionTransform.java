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

package io.questdb.griffin;

import io.questdb.cairo.PartitionDimension;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.NumericException;
import io.questdb.std.Numbers;

import java.util.function.Function;

/**
 * Resolves a parsed CREATE-TABLE partition-dimension expression node into a
 * {@link PartitionDimension}.
 * <p>
 * Handles four node shapes: a bare column-name literal (e.g. {@code exchange}),
 * which resolves to {@link PartitionDimension#KIND_IDENTITY} with the column name
 * as its alias; and calls to the built-in transform functions {@code identity(col)},
 * {@code hash(col, N)} and {@code truncate(col, N)}.
 * <p>
 * Any other node shape (an arbitrary expression meant to be aliased with
 * {@code AS asset_class}) is <em>not</em> resolved here: {@link PartitionDimension#KIND_EXPRESSION}
 * dimensions are built directly by the CREATE-TABLE parser, which owns the {@code AS} alias.
 * This method throws for any function it does not recognize.
 */
public final class PartitionTransform {

    private PartitionTransform() {
    }

    /**
     * @param node                 the parsed dimension expression (a {@code LITERAL} or a
     *                             {@code FUNCTION} call to {@code identity}/{@code hash}/{@code truncate})
     * @param symbolColumnResolver resolves a column name to its column index; may throw an
     *                             unchecked exception, or return a negative value, if the named
     *                             column is not a SYMBOL column
     */
    public static PartitionDimension resolve(
            ExpressionNode node,
            Function<CharSequence, Integer> symbolColumnResolver
    ) throws SqlException {
        if (node.type == ExpressionNode.LITERAL) {
            return identityDimension(node, node, symbolColumnResolver);
        }

        if (node.type == ExpressionNode.FUNCTION) {
            if (Chars.equalsIgnoreCase(node.token, "identity")) {
                requireArity(node, 1);
                return identityDimension(node, node.rhs, symbolColumnResolver);
            }
            if (Chars.equalsIgnoreCase(node.token, "hash")) {
                requireArity(node, 2);
                int columnIndex = resolveSymbolColumn(node, node.lhs, symbolColumnResolver);
                int n = parseBucketCount(node, node.rhs);
                return new PartitionDimension(
                        PartitionDimension.KIND_HASH, columnIndex, n, node.lhs.token.toString() + "_hash", null
                );
            }
            if (Chars.equalsIgnoreCase(node.token, "truncate")) {
                requireArity(node, 2);
                int columnIndex = resolveSymbolColumn(node, node.lhs, symbolColumnResolver);
                int n = parseBucketCount(node, node.rhs);
                return new PartitionDimension(
                        PartitionDimension.KIND_TRUNCATE, columnIndex, n, node.lhs.token.toString() + "_trunc", null
                );
            }
        }

        // Aliased expressions ((expr) AS alias) are not built here: the CREATE-TABLE parser
        // (Task 3) recognizes that shape itself and constructs KIND_EXPRESSION directly.
        throw SqlException.position(node.position).put("unsupported partition transform");
    }

    private static PartitionDimension identityDimension(
            ExpressionNode node,
            ExpressionNode colNode,
            Function<CharSequence, Integer> symbolColumnResolver
    ) throws SqlException {
        int columnIndex = resolveSymbolColumn(node, colNode, symbolColumnResolver);
        return new PartitionDimension(
                PartitionDimension.KIND_IDENTITY, columnIndex, 0, colNode.token.toString(), null
        );
    }

    private static int parseBucketCount(ExpressionNode node, ExpressionNode nNode) throws SqlException {
        int n;
        try {
            n = Numbers.parseInt(nNode.token);
        } catch (NumericException e) {
            throw SqlException.position(node.position).put("bucket count must be > 0");
        }
        if (n <= 0) {
            throw SqlException.position(node.position).put("bucket count must be > 0");
        }
        return n;
    }

    private static void requireArity(ExpressionNode node, int expected) throws SqlException {
        if (node.paramCount != expected) {
            throw SqlException.position(node.position)
                    .put(node.token)
                    .put("() requires ")
                    .put(expected)
                    .put(expected == 1 ? " argument" : " arguments");
        }
    }

    private static int resolveSymbolColumn(
            ExpressionNode node,
            ExpressionNode colNode,
            Function<CharSequence, Integer> symbolColumnResolver
    ) throws SqlException {
        int idx = symbolColumnResolver.apply(colNode.token);
        if (idx < 0) {
            throw SqlException.position(node.position).put("partition dimension must be a SYMBOL column");
        }
        return idx;
    }
}
