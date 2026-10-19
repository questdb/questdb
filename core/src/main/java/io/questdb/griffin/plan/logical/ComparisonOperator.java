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

/**
 * The binary comparison a function name spells: {@code =}, {@code !=} or {@code <>}, {@code <}, {@code <=},
 * {@code >} and {@code >=}.
 */
public enum ComparisonOperator {
    EQUAL,
    NOT_EQUAL,
    LESS,
    LESS_OR_EQUAL,
    GREATER,
    GREATER_OR_EQUAL;

    /**
     * The comparison the function name spells, or null when it spells none.
     */
    public static ComparisonOperator of(String name) {
        return switch (name) {
            case "=" -> EQUAL;
            case "!=", "<>" -> NOT_EQUAL;
            case "<" -> LESS;
            case "<=" -> LESS_OR_EQUAL;
            case ">" -> GREATER;
            case ">=" -> GREATER_OR_EQUAL;
            default -> null;
        };
    }

    /**
     * The comparison that holds exactly where this one fails, over operands that are not NULL.
     */
    public ComparisonOperator complement() {
        return switch (this) {
            case EQUAL -> NOT_EQUAL;
            case NOT_EQUAL -> EQUAL;
            case LESS -> GREATER_OR_EQUAL;
            case LESS_OR_EQUAL -> GREATER;
            case GREATER -> LESS_OR_EQUAL;
            case GREATER_OR_EQUAL -> LESS;
        };
    }

    /**
     * The comparison with its operands swapped: {@code a op b} holds exactly where {@code b op.mirror() a} does.
     */
    public ComparisonOperator mirror() {
        return switch (this) {
            case EQUAL, NOT_EQUAL -> this;
            case LESS -> GREATER;
            case LESS_OR_EQUAL -> GREATER_OR_EQUAL;
            case GREATER -> LESS;
            case GREATER_OR_EQUAL -> LESS_OR_EQUAL;
        };
    }

    public boolean test(long left, long right) {
        return switch (this) {
            case EQUAL -> left == right;
            case NOT_EQUAL -> left != right;
            case LESS -> left < right;
            case LESS_OR_EQUAL -> left <= right;
            case GREATER -> left > right;
            case GREATER_OR_EQUAL -> left >= right;
        };
    }
}
