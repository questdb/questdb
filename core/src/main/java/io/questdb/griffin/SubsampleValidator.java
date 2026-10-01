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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.window.LttbFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

final class SubsampleValidator {
    private SubsampleValidator() {
    }

    static boolean hasUnresolvableSdtCompdevReference(ExpressionNode compdevNode, SqlExecutionContext sqlExecutionContext) {
        // An independently invalid outer reference preserves SDT's shape error even when
        // parsing encounters another error first. Inspect only the error path: successful
        // constant folding can discard binds. Query models have their own metadata scope.
        final BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
        final ObjList<ExpressionNode> nodes = new ObjList<>();
        nodes.add(compdevNode);
        while (nodes.size() > 0) {
            final ExpressionNode node = nodes.popLast();
            if (node == null || node.type == ExpressionNode.QUERY) {
                continue;
            }
            switch (node.paramCount) {
                case 0 -> {
                    if (node.type == ExpressionNode.LITERAL) {
                        return true;
                    }
                    if (node.type == ExpressionNode.BIND_VARIABLE) {
                        if (node.token.charAt(0) == ':') {
                            if (bindVariableService != null && bindVariableService.getFunction(node.token) == null) {
                                return true;
                            }
                        } else {
                            try {
                                if (Numbers.parseInt(node.token, 1, node.token.length()) < 1) {
                                    return true;
                                }
                            } catch (NumericException e) {
                                return true;
                            }
                        }
                    }
                }
                case 1 -> nodes.add(node.rhs);
                case 2 -> {
                    nodes.add(node.lhs);
                    nodes.add(node.rhs);
                }
                default -> {
                    for (int i = 0; i < node.paramCount; i++) {
                        nodes.add(node.args.getQuick(i));
                    }
                }
            }
        }
        return false;
    }


    static void validateLttbGapOrThrow(ExpressionNode gapNode) throws SqlException {
        final CharSequence gapStr = gapNode.token;
        if (gapNode.type != ExpressionNode.CONSTANT || !Chars.isQuoted(gapStr)) {
            throw SqlException.$(gapNode.position, "gap threshold must be a string constant such as '1h'");
        }
        LttbFunctionFactory.parseGapThresholdMicros(Chars.toString(gapStr, 1, gapStr.length() - 1), gapNode.position);
    }

    static void validateNumericType(int valueType, int position) throws SqlException {
        final int valueTag = ColumnType.tagOf(valueType);
        if (valueTag != ColumnType.DOUBLE && valueTag != ColumnType.FLOAT
                && valueTag != ColumnType.INT && valueTag != ColumnType.LONG
                && valueTag != ColumnType.SHORT && valueTag != ColumnType.BYTE) {
            throw SqlException.$(position, "numeric column expected, got: ").put(ColumnType.nameOf(valueType));
        }
    }

    // Callers own the AST: FunctionParser can reassociate even a successfully parsed constant.
    static void validatePositionTargetOrThrow(
            ExpressionNode node,
            boolean isCadence,
            FunctionParser functionParser,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (node.type == ExpressionNode.LITERAL) {
            throw SqlException.$(node.position, isCadence ? "stride" : "target point count")
                    .put(" must be a constant or bind variable");
        }
        Function func = null;
        try {
            func = functionParser.parseFunction(node, EmptyRecordMetadata.INSTANCE, sqlExecutionContext);
            validatePositionTargetOrThrow(func, node.position, isCadence);
        } finally {
            Misc.free(func);
        }
    }

    /** Borrows the function; runtime values are validated by the window function at each execution. */
    static void validatePositionTargetOrThrow(Function function, int position, boolean isCadence) throws SqlException {
        final boolean isConstant = function.isConstant();
        if (!isConstant && !function.isRuntimeConstant()) {
            throw SqlException.$(position, isCadence ? "stride" : "target point count")
                    .put(" must be a constant or bind variable");
        }
        if (isConstant) {
            if (ColumnType.isNull(function.getType())) {
                throw SqlException.$(position, isCadence ? "stride must be set" : "target point count must be set");
            }
            final int tag = ColumnType.tagOf(function.getType());
            if (tag != ColumnType.INT && tag != ColumnType.LONG && tag != ColumnType.SHORT && tag != ColumnType.BYTE) {
                throw SqlException.$(position, isCadence ? "integer expected for stride" : "integer expected for target point count");
            }
            if (isCadence) {
                validateStride(function, tag, position);
            } else {
                validateTargetPoints(function, tag, position);
            }
        }
    }

    static void validateCadenceSeedOrThrow(
            ExpressionNode node,
            FunctionParser functionParser,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (node.type == ExpressionNode.LITERAL) {
            throw SqlException.$(node.position, "seed must be a constant, bind variable, or NULL");
        }
        Function function = null;
        try {
            function = functionParser.parseFunction(node, EmptyRecordMetadata.INSTANCE, sqlExecutionContext);
            validateCadenceSeedOrThrow(function, node.position);
        } finally {
            Misc.free(function);
        }
    }

    /** Borrows the function; a NULL seed selects random cadence. */
    static void validateCadenceSeedOrThrow(Function function, int position) throws SqlException {
        if (ColumnType.isNull(function.getType())) {
            return;
        }
        final boolean isConstant = function.isConstant();
        if (!isConstant && !function.isRuntimeConstant()) {
            throw SqlException.$(position, "seed must be a constant, bind variable, or NULL");
        }
        if (isConstant) {
            final int tag = ColumnType.tagOf(function.getType());
            if (tag != ColumnType.INT && tag != ColumnType.LONG && tag != ColumnType.SHORT && tag != ColumnType.BYTE) {
                throw SqlException.$(position, "integer or NULL expected for seed");
            }
        }
    }

    /** Borrows the function and retains no executable state. */
    static void validateSdtCompdev(Function function, int position) throws SqlException {
        if (function.isConstant()) {
            final int tag = ColumnType.tagOf(function.getType());
            if (tag == ColumnType.DOUBLE || tag == ColumnType.FLOAT
                    || tag == ColumnType.INT || tag == ColumnType.LONG
                    || tag == ColumnType.SHORT || tag == ColumnType.BYTE) {
                final double compdev = function.getDouble(null);
                if (compdev >= 0 && Numbers.isFinite(compdev)) {
                    return;
                }
            }
        }
        throw SqlException.$(position, "SUBSAMPLE sdt requires a constant, non-negative finite compdev");
    }

    private static void validateStride(Function targetFunc, int targetType, int position) throws SqlException {
        final long value;
        if (targetType == ColumnType.LONG) {
            value = targetFunc.getLong(null);
            if (value == Numbers.LONG_NULL) {
                throw SqlException.$(position, "stride must be set");
            }
        } else {
            final int intValue = targetFunc.getInt(null);
            if (intValue == Numbers.INT_NULL) {
                throw SqlException.$(position, "stride must be set");
            }
            value = intValue;
        }
        if (value < 1) {
            throw SqlException.$(position, "stride must be at least 1");
        }
        if (value > Integer.MAX_VALUE) {
            throw SqlException.$(position, "stride exceeds maximum of ").put(Integer.MAX_VALUE);
        }
    }

    private static void validateTargetPoints(Function targetFunc, int targetType, int position) throws SqlException {
        final long value;
        if (targetType == ColumnType.LONG) {
            value = targetFunc.getLong(null);
            if (value == Numbers.LONG_NULL) {
                throw SqlException.$(position, "target point count must be set");
            }
        } else {
            final int intValue = targetFunc.getInt(null);
            if (intValue == Numbers.INT_NULL) {
                throw SqlException.$(position, "target point count must be set");
            }
            value = intValue;
        }
        if (value < 2) {
            throw SqlException.$(position, "target points must be at least 2");
        }
        if (value > Integer.MAX_VALUE) {
            throw SqlException.$(position, "target points exceeds maximum of ").put(Integer.MAX_VALUE);
        }
    }
}
