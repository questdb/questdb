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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.window.LttbFunctionFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

final class SubsampleValidator {
    private SubsampleValidator() {
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
            final boolean isConstant = func.isConstant();
            if (!isConstant && !func.isRuntimeConstant()) {
                throw SqlException.$(node.position, isCadence ? "stride" : "target point count")
                        .put(" must be a constant or bind variable");
            }
            if (isConstant) {
                if (ColumnType.isNull(func.getType())) {
                    throw SqlException.$(node.position, isCadence ? "stride must be set" : "target point count must be set");
                }
                final int tag = ColumnType.tagOf(func.getType());
                if (tag != ColumnType.INT && tag != ColumnType.LONG && tag != ColumnType.SHORT && tag != ColumnType.BYTE) {
                    throw SqlException.$(node.position, isCadence ? "integer expected for stride" : "integer expected for target point count");
                }
                if (isCadence) {
                    validateStride(func, tag, node.position);
                } else {
                    validateTargetPoints(func, tag, node.position);
                }
            }
            // Existing window factories validate runtime constants and binds per execution.
        } finally {
            Misc.free(func);
        }
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
