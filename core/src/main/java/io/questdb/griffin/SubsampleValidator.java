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
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;

public final class SubsampleValidator {
    private SubsampleValidator() {
    }

    /**
     * Whether the compdev names a column or a bind variable no execution defines, outside its sub-queries: such a
     * reference keeps the sdt shape error even when binding meets another error first.
     */
    public static boolean hasUnresolvableSdtCompdevReference(ExpressionNode compdev, SqlExecutionContext sqlExecutionContext) {
        if (compdev == null || compdev.type == ExpressionNode.QUERY) {
            return false;
        }
        if (compdev.paramCount == 0) {
            return compdev.type == ExpressionNode.LITERAL
                    || compdev.type == ExpressionNode.BIND_VARIABLE && isUndefinedBindVariable(compdev.token, sqlExecutionContext.getBindVariableService());
        }
        if (compdev.paramCount < 3) {
            return hasUnresolvableSdtCompdevReference(compdev.lhs, sqlExecutionContext)
                    || hasUnresolvableSdtCompdevReference(compdev.rhs, sqlExecutionContext);
        }
        for (int i = 0; i < compdev.paramCount; i++) {
            if (hasUnresolvableSdtCompdevReference(compdev.args.getQuick(i), sqlExecutionContext)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Borrows the function; a NULL seed selects random cadence.
     */
    public static void validateCadenceSeedOrThrow(Function function, int position) throws SqlException {
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

    private static boolean isUndefinedBindVariable(CharSequence token, BindVariableService bindVariableService) {
        if (token.charAt(0) == ':') {
            return bindVariableService != null && bindVariableService.getFunction(token) == null;
        }
        try {
            return Numbers.parseInt(token, 1, token.length()) < 1;
        } catch (NumericException e) {
            return true;
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

    public static void validateLttbGapOrThrow(ExpressionNode gapNode) throws SqlException {
        final CharSequence gapStr = gapNode.token;
        if (gapNode.type != ExpressionNode.CONSTANT || !Chars.isQuoted(gapStr)) {
            throw SqlException.$(gapNode.position, "gap threshold must be a string constant such as '1h'");
        }
        LttbFunctionFactory.parseGapThresholdMicros(Chars.toString(gapStr, 1, gapStr.length() - 1), gapNode.position);
    }

    public static void validateNumericType(int valueType, int position) throws SqlException {
        final int valueTag = ColumnType.tagOf(valueType);
        if (valueTag != ColumnType.DOUBLE && valueTag != ColumnType.FLOAT
                && valueTag != ColumnType.INT && valueTag != ColumnType.LONG
                && valueTag != ColumnType.SHORT && valueTag != ColumnType.BYTE) {
            throw SqlException.$(position, "numeric column expected, got: ").put(ColumnType.nameOf(valueType));
        }
    }

    /**
     * Borrows the function; runtime values are validated by the window function at each execution.
     */
    public static void validatePositionTargetOrThrow(Function function, int position, boolean isCadence) throws SqlException {
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

    /**
     * Borrows the function and retains no executable state.
     */
    public static void validateSdtCompdev(Function function, int position) throws SqlException {
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
}
