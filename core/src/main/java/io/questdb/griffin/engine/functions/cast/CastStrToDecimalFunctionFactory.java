/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.DecimalUtil;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.AbstractDecimalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.Constants;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

import static io.questdb.cairo.ColumnType.GEOLONG_MAX_BITS;

public class CastStrToDecimalFunctionFactory implements FunctionFactory {
    /**
     * Create a new instance of a Function that can cast a string to a decimal.
     * @param decimal is a mutable instance that can be used to parse constant instances.
     * @param position of the value in the query string
     * @param targetType of the final decimal
     * @param value that will be used to retrieve the str
     * @return an instance of a function that will handle the parsing
     * @throws SqlException if constant folding failed
     */
    public static Function newInstance(Decimal256 decimal, int position, int targetType, Function value) throws SqlException {
        if (value.isConstant()) {
            return newConstantInstance(decimal, position, targetType, value);
        }
        return new Func(value, targetType, position);
    }

    private static ConstantFunction newConstantInstance(Decimal256 decimal, int position, int targetType, Function value) throws SqlException {
        final int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        final int targetScale = ColumnType.getDecimalScale(targetType);

        CharSequence cs = value.getStrA(null);
        if (cs == null) {
            return DecimalUtil.createNullDecimalConstant(targetPrecision, targetScale);
        }

        try {
            decimal.ofString(cs, targetPrecision, targetScale);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(cs, ColumnType.STRING, targetType).position(position);
        }
        return DecimalUtil.createDecimalConstant(decimal, targetPrecision, targetScale);
    }

    @Override
    public String getSignature() {
        return "cast(Sξ)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function value = args.getQuick(0);
        int argPosition = argPositions.getQuick(0);
        int targetType = args.getQuick(1).getType();
        return newInstance(sqlExecutionContext.getDecimal256(), argPosition, targetType, value);
    }

    private static class Func extends AbstractCastToDecimalFunction {
        public Func(Function value, int targetType, int position) {
            super(value, targetType, position);
        }

        protected boolean cast(Record rec) {
            CharSequence cs = this.arg.getStrA(rec);
            if (cs == null) {
                return false;
            }
            try {
                decimal.ofString(cs, precision, scale);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(cs, ColumnType.STRING, type).position(position);
            }
            return true;
        }
    }
}
