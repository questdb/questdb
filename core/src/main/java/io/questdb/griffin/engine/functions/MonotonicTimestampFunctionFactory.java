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


package io.questdb.griffin.engine.functions;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.std.Chars;
import io.questdb.std.Interval;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.StringSink;

/**
 * Declared by a factory whose {@code newInstance} can build a {@link MonotonicTimestampFunction}: answers, from a
 * bound call alone, what the function it would build for the call's arguments answers. A constant argument is a
 * {@link ConstantExpression}; any other argument is not constant, as for the built function.
 */
public interface MonotonicTimestampFunctionFactory {

    /**
     * The index of the argument the built function reads its timestamp from, or -1 when the built function is not
     * a {@link MonotonicTimestampFunction}.
     */
    int getTimestampArgumentIndex(FunctionExpression call, ConstantArguments arguments) throws SqlException;

    /**
     * The {@link MonotonicTimestampFunction#invertTimestampInterval} of the built function.
     *
     * @param isTimestampArgMonotonic whether the built function's timestamp argument is itself monotonic
     */
    int invertTimestampInterval(FunctionExpression call, Interval io, boolean isTimestampArgMonotonic, ConstantArguments arguments) throws SqlException;

    /**
     * Whether the built function is its timestamp argument itself rather than a function over it.
     */
    default boolean isIdentity(FunctionExpression call, ConstantArguments arguments) throws SqlException {
        return false;
    }

    /**
     * Reads a constant argument through the constant function the generator builds for it, so the read converts the
     * value as the built function does. The binder folds every constant argument to a {@link ConstantExpression}.
     */
    final class ConstantArguments {
        private static final int MAX_CACHED_CONSTANTS = 64;
        private final ObjList<Function> functions = new ObjList<>();
        private final ObjectPool<ConstantExpression> values = new ObjectPool<>(ConstantExpression.FACTORY, 4);
        private final StringSink zone = new StringSink();
        private TimeZoneRules zoneRules;
        private int zoneTimestampType = ColumnType.UNDEFINED;

        /**
         * The constant function of the argument, kept for later reads of the same value.
         */
        public Function constant(BoundExpression argument) {
            if (!(argument instanceof ConstantExpression constant)) {
                throw new IllegalStateException("constant argument expected");
            }
            for (int i = 0, n = functions.size(); i < n; i++) {
                if (values.peekQuick(i).isSameValue(constant)) {
                    return functions.getQuick(i);
                }
            }
            if (functions.size() == MAX_CACHED_CONSTANTS) {
                values.clear();
                functions.clear();
            }
            final Function function = FunctionInstantiator.constantFunction(constant);
            values.next().of(constant, null);
            functions.add(function);
            return function;
        }

        /**
         * The {@link TimestampDriver#getTimezoneRules} of the zone, kept for the next read of the same zone.
         */
        public TimeZoneRules getTimezoneRules(TimestampDriver driver, CharSequence timezone) {
            if (zoneTimestampType != driver.getTimestampType() || !Chars.equals(zone, timezone)) {
                zoneRules = driver.getTimezoneRules(DateLocaleFactory.EN_LOCALE, timezone);
                zoneTimestampType = driver.getTimestampType();
                zone.clear();
                zone.put(timezone);
            }
            return zoneRules;
        }
    }
}
