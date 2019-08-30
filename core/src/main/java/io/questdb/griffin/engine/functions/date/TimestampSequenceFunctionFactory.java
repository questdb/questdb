/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class TimestampSequenceFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "timestamp_sequence(nl)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        final long start = args.getQuick(0).getTimestamp(null);
        final long increment = args.getQuick(1).getLong(null);
        if (increment < 0) {
            throw SqlException.$(args.getQuick(1).getPosition(), "positive increment expected");
        }

        if (start == Numbers.LONG_NaN) {
            return new TimestampConstant(args.getQuick(0).getPosition(), Numbers.LONG_NaN);
        }

        return new Func(position, start, increment);
    }

    private static final class Func extends TimestampFunction {
        private final long increment;
        private final long start;
        private long next;

        public Func(int position, long start, long increment) {
            super(position);
            this.start = start;
            this.next = start;
            this.increment = increment;
        }

        @Override
        public void close() {
        }

        @Override
        public long getTimestamp(Record rec) {
            final long result = next;
            next += increment;
            return result;
        }

        @Override
        public void init(RecordCursor recordCursor, SqlExecutionContext executionContext) {
            toTop();
        }

        @Override
        public void toTop() {
            next = start;
        }
    }
}
