/*******************************************************************************
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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.*;

import java.lang.ThreadLocal;

public class EqLong256StrFunctionFactory implements FunctionFactory {
    private static final ThreadLocal<Long256ConstDecoder> DECODER = ThreadLocal.withInitial(Long256ConstDecoder::new);

    @Override
    public String getSignature() {
        return "=(HS)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function long256Func = args.getQuick(0);
        int strFuncPosition = argPositions.getQuick(1);
        Function strFunc = args.getQuick(1);
        if (strFunc.isConstant()) {
            CharSequence value = strFunc.getStrA(null);
            if (value == null) {
                return new ConstStrFunc(long256Func);
            }
            Long256ConstDecoder decoder = DECODER.get();
            decoder.decode(value);
            return new ConstStrFunc(long256Func, decoder.long0, decoder.long1, decoder.long2, decoder.long3);
        } else if (strFunc.isRuntimeConstant()) {
            return new RuntimeConstStrFunc(long256Func, strFunc);
        }
        throw SqlException.$(strFuncPosition, "STRING constant expected");
    }

    private static class ConstStrFunc extends NegatableBooleanFunction implements UnaryFunction {
        private final long constLong0;
        private final long constLong1;
        private final long constLong2;
        private final long constLong3;
        private final Function strFunc;

        public ConstStrFunc(Function strFunc) {
            this(
                    strFunc,
                    Long256Impl.NULL_LONG256.getLong0(),
                    Long256Impl.NULL_LONG256.getLong1(),
                    Long256Impl.NULL_LONG256.getLong2(),
                    Long256Impl.NULL_LONG256.getLong3()
            );
        }

        public ConstStrFunc(Function strFunc, long constLong0, long constLong1, long constLong2, long constLong3) {
            this.constLong0 = constLong0;
            this.constLong1 = constLong1;
            this.constLong2 = constLong2;
            this.constLong3 = constLong3;
            this.strFunc = strFunc;
        }

        @Override
        public Function getArg() {
            return strFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            final Long256 value = strFunc.getLong256A(rec);
            return negated != (value.getLong0() == constLong0 && value.getLong1() == constLong1 && value.getLong2() == constLong2 && value.getLong3() == constLong3);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(strFunc);
            if (negated) {
                sink.val('!');
            }
            sink.val('=');
            sink.valLong256(constLong0, constLong1, constLong2, constLong3);
        }
    }

    private static class Long256ConstDecoder {
        long long0;
        long long1;
        long long2;
        long long3;
        final Long256Acceptor decoder = this::setAll;

        public void setAll(long l0, long l1, long l2, long l3) {
            long0 = l0;
            long1 = l1;
            long2 = l2;
            long3 = l3;
        }

        private void decode(CharSequence hexLong256) {
            Long256FromCharSequenceDecoder.decode(hexLong256, 2, hexLong256.length(), decoder);
        }
    }

    private static class RuntimeConstStrFunc extends NegatableBooleanFunction implements BinaryFunction {
        private final Function long256Func;
        private final Function strFunc;
        private long constLong0;
        private long constLong1;
        private long constLong2;
        private long constLong3;
        final Long256Acceptor decoder = this::setAll;

        public RuntimeConstStrFunc(Function long256Func, Function strFunc) {
            this.long256Func = long256Func;
            this.strFunc = strFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            final Long256 value = long256Func.getLong256A(rec);
            return negated != (value.getLong0() == constLong0 && value.getLong1() == constLong1 && value.getLong2() == constLong2 && value.getLong3() == constLong3);
        }

        @Override
        public Function getLeft() {
            return long256Func;
        }

        @Override
        public Function getRight() {
            return strFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            CharSequence value = strFunc.getStrA(null);
            if (value == null) {
                constLong0 = Long256Impl.NULL_LONG256.getLong0();
                constLong1 = Long256Impl.NULL_LONG256.getLong1();
                constLong2 = Long256Impl.NULL_LONG256.getLong2();
                constLong3 = Long256Impl.NULL_LONG256.getLong3();
            } else {
                Long256FromCharSequenceDecoder.decode(value, 2, value.length(), decoder);
            }
        }

        public void setAll(long l0, long l1, long l2, long l3) {
            constLong0 = l0;
            constLong1 = l1;
            constLong2 = l2;
            constLong3 = l3;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(long256Func);
            if (negated) {
                sink.val('!');
            }
            sink.val('=');
            sink.val(strFunc);
        }
    }
}
