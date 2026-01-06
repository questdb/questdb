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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.std.Numbers.IPv4_NULL;
import static io.questdb.std.Numbers.getIPv4Subnet;

public class ContainsIPv4Utils {

    private ContainsIPv4Utils() {
    }

    public static Function containsIPv4Str(Function ipv4Func, Function strFunc, int strFuncPosition) throws SqlException {
        if (strFunc.isConstant()) {
            final CharSequence constValue = strFunc.getStrA(null);
            if (constValue == null) {
                return BooleanConstant.FALSE;
            }

            try {
                long subnetAndNetmask = getIPv4Subnet(constValue);
                int subnet = (int) (subnetAndNetmask >> 32);
                int netmask = (int) subnetAndNetmask;
                return new ConstStrFunc(ipv4Func, subnet & netmask, netmask);
            } catch (NumericException ne) {
                throw SqlException.$(strFuncPosition, "invalid argument: ").put(constValue);
            }
        } else if (strFunc.isRuntimeConstant()) {
            return new RuntimeConstStrFunc(ipv4Func, strFunc, strFuncPosition);
        }
        return new StrFunc(ipv4Func, strFunc);
    }

    public static Function containsIPv4Varchar(Function ipv4Func, Function varcharFunc, int varcharFuncPosition) throws SqlException {
        if (varcharFunc.isConstant()) {
            final CharSequence constValue = varcharFunc.getStrA(null);
            if (constValue == null) {
                return BooleanConstant.FALSE;
            }

            try {
                long subnetAndNetmask = getIPv4Subnet(constValue);
                int subnet = (int) (subnetAndNetmask >> 32);
                int netmask = (int) subnetAndNetmask;
                return new ConstStrFunc(ipv4Func, subnet & netmask, netmask);
            } catch (NumericException ne) {
                throw SqlException.$(varcharFuncPosition, "invalid argument: ").put(constValue);
            }
        } else if (varcharFunc.isRuntimeConstant()) {
            return new RuntimeConstStrFunc(ipv4Func, varcharFunc, varcharFuncPosition);
        }
        return new VarcharFunc(ipv4Func, varcharFunc);
    }

    private static class ConstStrFunc extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final int netmask;
        private final int subnet;

        public ConstStrFunc(Function arg, int subnet, int netmask) {
            this.arg = arg;
            this.subnet = subnet;
            this.netmask = netmask;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public boolean getBool(Record rec) {
            // if netmask = 32 then IP can't be strictly contained, if netmask = -1 that means arg is a single host - both are invalid
            if (netmask == 32 || netmask == -1) {
                return false;
            }
            return (arg.getIPv4(rec) & netmask) == subnet;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            sink.val("<<").val(subnet).val('\'');
        }
    }

    private static class RuntimeConstStrFunc extends BooleanFunction implements BinaryFunction {
        private final Function ipv4Func;
        private final Function strFunc;
        private final int strFuncPosition;
        private int netmask;
        private int subnet;

        public RuntimeConstStrFunc(Function ipv4Func, Function strFunc, int strFuncPosition) {
            this.ipv4Func = ipv4Func;
            this.strFunc = strFunc;
            this.strFuncPosition = strFuncPosition;
        }

        @Override
        public boolean getBool(Record rec) {
            // if netmask = 32 then IP can't be strictly contained, if netmask = -1 that means arg is a single host - both are invalid
            if (netmask == 32 || netmask == -1) {
                return false;
            }
            return (ipv4Func.getIPv4(rec) & netmask) == subnet;
        }

        @Override
        public Function getLeft() {
            return ipv4Func;
        }

        @Override
        public Function getRight() {
            return strFunc;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence value = strFunc.getStrA(null);
            if (value == null) {
                // act as a null check
                subnet = IPv4_NULL;
                netmask = -1;
            } else {
                try {
                    long subnetAndNetmask = getIPv4Subnet(value);
                    subnet = (int) (subnetAndNetmask >> 32);
                    netmask = (int) subnetAndNetmask;
                    subnet = subnet & netmask;
                } catch (NumericException ne) {
                    throw SqlException.$(strFuncPosition, "invalid argument: ").put(value);
                }
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(ipv4Func);
            sink.val("<<").val(strFunc).val('\'');
        }
    }

    private static class StrFunc extends BooleanFunction implements BinaryFunction {
        private final Function ipv4Func;
        private final Function strFunc;

        public StrFunc(Function ipv4Func, Function strFunc) {
            this.ipv4Func = ipv4Func;
            this.strFunc = strFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            final CharSequence value = strFunc.getStrA(rec);
            if (value == null) {
                return false;
            }

            int subnet;
            int netmask;
            try {
                long subnetAndNetmask = getIPv4Subnet(value);
                subnet = (int) (subnetAndNetmask >> 32);
                netmask = (int) subnetAndNetmask;
                subnet = subnet & netmask;
            } catch (NumericException ne) {
                return false;
            }

            // if netmask = 32 then IP can't be strictly contained, if netmask = -1 that means arg is a single host - both are invalid
            if (netmask == 32 || netmask == -1) {
                return false;
            }
            return (ipv4Func.getIPv4(rec) & netmask) == subnet;
        }

        @Override
        public Function getLeft() {
            return ipv4Func;
        }

        @Override
        public Function getRight() {
            return strFunc;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(ipv4Func);
            sink.val("<<").val(strFunc).val('\'');
        }
    }

    private static class VarcharFunc extends BooleanFunction implements BinaryFunction {
        private final Function ipv4Func;
        private final Function varcharFunc;

        public VarcharFunc(Function ipv4Func, Function varcharFunc) {
            this.ipv4Func = ipv4Func;
            this.varcharFunc = varcharFunc;
        }

        @Override
        public boolean getBool(Record rec) {
            final Utf8Sequence value = varcharFunc.getVarcharA(rec);
            if (value == null) {
                return false;
            }

            int subnet;
            int netmask;
            try {
                long subnetAndNetmask = getIPv4Subnet(value.asAsciiCharSequence());
                subnet = (int) (subnetAndNetmask >> 32);
                netmask = (int) subnetAndNetmask;
                subnet = subnet & netmask;
            } catch (NumericException ne) {
                return false;
            }

            // if netmask = 32 then IP can't be strictly contained, if netmask = -1 that means arg is a single host - both are invalid
            if (netmask == 32 || netmask == -1) {
                return false;
            }
            return (ipv4Func.getIPv4(rec) & netmask) == subnet;
        }

        @Override
        public Function getLeft() {
            return ipv4Func;
        }

        @Override
        public Function getRight() {
            return varcharFunc;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(ipv4Func);
            sink.val("<<").val(varcharFunc).val('\'');
        }
    }
}
