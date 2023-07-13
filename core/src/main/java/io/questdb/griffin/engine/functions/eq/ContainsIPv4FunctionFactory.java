/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import static io.questdb.std.Numbers.*;

public class ContainsIPv4FunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "<<(Xs)";
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
        return createHalfConstantFunc(args.getQuick(0), args.getQuick(1));
    }

    private Function createHalfConstantFunc(Function varFunc, Function constFunc) throws SqlException {
        CharSequence constValue = constFunc.getStr(null);

        if(constValue == null) {
            return new NullCheckFunc(varFunc);
        }

        int subnet = getIPv4Subnet(constValue);
        int netmask = getIPv4Netmask(constValue);

        if(subnet == -2 && netmask == -2) { //catches negative netmask
            throw SqlException.$(18, "invalid argument: ").put(constValue);
        }
        else if(subnet == -2) { // If true, the argument is either invalid OR is a subnet instead of an ip address (-2 used as sentinel because 0xffffffff (which is valid) is -1)

            subnet = parseSubnet(constValue); // Check is arg is subnet

            if(subnet == -2) { // Is true if arg is not a valid subnet/ip address
                throw SqlException.$(18, "invalid argument: ").put(constValue);
            } else {
                return new ConstCheckFunc(varFunc, subnet, netmask);
            }
        }

        return new ConstCheckFunc(varFunc, subnet, netmask);
    }

    private static class ConstCheckFunc extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        private final int subnet;
        private final int netmask;

        public ConstCheckFunc(Function arg, int subnet, int netmask) {
            this.arg = arg;
            this.subnet = subnet;
            this.netmask = netmask;
        }

        @Override
        public Function getArg() { return arg; }

        @Override
        public boolean getBool(Record rec) {
            if(netmask == 32 || netmask == -1) { //if netmask is 32 then IP can't be strictly contained because arg is a single host, if netmask is -1 that means no netmask was provided and the arg is a single host
                return false;
            }
            return (arg.getInt(rec) & netmask) == subnet;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            sink.val("<<").val(subnet).val('\'');
        }
    }

    public static class NullCheckFunc extends BooleanFunction implements UnaryFunction {
        private final Function arg;
        public NullCheckFunc(Function arg) { this.arg = arg; }

        @Override
        public Function getArg() { return arg; }

        @Override
        public boolean getBool(Record rec) { return arg.getInt(rec) == IPv4_NULL; }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(arg);
            sink.val( "is null");
        }
    }

}

