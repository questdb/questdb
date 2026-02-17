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

package io.questdb.griffin.engine.functions;

import org.jetbrains.annotations.TestOnly;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;

import static io.questdb.griffin.FunctionFactoryDescriptor.replaceSignatureName;

import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class NegatingFunctionFactory implements FunctionFactory {
    private final FunctionFactory delegate;
    private final String signature;

    public NegatingFunctionFactory(String name, FunctionFactory delegate) throws SqlException {
        this.signature = replaceSignatureName(name, delegate.getSignature());
        this.delegate = delegate;
    }

    @TestOnly
    public FunctionFactory getDelegate() {
        return delegate;
    }

    @Override
    public String getSignature() {
        return signature;
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function function = delegate.newInstance(position, args, argPositions, configuration, sqlExecutionContext);
        if (function instanceof NegatableBooleanFunction) {
            NegatableBooleanFunction negatableFunction = (NegatableBooleanFunction) function;
            negatableFunction.setNegated();
            return negatableFunction;
        }
        if (function instanceof BooleanConstant) {
            return BooleanConstant.of(!function.getBool(null));
        }
        throw SqlException.$(position, "negating operation is not supported for result of function ").put(delegate.getSignature());
    }
}
