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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.griffin.FunctionFactoryDescriptor.replaceSignatureNameAndSwapArgs;

public class ArgSwappingFunctionFactory implements FunctionFactory {
    private final FunctionFactory delegate;
    private final String signature;

    public ArgSwappingFunctionFactory(String name, FunctionFactory delegate) throws SqlException {
        this.signature = replaceSignatureNameAndSwapArgs(name, delegate.getSignature());
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
        Function tmpArg = args.getQuick(0);
        args.setQuick(0, args.getQuick(1));
        args.setQuick(1, tmpArg);
        int tmpPosition = argPositions.getQuick(0);
        argPositions.setQuick(0, argPositions.getQuick(1));
        argPositions.setQuick(1, tmpPosition);
        return delegate.newInstance(position, args, argPositions, configuration, sqlExecutionContext);
    }

    @Override
    public boolean supportImplicitCastCharToStr() {
        return delegate.supportImplicitCastCharToStr();
    }
}
