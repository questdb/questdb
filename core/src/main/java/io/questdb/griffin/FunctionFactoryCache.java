/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.engine.functions.NegatingFunctionFactory;
import io.questdb.griffin.engine.functions.SwappingArgsFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;

public class FunctionFactoryCache {

    static final IntHashSet invalidFunctionNameChars = new IntHashSet();
    static final CharSequenceHashSet invalidFunctionNames = new CharSequenceHashSet();
    private static final Log LOG = LogFactory.getLog(FunctionFactoryCache.class);
    private final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceHashSet groupByFunctionNames = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceHashSet cursorFunctionNames = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceHashSet runtimeConstantFunctionNames = new LowerCaseCharSequenceHashSet();

    public FunctionFactoryCache(CairoConfiguration configuration, Iterable<FunctionFactory> functionFactories) {
        boolean enableTestFactories = configuration.enableTestFactories();
        LOG.info().$("loading functions [test=").$(enableTestFactories).$(']').$();
        for (FunctionFactory factory : functionFactories) {
            if (!factory.getClass().getName().contains("test") || enableTestFactories) {
                try {
                    final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(factory);
                    final String name = descriptor.getName();
                    addFactoryToList(factories, descriptor);

                    // Add != counterparts to equality function factories
                    if (factory.isBoolean()) {
                        switch (name) {
                            case "=":
                                addFactoryToList(factories, createNegatingFactory("!=", factory));
                                addFactoryToList(factories, createNegatingFactory("<>", factory));
                                if (descriptor.getArgTypeMask(0) != descriptor.getArgTypeMask(1)) {
                                    FunctionFactory swappingFactory = createSwappingFactory("=", factory);
                                    addFactoryToList(factories, swappingFactory);
                                    addFactoryToList(factories, createNegatingFactory("!=", swappingFactory));
                                    addFactoryToList(factories, createNegatingFactory("<>", swappingFactory));
                                }
                                break;
                            case "<":
                                // `a < b` == `a >= b`
                                addFactoryToList(factories, createNegatingFactory(">=", factory));
                                FunctionFactory greaterThan = createSwappingFactory(">", factory);
                                // `a < b` == `b > a`
                                addFactoryToList(factories, greaterThan);
                                // `b > a` == !(`b <= a`)
                                addFactoryToList(factories, createNegatingFactory("<=", greaterThan));
                                break;
                        }
                    } else if (factory.isGroupBy()) {
                        groupByFunctionNames.add(name);
                    } else if (factory.isCursor()) {
                        cursorFunctionNames.add(name);
                    } else if (factory.isRuntimeConstant()) {
                        runtimeConstantFunctionNames.add(name);
                    }
                } catch (SqlException e) {
                    LOG.error().$((Sinkable) e).$(" [signature=").$(factory.getSignature()).$(",class=").$(factory.getClass().getName()).$(']').$();
                }
            }
        }
    }

    private FunctionFactory createNegatingFactory(String name, FunctionFactory factory) throws SqlException {
        return new NegatingFunctionFactory(name, factory);
    }

    private FunctionFactory createSwappingFactory(String name, FunctionFactory factory) throws SqlException {
        return new SwappingArgsFunctionFactory(name, factory);
    }

    public ObjList<FunctionFactoryDescriptor> getOverloadList(CharSequence token) {
        return factories.get(token);
    }

    public boolean isCursor(CharSequence name) {
        return cursorFunctionNames.contains(name);
    }

    public boolean isGroupBy(CharSequence name) {
        return groupByFunctionNames.contains(name);
    }

    public boolean isRuntimeConstant(CharSequence name) {
        return runtimeConstantFunctionNames.contains(name);
    }

    private void addFactoryToList(LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> list, FunctionFactory factory) throws SqlException {
        addFactoryToList(list, new FunctionFactoryDescriptor(factory));
    }

    private void addFactoryToList(LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> list, FunctionFactoryDescriptor descriptor) {
        String name = descriptor.getName();
        int index = list.keyIndex(name);
        ObjList<FunctionFactoryDescriptor> overload;
        if (index < 0) {
            overload = list.valueAtQuick(index);
        } else {
            overload = new ObjList<>(4);
            list.putAt(index, name, overload);
        }
        overload.add(descriptor);
    }

    int getFunctionCount() {
        return factories.size();
    }
}
