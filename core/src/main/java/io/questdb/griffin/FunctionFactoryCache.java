/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicInteger;

public class FunctionFactoryCache {

    static final IntHashSet invalidFunctionNameChars = new IntHashSet();
    static final CharSequenceHashSet invalidFunctionNames = new CharSequenceHashSet();
    private static final Log LOG = LogFactory.getLog(FunctionFactoryCache.class);
    private final LowerCaseCharSequenceHashSet cursorFunctionNames = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories = new LowerCaseCharSequenceObjHashMap<>();
    private final LowerCaseCharSequenceHashSet groupByFunctionNames = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceHashSet runtimeConstantFunctionNames = new LowerCaseCharSequenceHashSet();
    private final LowerCaseCharSequenceHashSet windowFunctionNames = new LowerCaseCharSequenceHashSet();

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
                    } else if (factory.isWindow()) {
                        windowFunctionNames.add(name);
                    } else if (factory.isCursor()) {
                        cursorFunctionNames.add(name);
                    } else if (factory.isRuntimeConstant()) {
                        runtimeConstantFunctionNames.add(name);
                    }
                } catch (SqlException e) {
                    LOG.error().$((Sinkable) e)
                            .$(" [signature=").$(factory.getSignature())
                            .$(", class=").$(factory.getClass().getName())
                            .I$();
                }
            }
        }
    }

    @TestOnly
    public LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> getFactories() {
        return factories;
    }

    public int getFunctionCount() {
        return factories.size();
    }

    public ObjList<FunctionFactoryDescriptor> getOverloadList(CharSequence token) {
        return factories.get(token);
    }

    public boolean isCursor(CharSequence name) {
        return name != null && cursorFunctionNames.contains(name);
    }

    public boolean isGroupBy(CharSequence name) {
        return name != null && groupByFunctionNames.contains(name);
    }

    public boolean isRuntimeConstant(CharSequence name) {
        return name != null && runtimeConstantFunctionNames.contains(name);
    }

    public boolean isValidNoArgFunction(ExpressionNode node) {
        final ObjList<FunctionFactoryDescriptor> overload = getOverloadList(node.token);
        if (overload == null) {
            return false;
        }

        for (int i = 0, n = overload.size(); i < n; i++) {
            FunctionFactoryDescriptor ffd = overload.getQuick(i);
            if (ffd.getSigArgCount() == 0) {
                return true;
            }
        }

        return false;
    }

    public boolean isWindow(CharSequence name) {
        return name != null && windowFunctionNames.contains(name);
    }

    public String summary() {
        final StringBuilder sb = new StringBuilder();
        sb.append("FunctionFactoryCache{factories=");
        sb.append(factories.size());
        sb.append(":[");
        AtomicInteger count = new AtomicInteger(0);
        factories.forEach((name, overload) -> {
            if (count.get() > 0) {
                sb.append(", ");
            }
            sb.append(name);
            sb.append("=[");
            for (int overloadIndex = 0, overloadSize = overload.size(); overloadIndex < overloadSize; overloadIndex++) {
                if (overloadIndex > 0) {
                    sb.append(", ");
                }
                sb.append(overload.getQuick(overloadIndex));
            }
            sb.append("]");
            count.incrementAndGet();
        });
        sb.append("]");
        sb.append(", cursorFunctionNames=");
        sb.append(cursorFunctionNames.size());
        sb.append(", groupByFunctionNames=");
        sb.append(groupByFunctionNames.size());
        sb.append(", runtimeConstantFunctionNames=");
        sb.append(runtimeConstantFunctionNames.size());
        sb.append(", windowFunctionNames=");
        sb.append(windowFunctionNames.size());
        sb.append('}');
        return sb.toString();
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

    private FunctionFactory createNegatingFactory(String name, FunctionFactory factory) throws SqlException {
        return new NegatingFunctionFactory(name, factory);
    }

    private FunctionFactory createSwappingFactory(String name, FunctionFactory factory) throws SqlException {
        return new SwappingArgsFunctionFactory(name, factory);
    }
}
