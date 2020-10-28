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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;

public class FunctionFactoryCache {

    static final IntHashSet invalidFunctionNameChars = new IntHashSet();
    static final CharSequenceHashSet invalidFunctionNames = new CharSequenceHashSet();
    private static final Log LOG = LogFactory.getLog(FunctionFactoryCache.class);
    private final CharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> booleanFactories = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> commutativeBooleanFactories = new CharSequenceObjHashMap<>();
    private final CharSequenceHashSet groupByFunctionNames = new CharSequenceHashSet();

    public FunctionFactoryCache(CairoConfiguration configuration, Iterable<FunctionFactory> functionFactories) {
        boolean enableTestFactories = configuration.enableTestFactories();
        LOG.info().$("loading functions [test=").$(enableTestFactories).$(']').$();
        for (FunctionFactory factory : functionFactories) {
            if (!factory.getClass().getName().contains("test") || enableTestFactories) {
                try {
                    final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(factory);
                    final String name = descriptor.getName();
                    addFactoryToList(factories, name, descriptor);

                    // Add != counterparts to equality function factories
                    if (factory instanceof AbstractBooleanFunctionFactory) {
                        switch (name) {
                            case "=":
                                addFactory(booleanFactories, "!=", descriptor);
                                break;
                            case "<":
                                // `a < b` == `a >= b`
                                addFactory(booleanFactories, ">=", descriptor);
                                if (descriptor.getArgTypeMask(0) == descriptor.getArgTypeMask(1)) {
                                    // `a < b` == `b > a`
                                    addFactory(commutativeBooleanFactories, ">", descriptor);
                                    // `a < b` == `b > a` == `b <= a`
                                    addFactory(booleanFactories, "<=", descriptor);
                                    addFactory(commutativeBooleanFactories, "<=", descriptor);
                                }
                                break;
                        }
                    } else if (factory.isGroupBy()) {
                        groupByFunctionNames.add(name);
                    }
                } catch (SqlException e) {
                    LOG.error().$((Sinkable) e).$(" [signature=").$(factory.getSignature()).$(",class=").$(factory.getClass().getName()).$(']').$();
                }
            }
        }
    }

    public ObjList<FunctionFactoryDescriptor> getOverloadList(CharSequence token) {
        return factories.get(token);
    }

    public boolean isFlipped(CharSequence token) {
        return commutativeBooleanFactories.get(token) != null;
    }

    public boolean isGroupBy(CharSequence name) {
        return groupByFunctionNames.contains(name);
    }

    public boolean isNegated(CharSequence token) {
        return booleanFactories.get(token) != null;
    }

    // Add a descriptor to `factories` and optionally `extraList`
    private void addFactory(CharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> extraList, String name, FunctionFactoryDescriptor descriptor) {
        addFactoryToList(factories, name, descriptor);
        addFactoryToList(extraList, name, descriptor);
    }

    private void addFactoryToList(CharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> list, String name, FunctionFactoryDescriptor descriptor) {
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
