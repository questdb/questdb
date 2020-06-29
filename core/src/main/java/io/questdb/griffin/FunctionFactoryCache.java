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
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Sinkable;

public class FunctionFactoryCache {

    private static final Log LOG = LogFactory.getLog(FunctionFactoryCache.class);
    private final CharSequenceObjHashMap<ObjList<FunctionFactory>> factories = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<ObjList<FunctionFactory>> booleanFactories = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<ObjList<FunctionFactory>> commutativeBooleanFactories = new CharSequenceObjHashMap<>();
    private final CharSequenceHashSet groupByFunctionNames = new CharSequenceHashSet();

    public FunctionFactoryCache(CairoConfiguration configuration, Iterable<FunctionFactory> functionFactories) {
        boolean enableTestFactories = configuration.enableTestFactories();
        LOG.info().$("loading functions [test=").$(enableTestFactories).$(']').$();
        for (FunctionFactory factory : functionFactories) {
            if (!factory.getClass().getName().contains("test") || enableTestFactories) {
                final String sig = factory.getSignature();
                final int openBraceIndex;
                try {
                    openBraceIndex = FunctionParser.validateSignatureAndGetNameSeparator(sig);
                } catch (SqlException e) {
                    LOG.error().$((Sinkable) e).$(" [signature=").$(factory.getSignature()).$(",class=").$(factory.getClass().getName()).$(']').$();
                    continue;
                }

                final String name = sig.substring(0, openBraceIndex);
                addFactoryToList(factories, name, factory);

                // Add != counterparts to equality function factories
                if (factory instanceof AbstractBooleanFunctionFactory) {
                    switch (name) {
                        case "=":
                            addFactory(booleanFactories, "!=", factory);
                            break;
                        case "<":
                            // `a < b` == `a >= b`
                            addFactory(booleanFactories, ">=", factory);
                            if (sig.charAt(2) == sig.charAt(3)) {
                                // `a < b` == `b > a`
                                addFactory(commutativeBooleanFactories, ">", factory);
                                // `a < b` == `b > a` == `b <= a`
                                addFactory(booleanFactories, "<=", factory);
                                addFactory(commutativeBooleanFactories, "<=", factory);
                            }
                            break;
                    }
                } else if (factory.isGroupBy()) {
                    groupByFunctionNames.add(name);
                }
            }
        }
    }

    int getFunctionCount() {
        return factories.size();
    }

    private void addFactoryToList(CharSequenceObjHashMap<ObjList<FunctionFactory>> list, String name, FunctionFactory factory) {
        int index = list.keyIndex(name);
        ObjList<FunctionFactory> overload;
        if (index < 0) {
            overload = list.valueAtQuick(index);
        } else {
            overload = new ObjList<>(4);
            list.putAt(index, name, overload);
        }
        overload.add(factory);
    }

    // Add a factory to `factories` and optionally `extraList`
    private void addFactory(CharSequenceObjHashMap<ObjList<FunctionFactory>> extraList, String name, FunctionFactory factory) {
        addFactoryToList(factories, name, factory);
        addFactoryToList(extraList, name, factory);
    }

    public boolean isGroupBy(CharSequence name) {
        return groupByFunctionNames.contains(name);
    }

    public ObjList<FunctionFactory> getOverloadList(CharSequence token) {
        return factories.get(token);
    }

    public boolean isNegated(CharSequence token) {
        return booleanFactories.get(token) != null;
    }

    public boolean isFlipped(CharSequence token) {
        return commutativeBooleanFactories.get(token) != null;
    }
}
