/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.lang.ast.factory;

import com.nfsdb.collections.ObjObjHashMap;
import com.nfsdb.lang.ast.Signature;
import com.nfsdb.storage.ColumnType;

public final class FunctionFactories {
    private static final ObjObjHashMap<Signature, FunctionFactory> factories = new ObjObjHashMap<>();

    public static FunctionFactory find(Signature sig) {
        return factories.get(sig);
    }

    static {
        factories.put(new Signature().setName("+").setParamCount(2).addParamType(ColumnType.DOUBLE).addParamType(ColumnType.DOUBLE), new AddDoubleOperatorFactory());
    }
}
