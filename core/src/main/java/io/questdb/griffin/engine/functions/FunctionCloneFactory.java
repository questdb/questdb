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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.bind.IndexedParameterLinkFunction;
import io.questdb.griffin.engine.functions.bind.NamedParameterLinkFunction;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.std.DeepCloneable;
import io.questdb.std.DelayInitialize;
import io.questdb.std.ObjList;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class FunctionCloneFactory {

    static Map<Class<?>, Object> DEFAULT_CLAZZ_VALUES = Stream
            .of(boolean.class, byte.class, char.class, double.class, float.class, int.class, long.class, short.class)
            .collect(toMap(clazz -> clazz, clazz -> Array.get(Array.newInstance(clazz, 1), 0)));
    static Map<Class<?>, FieldWrapper[]> FUNCTION_FIELDS_CACHE = new ConcurrentHashMap<>(1024);

    static {
        DEFAULT_CLAZZ_VALUES.put(CharSequence.class, "{}");
        DEFAULT_CLAZZ_VALUES.put(double[].class, new double[]{});
        DEFAULT_CLAZZ_VALUES.put(ObjList.class, new ObjList<>());
        DEFAULT_CLAZZ_VALUES.put(SymbolFunction.class, new SymbolColumn(0, false));
        DEFAULT_CLAZZ_VALUES.put(Function.class, new SymbolColumn(0, false));
    }

    /**
     * DeepClone does not clone the running-states, only the init states of the function,
     * must be called before Function::init.
     * Used for parallel filter/groupBy execution, so that avoid Function::parser for every worker.
     * Note: CursorFunction and WindowFunction are not supported now.
     *
     * @return deep clones of the function
     */
    public static Function deepCloneFunction(Function function) {
        if (!function.supportDeepClone()) {
            throw new UnsupportedOperationException();
        }

        Function cloneFunc = null;
        try {
            Class<?> cls = function.getClass();
            Constructor<?> funcCons = cls.getDeclaredConstructors()[0];
            funcCons.setAccessible(true);
            int parameterCount = funcCons.getParameterCount();
            Object[] pArgs = new Object[parameterCount];
            Class<?>[] pTypes = funcCons.getParameterTypes();
            for (int i = 0; i < parameterCount; i++) {
                pArgs[i] = DEFAULT_CLAZZ_VALUES.get(pTypes[i]);
            }
            DEFAULT_CLAZZ_VALUES.get(cls);
            cloneFunc = (Function) funcCons.newInstance(pArgs);

            while (cls != null) {
                FieldWrapper[] fields = FUNCTION_FIELDS_CACHE.get(cls);
                if (fields == null) {
                    Field[] fs = cls.getDeclaredFields();
                    fields = new FieldWrapper[fs.length];
                    for (int i = 0, size = fs.length; i < size; i++) {
                        fs[i].setAccessible(true);
                        fields[i] = new FieldWrapper(fs[i], hasDelayInitialize(fs[i]), Modifier.isStatic(fs[i].getModifiers()));
                    }
                    FUNCTION_FIELDS_CACHE.put(cls, fields);
                }
                for (FieldWrapper fw : fields) {
                    if (fw.isStatic) {
                        continue;
                    }

                    Field field = fw.field;
                    Object fValue = field.get(function);
                    if (fValue == null) {
                        continue;
                    }

                    if (fw.hasDelayInitialize) {
                        field.set(cloneFunc, null);
                    } else if (fValue instanceof Function) {
                        if (fValue instanceof IndexedParameterLinkFunction || fValue instanceof NamedParameterLinkFunction) {
                            field.set(cloneFunc, fValue);
                        } else {
                            field.set(cloneFunc, deepCloneFunction((Function) fValue));
                        }
                    } else if (fValue instanceof DeepCloneable<?>) {
                        field.set(cloneFunc, ((DeepCloneable<?>) fValue).deepClone());
                    } else if (fValue instanceof ObjList) {
                        field.set(cloneFunc, cloneObjList((ObjList<?>) fValue));
                    } else if (field.getType().isPrimitive() || field.get(cloneFunc) == null || field.getType() == CharSequence.class
                            || field.getType() == String.class) {
                        field.set(cloneFunc, fValue);
                    } // else just keep as it is.
                }
                cls = cls.getSuperclass();
            }
            return cloneFunc;
        } catch (Throwable e) {
            if (cloneFunc != null) {
                cloneFunc.close();
            }
            throw new UnsupportedOperationException(e);
        }
    }

    private static class FieldWrapper {
        private final Field field;
        private final boolean hasDelayInitialize;
        private final boolean isStatic;

        private FieldWrapper(Field field, boolean hasDelayInitialize, boolean isStatic) {
            this.field = field;
            this.hasDelayInitialize = hasDelayInitialize;
            this.isStatic = isStatic;
        }
    }

    private static ObjList<?> cloneObjList(ObjList<?> fValue) {
        if (fValue.size() == 0) {
            return new ObjList<>();
        }
        ObjList nList = new ObjList<>(fValue.size());
        for (int i = 0, size = fValue.size(); i < size; i++) {
            nList.add(cloneElem(fValue.getQuick(i)));
        }
        return nList;
    }

    private static Object cloneElem(Object fValue) {
        if (fValue instanceof Function) {
            if (fValue instanceof IndexedParameterLinkFunction || fValue instanceof NamedParameterLinkFunction) {
                return fValue;
            } else {
                return deepCloneFunction((Function) fValue);
            }
        } else if (fValue instanceof DeepCloneable<?>) {
            return ((DeepCloneable<?>) fValue).deepClone();
        } else {
            return fValue;
        }
    }

    private static boolean hasDelayInitialize(Field f) {
        DelayInitialize annotation = f.getAnnotation(DelayInitialize.class);
        return annotation != null;
    }
}
