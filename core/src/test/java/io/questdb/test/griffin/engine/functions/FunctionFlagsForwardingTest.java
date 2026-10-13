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


package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The planner asks {@link Function#hasPerCallState()}, {@link Function#isRandom()} and
 * {@link Function#isNonDeterministic()} of the outermost function of an expression only, and
 * decides by them whether the workers may each call a copy of it (SqlCodeGenerator.isCallOrderFree()
 * and isRowPure()). A function that holds other functions and inherits Function's defaults hides
 * an argument's properties: json_extract('{"a":' || timestamp_sequence(0, 1) || '}', '$.a') would
 * be copied per worker, and each copy would count from its own start. So every concrete function
 * class under io.questdb that holds a Function, an array of them or a collection of them must
 * forward the three, through UnaryFunction and its siblings or by its own override, or be in
 * {@link #ALLOWED} with the reason forwarding would be wrong.
 */
public class FunctionFlagsForwardingTest {
    private static final Map<String, String> ALLOWED = new HashMap<>();

    static {
        // A scalar sub-query is run once per execution: the function's value is one per
        // execution, whatever the sub-query's projection calls, so its flags are its own.
        ALLOWED.put("io.questdb.griffin.engine.functions.ScalarSubQueryTimestampFunction", "one value per execution");
        ALLOWED.put("io.questdb.griffin.engine.functions.bind.CompiledFilterScalarSubQuery$DoubleLink", "one value per execution");
        ALLOWED.put("io.questdb.griffin.engine.functions.bind.CompiledFilterScalarSubQuery$IntLink", "one value per execution");
        ALLOWED.put("io.questdb.griffin.engine.functions.bind.CompiledFilterScalarSubQuery$LongLink", "one value per execution");
    }
    private static final String[] METHODS = {"hasPerCallState", "isRandom", "isNonDeterministic"};

    @Test
    public void testFunctionsHoldingFunctionsForwardTheirFlags() throws Exception {
        final Path root = Paths.get(Function.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        final List<String> missing = new ArrayList<>();
        int checked = 0;
        try (Stream<Path> files = Files.walk(root.resolve("io/questdb"))) {
            for (Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".class"))::iterator) {
                final String name = root.relativize(file).toString().replace('/', '.').replaceAll("\\.class$", "");
                final Class<?> clazz;
                try {
                    clazz = Class.forName(name, false, FunctionFlagsForwardingTest.class.getClassLoader());
                } catch (Throwable e) {
                    continue;
                }
                if (!Function.class.isAssignableFrom(clazz) || clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
                    continue;
                }
                final List<String> held = heldFunctionFields(clazz);
                if (held.isEmpty()) {
                    continue;
                }
                checked++;
                if (ALLOWED.containsKey(name)) {
                    continue;
                }
                for (String m : METHODS) {
                    final Method method = clazz.getMethod(m);
                    if (method.getDeclaringClass() == Function.class) {
                        missing.add(name + "." + m + "() inherits Function's default, holds " + held);
                    }
                }
            }
        }
        Assert.assertTrue("scanned only " + checked + " classes from " + root, checked > 500);
        if (!missing.isEmpty()) {
            Assert.fail(missing.size() + " missing forwards:\n" + String.join("\n", missing));
        }
    }

    private static List<String> heldFunctionFields(Class<?> clazz) {
        final List<String> held = new ArrayList<>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers()) && !field.isSynthetic() && holdsFunction(field.getGenericType())) {
                    held.add(field.getName());
                }
            }
        }
        return held;
    }

    private static boolean holdsFunction(Type type) {
        if (type instanceof Class<?> c) {
            return Function.class.isAssignableFrom(c) || (c.isArray() && holdsFunction(c.getComponentType()));
        }
        if (type instanceof GenericArrayType g) {
            return holdsFunction(g.getGenericComponentType());
        }
        if (type instanceof ParameterizedType p) {
            for (Type arg : p.getActualTypeArguments()) {
                if (holdsFunction(arg)) {
                    return true;
                }
            }
            return holdsFunction(p.getRawType());
        }
        if (type instanceof java.lang.reflect.WildcardType w) {
            for (Type bound : w.getUpperBounds()) {
                if (holdsFunction(bound)) {
                    return true;
                }
            }
        }
        return false;
    }
}
