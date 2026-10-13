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
 * class under io.questdb that holds a Function, an array or a collection of them, or a
 * VirtualRecord over them (a window's PARTITION BY keys), must forward the three, or be in
 * {@link #ALLOWED} with the reason forwarding would be wrong. A forward through UnaryFunction and
 * its siblings covers their args(). A class's own override must be declared in the class that
 * declares the last of its function fields, or below it: an override in a base class does not
 * know the fields a subclass adds (lag()'s default value over BaseWindowFunction's arg). An
 * anonymous class's captured locals (val$x) count as its fields.
 */
public class FunctionFlagsForwardingTest {
    private static final Map<String, String> ALLOWED = new HashMap<>();
    // function fields an override need not read, with the reason
    private static final Map<String, String> ALLOWED_FIELDS = new HashMap<>();

    static {
        // A scalar sub-query is run once per execution: the function's value is one per
        // execution, whatever the sub-query's projection calls, so its flags are its own.
        ALLOWED.put("io.questdb.griffin.engine.functions.ScalarSubQueryTimestampFunction", "one value per execution");
        ALLOWED.put("io.questdb.griffin.engine.functions.bind.CompiledFilterScalarSubQuery$DoubleLink", "one value per execution");
        ALLOWED.put("io.questdb.griffin.engine.functions.bind.CompiledFilterScalarSubQuery$IntLink", "one value per execution");
        ALLOWED.put("io.questdb.griffin.engine.functions.bind.CompiledFilterScalarSubQuery$LongLink", "one value per execution");
        // built from keyBranches, which the forward reads
        ALLOWED_FIELDS.put("io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory$SymbolSwitchPicker#intMap", "alias of keyBranches");
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
                    final Class<?> declaring = declaringClass(clazz, m);
                    if (declaring == Function.class) {
                        missing.add(name + "." + m + "() inherits Function's default, holds " + held);
                    } else if (!declaring.isInterface()) {
                        final String unread = unreadFields(declaring, m);
                        if (unread != null) {
                            missing.add(name + "." + m + "(), declared in " + declaring.getName() + ", " + unread);
                        }
                        // the override must see every field: none may be declared below it
                        for (Class<?> c = clazz; c != declaring; c = c.getSuperclass()) {
                            final List<String> below = declaredFunctionFields(c);
                            if (!below.isEmpty()) {
                                missing.add(name + "." + m + "() is declared in " + declaring.getName() + ", above the fields " + below + " of " + c.getName());
                                break;
                            }
                        }
                    }
                }
            }
        }
        Assert.assertTrue("scanned only " + checked + " classes from " + root, checked > 500);
        if (!missing.isEmpty()) {
            Assert.fail(missing.size() + " missing forwards:\n" + String.join("\n", missing));
        }
    }

    // The function fields the class's own m() neither reads (GETFIELD) nor leaves to the
    // superclass's m() through super.m(): null for none. An m() that returns true outright needs
    // no field.
    private static String unreadFields(Class<?> declaring, String m) throws Exception {
        final ClassFile cf = ClassFile.read(declaring);
        final byte[] code = cf.code(m, "()Z");
        if (code == null || (code.length == 2 && code[0] == 0x04 && (code[1] & 0xff) == 0xac)) {
            return null;
        }
        final java.util.Set<String> read = cf.refs(code, 0xb4, 9);
        final boolean callsSuper = cf.refs(code, 0xb7, 10).contains(m);
        final List<String> unread = new ArrayList<>();
        for (Class<?> c = declaring; c != null && c != Object.class; c = c.getSuperclass()) {
            if (c != declaring && callsSuper) {
                break;
            }
            for (String field : declaredFunctionFields(c)) {
                if (!read.contains(field) && !ALLOWED_FIELDS.containsKey(c.getName() + "#" + field)) {
                    unread.add(c.getSimpleName() + "." + field);
                }
            }
        }
        return unread.isEmpty() ? null : "does not read " + unread;
    }

    // The class whose own code is the m() the class runs. javac adds a synthetic bridge to a
    // public class for a public method it inherits from a package-private one, which calls
    // super.m(): that bridge is skipped.
    private static Class<?> declaringClass(Class<?> clazz, String m) throws Exception {
        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            for (Method method : c.getDeclaredMethods()) {
                if (method.getName().equals(m) && method.getParameterCount() == 0 && !method.isSynthetic() && !method.isBridge()) {
                    return c;
                }
            }
        }
        return clazz.getMethod(m).getDeclaringClass();
    }

    private static List<String> declaredFunctionFields(Class<?> c) {
        final List<String> held = new ArrayList<>();
        for (Field field : c.getDeclaredFields()) {
            // a synthetic field is an anonymous or local class's captured local (val$x), or its
            // outer instance (this$0), which is not an argument
            if (!Modifier.isStatic(field.getModifiers())
                    && (!field.isSynthetic() || field.getName().startsWith("val$"))
                    && holdsFunction(field.getGenericType())) {
                held.add(field.getName());
            }
        }
        return held;
    }

    private static List<String> heldFunctionFields(Class<?> clazz) {
        final List<String> held = new ArrayList<>();
        for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
            held.addAll(declaredFunctionFields(c));
        }
        return held;
    }

    private static boolean holdsFunction(Type type) {
        if (type instanceof Class<?> c) {
            return Function.class.isAssignableFrom(c)
                    || io.questdb.cairo.sql.VirtualRecord.class.isAssignableFrom(c)
                    || (c.isArray() && holdsFunction(c.getComponentType()));
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

    // A minimal class file reader: the constant pool, and a no-argument method's bytecode.
    private static final class ClassFile {
        private final byte[] b;
        private final int[] offsets;
        private final byte[] tags;
        private int methodsAt;

        private ClassFile(byte[] b) {
            this.b = b;
            final int count = u2(8);
            offsets = new int[count];
            tags = new byte[count];
            int p = 10;
            for (int i = 1; i < count; i++) {
                tags[i] = b[p];
                offsets[i] = p + 1;
                switch (b[p]) {
                    case 1 -> p += 3 + u2(p + 1);
                    case 3, 4, 9, 10, 11, 12, 17, 18 -> p += 5;
                    case 5, 6 -> {
                        p += 9;
                        i++;
                    }
                    case 7, 8, 16, 19, 20 -> p += 3;
                    case 15 -> p += 4;
                    default -> throw new IllegalStateException("constant tag " + b[p]);
                }
            }
            p += 6;
            p += 2 + 2 * u2(p);
            p = skipMembers(p);
            methodsAt = p;
        }

        static ClassFile read(Class<?> c) throws Exception {
            try (java.io.InputStream in = c.getClassLoader().getResourceAsStream(c.getName().replace('.', '/') + ".class")) {
                Assert.assertNotNull(c.getName(), in);
                return new ClassFile(in.readAllBytes());
            }
        }

        byte[] code(String name, String desc) {
            int p = methodsAt;
            final int n = u2(p);
            p += 2;
            for (int i = 0; i < n; i++) {
                final boolean match = utf8(u2(p + 2)).equals(name) && utf8(u2(p + 4)).equals(desc);
                final int attrs = u2(p + 6);
                p += 8;
                for (int a = 0; a < attrs; a++) {
                    final int len = u4(p + 2);
                    if (match && utf8(u2(p)).equals("Code")) {
                        final int codeLen = u4(p + 10);
                        return java.util.Arrays.copyOfRange(b, p + 14, p + 14 + codeLen);
                    }
                    p += 6 + len;
                }
            }
            return null;
        }

        // the member names the instructions with the opcode reference through constants of the tag
        java.util.Set<String> refs(byte[] code, int opcode, int tag) {
            final java.util.Set<String> names = new java.util.HashSet<>();
            for (int i = 0; i + 2 < code.length; i++) {
                if ((code[i] & 0xff) == opcode) {
                    final int index = ((code[i + 1] & 0xff) << 8) | (code[i + 2] & 0xff);
                    if (index > 0 && index < tags.length && tags[index] == tag) {
                        names.add(utf8(u2(offsets[u2(offsets[index] + 2)])));
                    }
                }
            }
            return names;
        }

        private int skipMembers(int p) {
            final int n = u2(p);
            p += 2;
            for (int i = 0; i < n; i++) {
                final int attrs = u2(p + 6);
                p += 8;
                for (int a = 0; a < attrs; a++) {
                    p += 6 + u4(p + 2);
                }
            }
            return p;
        }

        private int u2(int p) {
            return ((b[p] & 0xff) << 8) | (b[p + 1] & 0xff);
        }

        private int u4(int p) {
            return (u2(p) << 16) | u2(p + 2);
        }

        private String utf8(int index) {
            final int p = offsets[index];
            return new String(b, p + 2, u2(p), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
