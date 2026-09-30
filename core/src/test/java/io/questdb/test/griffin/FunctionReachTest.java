/*+*****************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.types.TypeConformanceTypes;
import io.questdb.test.cairo.types.TypeConformanceValues;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * A type without functions of its own must never silently reach another type's function through
 * overload matching (User Story 8, scenario 2). For every type registered after the kit's
 * recording ({@link TypeConformanceTypes}' hook for later types), the test calls every function
 * with a column of that type in each argument slot the signature declares for a concrete value
 * type, the other arguments of their declared types. A call that compiles reached a function
 * written for another type, and the test lists it by its argument types (the overload the parser
 * chose is not visible from SQL): the list is the gap the type's PR closes, by functions of its
 * own or by stating that the reach is meant.
 * <p>
 * A slot some overload of the same name and arity declares for any type (a pseudo type) is not a
 * reach. A call that does not compile, for any reason, is not listed, so the list is a lower
 * bound. The test proves its harness on BYTE, which has no trigonometric functions of its own and
 * reaches DOUBLE's through its implicit casts.
 */
public class FunctionReachTest extends AbstractCairoTest {

    @Test
    public void testHarnessFindsAReach() throws Exception {
        assertMemoryLeak(() -> {
            final TreeSet<String> reaches = reaches(TypeConformanceTypes.byLabel("BYTE"));
            Assert.assertTrue("BYTE reaches acos(D) through its implicit casts: " + reaches, reaches.contains("BYTE -> acos(BYTE)"));
        });
    }

    @Test
    public void testLaterTypesReachNoOtherTypesFunction() throws Exception {
        assertMemoryLeak(() -> {
            final StringBuilder gaps = new StringBuilder();
            for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
                final TypeConformanceTypes.Entry type = TypeConformanceTypes.ALL.getQuick(i);
                if (type.isLater()) {
                    for (String reach : reaches(type)) {
                        gaps.append(reach).append('\n');
                    }
                }
            }
            // no type is registered later on this branch; a later type's PR lists its reaches here
            TestUtils.assertEquals("", gaps);
        });
    }

    // an expression of the declared type: a column of it, or a literal where the slot takes a constant
    @Nullable
    private static String argumentOf(short tag, boolean isConstant, Map<Short, String> columns, Map<Short, String> literals) {
        return isConstant ? literals.get(tag) : columns.get(tag);
    }

    // SELECT name(...) FROM reach with x in slot k; null when some slot has no expression of its type
    @Nullable
    private static String callOf(String name, FunctionFactoryDescriptor descriptor, int k, Map<Short, String> columns, Map<Short, String> literals) {
        final StringBuilder sql = new StringBuilder("SELECT ").append(name).append('(');
        for (int a = 0, c = descriptor.getSigArgCount(); a < c; a++) {
            if (a > 0) {
                sql.append(", ");
            }
            if (a == k) {
                sql.append('x');
                continue;
            }
            final int declared = descriptor.getArgTypeWithFlags(a);
            final String argument = FunctionFactoryDescriptor.isArray(declared)
                    ? null
                    : argumentOf(FunctionFactoryDescriptor.toTypeTag(declared), FunctionFactoryDescriptor.isConstant(declared), columns, literals);
            if (argument == null) {
                return null;
            }
            sql.append(argument);
        }
        return sql.append(") FROM reach").toString();
    }

    private static boolean compiles(String sql) {
        try (RecordCursorFactory ignore = select(sql)) {
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    // a slot that an overload of the same arity declares for a pseudo type takes any type
    private static boolean isAnyTypeSlot(ObjList<FunctionFactoryDescriptor> overloads, int argCount, int k) {
        for (int i = 0, n = overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor overload = overloads.getQuick(i);
            if (overload.getSigArgCount() == argCount
                    && ColumnType.findTypeDriver(FunctionFactoryDescriptor.toTypeTag(overload.getArgTypeWithFlags(k))) == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Every call a column of {@code type} compiles in, in a slot declared for another concrete
     * value type, as {@code <type> -> <name>(<argument types>)}.
     */
    private static TreeSet<String> reaches(TypeConformanceTypes.Entry type) throws Exception {
        // one column of every persisted kit type that is not the type itself, and the type as x
        final Map<Short, String> columns = new HashMap<>();
        final Map<Short, String> literals = new HashMap<>();
        final StringBuilder ddl = new StringBuilder("CREATE TABLE reach (x ").append(type.ddl);
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            final TypeConformanceTypes.Entry entry = TypeConformanceTypes.ALL.getQuick(i);
            final short tag = ColumnType.tagOf(entry.columnType);
            if (entry.isLater() || tag == ColumnType.tagOf(type.columnType) || columns.containsKey(tag)) {
                continue;
            }
            if (ColumnType.isPersisted(tag) && entry.columnType == tag) {
                columns.put(tag, "c" + i);
                ddl.append(", c").append(i).append(' ').append(entry.ddl);
            }
            final ObjList<TypeConformanceValues.Row> rows = TypeConformanceValues.rowsOf(entry);
            for (int r = 0, m = rows.size(); r < m && !literals.containsKey(tag); r++) {
                if (!rows.getQuick(r).isNull() && rows.getQuick(r).literal != null) {
                    literals.put(tag, rows.getQuick(r).literal);
                }
            }
        }
        execute(ddl.append(')').toString());
        final TreeSet<String> reaches = new TreeSet<>();
        try {
            final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories = engine.getFunctionFactoryCache().getFactories();
            final ObjList<CharSequence> names = factories.keys();
            for (int i = 0, n = names.size(); i < n; i++) {
                final ObjList<FunctionFactoryDescriptor> overloads = factories.get(names.getQuick(i));
                for (int j = 0, m = overloads.size(); j < m; j++) {
                    final FunctionFactoryDescriptor descriptor = overloads.getQuick(j);
                    final String name = descriptor.getName();
                    if (!Character.isLetter(name.charAt(0))) {
                        // operators need their SQL syntax, not a call; the operator functions they reach
                        // are the same factories as their named forms
                        continue;
                    }
                    for (int k = 0, c = descriptor.getSigArgCount(); k < c; k++) {
                        final int declared = descriptor.getArgTypeWithFlags(k);
                        final short tag = FunctionFactoryDescriptor.toTypeTag(declared);
                        if (FunctionFactoryDescriptor.isArray(declared) || ColumnType.findTypeDriver(tag) == null
                                || tag == ColumnType.tagOf(type.columnType) || isAnyTypeSlot(overloads, c, k)) {
                            continue;
                        }
                        final String sql = callOf(name, descriptor, k, columns, literals);
                        if (sql != null && compiles(sql)) {
                            reaches.add(type.label + " -> " + name + typesOf(descriptor, k, type));
                        }
                    }
                }
            }
        } finally {
            execute("DROP TABLE reach");
        }
        return reaches;
    }

    // the call's argument types: the type in slot k, the declared types elsewhere
    private static String typesOf(FunctionFactoryDescriptor descriptor, int k, TypeConformanceTypes.Entry type) {
        final StringBuilder types = new StringBuilder("(");
        for (int a = 0, c = descriptor.getSigArgCount(); a < c; a++) {
            if (a > 0) {
                types.append(", ");
            }
            types.append(a == k ? type.label : ColumnType.nameOf(FunctionFactoryDescriptor.toTypeTag(descriptor.getArgTypeWithFlags(a))));
        }
        return types.append(')').toString();
    }
}
