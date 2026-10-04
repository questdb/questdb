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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * A type without functions of its own must never silently reach another type's function through
 * overload matching (User Story 8, scenario 2). For every type registered after the kit's
 * recording ({@link TypeConformanceTypes}' hook for later types), the test calls every function
 * and operator with a column of that type in each argument slot the signature declares for a
 * concrete value type, the other arguments of their declared types (a DECIMAL, GEOHASH or DOUBLE
 * array slot takes a column of that family, a variadic slot one value of the slot before it). A
 * call that compiles reached a function written for another type, and the test lists it by its
 * argument types (the overload the parser chose is not visible from SQL): the list is the gap the
 * type's PR closes, by functions of its own or by stating that the reach is meant.
 * <p>
 * Operators take their SQL form: {@code a OP b}, {@code OP a}, {@code a IN (b)},
 * {@code a BETWEEN b AND c}; element access ({@code []}) and casts are left out (the kit's cast
 * path checks casts). A slot some overload of the same name and arity declares variadic is not a
 * reach, nor is a slot of the overload itself declared for a pseudo type such as a cursor; a cursor
 * or NULL slot of another overload does not hide this one's value slots. A call that does not
 * compile, for any reason, is not listed, so the list is a lower bound. The test proves its harness
 * on BYTE, which has no trigonometric functions of its own and reaches DOUBLE's through its implicit
 * casts, and INT's {@code &}, {@code =} and {@code <} operators, the last two of which also have
 * cursor and NULL overloads.
 */
public class FunctionReachTest extends AbstractCairoTest {

    @Test
    public void testHarnessFindsAReach() throws Exception {
        assertMemoryLeak(() -> {
            final TreeSet<String> reaches = reaches(TypeConformanceTypes.byLabel("BYTE"));
            Assert.assertTrue("BYTE reaches acos(D) through its implicit casts: " + reaches, reaches.contains("BYTE -> acos(BYTE)"));
            Assert.assertTrue("BYTE reaches INT's & through its implicit casts: " + reaches, reaches.contains("BYTE -> &(BYTE, INT)"));
            // = and < also have cursor and NULL overloads; they must not hide the value slots
            Assert.assertTrue("BYTE reaches INT's = through its implicit casts: " + reaches, reaches.contains("BYTE -> =(BYTE, INT)"));
            Assert.assertTrue("BYTE reaches INT's < through its implicit casts: " + reaches, reaches.contains("BYTE -> <(BYTE, INT)"));
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
    private static String argumentOf(int declared, Map<Short, String> columns, Map<Short, String> literals, @Nullable String doubleArray) {
        final short tag = FunctionFactoryDescriptor.toTypeTag(declared);
        if (FunctionFactoryDescriptor.isArray(declared)) {
            return tag == ColumnType.DOUBLE ? doubleArray : null;
        }
        return FunctionFactoryDescriptor.isConstant(declared) ? literals.get(tag) : columns.get(tag);
    }

    /**
     * SELECT with the call of {@code name} and x in slot k, in the operator's SQL form where the name
     * is an operator; null when a slot has no expression of its type or the name has no form here.
     */
    @Nullable
    private static String callOf(
            String name,
            FunctionFactoryDescriptor descriptor,
            int k,
            Map<Short, String> columns,
            Map<Short, String> literals,
            @Nullable String doubleArray
    ) {
        final int c = descriptor.getSigArgCount();
        final String[] args = new String[c];
        for (int a = 0; a < c; a++) {
            if (a == k) {
                args[a] = "x";
                continue;
            }
            int declared = descriptor.getArgTypeWithFlags(a);
            if (FunctionFactoryDescriptor.toTypeTag(declared) == ColumnType.VAR_ARG) {
                // one value of the slot before the variadic one, as a constant
                if (a == 0) {
                    return null;
                }
                declared = FunctionFactoryDescriptor.toTypeTag(descriptor.getArgTypeWithFlags(a - 1));
                args[a] = literals.get((short) declared);
            } else {
                args[a] = argumentOf(declared, columns, literals, doubleArray);
            }
            if (args[a] == null) {
                return null;
            }
        }
        final String expression;
        if (Character.isLetter(name.charAt(0)) && !isWordOperator(name)) {
            expression = name + '(' + String.join(", ", args) + ')';
        } else if ("in".equalsIgnoreCase(name) && c >= 2) {
            expression = args[0] + " IN (" + String.join(", ", Arrays.copyOfRange(args, 1, c)) + ')';
        } else if ("between".equalsIgnoreCase(name) && c == 3) {
            expression = args[0] + " BETWEEN " + args[1] + " AND " + args[2];
        } else if (c == 2 && !"[]".equals(name)) {
            expression = args[0] + ' ' + name + ' ' + args[1];
        } else if (c == 1 && !"[]".equals(name)) {
            expression = name + ' ' + args[0];
        } else {
            return null;
        }
        return "SELECT " + expression + " FROM reach";
    }

    private static boolean compiles(String sql) {
        try (RecordCursorFactory ignore = select(sql)) {
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    // a slot that an overload of the same arity declares variadic takes any type; a cursor or NULL
    // slot takes one specific pseudo type, so it does not hide the other overloads' value slots
    private static boolean isAnyTypeSlot(ObjList<FunctionFactoryDescriptor> overloads, int argCount, int k) {
        for (int i = 0, n = overloads.size(); i < n; i++) {
            final FunctionFactoryDescriptor overload = overloads.getQuick(i);
            if (overload.getSigArgCount() == argCount
                    && FunctionFactoryDescriptor.toTypeTag(overload.getArgTypeWithFlags(k)) == ColumnType.VAR_ARG) {
                return true;
            }
        }
        return false;
    }

    // SQL words the parser reads as operators, not as function calls
    private static boolean isWordOperator(String name) {
        return switch (name.toLowerCase()) {
            case "and", "or", "not", "like", "ilike", "in", "between", "within" -> true;
            default -> false;
        };
    }

    /**
     * Every call a column of {@code type} compiles in, in a slot declared for another concrete
     * value type, as {@code <type> -> <name>(<argument types>)}.
     */
    private static TreeSet<String> reaches(TypeConformanceTypes.Entry type) throws Exception {
        // one column of every persisted kit type that is not the type itself, and the type as x
        // a signature's DECIMAL and GEOHASH slots take any member of the family: the first kit type of it
        final Map<Short, String> columns = new HashMap<>();
        final Map<Short, String> literals = new HashMap<>();
        String doubleArray = null;
        final StringBuilder ddl = new StringBuilder("CREATE TABLE reach (x ").append(type.ddl);
        for (int i = 0, n = TypeConformanceTypes.ALL.size(); i < n; i++) {
            final TypeConformanceTypes.Entry entry = TypeConformanceTypes.ALL.getQuick(i);
            final short tag = ColumnType.isDecimal(entry.columnType)
                    ? ColumnType.DECIMAL
                    : ColumnType.isGeoHash(entry.columnType) ? ColumnType.GEOHASH : ColumnType.tagOf(entry.columnType);
            if (entry.isLater() || tag == ColumnType.tagOf(type.columnType) || columns.containsKey(tag)) {
                continue;
            }
            if (ColumnType.isArray(entry.columnType)) {
                if (doubleArray == null && ColumnType.decodeArrayElementType(entry.columnType) == ColumnType.DOUBLE) {
                    doubleArray = "c" + i;
                    ddl.append(", c").append(i).append(' ').append(entry.ddl);
                }
                continue;
            }
            if (ColumnType.isPersisted(ColumnType.tagOf(entry.columnType))
                    && (entry.columnType == tag || tag == ColumnType.DECIMAL || tag == ColumnType.GEOHASH)) {
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
                    for (int k = 0, c = descriptor.getSigArgCount(); k < c; k++) {
                        final int declared = descriptor.getArgTypeWithFlags(k);
                        final short tag = FunctionFactoryDescriptor.toTypeTag(declared);
                        if (FunctionFactoryDescriptor.isArray(declared) || ColumnType.findTypeDriver(tag) == null
                                || tag == ColumnType.tagOf(type.columnType) || isAnyTypeSlot(overloads, c, k)) {
                            continue;
                        }
                        final String sql = callOf(name, descriptor, k, columns, literals, doubleArray);
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
            final int declared = descriptor.getArgTypeWithFlags(a);
            if (a == k) {
                types.append(type.label);
            } else {
                types.append(ColumnType.nameOf(FunctionFactoryDescriptor.toTypeTag(declared)));
                if (FunctionFactoryDescriptor.isArray(declared)) {
                    types.append("[]");
                }
            }
        }
        return types.append(')').toString();
    }
}
