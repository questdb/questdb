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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.NullPolicy;
import io.questdb.cairo.TypeDriver;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.engine.functions.ArgSwappingFunctionFactory;
import io.questdb.griffin.engine.functions.NegatingFunctionFactory;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The lister of the function library's NULL-policy rule (F35, {@code r3-functions.md} "The rule").
 * A full-range type reaches a factory only when the factory computes its value in a static
 * {@code value(...)} method with no NULL test (step 2) and its function classes call it (step 3),
 * so the full-range type's NOT_NULL and BITMAP wrappers can call the same body. The lister names
 * every factory with an argument of a type that will have a full-range counterpart, a fixed-width
 * type whose NULL is a sentinel, that declares no such method itself. The list shrinks with every
 * converted factory; a new factory in scope without a value body fails here.
 * <p>
 * Operator aliases ({@code !=}, {@code <>}, swapped arguments) follow their delegate. Factories
 * whose step 2 is empty, because the function has no value computation apart from its NULL
 * handling, are listed in {@link #NO_VALUE_BODY} with the reason. The lists go to
 * {@code target/null-policy-rule-lister.txt}.
 */
public class NullPolicyRuleListerTest extends AbstractCairoTest {
    // falls with every converted factory
    private static final int EXPECTED_TO_CHANGE = 383;
    // of those, factories with a LONG or DOUBLE argument, the first full-range counterparts (F17)
    private static final int EXPECTED_TO_CHANGE_LONG_DOUBLE = 218;
    // class + signature -> why step 2 is empty
    private static final Map<String, String> NO_VALUE_BODY = new TreeMap<>();

    @Test
    public void testListFactoriesTheRuleMustChange() throws IOException {
        final LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories = engine.getFunctionFactoryCache().getFactories();
        final TreeSet<String> all = new TreeSet<>();
        final TreeSet<String> aliases = new TreeSet<>();
        final TreeSet<String> converted = new TreeSet<>();
        final TreeSet<String> noBody = new TreeSet<>();
        final TreeSet<String> toChange = new TreeSet<>();
        final TreeSet<String> toChangeLongDouble = new TreeSet<>();
        final TreeMap<String, TreeSet<String>> toChangeByPackage = new TreeMap<>();
        final TreeMap<String, Integer> scopeByType = new TreeMap<>();
        final ObjList<CharSequence> names = factories.keys();
        for (int i = 0, n = names.size(); i < n; i++) {
            final ObjList<FunctionFactoryDescriptor> overloads = factories.get(names.getQuick(i));
            for (int j = 0, m = overloads.size(); j < m; j++) {
                final FunctionFactoryDescriptor descriptor = overloads.getQuick(j);
                final FunctionFactory factory = descriptor.getFactory();
                final String id = factory.getClass().getName() + ' ' + factory.getSignature();
                all.add(id);
                final TreeSet<String> sentinelTypes = new TreeSet<>();
                boolean hasLongOrDouble = false;
                for (int k = 0, c = descriptor.getSigArgCount(); k < c; k++) {
                    final short tag = FunctionFactoryDescriptor.toTypeTag(descriptor.getArgTypeWithFlags(k));
                    if (isFixedWidthSentinel(tag)) {
                        sentinelTypes.add(ColumnType.nameOf(tag));
                        hasLongOrDouble |= tag == ColumnType.LONG || tag == ColumnType.DOUBLE;
                    }
                }
                if (sentinelTypes.isEmpty()) {
                    continue;
                }
                if (factory instanceof NegatingFunctionFactory || factory instanceof ArgSwappingFunctionFactory) {
                    aliases.add(id);
                    continue;
                }
                for (String type : sentinelTypes) {
                    scopeByType.merge(type, 1, Integer::sum);
                }
                if (hasValueBody(factory.getClass())) {
                    converted.add(id);
                } else if (NO_VALUE_BODY.containsKey(id)) {
                    noBody.add(id);
                } else {
                    toChange.add(id);
                    if (hasLongOrDouble) {
                        toChangeLongDouble.add(id);
                    }
                    final String pkg = factory.getClass().getPackageName().replace("io.questdb.griffin.engine.functions.", "");
                    toChangeByPackage.computeIfAbsent(pkg, k -> new TreeSet<>()).add(id);
                }
            }
        }
        final StringBuilder out = new StringBuilder();
        out.append("factories (class + signature): ").append(all.size()).append('\n');
        out.append("in scope (an argument of a fixed-width sentinel type, aliases excluded): ")
                .append(converted.size() + noBody.size() + toChange.size()).append('\n');
        out.append("  converted (a static value(...) on the factory): ").append(converted.size()).append('\n');
        out.append("  no value body (listed with the reason): ").append(noBody.size()).append('\n');
        out.append("  to change: ").append(toChange.size()).append('\n');
        out.append("    of which with a LONG or DOUBLE argument: ").append(toChangeLongDouble.size()).append('\n');
        out.append("operator aliases in scope, which follow their delegate: ").append(aliases.size()).append('\n');
        out.append("\n## scope by argument type (a factory counts once per type)\n");
        scopeByType.forEach((k, v) -> out.append(k).append(' ').append(v).append('\n'));
        out.append("\n## to change, by package\n");
        toChangeByPackage.forEach((k, v) -> out.append(k).append(' ').append(v.size()).append('\n'));
        out.append("\n## to change\n");
        toChangeByPackage.forEach((k, v) -> v.forEach(s -> out.append(s).append(toChangeLongDouble.contains(s) ? " *" : "").append('\n')));
        out.append("\n## no value body\n");
        noBody.forEach(s -> out.append(s).append(": ").append(NO_VALUE_BODY.get(s)).append('\n'));
        out.append("\n## converted\n");
        converted.forEach(s -> out.append(s).append('\n'));
        Files.writeString(Paths.get("target", "null-policy-rule-lister.txt"), out);
        System.out.println(out.substring(0, out.indexOf("\n## to change, by package")));

        for (String id : NO_VALUE_BODY.keySet()) {
            Assert.assertTrue("listed without a value body, but not in scope or converted: " + id, noBody.contains(id));
        }
        // a converted factory lowers the counts here; a new factory in scope without a value body raises them
        Assert.assertEquals(EXPECTED_TO_CHANGE, toChange.size());
        Assert.assertEquals(EXPECTED_TO_CHANGE_LONG_DOUBLE, toChangeLongDouble.size());
    }

    // the factory itself declares the body; a subclass does not inherit its parent's conversion
    private static boolean hasValueBody(Class<?> factoryClass) {
        for (Method method : factoryClass.getDeclaredMethods()) {
            if ("value".equals(method.getName()) && Modifier.isStatic(method.getModifiers())) {
                return true;
            }
        }
        return false;
    }

    // a type that will have a full-range counterpart: NULL is a reserved value in its data vector
    // (STRING, VARCHAR, SYMBOL and BINARY keep NULL in the length or the aux entry)
    private static boolean isFixedWidthSentinel(short tag) {
        final TypeDriver driver = ColumnType.findTypeDriver(tag);
        if (driver == null || driver.getNullPolicy() != NullPolicy.SENTINEL) {
            return false;
        }
        return switch (ColumnType.tagOf(tag)) {
            case ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL, ColumnType.BINARY -> false;
            default -> true;
        };
    }

    private static void noValueBody(String classAndSignature, String reason) {
        NO_VALUE_BODY.put("io.questdb.griffin.engine.functions." + classAndSignature, reason);
    }

    static {
        noValueBody("cast.CastBooleanToDateFunctionFactory cast(Tm)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastBooleanToDoubleFunctionFactory cast(Td)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastBooleanToFloatFunctionFactory cast(Tf)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastBooleanToIntFunctionFactory cast(Ti)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastBooleanToLong256FunctionFactory cast(Th)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastBooleanToLongFunctionFactory cast(Tl)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastBooleanToTimestampFunctionFactory cast(Tn)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastByteToDateFunctionFactory cast(Bm)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastByteToDoubleFunctionFactory cast(Bd)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastByteToFloatFunctionFactory cast(Bf)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastByteToIntFunctionFactory cast(Bi)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastByteToLong256FunctionFactory cast(Bh)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastByteToLongFunctionFactory cast(Bl)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastByteToTimestampFunctionFactory cast(Bn)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastCharToDateFunctionFactory cast(Am)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastCharToDoubleFunctionFactory cast(Ad)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastCharToFloatFunctionFactory cast(Af)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastCharToIntFunctionFactory cast(Ai)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastCharToLong256FunctionFactory cast(Ah)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastCharToLongFunctionFactory cast(Al)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastCharToTimestampFunctionFactory cast(An)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastDecimalToDoubleFunctionFactory cast(Ξd)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastDecimalToFloatFunctionFactory cast(Ξf)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastDecimalToIntFunctionFactory cast(Ξi)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastDecimalToLongFunctionFactory cast(Ξl)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastDoubleArrayToDoubleArrayFunctionFactory cast(D[]d[])", "structural: the cast prepends dimensions to the array or passes it through; no work on an element value");
        noValueBody("cast.CastDoubleArrayToStrFunctionFactory cast(D[]s)", "array formatting: the element formatting and its NULL rule live in ArrayTypeDriver.appendDoubleFromArrayToSink, which an array of a full-range element type splits");
        noValueBody("cast.CastDoubleArrayToVarcharFunctionFactory cast(D[]ø)", "array formatting: the element formatting and its NULL rule live in ArrayTypeDriver.appendDoubleFromArrayToSink, which an array of a full-range element type splits");
        noValueBody("cast.CastDoubleToDoubleArray cast(Dd[])", "structural: the cast wraps the value in a one-element array; no computation on the value");
        noValueBody("cast.CastShortToDateFunctionFactory cast(Em)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastShortToDoubleFunctionFactory cast(Ed)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastShortToFloatFunctionFactory cast(Ef)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastShortToIntFunctionFactory cast(Ei)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastShortToLong256FunctionFactory cast(Eh)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastShortToLongFunctionFactory cast(El)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastShortToTimestampFunctionFactory cast(En)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToDateFunctionFactory cast(Sm)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToDoubleArrayFunctionFactory cast(Sd[])", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToDoubleFunctionFactory cast(Sd)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToFloatFunctionFactory cast(Sf)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToIPv4FunctionFactory cast(Sx)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToIntFunctionFactory cast(Si)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToLong256FunctionFactory cast(Sh)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToLongFunctionFactory cast(Sl)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToTimestampFunctionFactory cast(Sn)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastStrToUuidFunctionFactory cast(Sz)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastSymbolToDateFunctionFactory cast(Km)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastSymbolToDoubleFunctionFactory cast(Kd)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastSymbolToFloatFunctionFactory cast(Kf)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastSymbolToIntFunctionFactory cast(Ki)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastSymbolToLong256FunctionFactory cast(Kh)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastSymbolToLongFunctionFactory cast(Kl)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastSymbolToTimestampFunctionFactory cast(Kn)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastVarcharToDateFunctionFactory cast(Øm)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastVarcharToDoubleFunctionFactory cast(Ød)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastVarcharToFloatFunctionFactory cast(Øf)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastVarcharToIPv4FunctionFactory cast(Øx)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastVarcharToIntFunctionFactory cast(Øi)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastVarcharToLong256FunctionFactory cast(Øh)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastVarcharToLongFunctionFactory cast(Øl)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastVarcharToTimestampFunctionFactory cast(Øn)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.CastVarcharToUuidFunctionFactory cast(Øz)", "the in-scope argument is the cast's target type, a constant");
        noValueBody("cast.VarcharCastHelperFunctionFactory VARCHAR(I)", "no value computation: the function ignores its INT argument and returns a NULL STRING constant");
        noValueBody("conditional.NullIfDoubleFunctionFactory nullif(DD)", "introduces NULL: the result is NULL where the operands are equal");
        noValueBody("conditional.NullIfIPv4FunctionFactory nullif(XS)", "introduces NULL: the result is NULL where the operands are equal");
        noValueBody("conditional.NullIfIntFunctionFactory nullif(II)", "introduces NULL: the result is NULL where the operands are equal");
        noValueBody("conditional.NullIfLongFunctionFactory nullif(LL)", "introduces NULL: the result is NULL where the operands are equal");
        noValueBody("eq.EqDoubleArrayFunctionFactory =(D[]D[])", "array comparison: the element comparison and its NULL rule live in ArrayView.arrayEquals, which an array of a full-range element type splits");
        noValueBody("math.CeilDecimalFunctionFactory ceil(ΞI)", "the INT argument is the rounding scale of a DECIMAL computation, which has no counterpart");
        noValueBody("math.CeilingDecimalFunctionFactory ceiling(ΞI)", "the INT argument is the rounding scale of a DECIMAL computation, which has no counterpart");
        noValueBody("math.FloorDecimalFunctionFactory floor(ΞI)", "the INT argument is the rounding scale of a DECIMAL computation, which has no counterpart");
        noValueBody("math.LateralLimitFunctionFactory __lateral_limit(L)", "no value computation: the function passes the LIMIT through unchanged and only rejects a negative one");
        noValueBody("math.RoundDecimalFunctionFactory round(ΞI)", "the INT argument is the rounding scale of a DECIMAL computation, which has no counterpart");
        noValueBody("math.RoundDownDecimalFunctionFactory round_down(ΞI)", "the INT argument is the rounding scale of a DECIMAL computation, which has no counterpart");
        noValueBody("math.RoundHalfEvenDecimalFunctionFactory round_half_even(ΞI)", "the INT argument is the rounding scale of a DECIMAL computation, which has no counterpart");
        noValueBody("math.RoundUpDecimalFunctionFactory round_up(ΞI)", "the INT argument is the rounding scale of a DECIMAL computation, which has no counterpart");
    }
}
