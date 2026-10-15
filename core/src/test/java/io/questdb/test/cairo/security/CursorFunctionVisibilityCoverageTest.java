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

package io.questdb.test.cairo.security;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionRequirements;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Guards {@link SecurityContext#isTableVisible(TableToken)} against new functions that would list
 * objects a principal may not see. Every function that may return a cursor must be classified as
 * one that filters what it discloses by visibility, one that discloses objects to admins only, or
 * one that exposes no database objects at all. A new cursor function fails this test until its
 * author decides which one it is.
 * <p>
 * The functions of the first two kinds evaluate against the security context of the caller, so
 * they must also declare {@link SqlExecutionRequirements#DISCLOSES_OBJECTS} or
 * {@link SqlExecutionRequirements#REQUIRES_ENTERPRISE_SECURITY_CONTEXT}, which restrict their use
 * in materialized and live views: those refresh detached from any caller, under a context that sees
 * every object.
 */
public class CursorFunctionVisibilityCoverageTest extends AbstractCairoTest {
    // discloses objects only to callers authorized as admins, others see nothing or only their own
    private static final Set<String> ADMIN_ONLY = Set.of(
            "io.questdb.griffin.engine.functions.activity.ExportActivityFunctionFactory",
            "io.questdb.griffin.engine.functions.activity.QueryActivityFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.FilesFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.GlobFilesFunctionFactory",
            "io.questdb.griffin.engine.functions.table.ReaderPoolFunctionFactory",
            "io.questdb.griffin.engine.functions.table.WriterPoolFunctionFactory"
    );
    private static final byte[] CURSOR_FUNCTION_CLASS = "io/questdb/griffin/engine/functions/CursorFunction".getBytes(StandardCharsets.UTF_8);
    // exposes no database objects
    private static final Set<String> EXPOSES_NO_OBJECTS = Set.of(
            "io.questdb.griffin.engine.functions.catalogue.CheckpointStatusFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.ExportFilesFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.FunctionListFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.ImportFilesFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.InformationSchemaCharacterSetsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.InformationSchemaFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.KeyColumnUsageFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.KeywordsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgDatabaseFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgDescriptionFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgEnumFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgExtensionFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgGetKeywordsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgIndexFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgInheritsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgLocksFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgNamespaceFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgProcFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgRangeFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgRolesFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgShDescriptionFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgTypeFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgDatabaseFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgDescriptionFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgExtensionFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgGetKeywordsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgIndexFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgInheritsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgLocksFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgNamespaceFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgRolesFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgTypeFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.ReferentialConstraintsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.TableConstraintsFunctionFactory",
            "io.questdb.griffin.engine.functions.date.GenerateSeriesDoubleDefaultFunctionFactory",
            "io.questdb.griffin.engine.functions.date.GenerateSeriesDoubleFunctionFactory",
            "io.questdb.griffin.engine.functions.date.GenerateSeriesLongDefaultFunctionFactory",
            "io.questdb.griffin.engine.functions.date.GenerateSeriesLongFunctionFactory",
            "io.questdb.griffin.engine.functions.date.GenerateSeriesTimestampLongFunctionFactory",
            "io.questdb.griffin.engine.functions.date.GenerateSeriesTimestampStringFunctionFactory",
            "io.questdb.griffin.engine.functions.date.SleepFunctionFactory",
            "io.questdb.griffin.engine.functions.rnd.LongSequenceFunctionFactory",
            "io.questdb.griffin.engine.functions.table.MemoryMetricsFunctionFactory",
            "io.questdb.griffin.engine.functions.table.ParquetScanFunctionFactory",
            "io.questdb.griffin.engine.functions.table.ReadParquetFunctionFactory",
            "io.questdb.griffin.engine.functions.table.TableWriterMetricsFunctionFactory",
            "io.questdb.griffin.engine.functions.test.TestOwnerCountingFunctionFactory",
            "io.questdb.griffin.engine.functions.test.TestTableReferenceOutOfDateFunctionFactory"
    );
    // lists only the objects the caller may see, or resolves an object by name only when the caller may see it
    private static final Set<String> FILTERS_BY_VISIBILITY = Set.of(
            "io.questdb.griffin.engine.functions.catalogue.AllTablesFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.InformationSchemaColumnsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.InformationSchemaQuestDBColumnsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.InformationSchemaTablesFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.LiveViewsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.MatViewsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgAttrDefFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgAttributeFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PgClassFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgAttrDefFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgAttributeFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.PrefixedPgClassFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.TablesFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.ViewsFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.WalTableListFunctionFactory",
            "io.questdb.griffin.engine.functions.catalogue.WalTransactionsFunctionFactory",
            "io.questdb.griffin.engine.functions.table.TableColumnsFunctionFactory",
            "io.questdb.griffin.engine.functions.table.TablePartitionsFunctionFactory",
            "io.questdb.griffin.engine.functions.table.TableStorageFunctionFactory"
    );

    /**
     * Asserts that every registered function factory in the given package that may return a cursor
     * is classified exactly once, that the classification names no unregistered factory, and that
     * the factories disclosing objects by the caller's security context declare so.
     */
    public static void assertCursorFunctionsClassified(
            CairoEngine engine,
            String packagePrefix,
            Set<String> filtersByVisibility,
            Set<String> adminOnly,
            Set<String> exposesNoObjects
    ) {
        final Map<String, FunctionFactory> registered = new HashMap<>();
        engine.getFunctionFactoryCache().getFactories().forEach((name, descriptors) -> {
            for (int i = 0, n = descriptors.size(); i < n; i++) {
                final FunctionFactory factory = descriptors.getQuick(i).getFactory();
                if (factory.getClass().getName().startsWith(packagePrefix)) {
                    registered.putIfAbsent(factory.getClass().getName(), factory);
                }
            }
        });

        final Set<String> unclassified = new TreeSet<>();
        final Set<String> ambiguous = new TreeSet<>();
        for (Map.Entry<String, FunctionFactory> entry : registered.entrySet()) {
            final String factoryClass = entry.getKey();
            final int kinds = (filtersByVisibility.contains(factoryClass) ? 1 : 0)
                    + (adminOnly.contains(factoryClass) ? 1 : 0)
                    + (exposesNoObjects.contains(factoryClass) ? 1 : 0);
            if (kinds > 1) {
                ambiguous.add(factoryClass);
            } else if (kinds == 0 && mayReturnCursor(entry.getValue().getClass())) {
                unclassified.add(factoryClass);
            }
        }
        Assert.assertEquals(
                "cursor functions must be classified: do they filter by SecurityContext.isTableVisible(), "
                        + "disclose objects to admins only, or expose no database objects at all?",
                "[]",
                unclassified.toString()
        );
        Assert.assertEquals("classified more than once", "[]", ambiguous.toString());

        final Set<String> stale = new TreeSet<>();
        for (Set<String> kind : List.of(filtersByVisibility, adminOnly, exposesNoObjects)) {
            for (String factoryClass : kind) {
                if (!registered.containsKey(factoryClass)) {
                    stale.add(factoryClass);
                }
            }
        }
        Assert.assertEquals("classified, but not registered", "[]", stale.toString());

        final Set<String> undeclared = new TreeSet<>();
        for (Set<String> kind : List.of(filtersByVisibility, adminOnly)) {
            for (String factoryClass : kind) {
                final int requirements = registered.get(factoryClass).getExecutionRequirements();
                if ((requirements & (SqlExecutionRequirements.DISCLOSES_OBJECTS | SqlExecutionRequirements.REQUIRES_ENTERPRISE_SECURITY_CONTEXT)) == 0) {
                    undeclared.add(factoryClass);
                }
            }
        }
        Assert.assertEquals(
                "functions that disclose objects by the caller's security context must declare DISCLOSES_OBJECTS or REQUIRES_ENTERPRISE_SECURITY_CONTEXT",
                "[]",
                undeclared.toString()
        );
    }

    @Test
    public void testEveryCursorFunctionIsClassified() {
        assertCursorFunctionsClassified(engine, "io.questdb.", FILTERS_BY_VISIBILITY, ADMIN_ONLY, EXPOSES_NO_OBJECTS);
    }

    private static byte[] classBytes(String className) {
        final String resource = className.replace('.', '/') + ".class";
        try (InputStream in = CursorFunctionVisibilityCoverageTest.class.getClassLoader().getResourceAsStream(resource)) {
            return in != null ? in.readAllBytes() : null;
        } catch (IOException e) {
            throw new AssertionError(resource, e);
        }
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0, n = haystack.length - needle.length; i <= n; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    // Functions return a cursor as a CursorFunction, which the class that creates it, the factory, a
    // superclass of the factory or a class nested in either, references in its constant pool.
    private static boolean mayReturnCursor(Class<?> factoryClass) {
        for (Class<?> c = factoryClass; c != null && c != Object.class; c = c.getSuperclass()) {
            if (referencesCursorFunction(c)) {
                return true;
            }
        }
        return false;
    }

    private static boolean referencesCursorFunction(Class<?> clazz) {
        final byte[] bytes = classBytes(clazz.getName());
        if (bytes != null && contains(bytes, CURSOR_FUNCTION_CLASS)) {
            return true;
        }
        for (Class<?> nested : clazz.getDeclaredClasses()) {
            if (referencesCursorFunction(nested)) {
                return true;
            }
        }
        // anonymous classes are numbered from 1, without gaps
        for (int i = 1; ; i++) {
            final byte[] anonymous = classBytes(clazz.getName() + '$' + i);
            if (anonymous == null) {
                return false;
            }
            if (contains(anonymous, CURSOR_FUNCTION_CLASS)) {
                return true;
            }
        }
    }
}
