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
 ******************************************************************************/

package io.questdb.test.griffin.udf;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.udf.AggregateUDF;
import io.questdb.griffin.udf.UDFRegistry;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests for the simplified UDF API (UDFRegistry).
 */
public class SimplifiedUDFApiTest extends AbstractCairoTest {

    private static final String PLUGIN_NAME = "test-plugin";

    private void registerFactory(FunctionFactory factory) throws Exception {
        ObjList<FunctionFactory> factories = new ObjList<>();
        factories.add(factory);
        engine.getFunctionFactoryCache().addPluginFunctions(PLUGIN_NAME, factories);
    }

    private void unregisterFactories() throws Exception {
        engine.getFunctionFactoryCache().removePluginFunctions(PLUGIN_NAME);
    }

    @Test
    public void testScalarDoubleFunction() throws Exception {
        // Register a simple square function
        FunctionFactory squareFactory = UDFRegistry.scalar(
                "test_square",
                Double.class,
                Double.class,
                x -> x * x
        );

        registerFactory(squareFactory);

        try {
            assertMemoryLeak(() -> assertSql(
                    "test_square\n" +
                            "25.0\n",
                    "SELECT \"test-plugin\".test_square(5.0)"
            ));
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testScalarStringFunction() throws Exception {
        // Register a string reverse function
        FunctionFactory reverseFactory = UDFRegistry.scalar(
                "test_reverse",
                String.class,
                String.class,
                s -> s == null ? null : new StringBuilder(s).reverse().toString()
        );

        registerFactory(reverseFactory);

        try {
            assertMemoryLeak(() -> assertSql(
                    "test_reverse\n" +
                            "olleh\n",
                    "SELECT \"test-plugin\".test_reverse('hello')"
            ));
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testScalarIntToStringFunction() throws Exception {
        // Register a function that converts int to string
        FunctionFactory intToStrFactory = UDFRegistry.scalar(
                "test_int_to_str",
                Integer.class,
                String.class,
                i -> i == null ? null : "Number: " + i
        );

        registerFactory(intToStrFactory);

        try {
            assertMemoryLeak(() -> assertSql(
                    "test_int_to_str\n" +
                            "Number: 42\n",
                    "SELECT \"test-plugin\".test_int_to_str(42)"
            ));
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testBinaryFunction() throws Exception {
        // Register a power function (base^exp)
        FunctionFactory powerFactory = UDFRegistry.binary(
                "test_power",
                Double.class,
                Double.class,
                Double.class,
                Math::pow
        );

        registerFactory(powerFactory);

        try {
            assertMemoryLeak(() -> assertSql(
                    "test_power\n" +
                            "8.0\n",
                    "SELECT \"test-plugin\".test_power(2.0, 3.0)"
            ));
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testBinaryStringFunction() throws Exception {
        // Register a concat function
        FunctionFactory concatFactory = UDFRegistry.binary(
                "test_concat",
                String.class,
                String.class,
                String.class,
                (a, b) -> (a == null ? "" : a) + (b == null ? "" : b)
        );

        registerFactory(concatFactory);

        try {
            assertMemoryLeak(() -> assertSql(
                    "test_concat\n" +
                            "helloworld\n",
                    "SELECT \"test-plugin\".test_concat('hello', 'world')"
            ));
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testAggregateFunction() throws Exception {
        // Register a sum aggregate function
        FunctionFactory sumFactory = UDFRegistry.aggregate(
                "test_sum",
                Double.class,
                Double.class,
                () -> new AggregateUDF<Double, Double>() {
                    private double sum = 0;

                    @Override
                    public void accumulate(Double value) {
                        if (value != null) {
                            sum += value;
                        }
                    }

                    @Override
                    public Double result() {
                        return sum;
                    }

                    @Override
                    public void reset() {
                        sum = 0;
                    }
                }
        );

        registerFactory(sumFactory);

        try {
            assertMemoryLeak(() -> {
                // Create test table
                execute("CREATE TABLE test_agg (value DOUBLE)");
                execute("INSERT INTO test_agg VALUES (1.0), (2.0), (3.0), (4.0), (5.0)");

                assertSql(
                        "test_sum\n" +
                                "15.0\n",
                        "SELECT \"test-plugin\".test_sum(value) FROM test_agg"
                );

                execute("DROP TABLE test_agg");
            });
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testUDFRegistryFunctionsHelper() throws Exception {
        // Test the functions() helper method
        ObjList<FunctionFactory> factories = UDFRegistry.functions(
                UDFRegistry.scalar("fn1", Double.class, Double.class, x -> x * 2),
                UDFRegistry.scalar("fn2", Double.class, Double.class, x -> x * 3),
                UDFRegistry.scalar("fn3", Double.class, Double.class, x -> x * 4)
        );

        org.junit.Assert.assertEquals(3, factories.size());

        // Register all factories
        engine.getFunctionFactoryCache().addPluginFunctions(PLUGIN_NAME, factories);

        try {
            assertMemoryLeak(() -> {
                assertSql("fn1\n10.0\n", "SELECT \"test-plugin\".fn1(5.0)");
                assertSql("fn2\n15.0\n", "SELECT \"test-plugin\".fn2(5.0)");
                assertSql("fn3\n20.0\n", "SELECT \"test-plugin\".fn3(5.0)");
            });
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testNullHandling() throws Exception {
        // Test null handling in scalar functions
        FunctionFactory nullSafeFactory = UDFRegistry.scalar(
                "test_null_safe",
                String.class,
                String.class,
                s -> s == null ? "was_null" : s.toUpperCase()
        );

        registerFactory(nullSafeFactory);

        try {
            assertMemoryLeak(() -> {
                assertSql(
                        "test_null_safe\n" +
                                "HELLO\n",
                        "SELECT \"test-plugin\".test_null_safe('hello')"
                );

                assertSql(
                        "test_null_safe\n" +
                                "was_null\n",
                        "SELECT \"test-plugin\".test_null_safe(NULL)"
                );
            });
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testBooleanFunction() throws Exception {
        // Test boolean scalar function
        FunctionFactory notFactory = UDFRegistry.scalar(
                "test_not",
                Boolean.class,
                Boolean.class,
                b -> b == null ? null : !b
        );

        registerFactory(notFactory);

        try {
            assertMemoryLeak(() -> {
                assertSql(
                        "test_not\n" +
                                "false\n",
                        "SELECT \"test-plugin\".test_not(true)"
                );

                assertSql(
                        "test_not\n" +
                                "true\n",
                        "SELECT \"test-plugin\".test_not(false)"
                );
            });
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testLongFunction() throws Exception {
        // Test long return type
        FunctionFactory longFactory = UDFRegistry.scalar(
                "test_to_long",
                Double.class,
                Long.class,
                d -> d == null ? null : d.longValue()
        );

        registerFactory(longFactory);

        try {
            assertMemoryLeak(() -> assertSql(
                    "test_to_long\n" +
                            "42\n",
                    "SELECT \"test-plugin\".test_to_long(42.9)"
            ));
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testFloatFunction() throws Exception {
        // Test float return type
        FunctionFactory floatFactory = UDFRegistry.scalar(
                "test_to_float",
                Double.class,
                Float.class,
                d -> d == null ? null : d.floatValue()
        );

        registerFactory(floatFactory);

        try {
            assertMemoryLeak(() -> assertSql(
                    "round\n" +
                            "3.14\n",
                    "SELECT round(\"test-plugin\".test_to_float(3.14159), 2)"
            ));
        } finally {
            unregisterFactories();
        }
    }

    @Test
    public void testIntFunction() throws Exception {
        // Test int return type
        FunctionFactory intFactory = UDFRegistry.scalar(
                "test_str_len",
                String.class,
                Integer.class,
                s -> s == null ? null : s.length()
        );

        registerFactory(intFactory);

        try {
            assertMemoryLeak(() -> assertSql(
                    "test_str_len\n" +
                            "5\n",
                    "SELECT \"test-plugin\".test_str_len('hello')"
            ));
        } finally {
            unregisterFactories();
        }
    }
}
