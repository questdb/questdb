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

package io.questdb.griffin.udf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a provider of plugin functions.
 * <p>
 * Classes annotated with {@code @PluginFunctions} must have a public static method
 * named {@code getFunctions()} that returns a collection of {@link io.questdb.griffin.FunctionFactory}.
 * <p>
 * This annotation is optional - the plugin system will also discover classes with
 * a static {@code getFunctions()} method even without this annotation. However,
 * using the annotation makes the intent explicit and improves discoverability.
 * <p>
 * Example usage:
 * <pre>{@code
 * @PluginFunctions
 * public class MyPluginFunctions {
 *     public static ObjList<FunctionFactory> getFunctions() {
 *         return UDFRegistry.functions(
 *             UDFRegistry.scalar("my_square", Double.class, Double.class, x -> x * x),
 *             UDFRegistry.scalar("my_cube", Double.class, Double.class, x -> x * x * x)
 *         );
 *     }
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PluginFunctions {
    /**
     * Optional description of the functions provided by this class.
     */
    String description() default "";

    /**
     * Optional version string for the plugin functions (e.g., "1.0.0").
     */
    String version() default "";

    /**
     * Optional author or organization name.
     */
    String author() default "";

    /**
     * Optional URL for documentation or project homepage.
     */
    String url() default "";

    /**
     * Optional license identifier (e.g., "Apache-2.0", "MIT").
     */
    String license() default "";
}
