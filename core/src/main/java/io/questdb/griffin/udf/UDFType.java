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

import io.questdb.cairo.ColumnType;

/**
 * Type mapping between Java types and QuestDB column types for UDFs.
 * <p>
 * Supported types:
 * <ul>
 *   <li>{@link Double} / double - maps to DOUBLE</li>
 *   <li>{@link Long} / long - maps to LONG</li>
 *   <li>{@link Integer} / int - maps to INT</li>
 *   <li>{@link String} - maps to STRING</li>
 *   <li>{@link Boolean} / boolean - maps to BOOLEAN</li>
 *   <li>{@link Float} / float - maps to FLOAT</li>
 *   <li>{@link Short} / short - maps to SHORT</li>
 *   <li>{@link Byte} / byte - maps to BYTE</li>
 *   <li>{@link Character} / char - maps to CHAR</li>
 * </ul>
 */
public final class UDFType {

    private UDFType() {
        // utility class
    }

    /**
     * Get the QuestDB column type for a Java class.
     *
     * @param clazz the Java class
     * @return the corresponding QuestDB column type
     * @throws IllegalArgumentException if the type is not supported
     */
    public static int toColumnType(Class<?> clazz) {
        if (clazz == Double.class || clazz == double.class) {
            return ColumnType.DOUBLE;
        } else if (clazz == Long.class || clazz == long.class) {
            return ColumnType.LONG;
        } else if (clazz == Integer.class || clazz == int.class) {
            return ColumnType.INT;
        } else if (clazz == String.class || clazz == CharSequence.class) {
            return ColumnType.STRING;
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return ColumnType.BOOLEAN;
        } else if (clazz == Float.class || clazz == float.class) {
            return ColumnType.FLOAT;
        } else if (clazz == Short.class || clazz == short.class) {
            return ColumnType.SHORT;
        } else if (clazz == Byte.class || clazz == byte.class) {
            return ColumnType.BYTE;
        } else if (clazz == Character.class || clazz == char.class) {
            return ColumnType.CHAR;
        }
        throw new IllegalArgumentException("Unsupported UDF type: " + clazz.getName());
    }

    /**
     * Get the signature type code for a Java class.
     * Uses uppercase (non-constant) type codes.
     *
     * @param clazz the Java class
     * @return the signature type character
     * @throws IllegalArgumentException if the type is not supported
     */
    public static char toSignatureChar(Class<?> clazz) {
        if (clazz == Double.class || clazz == double.class) {
            return 'D'; // Double
        } else if (clazz == Long.class || clazz == long.class) {
            return 'L'; // Long
        } else if (clazz == Integer.class || clazz == int.class) {
            return 'I'; // Int
        } else if (clazz == String.class || clazz == CharSequence.class) {
            return 'S'; // String
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return 'T'; // Boolean (T for True/false)
        } else if (clazz == Float.class || clazz == float.class) {
            return 'F'; // Float
        } else if (clazz == Short.class || clazz == short.class) {
            return 'E'; // Short (E)
        } else if (clazz == Byte.class || clazz == byte.class) {
            return 'B'; // Byte
        } else if (clazz == Character.class || clazz == char.class) {
            return 'A'; // Char (A)
        }
        throw new IllegalArgumentException("Unsupported UDF type: " + clazz.getName());
    }

    /**
     * Build a signature string for a function.
     *
     * @param name       function name
     * @param inputTypes input parameter types
     * @return the signature string (e.g., "my_func(DD)")
     */
    public static String buildSignature(String name, Class<?>... inputTypes) {
        StringBuilder sb = new StringBuilder(name);
        sb.append('(');
        for (Class<?> inputType : inputTypes) {
            sb.append(toSignatureChar(inputType));
        }
        sb.append(')');
        return sb.toString();
    }
}
