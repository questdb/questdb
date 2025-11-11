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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.sql.Record;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;

/**
 * Interface for transforming decimal values in place.
 * <p>
 * Implementations apply mathematical or logical transformations to decimal values
 * of varying precisions (64-bit, 128-bit, or 256-bit storage).
 */
public interface DecimalTransformer {
    /**
     * Returns the name of this transformer, typically used for function naming
     * and error reporting.
     *
     * @return the transformer name
     */
    String getName();

    /**
     * Transforms a 128-bit decimal value in place.
     * <p>
     * The transformer should modify the provided {@code value} object directly.
     * The value's scale is expected to be set before this method is called.
     *
     * @param value  the decimal value to transform, modified in place
     * @param record the current record context, may be used for additional data
     * @return {@code true} if the transformation succeeded and produced a valid result,
     * {@code false} if the transformation failed or the result should be NULL
     */
    boolean transform(Decimal128 value, Record record);

    /**
     * Transforms a 256-bit decimal value in place.
     * <p>
     * The transformer should modify the provided {@code value} object directly.
     * The value's scale is expected to be set before this method is called.
     *
     * @param value  the decimal value to transform, modified in place
     * @param record the current record context, may be used for additional data
     * @return {@code true} if the transformation succeeded and produced a valid result,
     * {@code false} if the transformation failed or the result should be NULL
     */
    boolean transform(Decimal256 value, Record record);

    /**
     * Transforms a 64-bit decimal value in place.
     * <p>
     * The transformer should modify the provided {@code value} object directly.
     * The value's scale is expected to be set before this method is called.
     *
     * @param value  the decimal value to transform, modified in place
     * @param record the current record context, may be used for additional data
     * @return {@code true} if the transformation succeeded and produced a valid result,
     * {@code false} if the transformation failed or the result should be NULL
     */
    boolean transform(Decimal64 value, Record record);
}
