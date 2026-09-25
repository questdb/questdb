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

import io.questdb.cairo.ColumnTypeTag;
import io.questdb.griffin.ExpressionParser;
import io.questdb.griffin.FunctionParser;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.lang.reflect.Method;

/**
 * Pins, per tag, the two parse-time cast-target predicates: whether a type name becomes a type
 * constant in {@code FunctionParser.createConstant} (the cast target of {@code cast(x as T)}),
 * and whether {@code ExpressionParser} refuses {@code T} as a cast target, from a value and from
 * {@code null}. Both were ranges over tag numbers; now they are exhaustive switches, and this
 * table is what the switches must keep saying.
 * <p>
 * Cell notation: {@code X} true, {@code .} false. The predicates are package-private, hence
 * reflection.
 */
public class CastTargetTagTest {

    @Test
    public void testCastTargetTags() throws Exception {
        final Method isTypeConstantTag = FunctionParser.class.getDeclaredMethod("isTypeConstantTag", ColumnTypeTag.class);
        isTypeConstantTag.setAccessible(true);
        final Method cannotCastTo = ExpressionParser.class.getDeclaredMethod("cannotCastTo", int.class, boolean.class);
        cannotCastTo.setAccessible(true);

        final StringSink sink = new StringSink();
        sink.put("tag           typeConst castFromValue castFromNull\n");
        for (ColumnTypeTag tag : ColumnTypeTag.values()) {
            sink.put(tag.name());
            for (int i = tag.name().length(); i < 14; i++) {
                sink.put(' ');
            }
            sink.put((boolean) isTypeConstantTag.invoke(null, tag) ? 'X' : '.').put("         ");
            sink.put((boolean) cannotCastTo.invoke(null, (int) tag.code(), false) ? '.' : 'X').put("             ");
            sink.put((boolean) cannotCastTo.invoke(null, (int) tag.code(), true) ? '.' : 'X').put('\n');
        }
        TestUtils.assertEquals(
                """
                        tag           typeConst castFromValue castFromNull
                        UNDEFINED     .         .             .
                        BOOLEAN       X         X             X
                        BYTE          X         X             X
                        SHORT         X         X             X
                        CHAR          X         X             X
                        INT           X         X             X
                        LONG          X         X             X
                        DATE          X         X             X
                        TIMESTAMP     X         X             X
                        FLOAT         X         X             X
                        DOUBLE        X         X             X
                        STRING        X         X             X
                        SYMBOL        X         X             X
                        LONG256       X         X             X
                        GEOBYTE       X         .             .
                        GEOSHORT      X         .             .
                        GEOINT        X         .             .
                        GEOLONG       X         .             .
                        BINARY        X         .             X
                        UUID          X         X             X
                        CURSOR        .         .             .
                        VAR_ARG       .         .             .
                        RECORD        .         .             .
                        GEOHASH       .         .             .
                        LONG128       .         .             .
                        IPv4          X         X             X
                        VARCHAR       X         X             X
                        ARRAY         X         X             X
                        DECIMAL8      .         .             .
                        DECIMAL16     .         .             .
                        DECIMAL32     .         .             .
                        DECIMAL64     .         .             .
                        DECIMAL128    .         .             .
                        DECIMAL256    .         .             .
                        DECIMAL       .         X             X
                        REGCLASS      X         .             .
                        REGPROCEDURE  X         .             .
                        ARRAY_STRING  X         .             .
                        PARAMETER     .         .             .
                        INTERVAL      X         .             X
                        VARCHAR_SLICE .         .             .
                        NULL          .         .             .
                        UNKNOWN       .         .             .
                        """,
                sink
        );
    }
}
