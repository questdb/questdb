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

package io.questdb.test.griffin.fuzz.types;

import io.questdb.std.Rnd;
import io.questdb.test.griffin.fuzz.expr.FuzzConstant;

public final class CharType implements FuzzColumnType {
    public static final CharType INSTANCE = new CharType();

    // Code points that sit on a boundary the filter layer treats specially.
    // A CHAR lives in a 16-bit lane the JIT backends read as signed, so
    // everything from 0x8000 up is where an unsigned CHAR order and a signed
    // lane order part ways; 0 is the CHAR null sentinel and reaches the
    // literal path only through the null keyword. Surrogates (0xD800-0xDFFF)
    // are left out: a lone surrogate does not survive a round trip through
    // any UTF-8 sink, which would turn the dump file and the log line into
    // an unusable repro.
    private static final char[] CORNER_CHARS = {
            '\u0001', '\u007f', '\u0080', '\u00ff', '\u0100',
            '\u7ffe', '\u7fff', '\u8000', '\u8001', '\ufffe', '\uffff'
    };

    private CharType() {
    }

    @Override
    public FuzzConstant generateConstant(Rnd rnd) {
        if (rnd.nextInt(32) == 0) {
            return FuzzConstant.nonBindable("null");
        }
        // rnd.nextChar() only draws 'B'..'Z', so on its own it never reaches
        // the half of the code space where the signed lane goes negative.
        final char c = rnd.nextInt(3) == 0
                ? CORNER_CHARS[rnd.nextInt(CORNER_CHARS.length)]
                : rnd.nextChar();
        return new FuzzConstant("'" + c + "'", "CHAR", String.valueOf(c));
    }

    @Override
    public String getDdl() {
        return "CHAR";
    }

    @Override
    public ColumnKind getKind() {
        return ColumnKind.CHAR;
    }

    @Override
    public String getRndCall() {
        // rnd_char() draws 'B'..'Z' and never NULL, which left both halves of
        // the CHAR ordering contract - the null sentinel a range predicate has
        // to drop, and the code points a signed 16-bit lane reads as negative
        // - out of the corpus. The two cast branches cover the rest of the
        // code space either side of the surrogate range; the null rate on
        // rnd_int() reaches CHAR through the INT_NULL -> 0 cast.
        return "CASE WHEN rnd_boolean() THEN rnd_char()"
                + " WHEN rnd_boolean() THEN rnd_int(1, 55_295, 6)::CHAR"
                + " ELSE rnd_int(57_344, 65_535, 6)::CHAR END";
    }

    @Override
    public String randomLiteral(Rnd rnd) {
        return generateConstant(rnd).literal();
    }
}
