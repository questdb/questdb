/*
 * @(#)FastDoubleParserHandPickedTest.java
 * Copyright © 2021. Werner Randelshofer, Switzerland. MIT License.
 */

package io.questdb.std.fastdouble;

import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;

public class FastDoubleParserFromMemHandPickedTest extends AbstractDoubleHandPickedTest {
    @Override
    double parse(CharSequence str, boolean rejectOverflow) throws NumericException {
        int len = str.length();
        long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Chars.asciiStrCpy(str, len, mem);
            return FastDoubleParser.parseDouble(mem, len, rejectOverflow);
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Override
    protected double parse(String str, int offset, int length, boolean rejectOverflow) throws NumericException {
        int len = offset + length;
        long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Chars.asciiStrCpy(str, len, mem);
            return FastDoubleParser.parseDouble(mem, offset, length, rejectOverflow);
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
