/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std.str;

import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;

import java.util.Set;

import static io.questdb.std.Numbers.hexDigits;

public abstract class AbstractCharSink implements CharSink {

    private static final ThreadLocal<ObjHashSet<Throwable>> tlSet = ThreadLocal.withInitial(ObjHashSet::new);
    private final char[] doubleDigits = new char[21];

    @Override
    public char[] getDoubleDigitsBuffer() {
        return doubleDigits;
    }

    @Override
    public CharSink put(Throwable e) {
        ObjHashSet<Throwable> dejaVu = tlSet.get();
        dejaVu.add(e);
        put0(e);
        put(Misc.EOL);

        StackTraceElement[] trace = e.getStackTrace();
        for (int i = 0, n = trace.length; i < n; i++) {
            put(trace[i]);
        }

        // Print suppressed exceptions, if any
        Throwable[] suppressed = e.getSuppressed();
        for (int i = 0, n = suppressed.length; i < n; i++) {
            put(suppressed[i], trace, "Suppressed: ", "\t", dejaVu);
        }

        // Print cause, if any
        Throwable ourCause = e.getCause();
        if (ourCause != null) {
            put(ourCause, trace, "Caused by: ", "", dejaVu);
        }

        return this;
    }

    public void putAsPrintable(CharSequence nonPrintable) {
        for (int i = 0, n = nonPrintable.length(); i < n; i++) {
            char c = nonPrintable.charAt(i);
            if (c > 0x1F && c != 0x7F) {
                put(c);
            } else {
                put('\\');
                put('u');

                final int s = (int) c & 0xFF;
                put('0');
                put('0');
                put(hexDigits[s / 0x10]);
                put(hexDigits[s % 0x10]);
            }
        }
    }

    private void put(StackTraceElement e) {
        put("\tat ");
        put(e.getClassName());
        put('.');
        put(e.getMethodName());
        if (e.isNativeMethod()) {
            put("(Native Method)");
        } else {
            if (e.getFileName() != null && e.getLineNumber() > -1) {
                put('(').put(e.getFileName()).put(':').put(e.getLineNumber()).put(')');
            } else if (e.getFileName() != null) {
                put('(').put(e.getFileName()).put(')');
            } else {
                put("(Unknown Source)");
            }
        }
        put(Misc.EOL);
    }

    @Override
    public int encodeSurrogate(char c, CharSequence in, int pos, int hi) {
        int dword;
        if (Character.isHighSurrogate(c)) {
            if (hi - pos < 1) {
                put('?');
                return pos;
            } else {
                char c2 = in.charAt(pos++);
                if (Character.isLowSurrogate(c2)) {
                    dword = Character.toCodePoint(c, c2);
                } else {
                    put('?');
                    return pos;
                }
            }
        } else if (Character.isLowSurrogate(c)) {
            put('?');
            return pos;
        } else {
            dword = c;
        }
        put((char) (240 | dword >> 18)).
                put((char) (128 | dword >> 12 & 63)).
                put((char) (128 | dword >> 6 & 63)).
                put((char) (128 | dword & 63));
        return pos;
    }

    private void put(Throwable throwable, StackTraceElement[] enclosingTrace, String caption, String prefix, Set<Throwable> dejaVu) {
        if (dejaVu.contains(throwable)) {
            put("\t[CIRCULAR REFERENCE:");
            put0(throwable);
            put(']');
        } else {
            dejaVu.add(throwable);

            // Compute number of frames in common between this and enclosing trace
            StackTraceElement[] trace = throwable.getStackTrace();
            int m = trace.length - 1;
            int n = enclosingTrace.length - 1;
            while (m >= 0 && n >= 0 && trace[m].equals(enclosingTrace[n])) {
                m--;
                n--;
            }
            int framesInCommon = trace.length - 1 - m;

            put(prefix).put(caption);
            put0(throwable);
            put(Misc.EOL);

            for (int i = 0; i <= m; i++) {
                put(prefix);
                put(trace[i]);
            }
            if (framesInCommon != 0) {
                put(prefix).put("\t...").put(framesInCommon).put(" more");
            }

            // Print suppressed exceptions, if any
            Throwable[] suppressed = throwable.getSuppressed();
            for (int i = 0, k = suppressed.length; i < k; i++) {
                put(suppressed[i], trace, "Suppressed: ", prefix + '\t', dejaVu);
            }

            // Print cause, if any
            Throwable cause = throwable.getCause();
            if (cause != null) {
                put(cause, trace, "Caused by: ", prefix, dejaVu);
            }
        }
    }

    public CharSink repeat(CharSequence value, int n) {
        for (int i = 0; i < n; i++) {
            put(value);
        }
        return this;
    }

    private void put0(Throwable e) {
        put(e.getClass().getName());
        if (e.getMessage() != null) {
            put(": ").put(e.getMessage());
        }
    }
}
