/*******************************************************************************
 * _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 * <p/>
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.io.sink;

import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.ObjHashSet;

import java.util.Set;

public abstract class AbstractCharSink implements CharSink {

    private static final ThreadLocal<ObjHashSet<Throwable>> tlSet = new ThreadLocal<ObjHashSet<Throwable>>() {
        @Override
        protected ObjHashSet<Throwable> initialValue() {
            return new ObjHashSet<>();
        }
    };

    @Override
    public CharSink put(int value) {
        Numbers.append(this, value);
        return this;
    }

    @Override
    public CharSink put(long value) {
        Numbers.append(this, value);
        return this;
    }

    @Override
    public CharSink put(float value, int scale) {
        Numbers.append(this, value, scale);
        return this;
    }

    @Override
    public CharSink put(double value, int scale) {
        Numbers.append(this, value, scale);
        return this;
    }

    @Override
    public CharSink put(boolean value) {
        this.put(value ? "true" : "false");
        return this;
    }

    @Override
    public CharSink put(Throwable e) {
        ObjHashSet<Throwable> dejaVu = tlSet.get();
        dejaVu.add(e);
        put0(e);
        put(Misc.EOL);

        StackTraceElement[] trace = e.getStackTrace();
        for (int i = 0, n = trace.length; i < n; i++) {
            put(Unsafe.arrayGet(trace, i));
        }

        // Print suppressed exceptions, if any
        Throwable[] suppressed = e.getSuppressed();
        for (int i = 0, n = suppressed.length; i < n; i++) {
            put(Unsafe.arrayGet(suppressed, i), trace, "Suppressed: ", "\t", dejaVu);
        }

        // Print cause, if any
        Throwable ourCause = e.getCause();
        if (ourCause != null) {
            put(ourCause, trace, "Caused by: ", "", dejaVu);
        }

        return this;
    }

    @Override
    public CharSink putISODate(long value) {
        Dates.appendDateTime(this, value);
        return this;
    }

    @Override
    public CharSink putQuoted(CharSequence cs) {
        put('\"').put(cs).put('\"');
        return this;
    }

    @Override
    public CharSink putTrim(double value, int scale) {
        Numbers.appendTrim(this, value, scale);
        return this;
    }

    @Override
    public CharSink putUtf8Escaped(CharSequence cs) {
        int hi = cs.length();
        int i = 0;
        while (i < hi) {
            char c = cs.charAt(i++);
            if (c > 31 && c < 128) {
                switch (c) {
                    case '/':
                    case '\"':
                    case '\\':
                        put('\\');
                        // intentional fall through
                    default:
                        put(c);
                        break;
                }
            } else if (c < 32) {
                putEscaped(c);
            } else if (c < 2048) {
                put((char) (192 | c >> 6)).put((char) (128 | c & 63));
            } else if (Character.isSurrogate(c)) {
                i = encodeSurrogate(c, cs, i, hi);
            } else {
                put((char) (224 | c >> 12)).put((char) (128 | c >> 6 & 63)).put((char) (128 | c & 63));
            }
        }
        return this;
    }

    @Override
    public CharSink putUtf8EscapedAndQuoted(CharSequence cs) {
        put('\"').putUtf8Escaped(cs).put('\"');
        return this;
    }

    private int encodeSurrogate(char c, CharSequence in, int pos, int hi) {
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
                put(Unsafe.arrayGet(trace, i));
            }
            if (framesInCommon != 0) {
                put(prefix).put("\t...").put(framesInCommon).put(" more");
            }

            // Print suppressed exceptions, if any
            Throwable[] suppressed = throwable.getSuppressed();
            for (int i = 0, k = suppressed.length; i < k; i++) {
                put(Unsafe.arrayGet(suppressed, i), trace, "Suppressed: ", prefix + '\t', dejaVu);
            }

            // Print cause, if any
            Throwable cause = throwable.getCause();
            if (cause != null) {
                put(cause, trace, "Caused by: ", prefix, dejaVu);
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

    private void put0(Throwable e) {
        put(e.getClass().getName());
        if (e.getMessage() != null) {
            put(": ").put(e.getMessage());
        }
    }

    private void putEscaped(char c) {
        switch (c) {
            case '\b':
                put("\\b");
                break;
            case '\f':
                put("\\f");
                break;
            case '\n':
                put("\\n");
                break;
            case '\r':
                put("\\r");
                break;
            case '\t':
                put("\\t");
                break;
            default:
                put(c);
                break;
        }
    }
}
