/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std.str;

import com.questdb.std.*;
import com.questdb.std.microtime.DateFormatUtils;

import java.lang.ThreadLocal;
import java.util.Set;

public abstract class AbstractCharSink implements CharSink {

    private static final ThreadLocal<ObjHashSet<Throwable>> tlSet = ThreadLocal.withInitial(ObjHashSet::new);

    @Override
    public CharSink encodeUtf8(CharSequence cs) {
        return encodeUtf8(cs, 0, cs.length());
    }

    @Override
    public CharSink encodeUtf8(CharSequence cs, int from, int to) {
        int i = from;
        while (i < to) {
            char c = cs.charAt(i++);
            if (c < 128) {
                putUtf8Special(c);
            } else {
                i = putUtf8Internal(cs, to, i, c);
            }
        }
        return this;
    }

    @Override
    public CharSink putUtf8(char c) {
        if (c < 128) {
            putUtf8Special(c);
        } else if (c < 2048) {
            put((char) (192 | c >> 6)).put((char) (128 | c & 63));
        } else if (Character.isSurrogate(c)) {
            put('?');
        } else {
            put((char) (224 | c >> 12)).put((char) (128 | c >> 6 & 63)).put((char) (128 | c & 63));
        }
        return this;
    }

    @Override
    public CharSink encodeUtf8AndQuote(CharSequence cs) {
        put('\"').encodeUtf8(cs).put('\"');
        return this;
    }

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
    public CharSink put(Sinkable sinkable) {
        sinkable.toSink(this);
        return this;
    }

    @Override
    public CharSink putISODate(long value) {
        DateFormatUtils.appendDateTimeUSec(this, value);
        return this;
    }

    @Override
    public CharSink putISODateMillis(long value) {
        com.questdb.std.time.DateFormatUtils.appendDateTime(this, value);
        return this;
    }

    @Override
    public CharSink putQuoted(CharSequence cs) {
        put('\"').put(cs).put('\"');
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

    private void put0(Throwable e) {
        put(e.getClass().getName());
        if (e.getMessage() != null) {
            put(": ").put(e.getMessage());
        }
    }

    private int putUtf8Internal(CharSequence cs, int hi, int i, char c) {
        if (c < 2048) {
            put((char) (192 | c >> 6)).put((char) (128 | c & 63));
        } else if (Character.isSurrogate(c)) {
            i = encodeSurrogate(c, cs, i, hi);
        } else {
            put((char) (224 | c >> 12)).put((char) (128 | c >> 6 & 63)).put((char) (128 | c & 63));
        }
        return i;
    }

    protected void putUtf8Special(char c) {
        put(c);
    }
}
