/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.io.sink;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

@SuppressFBWarnings({"CD_CIRCULAR_DEPENDENCY", "SCII_SPOILED_CHILD_INTERFACE_IMPLEMENTOR"})
public class StringSink extends AbstractCharSink implements CharSequence {
    private final StringBuilder builder = new StringBuilder();

    public void clear() {
        builder.setLength(0);
    }

    public void clear(int pos) {
        builder.setLength(pos);
    }

    @Override
    public void flush() {
    }

    @Override
    public CharSink put(CharSequence cs) {
        builder.append(cs);
        return this;
    }

    @Override
    public CharSink put(char c) {
        builder.append(c);
        return this;
    }

    @Override
    public int length() {
        return builder.length();
    }

    @Override
    public char charAt(int index) {
        return builder.charAt(index);
    }

    @Override
    public CharSequence subSequence(int lo, int hi) {
        return builder.subSequence(lo, hi);
    }

    /* Either IDEA or FireBug complain, annotation galore */
    @SuppressFBWarnings({"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"})
    @NotNull
    @Override
    public String toString() {
        return builder.toString();
    }
}
