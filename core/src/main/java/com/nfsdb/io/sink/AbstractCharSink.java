/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.io.sink;

import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Numbers;

import java.io.IOException;

public abstract class AbstractCharSink implements CharSink {
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
    public CharSink putISODate(long value) {
        Dates.appendDateTime(this, value);
        return this;
    }

    @Override
    public CharSink putTrim(double value, int scale) {
        Numbers.appendTrim(this, value, scale);
        return this;
    }

    @Override
    public Appendable append(CharSequence csq) throws IOException {
        put(csq);
        return this;
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) throws IOException {
        for(int i = start; i < end; i++) {
            put(csq.charAt(i));
        }
        return this;
    }

    @Override
    public Appendable append(char c) throws IOException {
        put(c);
        return this;
    }

}
