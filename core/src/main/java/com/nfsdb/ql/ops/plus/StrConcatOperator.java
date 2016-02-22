/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.ops.plus;

import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.ops.AbstractBinaryOperator;
import com.nfsdb.ql.ops.Function;
import com.nfsdb.std.SplitCharSequence;
import com.nfsdb.store.ColumnType;

public class StrConcatOperator extends AbstractBinaryOperator {
    public final static StrConcatOperator FACTORY = new StrConcatOperator();

    private final SplitCharSequence cs = new SplitCharSequence();

    private StrConcatOperator() {
        super(ColumnType.STRING);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        cs.init(lhs.getFlyweightStr(rec), rhs.getFlyweightStr(rec));
        return cs;
    }

    @Override
    public CharSequence getStr(Record rec) {
        cs.init(lhs.getStr(rec), rhs.getStr(rec));
        return cs;
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        lhs.getStr(rec, sink);
        rhs.getStr(rec, sink);
    }

    @Override
    public int getStrLen(Record rec) {
        return lhs.getStrLen(rec) + rhs.getStrLen(rec);
    }

    @Override
    public Function newInstance() {
        return new StrConcatOperator();
    }
}
