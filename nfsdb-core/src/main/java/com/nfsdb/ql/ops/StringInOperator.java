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

package com.nfsdb.ql.ops;

import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.ql.RecordSourceState;
import com.nfsdb.ql.parser.ParserException;
import com.nfsdb.storage.ColumnType;

public class StringInOperator extends AbstractVirtualColumn implements Function {

    private final ObjHashSet<CharSequence> set = new ObjHashSet<>();
    private VirtualColumn lhs;
    private int keyIndex;

    public StringInOperator() {
        super(ColumnType.BOOLEAN);
    }

    @Override
    public void configureSource(RecordSourceState state) {
        super.configureSource(state);
        lhs.configureSource(state);
    }

    @Override
    public boolean getBool() {
        return set.contains(lhs.getFlyweightStr());
    }

    @Override
    public boolean isConstant() {
        return lhs.isConstant();
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        if (pos == keyIndex) {
            lhs = arg;
        } else {
            set.add(arg.getStr().toString());
        }
    }

    @Override
    public void setArgCount(int count) {
        this.keyIndex = count - 1;
    }
}
