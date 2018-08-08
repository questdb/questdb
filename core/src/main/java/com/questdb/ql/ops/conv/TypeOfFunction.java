/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.ops.conv;

import com.questdb.ex.ParserException;
import com.questdb.ql.ops.AbstractUnaryOperator;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.ql.ops.VirtualColumnFactory;
import com.questdb.store.ColumnType;
import com.questdb.store.Record;
import com.questdb.store.SymbolTable;

public class TypeOfFunction extends AbstractUnaryOperator implements SymbolTable {

    public final static VirtualColumnFactory<Function> FACTORY = (position, configuration) -> new TypeOfFunction(position);

    private int valueType;
    private String typeName;
    private int typeNameLen;

    private TypeOfFunction(int position) {
        super(ColumnType.SYMBOL, position);
    }

    @Override
    public CharSequence getFlyweightStr(Record rec) {
        return typeName;
    }

    @Override
    public CharSequence getFlyweightStrB(Record rec) {
        return getFlyweightStr(rec);
    }

    @Override
    public int getInt(Record rec) {
        return valueType;
    }

    @Override
    public int getStrLen(Record rec) {
        return typeNameLen;
    }

    @Override
    public String getSym(Record rec) {
        return typeName;
    }

    @Override
    public SymbolTable getSymbolTable() {
        return this;
    }

    @Override
    public int getQuick(CharSequence value) {
        return valueType;
    }

    @Override
    public int size() {
        return ColumnType.count();
    }

    @Override
    public String value(int key) {
        return typeName;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public void setArg(int pos, VirtualColumn arg) throws ParserException {
        super.setArg(pos, arg);
        valueType = value.getType();
        typeName = ColumnType.nameOf(valueType);
        typeNameLen = typeName.length();
    }
}
