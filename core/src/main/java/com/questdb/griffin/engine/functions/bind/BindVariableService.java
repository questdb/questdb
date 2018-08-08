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

package com.questdb.griffin.engine.functions.bind;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.Function;
import com.questdb.griffin.SqlException;
import com.questdb.std.BinarySequence;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Chars;

public class BindVariableService {
    private final CharSequenceObjHashMap<Function> variables = new CharSequenceObjHashMap<>();

    public void clear() {
        variables.clear();
    }

    public Function getFunction(CharSequence name) {
        assert name != null;
        assert Chars.startsWith(name, ':');

        int index = variables.keyIndex(name, 1, name.length());
        if (index > -1) {
            return null;
        }
        return variables.valueAt(index);
    }

    public void setBin(CharSequence name, BinarySequence value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new BinBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof BinBindVariable) {
                ((BinBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setBoolean(CharSequence name, boolean value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new BooleanBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof BooleanBindVariable) {
                ((BooleanBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setByte(CharSequence name, byte value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new ByteBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof ByteBindVariable) {
                ((ByteBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setDate(CharSequence name, long value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new DateBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof DateBindVariable) {
                ((DateBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setDouble(CharSequence name, double value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new DoubleBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof DoubleBindVariable) {
                ((DoubleBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setFloat(CharSequence name, float value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new FloatBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof FloatBindVariable) {
                ((FloatBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setInt(CharSequence name, int value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new IntBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof IntBindVariable) {
                ((IntBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setLong(CharSequence name, long value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new LongBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof LongBindVariable) {
                ((LongBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setShort(CharSequence name, short value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new ShortBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof ShortBindVariable) {
                ((ShortBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setStr(CharSequence name, CharSequence value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new StrBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof StrBindVariable) {
                ((StrBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setTimestamp(CharSequence name, long value) throws SqlException {
        int index = variables.keyIndex(name);
        if (index > -1) {
            variables.putAt(index, name, new TimestampBindVariable(value));
        } else {
            Function function = variables.valueAt(index);
            if (function instanceof TimestampBindVariable) {
                ((TimestampBindVariable) function).value = value;
            } else {
                throw SqlException.position(0).put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }
}
