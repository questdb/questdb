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

package com.questdb.griffin.engine.functions.bind;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.sql.Function;
import com.questdb.std.BinarySequence;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Chars;
import com.questdb.std.ObjList;

public class BindVariableService {
    private final CharSequenceObjHashMap<Function> namedVariables = new CharSequenceObjHashMap<>();
    private final ObjList<Function> indexedVariables = new ObjList<>();

    public void clear() {
        namedVariables.clear();
        indexedVariables.clear();
    }

    public int getIndexedVariableCount() {
        return indexedVariables.size();
    }

    public Function getFunction(CharSequence name) {
        assert name != null;
        assert Chars.startsWith(name, ':');

        int index = namedVariables.keyIndex(name, 1, name.length());
        if (index > -1) {
            return null;
        }
        return namedVariables.valueAt(index);
    }

    public Function getFunction(int index) {
        if (index < indexedVariables.size()) {
            return indexedVariables.getQuick(index);
        }
        return null;
    }

    public void setBin(CharSequence name, BinarySequence value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new BinBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof BinBindVariable) {
                ((BinBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setBin(int index, BinarySequence value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new BinBindVariable(value));
            } else if (function instanceof BinBindVariable) {
                ((BinBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new BinBindVariable(value));
        }
    }

    public void setBoolean(CharSequence name, boolean value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new BooleanBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof BooleanBindVariable) {
                ((BooleanBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setBoolean(int index, boolean value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new BooleanBindVariable(value));
            } else if (function instanceof BooleanBindVariable) {
                ((BooleanBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new BooleanBindVariable(value));
        }
    }

    public void setByte(CharSequence name, byte value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new ByteBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof ByteBindVariable) {
                ((ByteBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setByte(int index, byte value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new ByteBindVariable(value));
            } else if (function instanceof ByteBindVariable) {
                ((ByteBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new ByteBindVariable(value));
        }
    }

    public void setDate(CharSequence name, long value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new DateBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof DateBindVariable) {
                ((DateBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setDate(int index, long value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new DateBindVariable(value));
            } else if (function instanceof DateBindVariable) {
                ((DateBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new DateBindVariable(value));
        }
    }

    public void setDouble(CharSequence name, double value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new DoubleBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof DoubleBindVariable) {
                ((DoubleBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setDouble(int index, double value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new DoubleBindVariable(value));
            } else if (function instanceof DoubleBindVariable) {
                ((DoubleBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new DoubleBindVariable(value));
        }
    }

    public void setFloat(CharSequence name, float value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new FloatBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof FloatBindVariable) {
                ((FloatBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setFloat(int index, float value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new FloatBindVariable(value));
            } else if (function instanceof FloatBindVariable) {
                ((FloatBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new FloatBindVariable(value));
        }
    }

    public void setInt(CharSequence name, int value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new IntBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof IntBindVariable) {
                ((IntBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setInt(int index, int value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new IntBindVariable(value));
            } else if (function instanceof IntBindVariable) {
                ((IntBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new IntBindVariable(value));
        }
    }

    public void setLong(CharSequence name, long value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new LongBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof LongBindVariable) {
                ((LongBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setLong(int index, long value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new LongBindVariable(value));
            } else if (function instanceof LongBindVariable) {
                ((LongBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new LongBindVariable(value));
        }
    }

    public void setShort(int index, short value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new ShortBindVariable(value));
            } else if (function instanceof ShortBindVariable) {
                ((ShortBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new ShortBindVariable(value));
        }
    }

    public void setShort(CharSequence name, short value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new ShortBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof ShortBindVariable) {
                ((ShortBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setStr(int index, CharSequence value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new StrBindVariable(value));
            } else if (function instanceof StrBindVariable) {
                ((StrBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new StrBindVariable(value));
        }
    }

    public void setStr(CharSequence name, CharSequence value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new StrBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof StrBindVariable) {
                ((StrBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }

    public void setTimestamp(int index, long value) {
        if (index < indexedVariables.size()) {
            Function function = indexedVariables.getQuick(index);
            if (function == null) {
                indexedVariables.setQuick(index, new TimestampBindVariable(value));
            } else if (function instanceof TimestampBindVariable) {
                ((TimestampBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable at ").put(index).put(" is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        } else {
            indexedVariables.extendAndSet(index, new TimestampBindVariable(value));
        }
    }

    public void setTimestamp(CharSequence name, long value) {
        int index = namedVariables.keyIndex(name);
        if (index > -1) {
            namedVariables.putAt(index, name, new TimestampBindVariable(value));
        } else {
            Function function = namedVariables.valueAt(index);
            if (function instanceof TimestampBindVariable) {
                ((TimestampBindVariable) function).value = value;
            } else {
                throw BindException.init().put("bind variable '").put(name).put("' is already defined as ").put(ColumnType.nameOf(function.getType()));
            }
        }
    }
}
