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

package com.questdb.printer;


import com.questdb.printer.appender.Appender;
import com.questdb.printer.appender.StdOutAppender;
import com.questdb.printer.converter.*;
import com.questdb.std.Unsafe;
import com.questdb.store.JournalRuntimeException;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

public class JournalPrinter implements Closeable {

    private static final long OBJECT_VALUE_OFFSET = -1;
    private final StringBuilder rowBuilder = new StringBuilder();
    private final List<Field> ff = new ArrayList<>();
    private Class[] typeTemplate = new Class[]{};
    private String delimiter = "\t";
    private Appender appender = StdOutAppender.INSTANCE;
    private String nullString;
    private boolean configured = false;

    public void close() {
        appender.close();
        ff.clear();
        configured = false;
    }

    public Field f(String name) {
        Field field = new Field(name, this);
        ff.add(field);
        return field;
    }

    public String getNullString() {
        return nullString;
    }

    public JournalPrinter setNullString(String forNull) {
        nullString = forNull;
        return this;
    }

    public void header() {
        rowBuilder.setLength(0);
        for (int i = 0; i < ff.size(); i++) {
            if (i > 0) {
                rowBuilder.append(delimiter);
            }

            Field f = ff.get(i);
            String header = f.header;
            if (header == null) {
                rowBuilder.append(f.name);
            } else {
                rowBuilder.append(f.header);
            }
        }
        appender.append(rowBuilder);
    }

    public void out(Object... instances) {
        configure();
        rowBuilder.setLength(0);
        for (int i = 0, sz = ff.size(); i < sz; i++) {
            if (i > 0) {
                rowBuilder.append(delimiter);
            }
            Field f = ff.get(i);
            Object instance = instances[f.typeTemplateIndex];
            if (instance != null) {
                f.converter.convert(rowBuilder, f, instances[f.typeTemplateIndex]);
            }
        }
        appender.append(rowBuilder);
    }

    public JournalPrinter setAppender(Appender appender) {
        this.appender = appender;
        return this;
    }

    @SuppressWarnings("unused")
    public JournalPrinter setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public JournalPrinter types(Class... clazz) {
        typeTemplate = clazz;
        return this;
    }

    public Field v(int typeIndex) {
        Field field = new Field(typeIndex, this);
        ff.add(field);
        return field;
    }

    private void configure() {
        if (configured) {
            return;
        }

        try {
            for (int i1 = 0, k = ff.size(); i1 < k; i1++) {
                Field f = ff.get(i1);
                // value field
                if (f.name == null) {
                    f.fromType = getType(f.typeTemplateIndex);
                    f.offset = OBJECT_VALUE_OFFSET;
                } else if (f.typeTemplateIndex == -1) {
                    // reference field without explicit type index

                    // find which type contains this field name
                    // first type in typeTemplate array wins
                    for (int i = 0; i < typeTemplate.length; i++) {
                        Class clazz = typeTemplate[i];
                        java.lang.reflect.Field[] declaredFields = clazz.getDeclaredFields();
                        for (int i2 = 0; i2 < declaredFields.length; i2++) {
                            if (f.name.equals(declaredFields[i2].getName())) {
                                f.fromType = declaredFields[i2].getType();
                                f.typeTemplateIndex = i;
                                break;
                            }
                        }
                        // found type
                        if (f.typeTemplateIndex != -1) {
                            break;
                        }
                    }

                    // finished loop without finding type
                    if (f.typeTemplateIndex == -1) {
                        throw new RuntimeException("No such field: " + f.name);
                    }
                    f.offset = Unsafe.getUnsafe().objectFieldOffset(getType(f.typeTemplateIndex).getDeclaredField(f.name));
                } else {
                    // reference field with known type template index
                    Class t = getType(f.typeTemplateIndex);
                    f.fromType = t.getDeclaredField(f.name).getType();
                    f.offset = Unsafe.getUnsafe().objectFieldOffset(t.getDeclaredField(f.name));
                }

                setConverter(f);
            }
            configured = true;
        } catch (NoSuchFieldException e) {
            throw new JournalRuntimeException(e);
        }
    }

    private Class getType(int typeIndex) {
        if (typeIndex < 0 || typeIndex >= typeTemplate.length) {
            throw new JournalRuntimeException("Invalid index: %d", typeIndex);
        }
        return typeTemplate[typeIndex];
    }

    private void setConverter(Field f) {
        if (f.converter == null) {
            if (f.offset == OBJECT_VALUE_OFFSET) {
                f.converter = new ObjectToStringConverter(this);
            } else if (f.fromType == int.class) {
                f.converter = new IntConverter();
            } else if (f.fromType == long.class) {
                f.converter = new LongConverter();
            } else if (f.fromType == double.class) {
                f.converter = new DoubleConverter();
            } else if (f.fromType == boolean.class) {
                f.converter = new BooleanConverter();
            } else if (f.fromType == byte.class) {
                f.converter = new ByteConverter();
            } else if (f.fromType == String.class) {
                f.converter = new StringConverter(this);
            } else if (f.fromType == short.class) {
                f.converter = new ShortConverter();
            } else {
                throw new JournalRuntimeException("Unsupported type: " + f.fromType.getName());
            }
        }
    }

    public static class Field {
        private final JournalPrinter printer;
        private String header;
        private String name;
        private int typeTemplateIndex = -1;
        private long offset;
        private Class fromType;
        private Converter converter;

        public Field(String name, JournalPrinter printer) {
            this.name = name;
            this.printer = printer;
        }

        public Field(int typeTemplateIndex, JournalPrinter printer) {
            this.typeTemplateIndex = typeTemplateIndex;
            this.printer = printer;
        }

        public Field c(Converter converter) {
            this.converter = converter;
            return this;
        }

        public Field f(String name) {
            return printer.f(name);
        }

        public long getOffset() {
            return offset;
        }

        public Field h(String header) {
            this.header = header;
            return this;
        }

        public Field i(int instanceIndex) {
            this.typeTemplateIndex = instanceIndex;
            return this;
        }

        /**
         * Creates field supplied by value. Typically a calculated field that isn't a part of an object but
         * present in report.
         *
         * @param typeIndex field index in out() method.
         * @return Field instance to support builder pattern.while
         */
        public Field v(int typeIndex) {
            return printer.v(typeIndex);
        }
    }
}

