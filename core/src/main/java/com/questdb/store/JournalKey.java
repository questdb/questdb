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

package com.questdb.store;

import com.questdb.std.ByteBuffers;
import com.questdb.store.factory.configuration.Constants;

import java.nio.ByteBuffer;

public class JournalKey<T> {
    private final Class<T> modelClass;
    private final String name;
    private final int partitionBy;
    private final int recordHint;
    private final boolean ordered;

    public JournalKey(String name) {
        this(null, name);
    }

    public JournalKey(Class<T> clazz) {
        this(clazz, null);
    }

    public JournalKey(Class<T> clazz, String name) {
        this(clazz, name, PartitionBy.DEFAULT);
    }

    public JournalKey(Class<T> clazz, String name, int partitionBy) {
        this(clazz, name, partitionBy, Constants.NULL_RECORD_HINT);
    }

    public JournalKey(Class<T> clazz, String name, int partitionBy, int recordHint) {
        this(clazz, name, partitionBy, recordHint, true);
    }

    public JournalKey(Class<T> clazz, String name, int partitionBy, int recordHint, boolean ordered) {
        this.modelClass = clazz;
        this.name = name == null ? clazz.getName() : name;
        this.partitionBy = partitionBy;
        this.recordHint = recordHint;
        this.ordered = ordered;
    }

    public static <X> JournalKey<X> fromBuffer(ByteBuffer buffer) {
        // name
        int locLen = buffer.getInt();
        char[] name;
        name = new char[locLen];
        for (int i = 0; i < name.length; i++) {
            name[i] = buffer.getChar();
        }
        // partitionBy
        int partitionBy = buffer.get();
        // recordHint
        int recordHint = buffer.getInt();
        // ordered
        boolean ordered = buffer.get() == 1;

        return new JournalKey<>(null, new String(name), partitionBy, recordHint, ordered);
    }

    public int getBufferSize() {
        return 4 + 2 * (name == null ? 0 : name.length()) + 1 + 1 + 4;
    }

    public Class<T> getModelClass() {
        return modelClass;
    }

    public String getModelClassName() {
        return modelClass == null ? null : modelClass.getName();
    }

    public String getName() {
        return name;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public int getRecordHint() {
        return recordHint;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JournalKey<?> that = (JournalKey<?>) o;

        return name.equals(that.name);
    }

    @Override
    public String toString() {
        return "JournalKey{" +
                "modelClass=" + modelClass +
                ", name='" + name + '\'' +
                ", partitionBy=" + partitionBy +
                ", recordHint=" + recordHint +
                ", ordered=" + ordered +
                '}';
    }

    public boolean isOrdered() {
        return ordered;
    }

    public void write(ByteBuffer buffer) {
        // location
        ByteBuffers.putStringDW(buffer, name);
        // partition type
        buffer.put((byte) partitionBy);
        // recordHint
        buffer.putInt(recordHint);
        // ordered
        buffer.put((byte) (ordered ? 1 : 0));
    }
}
