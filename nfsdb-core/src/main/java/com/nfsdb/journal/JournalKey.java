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

package com.nfsdb.journal;

import com.nfsdb.journal.factory.configuration.Constants;
import com.nfsdb.journal.utils.ByteBuffers;
import com.nfsdb.journal.utils.Files;

import java.nio.ByteBuffer;

public class JournalKey<T> {
    private final String modelClassName;
    private final Class<T> modelClass;
    private String location;
    private PartitionType partitionType = PartitionType.DEFAULT;
    private int recordHint = Constants.NULL_RECORD_HINT;
    private boolean ordered = true;

    public JournalKey(Class<T> clazz) {
        this.modelClass = clazz;
        this.modelClassName = clazz.getName();
    }

    public JournalKey(Class<T> clazz, int recordHint) {
        this.modelClass = clazz;
        this.modelClassName = clazz.getName();
        this.recordHint = recordHint;
    }

    public JournalKey(Class<T> clazz, String location) {
        this.modelClass = clazz;
        this.modelClassName = clazz.getName();
        this.location = location;
    }

    public JournalKey(Class<T> clazz, String location, PartitionType partitionType) {
        this.modelClass = clazz;
        this.modelClassName = clazz.getName();
        this.location = location;
        this.partitionType = partitionType;
    }

    public JournalKey(Class<T> clazz, String location, PartitionType partitionType, int recordHint) {
        this.modelClass = clazz;
        this.modelClassName = clazz.getName();
        this.location = location;
        this.partitionType = partitionType;
        this.recordHint = recordHint;
    }

    public JournalKey(Class<T> clazz, String location, PartitionType partitionType, int recordHint, boolean ordered) {
        this.modelClass = clazz;
        this.modelClassName = clazz.getName();
        this.location = location;
        this.partitionType = partitionType;
        this.recordHint = recordHint;
        this.ordered = ordered;
    }

    public JournalKey(Class<T> clazz, String location, PartitionType partitionType, boolean ordered) {
        this.modelClass = clazz;
        this.modelClassName = clazz.getName();
        this.location = location;
        this.partitionType = partitionType;
        this.ordered = ordered;
    }

    public static JournalKey<Object> fromBuffer(ByteBuffer buffer) {
        // modelClassName
        int clazzLen = buffer.getInt();
        byte[] clazz = new byte[clazzLen];
        buffer.get(clazz);
        // location
        int locLen = buffer.getInt();
        char[] location = null;
        if (locLen > 0) {
            location = new char[locLen];
            for (int i = 0; i < locLen; i++) {
                location[i] = buffer.getChar();
            }
        }
        // partitionType
        PartitionType partitionType = PartitionType.values()[buffer.get()];
        // recordHint
        int recordHint = buffer.getInt();
        // ordered
        boolean ordered = buffer.get() == 1;

        return new JournalKey<>(new String(clazz, Files.UTF_8), location == null ? null : new String(location), partitionType, recordHint, ordered);
    }

    public String getModelClassName() {
        return modelClassName;
    }

    public Class<T> getModelClass() {
        return modelClass;
    }

    public String getLocation() {
        return location;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public int getRecordHint() {
        return recordHint;
    }

    public boolean isOrdered() {
        return ordered;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JournalKey)) return false;
        JournalKey that = (JournalKey) o;
        return ordered == that.ordered && recordHint == that.recordHint && !(modelClassName != null ? !modelClassName.equals(that.modelClassName) : that.modelClassName != null) && !(location != null ? !location.equals(that.location) : that.location != null) && partitionType == that.partitionType;

    }

    @Override
    public int hashCode() {
        int result = modelClassName != null ? modelClassName.hashCode() : 0;
        result = 31 * result + (location != null ? location.hashCode() : 0);
        result = 31 * result + (partitionType != null ? partitionType.hashCode() : 0);
        result = 31 * result + recordHint;
        result = 31 * result + (ordered ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JournalKey{" +
                "modelClassName=" + modelClassName +
                ", location='" + location + '\'' +
                ", partitionType=" + partitionType +
                ", recordHint=" + recordHint +
                ", ordered=" + ordered +
                '}';
    }

    //////////////////////// REPLICATION CODE //////////////////////

    public int getBufferSize() {
        return 4 + modelClassName.getBytes(Files.UTF_8).length + 4 + 2 * (location == null ? 0 : location.length()) + 1 + 1 + 4;
    }

    public void write(ByteBuffer buffer) {
        // modelClassName
        buffer.putInt(modelClassName.length());
        for (byte b : modelClassName.getBytes(Files.UTF_8)) {
            buffer.put(b);
        }
        // location
        ByteBuffers.putStringDW(buffer, location);
        // partition type
        buffer.put((byte) partitionType.ordinal());
        // recordHint
        buffer.putInt(recordHint);
        // ordered
        buffer.put((byte) (ordered ? 1 : 0));
    }

    private JournalKey(String clazz, String location, PartitionType partitionType, int recordHint, boolean ordered) {
        this.modelClass = null;
        this.modelClassName = clazz;
        this.location = location;
        this.partitionType = partitionType;
        this.recordHint = recordHint;
        this.ordered = ordered;
    }
}
