/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb;

import com.questdb.factory.configuration.Constants;
import com.questdb.misc.ByteBuffers;
import com.questdb.misc.Files;

import java.nio.ByteBuffer;

public class JournalKey<T> {
    private final String id;
    private final Class<T> modelClass;
    private String location;
    private int partitionBy = PartitionBy.DEFAULT;
    private int recordHint = Constants.NULL_RECORD_HINT;
    private boolean ordered = true;

    public JournalKey(String id) {
        this.id = id;
        this.modelClass = null;
    }

    public JournalKey(String id, Class<T> modelClass, String location, int recordHint) {
        this.id = id;
        this.modelClass = modelClass;
        this.location = location;
        this.recordHint = recordHint;
    }

    public JournalKey(Class<T> clazz) {
        this.modelClass = clazz;
        this.id = clazz.getName();
    }

    public JournalKey(Class<T> clazz, int recordHint) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.recordHint = recordHint;
    }

    public JournalKey(Class<T> clazz, String location) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.location = location;
    }

    public JournalKey(Class<T> clazz, String location, int partitionBy) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.location = location;
        this.partitionBy = partitionBy;
    }

    public JournalKey(Class<T> clazz, String location, int partitionBy, int recordHint) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.location = location;
        this.partitionBy = partitionBy;
        this.recordHint = recordHint;
    }

    public JournalKey(Class<T> clazz, String location, int partitionBy, int recordHint, boolean ordered) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.location = location;
        this.partitionBy = partitionBy;
        this.recordHint = recordHint;
        this.ordered = ordered;
    }

    public JournalKey(Class<T> clazz, String location, int partitionBy, boolean ordered) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.location = location;
        this.partitionBy = partitionBy;
        this.ordered = ordered;
    }

    private JournalKey(String clazz, String location, int partitionBy, int recordHint, boolean ordered) {
        this.modelClass = null;
        this.id = clazz;
        this.location = location;
        this.partitionBy = partitionBy;
        this.recordHint = recordHint;
        this.ordered = ordered;
    }

    public static <X> JournalKey<X> fromBuffer(ByteBuffer buffer) {
        // id
        int clazzLen = buffer.getInt();
        byte[] clazz = new byte[clazzLen];
        buffer.get(clazz);
        // location
        int locLen = buffer.getInt();
        char[] location = null;
        if (locLen > 0) {
            location = new char[locLen];
            for (int i = 0; i < location.length; i++) {
                location[i] = buffer.getChar();
            }
        }
        // partitionBy
        int partitionBy = buffer.get();
        // recordHint
        int recordHint = buffer.getInt();
        // ordered
        boolean ordered = buffer.get() == 1;

        return new JournalKey<>(new String(clazz, Files.UTF_8), location == null ? null : new String(location), partitionBy, recordHint, ordered);
    }

    public int getBufferSize() {
        return 4 + id.getBytes(Files.UTF_8).length + 4 + 2 * (location == null ? 0 : location.length()) + 1 + 1 + 4;
    }

    public String getId() {
        return id;
    }

    public String getLocation() {
        return location;
    }

    public Class<T> getModelClass() {
        return modelClass;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public int getRecordHint() {
        return recordHint;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (location != null ? location.hashCode() : 0);
        result = 31 * result + partitionBy;
        result = 31 * result + recordHint;
        return 31 * result + (ordered ? 1 : 0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JournalKey)) return false;
        JournalKey that = (JournalKey) o;
        return ordered == that.ordered && recordHint == that.recordHint && !(id != null ? !id.equals(that.id) : that.id != null) && !(location != null ? !location.equals(that.location) : that.location != null) && partitionBy == that.partitionBy;

    }

    //////////////////////// REPLICATION CODE //////////////////////

    @Override
    public String toString() {
        return "JournalKey{" +
                "id=" + id +
                ", location='" + location + '\'' +
                ", partitionBy=" + PartitionBy.toString(partitionBy) +
                ", recordHint=" + recordHint +
                ", ordered=" + ordered +
                '}';
    }

    public boolean isOrdered() {
        return ordered;
    }

    public String path() {
        return location == null ? id : location;
    }

    public void write(ByteBuffer buffer) {
        // id
        buffer.putInt(id.length());
        byte[] bytes = id.getBytes(Files.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            buffer.put(bytes[i]);
        }
        // location
        ByteBuffers.putStringDW(buffer, location);
        // partition type
        buffer.put((byte) partitionBy);
        // recordHint
        buffer.putInt(recordHint);
        // ordered
        buffer.put((byte) (ordered ? 1 : 0));
    }
}
