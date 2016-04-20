/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
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
    private PartitionType partitionType = PartitionType.DEFAULT;
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

    public JournalKey(Class<T> clazz, String location, PartitionType partitionType) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.location = location;
        this.partitionType = partitionType;
    }

    public JournalKey(Class<T> clazz, String location, PartitionType partitionType, int recordHint) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.location = location;
        this.partitionType = partitionType;
        this.recordHint = recordHint;
    }

    public JournalKey(Class<T> clazz, String location, PartitionType partitionType, int recordHint, boolean ordered) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.location = location;
        this.partitionType = partitionType;
        this.recordHint = recordHint;
        this.ordered = ordered;
    }

    public JournalKey(Class<T> clazz, String location, PartitionType partitionType, boolean ordered) {
        this.modelClass = clazz;
        this.id = clazz.getName();
        this.location = location;
        this.partitionType = partitionType;
        this.ordered = ordered;
    }

    private JournalKey(String clazz, String location, PartitionType partitionType, int recordHint, boolean ordered) {
        this.modelClass = null;
        this.id = clazz;
        this.location = location;
        this.partitionType = partitionType;
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
        // partitionType
        PartitionType partitionType = PartitionType.values()[buffer.get()];
        // recordHint
        int recordHint = buffer.getInt();
        // ordered
        boolean ordered = buffer.get() == 1;

        return new JournalKey<>(new String(clazz, Files.UTF_8), location == null ? null : new String(location), partitionType, recordHint, ordered);
    }

    public String derivedLocation() {
        return location == null ? id : location;
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

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public int getRecordHint() {
        return recordHint;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (location != null ? location.hashCode() : 0);
        result = 31 * result + (partitionType != null ? partitionType.hashCode() : 0);
        result = 31 * result + recordHint;
        return 31 * result + (ordered ? 1 : 0);
    }

    //////////////////////// REPLICATION CODE //////////////////////

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JournalKey)) return false;
        JournalKey that = (JournalKey) o;
        return ordered == that.ordered && recordHint == that.recordHint && !(id != null ? !id.equals(that.id) : that.id != null) && !(location != null ? !location.equals(that.location) : that.location != null) && partitionType == that.partitionType;

    }

    @Override
    public String toString() {
        return "JournalKey{" +
                "id=" + id +
                ", location='" + location + '\'' +
                ", partitionType=" + partitionType +
                ", recordHint=" + recordHint +
                ", ordered=" + ordered +
                '}';
    }

    public boolean isOrdered() {
        return ordered;
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
        buffer.put((byte) partitionType.ordinal());
        // recordHint
        buffer.putInt(recordHint);
        // ordered
        buffer.put((byte) (ordered ? 1 : 0));
    }
}
