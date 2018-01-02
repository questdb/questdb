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

package com.questdb.model;

import java.nio.ByteBuffer;

@SuppressWarnings("unused")
public class Band {
    private long timestamp;
    private String name;
    private String url;
    private String type;
    private ByteBuffer image;

    public ByteBuffer getImage() {
        return image;
    }

    public Band setImage(byte[] bytes) {
        return setImage(ByteBuffer.wrap(bytes));
    }

    public String getName() {
        return name;
    }

    public Band setName(String name) {
        this.name = name;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    public Band setType(String type) {
        this.type = type;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public Band setUrl(String url) {
        this.url = url;
        return this;
    }

    public Band setImage(ByteBuffer image) {
        this.image = image;
        return this;
    }

    @Override
    public String toString() {
        return "Band{" +
                "timestamp=" + timestamp +
                ", name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", type='" + type + '\'' +
                ", image=" + image +
                '}';
    }
}
