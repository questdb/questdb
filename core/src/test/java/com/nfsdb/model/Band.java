/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.model;

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
