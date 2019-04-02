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

package com.questdb.cutlass.line.udp;

import com.questdb.network.Net;
import com.questdb.network.NetworkFacadeImpl;
import org.junit.Test;

public class LineProtoSenderTest {

    @Test
    public void testSimple() {
        try (LineProtoSender sender = new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, Net.parseIPv4("234.5.6.7"), 4567, 110)) {
            sender.metric("weather").tag("location", "london").tag("by", "quest").field("temp", 3400).$(System.currentTimeMillis());
            sender.metric("weather2").tag("location", "london").tag("by", "quest").field("temp", 3400).$(System.currentTimeMillis());
            sender.metric("weather3").tag("location", "london").tag("by", "quest").field("temp", 3400).$(System.currentTimeMillis());
            sender.flush();
        }
    }
}