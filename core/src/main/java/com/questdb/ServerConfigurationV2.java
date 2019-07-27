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

package com.questdb;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cutlass.http.HttpServerConfiguration;
import com.questdb.cutlass.line.udp.LineUdpReceiverConfiguration;
import com.questdb.cutlass.pgwire.PGWireConfiguration;
import com.questdb.mp.WorkerPoolConfiguration;

public interface ServerConfigurationV2 {

    CairoConfiguration getCairoConfiguration();

    HttpServerConfiguration getHttpServerConfiguration();

    LineUdpReceiverConfiguration getLineUdpReceiverConfiguration();

    WorkerPoolConfiguration getWorkerPoolConfiguration();

    PGWireConfiguration getPGWireConfiguration();
}
