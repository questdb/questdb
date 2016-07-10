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
 ******************************************************************************/

package com.questdb.ql.impl;

import com.questdb.ex.DisconnectedChannelRuntimeException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Net;
import com.questdb.misc.Numbers;
import com.questdb.ql.CancellationHandler;

public class ChannelCheckCancellationHandler implements CancellationHandler {
    private static final Log LOG = LogFactory.getLog(ChannelCheckCancellationHandler.class);
    private final long fd;
    private final long mask;
    private long loop = 0;

    public ChannelCheckCancellationHandler(long fd, int cyclesBeforeCheck) {
        this.fd = fd;
        this.mask = Numbers.ceilPow2(cyclesBeforeCheck) - 1;
    }

    @Override
    public void check() {
        if (loop > 0 && (loop & mask) == 0) {
            checkChannel();
        }
        loop++;
    }

    public void reset() {
        this.loop = 0;
    }

    private void checkChannel() {
        if (Net.isDead(fd)) {
            LOG.info().$("Socket closed, possibly cancelled request. FD=").$(fd).$();
            throw DisconnectedChannelRuntimeException.INSTANCE;
        }
    }
}
