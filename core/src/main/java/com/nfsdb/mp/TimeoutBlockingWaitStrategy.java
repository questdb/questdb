/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 *
 ******************************************************************************/

package com.nfsdb.mp;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.ex.TimeoutException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TimeoutBlockingWaitStrategy extends AbstractWaitStrategy {
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long time;
    private final TimeUnit unit;

    public TimeoutBlockingWaitStrategy(long time, TimeUnit unit) {
        this.time = time;
        this.unit = unit;
    }

    @SuppressFBWarnings({"WA_AWAIT_NOT_IN_LOOP", "EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
    @Override
    public void await() {
        lock.lock();
        try {
            if (alerted) {
                throw AlertedException.INSTANCE;
            }
            if (!condition.await(time, unit)) {
                throw TimeoutException.INSTANCE;
            }
        } catch (InterruptedException e) {
            throw new JournalRuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void signal() {
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }

    }
}
