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

#define _GNU_SOURCE

#include <unistd.h>
#include <mach/mach_types.h>
#include <pthread/pthread.h>
#include <mach/thread_act.h>
#include "../share/os.h"

JNIEXPORT jint JNICALL Java_com_questdb_std_Os_setCurrentThreadAffinity0
        (JNIEnv *e, jclass cl, jint cpu) {
    thread_affinity_policy_data_t policy_data = {cpu};
    mach_port_t mach_thread = pthread_mach_thread_np(pthread_self());
    return thread_policy_set(mach_thread, THREAD_AFFINITY_POLICY, (thread_policy_t) &policy_data,
                             THREAD_AFFINITY_POLICY_COUNT);
}

