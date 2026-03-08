/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#define _GNU_SOURCE

#include <unistd.h>
#include <mach/mach_types.h>
#include <pthread/pthread.h>
#include <mach/thread_act.h>
#include "../share/os.h"

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_setCurrentThreadAffinity0
        (JNIEnv *e, jclass cl, jint cpu) {
    thread_affinity_policy_data_t policy_data = {cpu};
    mach_port_t mach_thread = pthread_mach_thread_np(pthread_self());
    return thread_policy_set(mach_thread, THREAD_AFFINITY_POLICY, (thread_policy_t) &policy_data,
                             THREAD_AFFINITY_POLICY_COUNT);
}

