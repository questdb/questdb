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

/**
 * Portable timer code was taken with thanks from:
 * https://stackoverflow.com/questions/1497702/c-windows-time
 */

#include <processthreadsapi.h>
#include <rpc.h>
#include <psdk_inc/intrin-impl.h>
#include <stdint.h>
#include "timer.h"

struct init {
    long long stamp; // last adjustment time
    long long epoch; // last sync time as FILETIME
    long long start; // counter ticks to match epoch
    long long freq;  // counter frequency (ticks per 10ms)
};

typedef long long (WINAPI *PNOW)();
typedef void (WINAPI *PGSTPAFT)(LPFILETIME);
static const uint64_t EPOCH_DIFFERENCE_MICROS = 11644473600000000ull;
struct init data_[2] = {};
const struct init *volatile init_ = &data_[0];
PNOW _pnow;
PGSTPAFT _GetSystemTimePreciseAsFileTime;

void calibrate(DWORD sleep) {
    LARGE_INTEGER t1, t2, p1, p2, r1, r2, f;
    int cpu[4] = {};

    // prepare for rdtsc calibration - affinity and priority
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
    SetThreadAffinityMask(GetCurrentThread(), 2);
    Sleep(10);

    // frequency for time measurement during calibration
    QueryPerformanceFrequency(&f);

    // for explanation why RDTSC is safe on modern CPUs, look for "Constant TSC" and "Invariant TSC" in
    // Intel(R) 64 and IA-32 Architectures Software Developer’s Manual (document 253668.pdf)

    __cpuid(cpu, 0); // flush CPU pipeline
    r1.QuadPart = __rdtsc();
    __cpuid(cpu, 0);
    QueryPerformanceCounter(&p1);

    // sleep some time, doesn't matter it's not accurate.
    Sleep(sleep);

    // wait for the system clock to move, so we have exact epoch
    GetSystemTimeAsFileTime((FILETIME *) (&t1.u));
    do {
        Sleep(0);
        GetSystemTimeAsFileTime((FILETIME *) (&t2.u));
        __cpuid(cpu, 0); // flush CPU pipeline
        r2.QuadPart = __rdtsc();
    } while (t2.QuadPart == t1.QuadPart);

    // measure how much time has passed exactly, using more expensive QPC
    __cpuid(cpu, 0);
    QueryPerformanceCounter(&p2);

    data_->stamp = t2.QuadPart;
    data_->epoch = t2.QuadPart;
    data_->start = r2.QuadPart;

    // calculate counter ticks per 10ms
    data_->freq = f.QuadPart * (r2.QuadPart - r1.QuadPart) / 100 / (p2.QuadPart - p1.QuadPart);

    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_NORMAL);
    SetThreadAffinityMask(GetCurrentThread(), 0xFF);
}

void adjust() {
    // must make a copy
    const struct init *it = init_;
    struct init *r = (init_ == &data_[0] ? &data_[1] : &data_[0]);
    LARGE_INTEGER t1, t2;

    // wait for the system clock to move, so we have exact time to compare against
    GetSystemTimeAsFileTime((FILETIME *) (&t1.u));
    long long p = 0;
    int cpu[4] = {};
    do {
        Sleep(0);
        GetSystemTimeAsFileTime((FILETIME *) (&t2.u));
        __cpuid(cpu, 0); // flush CPU pipeline
        p = __rdtsc();
    } while (t2.QuadPart == t1.QuadPart);

    long long d = (p - it->start);
    // convert 10ms to 100ns periods
    d *= 100000ll;
    d /= it->freq;

    r->start = p;
    r->epoch = d + it->epoch;
    r->stamp = t2.QuadPart;

    const long long dt1 = t2.QuadPart - it->epoch;
    const long long dt2 = t2.QuadPart - it->stamp;
    const double s1 = (double) d / dt1;
    const double s2 = (double) d / dt2;

    r->freq = (long long) (it->freq * (s1 + s2 - 1) + 0.5);

    InterlockedExchangePointer((volatile PVOID *) &init_, r);
}

long long portableNow() {
    // must make a copy
    const struct init *it = init_;
    const long long p = __rdtsc();
    // time passed from epoch in counter ticks
    long long d = (p - it->start);
    if (d > 0x80000000000ll) {
        // closing to integer overflow, must adjust now
        adjust();
    }
    // convert 10ms to 100ns periods
    d *= 100000ll;
    d /= it->freq;
    // and add to epoch, so we have proper FILETIME
    d += it->epoch;
    d /= 10;
    d -= EPOCH_DIFFERENCE_MICROS;
    return d;
}

long long win8now() {
    FILETIME ft;
    _GetSystemTimePreciseAsFileTime(&ft);
    return (((uint64_t) ft.dwHighDateTime << 32) | (uint64_t) ft.dwLowDateTime) / 10 - EPOCH_DIFFERENCE_MICROS;
}

long long inline now() {
    return _pnow();
}

void setupTimer() {
    _GetSystemTimePreciseAsFileTime = (PGSTPAFT) GetProcAddress(GetModuleHandle(TEXT("kernel32.dll")),
                                                                "GetSystemTimePreciseAsFileTime");
    if (_GetSystemTimePreciseAsFileTime != NULL) {
        _pnow = &win8now;
    } else {
        calibrate(1000);
        _pnow = &portableNow;
    }
}
