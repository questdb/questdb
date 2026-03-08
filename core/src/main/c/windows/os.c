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

#include <processthreadsapi.h>
#include <errhandlingapi.h>
#include <psapi.h>

#define SECURITY_WIN32

#include <sspi.h>
#include <rpc.h>
#include <sys/timeb.h>
#include "../share/os.h"
#include "errno.h"
#include "timer.h"

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_getPid
        (JNIEnv *e, jclass cl) {
    return (jint) GetCurrentProcessId();
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_getRss
        (JNIEnv *e, jclass cl) {
    PROCESS_MEMORY_COUNTERS procInfo;
    BOOL status = GetProcessMemoryInfo(GetCurrentProcess(), &procInfo, sizeof(procInfo));
    if ( status != 0 ) {
        return (jlong) procInfo.WorkingSetSize;
    } else {
        return (jlong)0L;
    }
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_errno
        (JNIEnv *e, jclass cl) {
    return (jint) (intptr_t) TlsGetValue(dwTlsIndexLastError);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_getEnvironmentType
        (JNIEnv *e, jclass cl) {
    return 0; // no-op
}

typedef struct {
    SECURITY_STATUS status;
    long cbToken;
    void *token;
} KRB_TOKEN, *PKRB_TOKEN;

#define SEC_PACKAGE_NAME "Kerberos"

jlong JNICALL Java_io_questdb_std_Os_generateKrbToken
        (JNIEnv *e, jclass cl, jlong spn) {

    PKRB_TOKEN result = malloc(sizeof(KRB_TOKEN));
    result->token = NULL;
    result->cbToken = 0;

    PSecPkgInfoA pkgInfo;
    result->status = QuerySecurityPackageInfoA(SEC_PACKAGE_NAME, &pkgInfo);

    if (result->status != SEC_E_OK) {
        FreeContextBuffer(pkgInfo);
        return (jlong) result;
    }

    const unsigned long cbMaxToken = pkgInfo->cbMaxToken;
    FreeContextBuffer(pkgInfo);

    CredHandle clientCred;
    result->status = AcquireCredentialsHandleA(
            NULL,
            SEC_PACKAGE_NAME,
            SECPKG_CRED_OUTBOUND,
            NULL,
            NULL,
            NULL,
            NULL,
            &clientCred,
            NULL);

    if (result->status != SEC_E_OK) {
        return (jlong) result;
    }

    result->token = malloc(cbMaxToken);

    SecBufferDesc outSecBufDesc;
    SecBuffer outSecBuf;

    outSecBufDesc.ulVersion = SECBUFFER_VERSION;
    outSecBufDesc.cBuffers = 1;
    outSecBufDesc.pBuffers = &outSecBuf;
    outSecBuf.cbBuffer = cbMaxToken;
    outSecBuf.BufferType = SECBUFFER_TOKEN;
    outSecBuf.pvBuffer = result->token;

    DWORD dwClientFlags;

    result->status = InitializeSecurityContext(
            &clientCred,
            NULL,
            (char *) spn,
            ISC_REQ_CONFIDENTIALITY | ISC_REQ_IDENTIFY | ISC_REQ_SEQUENCE_DETECT |
            ISC_REQ_REPLAY_DETECT,
            0,
            SECURITY_NATIVE_DREP,
            NULL,
            0,
            NULL,
            &outSecBufDesc,
            &dwClientFlags,
            NULL
    );

    result->cbToken = outSecBuf.cbBuffer;

    FreeCredentialsHandle(&clientCred);
    return (jlong) result;
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_setCurrentThreadAffinity0
        (JNIEnv *e, jclass fd, jint cpu) {
    DWORD_PTR mask = (1L << cpu);
    if (SetThreadAffinityMask(GetCurrentThread(), mask) == 0) {
        SaveLastError();
        return -1;
    }
    return 0;
}

#define exp7           10000000LL     //1E+7     //C-file part
#define exp9         1000000000LL     //1E+9
#define w2ux 116444736000000000LL     //1.jan1601 to 1.jan1970

void unix_time(struct timespec *spec) {
    __int64 wintime;
    GetSystemTimeAsFileTime((FILETIME *) &wintime);
    wintime -= w2ux;
    spec->tv_sec = wintime / exp7;
    spec->tv_nsec = wintime % exp7 * 100;
}

int clock_gettime(struct timespec *spec) {
    static struct timespec startspec;
    static double ticks2nano;
    static __int64 startticks, tps = 0;
    __int64 tmp, curticks;

    QueryPerformanceFrequency((LARGE_INTEGER *) &tmp);

    if (tps != tmp) {
        tps = tmp; //init ~~ONCE
        // possibly change freq ?
        QueryPerformanceCounter((LARGE_INTEGER *) &startticks);
        unix_time(&startspec);
        ticks2nano = (double) exp9 / tps;
    }
    QueryPerformanceCounter((LARGE_INTEGER *) &curticks);
    curticks -= startticks;
    spec->tv_sec = startspec.tv_sec + (curticks / tps);
    spec->tv_nsec = (long) (startspec.tv_nsec + (double) (curticks % tps) * ticks2nano);
    if (spec->tv_nsec >= exp9) {
        spec->tv_sec++;
        spec->tv_nsec -= exp9;
    }
    return 0;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_currentTimeNanos
        (JNIEnv *e, jclass cl) {
    struct timespec spec;
    clock_gettime(&spec);
    return spec.tv_sec * 1000000000 + spec.tv_nsec;
}

JNIEXPORT void JNICALL Java_io_questdb_std_Os_freeKrbToken
        (JNIEnv *e, jclass cl, jlong ptr) {

    PKRB_TOKEN ptoken = (PKRB_TOKEN) ptr;
    if (ptoken->token) {
        free(ptoken->token);
    }
    free(ptoken);
}

BOOL WINAPI DllMain(
        _In_  HINSTANCE hinstDLL,
        _In_  DWORD fdwReason,
        _In_  LPVOID lpvReserved
) {
    switch (fdwReason) {
        case DLL_PROCESS_ATTACH:
            dwTlsIndexLastError = TlsAlloc();
            setupTimer();
            break;
        case DLL_PROCESS_DETACH:
            TlsFree(dwTlsIndexLastError);
            dwTlsIndexLastError = 0;
            break;
        default:
            break;
    }
    return TRUE;
}

JNIEXPORT void JNICALL Java_io_questdb_std_Os_setupTimer
        (JNIEnv *e, jclass cl) {
    setupTimer();
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Os_currentTimeMicros
        (JNIEnv *e, jclass cl) {
    return now();
}

void SaveLastError() {
    TlsSetValue(dwTlsIndexLastError, (LPVOID) (DWORD_PTR) GetLastError());
};
