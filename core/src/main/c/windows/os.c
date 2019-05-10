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

#include <processthreadsapi.h>
#include <errhandlingapi.h>

#define SECURITY_WIN32

#include <sspi.h>
#include <issper16.h>
#include <rpc.h>
#include <sys/timeb.h>
#include "../share/os.h"
#include "errno.h"
#include "timer.h"

JNIEXPORT jint JNICALL Java_com_questdb_std_Os_getPid
        (JNIEnv *e, jclass cl) {
    return GetCurrentProcessId();
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Os_errno
        (JNIEnv *e, jclass cl) {
    return (jint) (intptr_t) TlsGetValue(dwTlsIndexLastError);
}

typedef struct {
    SECURITY_STATUS status;
    long cbToken;
    void *token;
} KRB_TOKEN, *PKRB_TOKEN;

#define SEC_PACKAGE_NAME "Kerberos"

jlong JNICALL Java_com_questdb_std_Os_generateKrbToken
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

JNIEXPORT jint JNICALL Java_com_questdb_std_Os_setCurrentThreadAffinity0
        (JNIEnv *e, jclass fd, jint cpu) {
    DWORD_PTR mask = (DWORD_PTR) (1L << cpu);
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

JNIEXPORT jlong JNICALL Java_com_questdb_std_Os_currentTimeNanos
        (JNIEnv *e, jclass cl) {
    struct timespec spec;
    clock_gettime(&spec);
    return spec.tv_sec * 1000000000 + spec.tv_nsec;
}

JNIEXPORT void JNICALL Java_com_questdb_std_Os_freeKrbToken
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

JNIEXPORT void JNICALL Java_com_questdb_std_Os_setupTimer
        (JNIEnv *e, jclass cl) {
    setupTimer();
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Os_currentTimeMicros
        (JNIEnv *e, jclass cl) {
    return now();
}

void SaveLastError() {
    TlsSetValue(dwTlsIndexLastError, (LPVOID) (DWORD_PTR) GetLastError());
};