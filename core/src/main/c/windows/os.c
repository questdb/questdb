/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
#include <winerror.h>

#define SECURITY_WIN32

#include <sspi.h>
#include <issper16.h>
#include <errno.h>
#include <rpc.h>
#include <sys/timeb.h>
#include "../share/os.h"
#include "errno.h"
#include "timer.h"

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_getPid
        (JNIEnv *e, jclass cl) {
    return GetCurrentProcessId();
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_errno
        (JNIEnv *e, jclass cl) {
    return (jint) (intptr_t) TlsGetValue(dwTlsIndexLastError);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Os_translateSysErrno
        (JNIEnv *e, jclass cl, jint errno) {
      switch (errno) {
        case ERROR_NOACCESS:                    return EACCES;
        case WSAEACCES:                         return EACCES;
        case ERROR_CANT_ACCESS_FILE:            return EACCES;
        case ERROR_ADDRESS_ALREADY_ASSOCIATED:  return EADDRINUSE;
        case WSAEADDRINUSE:                     return EADDRINUSE;
        case WSAEADDRNOTAVAIL:                  return EADDRNOTAVAIL;
        case WSAEAFNOSUPPORT:                   return EAFNOSUPPORT;
        case WSAEWOULDBLOCK:                    return EAGAIN;
        case WSAEALREADY:                       return EALREADY;
        case ERROR_INVALID_FLAGS:               return EBADF;
        case ERROR_INVALID_HANDLE:              return EBADF;
        case ERROR_LOCK_VIOLATION:              return EBUSY;
        case ERROR_PIPE_BUSY:                   return EBUSY;
        case ERROR_SHARING_VIOLATION:           return EBUSY;
        case ERROR_OPERATION_ABORTED:           return ECANCELED;
        case WSAEINTR:                          return ECANCELED;
        case ERROR_CONNECTION_ABORTED:          return ECONNABORTED;
        case WSAECONNABORTED:                   return ECONNABORTED;
        case ERROR_CONNECTION_REFUSED:          return ECONNREFUSED;
        case WSAECONNREFUSED:                   return ECONNREFUSED;
        case ERROR_NETNAME_DELETED:             return ECONNRESET;
        case WSAECONNRESET:                     return ECONNRESET;
        case ERROR_ALREADY_EXISTS:              return EEXIST;
        case ERROR_FILE_EXISTS:                 return EEXIST;
        case ERROR_BUFFER_OVERFLOW:             return EFAULT;
        case WSAEFAULT:                         return EFAULT;
        case ERROR_HOST_UNREACHABLE:            return EHOSTUNREACH;
        case WSAEHOSTUNREACH:                   return EHOSTUNREACH;
        case ERROR_INSUFFICIENT_BUFFER:         return EINVAL;
        case ERROR_INVALID_DATA:                return EINVAL;
        case ERROR_INVALID_PARAMETER:           return EINVAL;
        case ERROR_SYMLINK_NOT_SUPPORTED:       return EINVAL;
        case WSAEINVAL:                         return EINVAL;
        case WSAEPFNOSUPPORT:                   return EINVAL;
        case ERROR_BEGINNING_OF_MEDIA:          return EIO;
        case ERROR_BUS_RESET:                   return EIO;
        case ERROR_CRC:                         return EIO;
        case ERROR_DEVICE_DOOR_OPEN:            return EIO;
        case ERROR_DEVICE_REQUIRES_CLEANING:    return EIO;
        case ERROR_DISK_CORRUPT:                return EIO;
        case ERROR_EOM_OVERFLOW:                return EIO;
        case ERROR_FILEMARK_DETECTED:           return EIO;
        case ERROR_GEN_FAILURE:                 return EIO;
        case ERROR_INVALID_BLOCK_LENGTH:        return EIO;
        case ERROR_IO_DEVICE:                   return EIO;
        case ERROR_NO_DATA_DETECTED:            return EIO;
        case ERROR_NO_SIGNAL_SENT:              return EIO;
        case ERROR_OPEN_FAILED:                 return EIO;
        case ERROR_SETMARK_DETECTED:            return EIO;
        case ERROR_SIGNAL_REFUSED:              return EIO;
        case WSAEISCONN:                        return EISCONN;
        case ERROR_CANT_RESOLVE_FILENAME:       return ELOOP;
        case ERROR_TOO_MANY_OPEN_FILES:         return EMFILE;
        case WSAEMFILE:                         return EMFILE;
        case WSAEMSGSIZE:                       return EMSGSIZE;
        case ERROR_FILENAME_EXCED_RANGE:        return ENAMETOOLONG;
        case ERROR_NETWORK_UNREACHABLE:         return ENETUNREACH;
        case WSAENETUNREACH:                    return ENETUNREACH;
        case WSAENOBUFS:                        return ENOBUFS;
        case ERROR_BAD_PATHNAME:                return ENOENT;
        case ERROR_DIRECTORY:                   return ENOENT;
        case ERROR_ENVVAR_NOT_FOUND:            return ENOENT;
        case ERROR_FILE_NOT_FOUND:              return ENOENT;
        case ERROR_INVALID_NAME:                return ENOENT;
        case ERROR_INVALID_DRIVE:               return ENOENT;
        case ERROR_INVALID_REPARSE_DATA:        return ENOENT;
        case ERROR_MOD_NOT_FOUND:               return ENOENT;
        case ERROR_PATH_NOT_FOUND:              return ENOENT;
        case WSAHOST_NOT_FOUND:                 return ENOENT;
        case WSANO_DATA:                        return ENOENT;
        case ERROR_NOT_ENOUGH_MEMORY:           return ENOMEM;
        case ERROR_OUTOFMEMORY:                 return ENOMEM;
        case ERROR_CANNOT_MAKE:                 return ENOSPC;
        case ERROR_DISK_FULL:                   return ENOSPC;
        case ERROR_EA_TABLE_FULL:               return ENOSPC;
        case ERROR_END_OF_MEDIA:                return ENOSPC;
        case ERROR_HANDLE_DISK_FULL:            return ENOSPC;
        case ERROR_NOT_CONNECTED:               return ENOTCONN;
        case WSAENOTCONN:                       return ENOTCONN;
        case ERROR_DIR_NOT_EMPTY:               return ENOTEMPTY;
        case WSAENOTSOCK:                       return ENOTSOCK;
        case ERROR_NOT_SUPPORTED:               return ENOTSUP;
        case ERROR_BROKEN_PIPE:                 return EOF;
        case ERROR_ACCESS_DENIED:               return EPERM;
        case ERROR_PRIVILEGE_NOT_HELD:          return EPERM;
        case ERROR_BAD_PIPE:                    return EPIPE;
        case ERROR_NO_DATA:                     return EPIPE;
        case ERROR_PIPE_NOT_CONNECTED:          return EPIPE;
        case WSAESHUTDOWN:                      return EPIPE;
        case WSAEPROTONOSUPPORT:                return EPROTONOSUPPORT;
        case ERROR_WRITE_PROTECT:               return EROFS;
        case ERROR_SEM_TIMEOUT:                 return ETIMEDOUT;
        case WSAETIMEDOUT:                      return ETIMEDOUT;
        case ERROR_NOT_SAME_DEVICE:             return EXDEV;
        case ERROR_INVALID_FUNCTION:            return EISDIR;
        case ERROR_META_EXPANSION_TOO_LONG:     return E2BIG;
        default:                                return errno;
     }
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