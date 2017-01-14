/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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
#include <lm.h>
#include "../share/os.h"

JNIEXPORT jint JNICALL Java_com_questdb_misc_Os_getPid
        (JNIEnv *e, jclass cl) {
    return GetCurrentProcessId();
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Os_errno
        (JNIEnv *e, jclass cl) {
    return GetLastError();
}

typedef struct {
    SECURITY_STATUS status;
    long cbToken;
    void *token;
} KRB_TOKEN, *PKRB_TOKEN;

#define SEC_PACKAGE_NAME "Kerberos"

jlong JNICALL Java_com_questdb_misc_Os_generateKrbToken
        (JNIEnv *e, jclass cl, jlong spn) {

    PKRB_TOKEN result = malloc(sizeof(KRB_TOKEN));
    result->token = NULL;
    result->cbToken = 0;

    PSecPkgInfoA pkgInfo;
    result->status = QuerySecurityPackageInfoA(SEC_PACKAGE_NAME, &pkgInfo);

    if (result->status != SEC_E_OK) {
        FreeContextBuffer(pkgInfo);
//        printSecStatus(result->status);
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
//        printSecStatus(result->status);
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

JNIEXPORT void JNICALL Java_com_questdb_misc_Os_freeKrbToken
        (JNIEnv *e, jclass cl, jlong ptr) {

    PKRB_TOKEN ptoken = (PKRB_TOKEN) ptr;
    if (ptoken->token) {
        free(ptoken->token);
    }
    free(ptoken);
}
