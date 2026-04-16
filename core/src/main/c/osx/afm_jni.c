/*+*****************************************************************************
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

#include <stddef.h>
#include "jni.h"

extern char *afm_generate(const char *prompt, char **error_out);
extern void afm_free(char *ptr);
extern void *afm_stream_start(const char *prompt, char **error_out);
extern char *afm_stream_next(void *handle, char **error_out);
extern void afm_stream_close(void *handle);

JNIEXPORT jstring JNICALL Java_io_questdb_std_afm_AppleFoundationModel_generate0
        (JNIEnv *env, jclass cls, jstring prompt) {
    (void) cls;
    if (prompt == NULL) {
        (*env)->ThrowNew(env,
                         (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                         "prompt is null");
        return NULL;
    }

    const char *c_prompt = (*env)->GetStringUTFChars(env, prompt, NULL);
    if (c_prompt == NULL) {
        return NULL; // OOM, JVM already threw
    }

    char *error_msg = NULL;
    char *result = afm_generate(c_prompt, &error_msg);
    (*env)->ReleaseStringUTFChars(env, prompt, c_prompt);

    if (result == NULL) {
        const char *msg = error_msg != NULL ? error_msg : "unknown AFM error";
        jclass re = (*env)->FindClass(env, "java/lang/RuntimeException");
        (*env)->ThrowNew(env, re, msg);
        if (error_msg != NULL) {
            afm_free(error_msg);
        }
        return NULL;
    }

    jstring j_result = (*env)->NewStringUTF(env, result);
    afm_free(result);
    return j_result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_afm_AppleFoundationModel_streamStart0
        (JNIEnv *env, jclass cls, jstring prompt) {
    (void) cls;
    if (prompt == NULL) {
        (*env)->ThrowNew(env,
                         (*env)->FindClass(env, "java/lang/IllegalArgumentException"),
                         "prompt is null");
        return 0;
    }
    const char *c_prompt = (*env)->GetStringUTFChars(env, prompt, NULL);
    if (c_prompt == NULL) {
        return 0;
    }
    char *error_msg = NULL;
    void *handle = afm_stream_start(c_prompt, &error_msg);
    (*env)->ReleaseStringUTFChars(env, prompt, c_prompt);

    if (handle == NULL) {
        const char *msg = error_msg != NULL ? error_msg : "afm_stream_start failed";
        jclass re = (*env)->FindClass(env, "java/lang/RuntimeException");
        (*env)->ThrowNew(env, re, msg);
        if (error_msg != NULL) {
            afm_free(error_msg);
        }
        return 0;
    }
    return (jlong) handle;
}

JNIEXPORT jstring JNICALL Java_io_questdb_std_afm_AppleFoundationModel_streamNext0
        (JNIEnv *env, jclass cls, jlong handle) {
    (void) cls;
    if (handle == 0) {
        (*env)->ThrowNew(env,
                         (*env)->FindClass(env, "java/lang/IllegalStateException"),
                         "stream handle is 0");
        return NULL;
    }
    char *error_msg = NULL;
    char *token = afm_stream_next((void *) handle, &error_msg);
    if (token == NULL) {
        if (error_msg != NULL) {
            jclass re = (*env)->FindClass(env, "java/lang/RuntimeException");
            (*env)->ThrowNew(env, re, error_msg);
            afm_free(error_msg);
        }
        return NULL; // null signals end-of-stream to Java when no error
    }
    jstring j_token = (*env)->NewStringUTF(env, token);
    afm_free(token);
    return j_token;
}

JNIEXPORT void JNICALL Java_io_questdb_std_afm_AppleFoundationModel_streamClose0
        (JNIEnv *env, jclass cls, jlong handle) {
    (void) env;
    (void) cls;
    if (handle != 0) {
        afm_stream_close((void *) handle);
    }
}
