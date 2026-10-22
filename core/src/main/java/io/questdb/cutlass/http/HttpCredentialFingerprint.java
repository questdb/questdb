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

package io.questdb.cutlass.http;

import io.questdb.std.Mutable;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Fingerprint of the Authorization header of a request: a SHA-256 hash of the header value, keyed with a
 * random key that never leaves this process.
 * <p>
 * An HTTP session keeps the fingerprint of the credential it authenticated last, so that a later request
 * can tell whether it carries the same credential, without the session keeping the credential itself.
 * <p>
 * Not thread-safe: each connection owns an instance. The fingerprint is computed once per request, and
 * kept until {@link #clear()}.
 */
final class HttpCredentialFingerprint implements Mutable {
    private static final int CHUNK_SIZE = 256;
    private static final int FINGERPRINT_SIZE = 32;
    private static final byte[] KEY = newKey();
    private final byte[] chunk = new byte[CHUNK_SIZE];
    private final MessageDigest digest;
    private final byte[] fingerprint = new byte[FINGERPRINT_SIZE];
    private boolean isComputed;

    HttpCredentialFingerprint() {
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // every Java platform has to support SHA-256
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void clear() {
        isComputed = false;
    }

    private static byte[] newKey() {
        final byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return key;
    }

    /**
     * Returns the fingerprint of the Authorization header of the current request. The array is reused,
     * a caller keeping the fingerprint beyond the request has to copy it.
     *
     * @param authorization value of the Authorization header of the current request
     * @return fingerprint of the header value
     */
    byte[] of(@NotNull Utf8Sequence authorization) {
        if (!isComputed) {
            digest.reset();
            digest.update(KEY);
            final int size = authorization.size();
            for (int lo = 0; lo < size; lo += CHUNK_SIZE) {
                final int len = Math.min(CHUNK_SIZE, size - lo);
                for (int i = 0; i < len; i++) {
                    chunk[i] = authorization.byteAt(lo + i);
                }
                digest.update(chunk, 0, len);
            }
            // do not leave the credential behind
            Arrays.fill(chunk, 0, Math.min(CHUNK_SIZE, size), (byte) 0);
            try {
                digest.digest(fingerprint, 0, FINGERPRINT_SIZE);
            } catch (DigestException e) {
                // the array always has room for the hash
                throw new IllegalStateException(e);
            }
            isComputed = true;
        }
        return fingerprint;
    }
}
