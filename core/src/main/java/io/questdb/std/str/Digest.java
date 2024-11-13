/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.std.str;

import io.questdb.std.BinarySequence;
import org.jetbrains.annotations.NotNull;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.DigestException;


public class Digest {
    public enum DigestAlgorithm {
        MD5,
        SHA1,
        SHA256,
    }

    private final MessageDigest digest;
    private final byte[] buffer;

    public Digest(@NotNull DigestAlgorithm algorithm) {
        String algo;
        switch (algorithm) {
            case MD5:
                algo = "MD5";
                break;
            case SHA1:
                algo = "SHA-1";
                break;
            case SHA256:
            default:
                algo = "SHA-256";
                break;
        }
        try {
            this.digest = MessageDigest.getInstance(algo);
        } catch (NoSuchAlgorithmException e) {
            /*
             * Every implementation of the Java platform is required to support the
             * following standard MessageDigest algorithms:
             * - MD5
             * - SHA-1
             * - SHA-256
             */
            throw new RuntimeException("unreachable");
        }
        this.buffer = new byte[digest.getDigestLength()];
    }

    public void hash(@NotNull BinarySequence sequence, @NotNull CharSink<?> sink) {
        for (int i = 0; i < sequence.length(); i++) {
            this.digest.update(sequence.byteAt(i));
        }
        try {
            this.digest.digest(this.buffer, 0, this.buffer.length);
        } catch (DigestException e) {
            // buffer always has enough space
        }
        hexencode(sink);
    }

    public void hash(@NotNull CharSequence sequence, @NotNull CharSink<?> sink) {
        for (int i = 0; i < sequence.length(); i++) {
            this.digest.update((byte) sequence.charAt(i));
        }
        try {
            this.digest.digest(this.buffer, 0, this.buffer.length);
        } catch (DigestException e) {
            // buffer always has enough space
        }
        hexencode(sink);
    }

    private void hexencode(@NotNull CharSink<?> sink) {
        char[] hexChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        for (byte b : this.buffer) {
            sink.put(hexChars[(0xf0 & b) >> 4]);
            sink.put(hexChars[0x0f & b]);
        }
    }
}
