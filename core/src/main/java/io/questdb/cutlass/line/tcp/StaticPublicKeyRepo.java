/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cutlass.auth.PublicKeyRepo;
import io.questdb.cutlass.auth.AuthUtils;
import io.questdb.std.CharSequenceObjHashMap;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.security.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StaticPublicKeyRepo implements PublicKeyRepo {
    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\s*(\\S+)(.*)");
    private final CharSequenceObjHashMap<PublicKey> publicKeyByKeyId = new CharSequenceObjHashMap<>();

    public StaticPublicKeyRepo(String authDbPath) {
        int nLine = 0;
        String[] tokens = new String[4];
        try (BufferedReader r = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(authDbPath))))) {
            String line;
            do {
                int nTokens = 0;
                line = r.readLine();
                nLine++;
                while (null != line) {
                    Matcher m = TOKEN_PATTERN.matcher(line);
                    if (!m.matches()) {
                        break;
                    }
                    String token = m.group(1);
                    if (token.startsWith("#")) {
                        break;
                    }
                    if (nTokens == tokens.length) {
                        throw new IllegalArgumentException("Too many tokens");
                    }
                    tokens[nTokens] = m.group(1);
                    nTokens++;
                    line = m.group(2);
                }

                if (nTokens == 0) {
                    continue;
                }

                if (nTokens != 4) {
                    throw new IllegalArgumentException("Was expecting 4 tokens");
                }

                if (!"ec-p-256-sha256".equals(tokens[1])) {
                    throw new IllegalArgumentException("Unrecognized type " + tokens[1]);
                }

                CharSequence keyId = tokens[0];
                if (!publicKeyByKeyId.excludes(keyId)) {
                    throw new IllegalArgumentException("Duplicate keyId " + keyId);
                }

                PublicKey publicKey = AuthUtils.toPublicKey(tokens[2], tokens[3]);
                publicKeyByKeyId.put(keyId, publicKey);
            } while (null != line);
        } catch (Exception ex) {
            throw new IllegalArgumentException("IO error, failed to read auth db file " + authDbPath + " at line " + nLine, ex);
        }
    }

    @Override
    public PublicKey getPublicKey(CharSequence keyId) {
        return publicKeyByKeyId.get(keyId);
    }
}
