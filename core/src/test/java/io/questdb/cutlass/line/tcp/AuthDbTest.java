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

package io.questdb.cutlass.line.tcp;

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;

public class AuthDbTest {
    @Test
    public void testCryptoAlgorithm() throws Exception {
        // # "d": "5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48",
        // # "crv": "P-256",
        // # "kid": "testUser1",
        // # "x": "fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU",
        // # "y": "Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac"

        PrivateKey secretKey = AuthDb.importPrivateKey("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48");
        PublicKey publicKey = AuthDb.importPublicKey("fLKYEaoEb9lrn3nkwLDA-M_xnuFOdSt9y0Z7_vWSHLU", "Dt5tbS1dEDMSYfym3fgMv0B99szno-dFc1rYF9t0aac");
        String challenge = "XV54KEZwck9bNThoTlMpJnVNO216b2RAWlFneF1YVltYKi91PlItd1d7OEAuSVRQVysjQ09bMn4wSkVaWklcMlcoJHVwRGR7OiZzOUM2ZGkhdy9aKkEuPXJnPyQjMEtPQD1uVCBMLV4wWCs6ekx5NnwkdDF7dVxhO0goJy1uV2lhdSF2cjktWUlZVCA8M0EmWypCaFMhP0U7VFV5TyQgY2JWLyxJSkJvI1M8c0lUaXU6OzIlKVRGcEh4IVJHP0Bgdmkjfi42fVpTdiw8ZCo5Y3RhXFV0W2MwOm5Od0M4Tj0vWER3MGJiUEVBUlwyMS5RfEAvNWNwZnt7QzpReHN2SCdUZzR+MjYqeS51Lz5xXkVwXztUNyJtJE1NMCJcSDlZTSMzazxnUHksN3dGbFVsN15kPG5AUXNiSTY3Y20pXzFRXnhzZj5CM3EoI2dsVnpVZT41L25Se2wobChwSENHVkRxQyUlU3ExOUJIb3wgdGwweURoQnhkKlM7O0k1M2dwRU9Qe2NqaUwvey1wXFFJW08uWHIpM11PTFRETX1jK2JWU2MmJTBWZDk7YVZpI1A8THZaLykgVyFhe2x9O0xzICVGKGx3TD8tMz1bKUV+YUsmVyZvYXxiWTN5KT9DS2UlKVh+SFxUeTtXOnlidV0lejl6MFN9KH43Sn5+eEM3fXcK";
        String signature = "xo4A5gJRqRKl0MeFbrylIUMzcTkvmVrbVw4t2GsIl2mCBiuZ0h66O0hK1oudRYIC4i6Pw3mYPX0XxWArn9Wzkw==";
        byte[] signatureRaw = Base64.getDecoder().decode(signature);
        Signature sig;

        try {
            if (signatureRaw.length == 64) {
                sig = Signature.getInstance(AuthDb.SIGNATURE_TYPE_P1363);
            } else {
                sig = Signature.getInstance(AuthDb.SIGNATURE_TYPE_DER);
            }
        } catch (NoSuchAlgorithmException ex) {
            if (TestUtils.getJavaVersion() <= 8) {
                return;
            }
            throw ex;
        }

        sig.initSign(secretKey);
        sig.update(challenge.getBytes());
        byte[] sig2 = sig.sign();
        sig.initVerify(publicKey);
        sig.update(challenge.getBytes());
        boolean verified = sig.verify(sig2);
        Assert.assertTrue(verified);

        sig.initVerify(publicKey);
        sig.update(challenge.getBytes());
        verified = sig.verify(signatureRaw);
        Assert.assertTrue(verified);
    }
}
