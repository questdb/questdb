package io.questdb.cutlass.auth;

import io.questdb.std.QuietCloseable;

public interface ChallengeResponseMatcher extends QuietCloseable {
    @Override
    default void close() {

    }

    boolean verifyLineToken(CharSequence keyId, long challengePtr, int challengeLen, long base64SignaturePtr, int base64SignatureLen);
}
