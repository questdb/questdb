package io.questdb.cutlass.auth;

import io.questdb.std.QuietCloseable;

public interface ChallengeResponseMatcher extends QuietCloseable {
    @Override
    default void close() {

    }

    /**
     * Verify a signature of a challenge. The signature is expected to be encoded in base64. The challenge is taken as is.
     * <p>
     * This also returns false if user does not have an active JWK token or when username is null or empty.
     *
     * @param username           user name
     * @param challengePtr       pointer to challenge
     * @param challengeLen       length of challenge
     * @param base64SignaturePtr pointer to base64 encoded signature
     * @param base64SignatureLen length of base64 encoded signature
     * @return true if signature matches
     */
    boolean verifyLineToken(CharSequence username, long challengePtr, int challengeLen, long base64SignaturePtr, int base64SignatureLen);
}
