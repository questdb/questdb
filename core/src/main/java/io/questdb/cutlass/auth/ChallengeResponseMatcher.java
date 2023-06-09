package io.questdb.cutlass.auth;

public interface ChallengeResponseMatcher {
    boolean verifyLineToken(CharSequence keyId, byte[] challenge, byte[] response);
}
