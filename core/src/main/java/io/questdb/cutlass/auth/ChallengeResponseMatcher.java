package io.questdb.cutlass.auth;

public interface ChallengeResponseMatcher {
    boolean match(CharSequence keyId, byte[] challenge, byte[] response);
}
