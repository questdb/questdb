package io.questdb.cutlass.auth;

public interface ChallengeResponseMatcher {
    boolean verifyLineToken(CharSequence keyId, long challengePtr, int challengeLen, long base64SignaturePtr, int base64SignatureLen);
}
