package io.questdb.cutlass.line.tcp;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.AlgorithmParameters;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.interfaces.ECKey;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.ECPrivateKeySpec;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.questdb.std.CharSequenceObjHashMap;

public class AuthDb {
    private static final Pattern TOKEN_PATTERN = Pattern.compile("\\s*(\\S+)(.*)");
    public static final String EC_ALGORITHM = "EC";
    public static final String EC_CURVE = "secp256r1";
    public static final String SIGNATURE_TYPE = "SHA256withECDSA";

    public static PrivateKey importPrivateKey(String encodedPrivateKey) {
        byte[] dBytes = Base64.getUrlDecoder().decode(encodedPrivateKey);

        try {
            BigInteger privateKeyInt = new BigInteger(dBytes);
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(EC_ALGORITHM);
            AlgorithmParameterSpec prime256v1ParamSpec = new ECGenParameterSpec(EC_CURVE);
            keyPairGenerator.initialize(prime256v1ParamSpec);
            ECParameterSpec parameterSpec = ((ECKey) keyPairGenerator.generateKeyPair().getPrivate()).getParams();
            ECPrivateKeySpec privateKeySpec = new ECPrivateKeySpec(privateKeyInt, parameterSpec);
            PrivateKey privateKey = KeyFactory.getInstance(EC_ALGORITHM).generatePrivate(privateKeySpec);
            return privateKey;
        } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException | InvalidKeySpecException ex) {
            throw new IllegalArgumentException("Failed to decode " + encodedPrivateKey, ex);
        }
    }

    private final CharSequenceObjHashMap<PublicKey> publicKeyByKeyId = new CharSequenceObjHashMap<>();

    AuthDb(LineTcpReceiverConfiguration configuration) {
        int nLine = 0;
        String[] tokens = new String[4];
        try (BufferedReader r = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(new File(configuration.getAuthDbPath())))))) {
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

                byte[] xBytes = Base64.getUrlDecoder().decode(tokens[2]);
                byte[] yBytes = Base64.getUrlDecoder().decode(tokens[3]);
                BigInteger x = new BigInteger(xBytes);
                BigInteger y = new BigInteger(yBytes);
                ECPoint point = new ECPoint(x, y);

                AlgorithmParameters parameters = AlgorithmParameters.getInstance(EC_ALGORITHM);
                parameters.init(new ECGenParameterSpec(EC_CURVE));
                ECParameterSpec ecParameters = parameters.getParameterSpec(ECParameterSpec.class);
                ECPublicKeySpec pubKeySpec = new ECPublicKeySpec(point, ecParameters);
                PublicKey publicKey = KeyFactory.getInstance(EC_ALGORITHM).generatePublic(pubKeySpec);
                publicKeyByKeyId.put(keyId, publicKey);
            } while (null != line);
        } catch (Exception ex) {
            throw new IllegalArgumentException("IO error, failed to read auth db file " + configuration.getAuthDbPath() + " at line " + nLine, ex);
        }
    }

    public PublicKey getPublicKey(CharSequence keyId) {
        return publicKeyByKeyId.get(keyId);
    }
}
