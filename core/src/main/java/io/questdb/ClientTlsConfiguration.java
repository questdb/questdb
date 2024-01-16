package io.questdb;

public class ClientTlsConfiguration {
    public static final int TLS_VALIDATION_MODE_FULL = 0;
    public static final ClientTlsConfiguration DEFAULT = new ClientTlsConfiguration(null, null, TLS_VALIDATION_MODE_FULL);
    public static final int TLS_VALIDATION_MODE_NONE = 1;
    public static final ClientTlsConfiguration INSECURE_NO_VALIDATION = new ClientTlsConfiguration(null, null, TLS_VALIDATION_MODE_NONE);
    private final int tlsValidationMode;
    private final char[] trustStorePassword;
    private final String trustStorePath;

    public ClientTlsConfiguration(String trustStorePath, char[] trustStorePassword, int tlsValidationMode) {
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.tlsValidationMode = tlsValidationMode;
    }

    public int tlsValidationMode() {
        return tlsValidationMode;
    }

    public char[] trustStorePassword() {
        return trustStorePassword;
    }

    public String trustStorePath() {
        return trustStorePath;
    }
}
