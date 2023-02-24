package io.questdb.jar.jni;

public enum OsInfo {
    INSTANCE();

    private final String platform;
    private final String libPrefix;
    private final String libSuffix;

    OsInfo() {
        final String osName = System.getProperty("os.name").toLowerCase();
        final String osArch = System.getProperty("os.arch").toLowerCase();
        this.platform = (osName + "-" + osArch).replace(' ', '_');
        this.libPrefix = osName.startsWith("windows") ? "" : "lib";
        this.libSuffix = osName.startsWith("windows")
                ? ".dll" : osName.contains("mac")
                ? ".dylib" : ".so";
    }

    public String getPlatform() {
        return platform;
    }

    public String getLibPrefix() {
        return libPrefix;
    }

    public String getLibSuffix() {
        return libSuffix;
    }
}