package io.questdb.jar.jni;

public class LibInfo {
    private final String platform;
    private final String name;
    private final String prefix;
    private final String suffix;

    public LibInfo(String name) {
        this(
                OsInfo.INSTANCE.getPlatform(),
                name,
                OsInfo.INSTANCE.getLibPrefix(),
                OsInfo.INSTANCE.getLibSuffix()
        );
    }

    public LibInfo(String platform, String name, String prefix, String suffix) {
        this.platform = platform;
        this.name = name;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    public String getPlatform() {
        return platform;
    }

    public String getName() {
        return name;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getSuffix() {
        return suffix;
    }

    public String getFullName() {
        return prefix + name + suffix;
    }

    public String getPath() {
        return platform + "/" + getFullName();
    }
}
