package io.questdb.cairo;

import org.jetbrains.annotations.NotNull;

public interface TableFlagResolver {
    default boolean isProtected(@NotNull CharSequence tableName) {
        return false;
    }

    boolean isPublic(@NotNull CharSequence tableName);

    boolean isSystem(@NotNull CharSequence tableName);
}
