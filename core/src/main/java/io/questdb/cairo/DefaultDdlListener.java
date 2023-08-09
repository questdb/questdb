package io.questdb.cairo;

public class DefaultDdlListener implements DdlListener {
    public static final DdlListener INSTANCE = new DefaultDdlListener();

    @Override
    public void onColumnAdded(SecurityContext securityContext, TableToken tableToken, CharSequence columnName) {
    }

    @Override
    public void onTableCreated(SecurityContext securityContext, TableToken tableToken) {
    }
}
