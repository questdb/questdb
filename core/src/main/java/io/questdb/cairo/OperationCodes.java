package io.questdb.cairo;

public class OperationCodes {
    public static final int CREATE_TABLE = 1;
    public static final int DROP_TABLE = CREATE_TABLE + 1;
    public static final int DROP_ALL = DROP_TABLE + 1;
    public static final int CREATE_MAT_VIEW = DROP_ALL + 1;
    public static final int DROP_MAT_VIEW = CREATE_MAT_VIEW + 1;
    public static final int CREATE_VIEW = DROP_MAT_VIEW + 1;
    public static final int DROP_VIEW = CREATE_VIEW + 1;
    public static final int MAX = DROP_VIEW;
}
