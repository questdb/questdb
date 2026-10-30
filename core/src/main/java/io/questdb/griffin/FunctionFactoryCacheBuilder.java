package io.questdb.griffin;

import io.questdb.log.Log;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayList;

public class FunctionFactoryCacheBuilder {
    private static ArrayList<FunctionFactory> functionFactoriesCache;
    private static final String FUNCTION_LIST_FILE_NAME = "function_list.txt";
    private final ObjList<String> droppedClassNames = new ObjList<>();
    ArrayList<FunctionFactory> functionFactories;

    @TestOnly
    public static void clearCache() {
        functionFactoriesCache = null;
    }

    @TestOnly
    public ObjList<String> getDroppedClassNames() {
        return droppedClassNames;
    }

    public FunctionFactoryCacheBuilder scan(@Nullable Log log) {
        return scan("io.questdb.griffin.engine.functions", FUNCTION_LIST_FILE_NAME, FunctionFactoryCacheBuilder.class, "io.questdb", log);
    }

    public FunctionFactoryCacheBuilder scan(String packageName, String functionListFileName, Class<?> clazz, String moduleName, @Nullable Log log) {
        if (functionFactoriesCache != null) {
            return this;
        }

        if (functionFactories == null) {
            functionFactories = new ArrayList<>();
        }

        FunctionFactoryScanner.scan(functionFactories, packageName, functionListFileName, clazz, moduleName, log, droppedClassNames);
        return this;
    }

    public Iterable<FunctionFactory> build() {
        if (functionFactoriesCache != null) {
            return functionFactoriesCache;
        }

        functionFactoriesCache = functionFactories;
        return functionFactories;
    }
}
