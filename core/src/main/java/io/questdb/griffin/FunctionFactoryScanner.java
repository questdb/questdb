package io.questdb.griffin;

import io.questdb.cairo.CairoError;
import io.questdb.log.Log;
import io.questdb.std.CharSequenceIntHashMap;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.lang.reflect.Modifier;

public class FunctionFactoryScanner {

    private static List<FunctionFactory> functionFactoriesCache;

    @TestOnly
    public static void clearCache() {
        functionFactoriesCache = null;
    }

    public static List<FunctionFactory> findAllClassesFromModules(String packageName, @Nullable Log log) {
        List<FunctionFactory> factories = new ArrayList<>();
        var loader = ClassLoader.getSystemClassLoader();
        try {
            try (var fs = FileSystems.newFileSystem(URI.create("jrt:/"), new HashMap<>(), loader)) {

                Path questdbPath = fs.getPath("modules", "io.questdb", "io");
                try (var questdbPathFiles = Files.list(questdbPath)) {
                    questdbPathFiles.forEach(
                            mdl -> {
                                String pathPattern = "modules/io.questdb/" + packageName.replace('.', '/');
                                if (log != null) {
                                    log.advisory().$("loading functions from ").$(mdl.getFileName()).$();
                                }
                                try (var walk = Files.walk(mdl)) {
                                    walk.forEach(
                                            classFile -> {
                                                if (classFile.startsWith(pathPattern)) {
                                                    String className = classFile.toString().substring("modules/io.questdb/".length())
                                                            .replace("/", ".")
                                                            .replace(".class", "");


                                                    if (className.startsWith(packageName)) {
                                                        FunctionFactory factory = getClass(className, log, className);
                                                        if (factory != null) {
                                                            factories.add(factory);
                                                        }
                                                    }
                                                }
                                            }
                                    );
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    );
                } catch (NoSuchFileException e) {
                    // It's ok, if it's not a module
                }
            }
        } catch (IOException e) {
            // It's ok, if it's not a module
        }

        return factories;
    }

    public static Iterable<FunctionFactory> scan(String packageName, @Nullable Log log) {
        if (functionFactoriesCache != null) {
            return functionFactoriesCache;
        }

        try {
            var orderMap = loadFunctionOrderMap();

            var functionFactories = findAllClassesFromModules(packageName, log);
            if (functionFactories.isEmpty()) {
                // Get the JAR or directory path from the current class's code source
                String locationPath = getCurrentLocationPath();
                if (locationPath == null) {
                    throw new CairoError("no functions found in " + packageName + ", cannot determine location path");
                }
                if (log != null) {
                    log.advisory().$("loading functions from ").$(locationPath).$();
                }

                // Check if the location is a JAR file or a directory
                if (locationPath.endsWith(".jar")) {
                    // If it's a JAR file, scan it
                    scanJar(functionFactories, locationPath, packageName, log);
                } else {
                    // If it's a directory, scan for class files
                    scanDirectory(functionFactories, locationPath, packageName, log);
                }
            }

            if (functionFactories.isEmpty()) {
                throw new CairoError("no functions found in " + packageName);
            }

            // Function factories sometimes have conflict and have to be loaded in a specific order
            // For example RndSymbolFunctionFactory has to be before RndSymbolListFunctionFactory
            functionFactories.sort((f1, f2) -> compareFactories(f1, f2, orderMap));

            if (log != null) {
                log.advisory().$("loaded ").$(functionFactories.size()).$(" functions").$();
            }
            functionFactoriesCache = functionFactories;
            return functionFactories;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Iterable<FunctionFactory> scan(@Nullable Log log) {
        return scan("io.questdb.griffin.engine.functions", log);
    }

    private static int compareFactories(FunctionFactory f1, FunctionFactory f2, CharSequenceIntHashMap orderMap) {
        int o1 = getOrder(f1, orderMap);
        int o2 = getOrder(f2, orderMap);
        return Integer.compare(o1, o2);
    }

    @Nullable
    private static FunctionFactory getClass(String className, @Nullable Log log, String name) {
        @SuppressWarnings("rawtypes") Class clazz;
        try {
            clazz = Class.forName(name);
        } catch (ClassNotFoundException e) {
            return null;
        }

        if (FunctionFactory.class.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())) {
            try {
                // Instantiate the class and add to the list
                @SuppressWarnings("unchecked") FunctionFactory instance = (FunctionFactory) clazz.getDeclaredConstructor().newInstance();
                return instance;
            } catch (NoSuchMethodException e) {
                // It's ok, not a function factory but a wrapper
            } catch (Exception e) {
                if (log != null) {
                    log.advisory().$("error loading function: ").$(className).$(", error: ").$(e).$();
                } else {
                    System.out.println("error loading function: " + className + ", error: " + e);
                    e.printStackTrace(System.out);
                }
            }
        }
        return null;
    }

    // Helper method to get the path of the JAR or directory the current class is in
    private static String getCurrentLocationPath() {
        URL url = FunctionFactoryScanner.class.getProtectionDomain().getCodeSource().getLocation();
        if (url != null) {
            return url.getPath().replace("file:", "");
        }
        return null;
    }

    private static int getOrder(FunctionFactory f1, CharSequenceIntHashMap orderMap) {
        int index = orderMap.keyIndex(f1.getClass().getName());
        if (index < 0) {
            return orderMap.valueAt(index);
        }
        // Unknown functions at the end
        return orderMap.size() + 1;
    }

    private static CharSequenceIntHashMap loadFunctionOrderMap() {
        var fileName = "function_list.txt";
        var map = new CharSequenceIntHashMap();

        try (InputStream inputStream = FunctionFactoryScanner.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream != null) {
                var lines = new String(inputStream.readAllBytes()).split("\n");

                int order = 0;
                for (var line : lines) {
                    var trimmed = line.trim();
                    if (!trimmed.isBlank() && !trimmed.startsWith("#")) {
                        map.put(line.trim(), order++);
                    }
                }
            }
        } catch (IOException e) {
            // return empty map
        }
        return map;
    }

    // Scan for class files in a directory, including subdirectories
    private static void scanDirectory(List<FunctionFactory> functionFactories, String dirPath, String packageName, @Nullable Log log) {
        String packagePath = packageName.replace('.', '/');
        File dir = new File(dirPath + "/" + packagePath);

        if (dir.exists() && dir.isDirectory()) {
            // Start recursive directory scan
            scanDirectoryRecursively(functionFactories, dir, packageName, log);
        } else {
            throw new UnsupportedOperationException("cannot load functions, directory not found: " + dirPath);
        }
    }

    // Recursive method to scan a directory and its subdirectories
    private static void scanDirectoryRecursively(List<FunctionFactory> functionFactories, File dir, String packageName, @Nullable Log log) {
        File[] files = dir.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    // Recursively scan subdirectories
                    scanDirectoryRecursively(functionFactories, file, packageName + '.' + file.getName(), log);
                } else if (file.getName().endsWith(".class")) {
                    // Get the class name from the file path
                    String className = packageName + '.' + file.getName().replace(".class", "");
                    FunctionFactory factory = getClass(className, log, className);
                    if (factory != null) {
                        functionFactories.add(factory);
                    }
                }
            }
        }
    }

    // Scan for classes inside a JAR file
    private static void scanJar(List<FunctionFactory> functionFactories, String jarPath, String packageName, @Nullable Log log) {
        try {
            // Get the package path from the package name
            String path = packageName.replace('.', '/');

            // Open the JAR file
            try (JarFile jarFile = new JarFile(jarPath)) {
                Enumeration<JarEntry> entries = jarFile.entries();

                // Iterate over the JAR file entries
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();

                    // Only process class files in the specified package or subpackages
                    if (entry.getName().startsWith(path) && entry.getName().endsWith(".class")) {
                        // Convert entry name to fully qualified class name
                        String className = entry.getName().replace('/', '.').replace(".class", "");
                        FunctionFactory factory = getClass(className, log, className);
                        if (factory != null) {
                            functionFactories.add(factory);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
