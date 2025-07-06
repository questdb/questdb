package io.questdb.griffin;

import io.questdb.cairo.CairoError;
import io.questdb.log.Log;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.jar.JarFile;
import java.lang.reflect.Modifier;
import java.util.zip.ZipFile;

// This class loads Function Factories using reflection
// It scans the classpath for classes that implement the FunctionFactory interface
//
// It handles 3 different cases:
// 1. The code is built as modules file
// 2. The code is built as a directory
// 3. The code is built as a single JAR file
//
// In case of a change all cases have to be tested
// Loading when QuestDB runs as RT built in JDK
// Loading when QuestDB runs as a JAR file
// Loading when QuestDB runs as a directory, that happens when running tests from maven, IDEs
public class FunctionFactoryScanner {

    public static void scan(ArrayList<FunctionFactory> functionFactories, String packageName, String functionListFileName, Class<?> clazz, String moduleName, @Nullable Log log) {
        try {
            int initialSize = functionFactories.size();
            // Load function factories in case the code is built as modules file
            // This is usually the case when binaries are build with JDK baked in
            var classLoader = clazz.getClassLoader();
            findAllClassesFromModules(functionFactories, packageName, classLoader, moduleName, log);


            // In case the previous load failed (returned an empty list)
            // there are 2 more options:
            // 1. The code is built as a directory, usually the case when running from IDE
            // 2. The code is built as a single JAR file, the case of binaries built as No JRE
            if (functionFactories.size() == initialSize) {
                // Get the JAR or directory path from the current class's code source
                String locationPath;
                URL url = clazz.getProtectionDomain().getCodeSource().getLocation();

                if (url != null) {
                    locationPath = url.toURI().getPath().replace("file:", "");
                } else {
                    // If the location path is null, throw an error
                    throw new CairoError("no functions found in " + packageName + ", cannot determine location path");
                }

                if (log != null) {
                    log.advisory().$("loading functions from ").$(locationPath).$();
                }

                // Check if the location is a JAR file or a directory
                if (locationPath.endsWith(".jar")) {
                    // If it's a JAR file, scan it
                    scanJar(functionFactories, locationPath, packageName, classLoader, log);
                } else {
                    // If it's a directory, scan for class files
                    scanDirectory(functionFactories, locationPath, packageName, classLoader, log);
                }
            }

            if (functionFactories.size() == initialSize) {
                throw new CairoError("no functions found in " + packageName);
            }


            var orderMap = new CharSequenceIntHashMap();
            loadFunctionOrderMap(functionListFileName, classLoader, orderMap);
            // Function factories sometimes have conflict and have to be loaded in a specific order
            // For example RndSymbolFunctionFactory has to be before RndSymbolListFunctionFactory
            if (initialSize > 0) {
                functionFactories
                        .subList(initialSize, functionFactories.size())
                        .sort((f1, f2) -> compareFactories(f1, f2, orderMap));
            } else {
                functionFactories.sort((f1, f2) -> compareFactories(f1, f2, orderMap));
            }

            if (log != null) {
                log.advisory().$("loaded ").$(functionFactories.size() - initialSize).$(" functions").$();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int compareFactories(FunctionFactory f1, FunctionFactory f2, CharSequenceIntHashMap orderMap) {
        int o1 = getOrder(f1, orderMap);
        int o2 = getOrder(f2, orderMap);
        return Integer.compare(o1, o2);
    }

    private static void findAllClassesFromModules(ArrayList<FunctionFactory> factories, String packageName, ClassLoader classLoader, String moduleName, @Nullable Log log) {
        try {
            try (var fs = FileSystems.newFileSystem(URI.create("jrt:/"), Collections.emptyMap(), classLoader)) {
                Path questdbPath = fs.getPath("modules", moduleName, moduleName.substring(0, moduleName.indexOf('.')));
                try (var questdbPathFiles = Files.list(questdbPath)) {
                    var sink = new StringSink();
                    questdbPathFiles.forEach(
                            mdl -> {
                                String pathPattern = "modules/" + moduleName + "/" + packageName.replace('.', '/');
                                int replaceLen = "modules/".length() + moduleName.length() + 1;
                                if (log != null) {
                                    log.advisory().$("loading functions from ").$(moduleName).$();
                                }
                                try (var walk = Files.walk(mdl)) {
                                    walk.forEach(
                                            classFile -> {
                                                if (classFile.startsWith(pathPattern)) {
                                                    sink.clear();
                                                    String classNameStr = classFile.toString();
                                                    sink.put(classNameStr, replaceLen, classNameStr.length());
                                                    if (Chars.endsWith(sink, ".class")) {
                                                        sink.trimTo(sink.length() - ".class".length());
                                                    }
                                                    sink.replace('/', '.');

                                                    FunctionFactory factory = getClass(sink, classLoader, log);
                                                    if (factory != null) {
                                                        factories.add(factory);
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

    }

    @Nullable
    private static FunctionFactory getClass(CharSequence className, ClassLoader classLoader, @Nullable Log log) {
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(className.toString());
        } catch (ClassNotFoundException e) {
            return null;
        }

        if (FunctionFactory.class.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())) {
            try {
                // Instantiate the class and add to the list
                return (FunctionFactory) clazz.getDeclaredConstructor().newInstance();
            } catch (NoSuchMethodException e) {
                // It's ok, not a function factory but a wrapper
            } catch (Exception e) {
                if (log != null) {
                    log.critical().$("error loading function: ").$(className).$(", error: ").$(e).$();
                } else {
                    System.out.println("error loading function: " + className + ", error: " + e);
                    e.printStackTrace(System.out);
                }
            }
        }
        return null;
    }

    // Helper method to get the path of the JAR or directory the current class is in

    private static int getOrder(FunctionFactory f1, CharSequenceIntHashMap orderMap) {
        int index = orderMap.keyIndex(f1.getClass().getName());
        if (index < 0) {
            return orderMap.valueAt(index);
        }
        // Unknown functions at the end, longest signature first
        // If name of the signatures match then the functions with fewer arguments come last
        // so that VARARG functions are always last
        return orderMap.size() + Math.abs(10000 - f1.getSignature().length());
    }

    private static void loadFunctionOrderMap(String functionListFileName, ClassLoader classLoader, CharSequenceIntHashMap map) {
        try (var inputStream = classLoader.getResourceAsStream(functionListFileName)) {
            if (inputStream != null) {
                var lines = new String(inputStream.readAllBytes()).split("\n");

                int order = 0;
                for (var line : lines) {
                    var trimmed = line.trim();
                    if (!trimmed.isBlank() && !trimmed.startsWith("#")) {
                        map.put(line.trim(), order++);
                    }
                }
            } else {
                throw new CairoError("functions order file " + functionListFileName + " not found");
            }
        } catch (IOException e) {
            // return empty map
        }
    }

    // Scan for class files in a directory, including subdirectories
    private static void scanDirectory(ArrayList<FunctionFactory> functionFactories, String dirPath, String packageName, ClassLoader classLoader, @Nullable Log log) {
        String packagePath = packageName.replace('.', '/');

        // Java File supports both \ and / as file separators on all platforms, this will work on windows and linux:
        File dir = new File(dirPath + "/" + packagePath);

        if (dir.exists() && dir.isDirectory()) {
            StringSink packageNameSink = new StringSink();
            packageNameSink.put(packageName);
            // Start recursive directory scan
            scanDirectoryRecursively(functionFactories, dir, packageNameSink, classLoader, log);
        } else {
            throw new UnsupportedOperationException("cannot load functions, directory not found: " + dir);
        }
    }

    // Recursive method to scan a directory and its subdirectories
    private static void scanDirectoryRecursively(ArrayList<FunctionFactory> functionFactories, File dir, StringSink packageName, ClassLoader classLoader, @Nullable Log log) {
        File[] files = dir.listFiles();

        int len = packageName.length();
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                if (fileName.endsWith(".class")) {
                    // Get the class name from the file path
                    packageName.trimTo(len);
                    packageName.put('.').put(fileName, 0, fileName.length() - ".class".length());
                    FunctionFactory factory = getClass(packageName, classLoader, log);
                    if (factory != null) {
                        functionFactories.add(factory);
                    }
                } else if (file.isDirectory()) {
                    // Recursively scan subdirectories
                    packageName.trimTo(len);
                    packageName.put('.').put(fileName);
                    scanDirectoryRecursively(functionFactories, file, packageName, classLoader, log);
                }
            }
        }
    }

    // Scan for classes inside a JAR file
    private static void scanJar(ArrayList<FunctionFactory> functionFactories, String jarPath, String packageName, ClassLoader classLoader, @Nullable Log log) {
        try {
            // Get the package path from the package name
            // Jar file separators are always '/' on all platforms
            String pathFilterPrefix = packageName.replace('.', '/');

            StringSink sink = new StringSink();
            // Open the JAR file
            try (var jarFile = new JarFile(new File(jarPath), false, ZipFile.OPEN_READ)) {
                jarFile.stream()
                        .filter(entry -> {
                            String entryName = entry.getName();
                            return entryName.startsWith(pathFilterPrefix) && entryName.endsWith(".class");
                        })
                        .forEach(entry -> {
                                    String entryName = entry.getName();
                                    // Convert entry name to fully qualified class name
                                    sink.clear();
                                    sink.put(entryName, 0, entryName.length() - ".class".length());
                                    sink.replace('/', '.');
                                    FunctionFactory factory = getClass(sink, classLoader, log);
                                    if (factory != null) {
                                        functionFactories.add(factory);
                                    }
                                }
                        );
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
