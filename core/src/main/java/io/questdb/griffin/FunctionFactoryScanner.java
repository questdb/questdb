package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.log.Log;
import io.questdb.std.CharSequenceIntHashMap;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.lang.reflect.Modifier;

public class FunctionFactoryScanner {

    public static Iterable<FunctionFactory> scan(@Nullable Log log) {
        return scan("io.questdb.griffin.engine", log);
    }

    public static Iterable<FunctionFactory> scan(String packageName, @Nullable Log log) {
        try {
            List<FunctionFactory> functionFactories = new ArrayList<>();

            var orderMap = loadFunctionOrderMap();

            // Get the JAR or directory path from the current class's code source
            String locationPath = getCurrentLocationPath();
            if (locationPath == null) {
                throw new RuntimeException("Could not determine the JAR or directory.");
            }

            // Check if the location is a JAR file or a directory
            if (locationPath.endsWith(".jar")) {
                // If it's a JAR file, scan it
                scanJar(functionFactories, locationPath, packageName, log);
            } else {
                // If it's a directory, scan for class files
                scanDirectory(functionFactories, locationPath, packageName, log);
            }


            // Function factories sometimes have conflict and have to be loaded in a specific order
            // For example RndSymbolFunctionFactory has to be before RndSymbolListFunctionFactory
            functionFactories.sort((f1, f2) -> compareFactories(f1, f2, orderMap));

            return functionFactories;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int compareFactories(FunctionFactory f1, FunctionFactory f2, CharSequenceIntHashMap orderMap) {
        int o1 = getOrder(f1, orderMap);
        int o2 = getOrder(f2, orderMap);
        return Integer.compare(o1, o2);
    }

    // Helper method to get the path of the JAR or directory the current class is in
    private static String getCurrentLocationPath() {
        URL url = FunctionFactoryScanner.class.getProtectionDomain().getCodeSource().getLocation();
        if (url != null) {
            return url.getPath().replace("file:", "");
        }
        return null;
    }

    private static int getDescriptorScore(FunctionFactoryDescriptor descriptor) {
        try {
            if (descriptor.getSigArgCount() == 0) {
                return 0;
            }

            final int lastSigArgMask = descriptor.getArgTypeMask(descriptor.getSigArgCount() - 1);
            var sigVarArg = FunctionFactoryDescriptor.toType(lastSigArgMask) == ColumnType.VAR_ARG;
            var sigVarArgConst = sigVarArg && FunctionFactoryDescriptor.isConstant(lastSigArgMask);

            // Push vararg to the end
            // and const vararg to the very end
            if (sigVarArgConst) {
                return Integer.MAX_VALUE - 1;
            }
            if (sigVarArg) {
                return Integer.MAX_VALUE - 2;
            }

            // Default
            return 1000 + descriptor.getSigArgCount();
        } catch (Exception e) {
            return Integer.MAX_VALUE;
        }
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
            scanDirectoryRecursively(functionFactories, dir, packagePath, packageName, log);
        }
    }

    // Recursive method to scan a directory and its subdirectories
    private static void scanDirectoryRecursively(List<FunctionFactory> functionFactories, File dir, String packagePath, String packageName, @Nullable Log log) {
        File[] files = dir.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    // Recursively scan subdirectories
                    scanDirectoryRecursively(functionFactories, file, packagePath, packageName + '.' + file.getName(), log);
                } else if (file.getName().endsWith(".class")) {
                    // Get the class name from the file path
                    String className = packageName + '.' + file.getName().replace(".class", "");
                    Class<?> clazz = null;
                    try {
                        clazz = Class.forName(className);
                    } catch (ClassNotFoundException e) {
                        // Ignore classes that can't be loaded
                        log.advisory().$("could not load function class: ").$(className).$();
                    }

                    // Check if the class implements FunctionFactory and is not abstract
                    if (FunctionFactory.class.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())) {
                        try {
                            // Instantiate the class and add to the list
                            FunctionFactory instance = (FunctionFactory) clazz.getDeclaredConstructor().newInstance();
                            functionFactories.add(instance);
                            System.out.println(className);
                        } catch (NoSuchMethodException e) {
                            // It's ok, not a function factory but a wrapper
                        } catch (Exception e) {
                            log.advisory().$("error loading function: ").$(className).$();
                        }
                    }
                }
            }
        }
    }

    // Scan for classes inside a JAR file
    private static void scanJar(List<FunctionFactory> functionFactories, String jarPath, String packageName, @Nullable Log log) {
        try {
            // Create a URLClassLoader to load classes from the JAR file
            URL jarFileUrl = new URL("jar:file:" + jarPath + "!/");
            URLClassLoader classLoader = new URLClassLoader(new URL[]{jarFileUrl});

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

                        // Load the class and check if it implements FunctionFactory
                        try {
                            Class<?> clazz = classLoader.loadClass(className);
                            if (FunctionFactory.class.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())) {
                                try {
                                    // Instantiate the class and add to the list
                                    FunctionFactory instance = (FunctionFactory) clazz.getDeclaredConstructor().newInstance();
                                    functionFactories.add(instance);
                                } catch (NoSuchMethodException e) {
                                    // It's ok, not a function factory but a wrapper
                                } catch (Exception e) {
                                    log.advisory().$("error loading function: ").$(className).$();
                                }
                            }
                        } catch (Exception e) {
                            log.advisory().$("could not load class: ").$(className).$();
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
