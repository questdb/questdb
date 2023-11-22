package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram;
import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public class HistogramTestUtils {
    static AbstractHistogram constructHistogram(Class<?> c, Object... constructorArgs) {
        try {
            Class<?>[] argTypes;
            if (constructorArgs.length == 1) {
                if (constructorArgs[0] instanceof AbstractHistogram) {
                    argTypes = new Class[]{AbstractHistogram.class};
                } else {
                    argTypes = new Class[]{int.class};
                }
            } else if (constructorArgs.length == 2) {
                argTypes = new Class[]{long.class, int.class};
            } else if (constructorArgs.length == 3) {
                argTypes = new Class[]{long.class, long.class, int.class};
            } else {
                throw new RuntimeException("Not an expected signature for Histogram constructor");
            }
            return (AbstractHistogram) c.getConstructor(argTypes).newInstance(constructorArgs);
        } catch (InvocationTargetException ex) {
            if (ex.getTargetException() instanceof IllegalArgumentException) {
                throw new IllegalArgumentException(ex.getTargetException().getMessage(), ex);
            } else {
                throw new RuntimeException("Re-throwing: ", ex);
            }
        } catch (NoSuchMethodException | InstantiationException |
                IllegalAccessException  ex) {
            throw new RuntimeException("Re-throwing: ", ex);
        }
    }

    static AbstractHistogram decodeFromCompressedByteBuffer(Class<?> c,
                                                            final ByteBuffer buffer,
                                                            final long minBarForHighestTrackableValue) {
        try {
            Class<?>[] argTypes = {ByteBuffer.class, long.class};
            Method m = c.getMethod("decodeFromCompressedByteBuffer", argTypes);
            return (AbstractHistogram) m.invoke(null, buffer, minBarForHighestTrackableValue);
        } catch (InvocationTargetException ex) {
            if (ex.getTargetException() instanceof IllegalArgumentException) {
                throw new IllegalArgumentException(ex.getTargetException().getMessage(), ex);
            } else {
                throw new RuntimeException("Re-throwing: ", ex);
            }
        } catch (NoSuchMethodException | IllegalAccessException  ex) {
            throw new RuntimeException("Re-throwing: ", ex);
        }
    }

    static DoubleHistogram constructDoubleHistogram(Class<?> c, Object... constructorArgs) {
        try {
            Class<?>[] argTypes;
            if (constructorArgs.length == 1) {
                if (constructorArgs[0] instanceof DoubleHistogram) {
                    argTypes = new Class[]{DoubleHistogram.class};
                } else {
                    argTypes = new Class[]{int.class};
                }
            } else if (constructorArgs.length == 2) {
                if (constructorArgs[1] instanceof Class) {
                    argTypes = new Class[]{int.class, Class.class};
                } else {
                    argTypes = new Class[]{long.class, int.class};
                }
            } else if (constructorArgs.length == 3) {
                argTypes = new Class[]{long.class, int.class, Class.class};
            } else {
                throw new RuntimeException("Not an expected signature for DoubleHistogram constructor");
            }
            return (DoubleHistogram) c.getDeclaredConstructor(argTypes).newInstance(constructorArgs);
        } catch (InvocationTargetException ex) {
            if (ex.getTargetException() instanceof IllegalArgumentException) {
                throw new IllegalArgumentException(ex.getTargetException().getMessage(), ex);
            } else {
                throw new RuntimeException("Re-throwing: ", ex);
            }
        } catch (NoSuchMethodException | InstantiationException |
                IllegalAccessException  ex) {
            throw new RuntimeException("Re-throwing: ", ex);
        }
    }

    static DoubleHistogram decodeDoubleHistogramFromCompressedByteBuffer(Class<?> c,
                                                            final ByteBuffer buffer,
                                                            final long minBarForHighestTrackableValue) {
        try {
            Class<?>[] argTypes = {ByteBuffer.class, long.class};
            Method m = c.getMethod("decodeFromCompressedByteBuffer", argTypes);
            return (DoubleHistogram) m.invoke(null, buffer, minBarForHighestTrackableValue);
        } catch (InvocationTargetException ex) {
            if (ex.getTargetException() instanceof IllegalArgumentException) {
                throw new IllegalArgumentException(ex.getTargetException().getMessage(), ex);
            } else {
                throw new RuntimeException("Re-throwing: ", ex);
            }
        } catch (NoSuchMethodException | IllegalAccessException  ex) {
            throw new RuntimeException("Re-throwing: ", ex);
        }
    }
}
