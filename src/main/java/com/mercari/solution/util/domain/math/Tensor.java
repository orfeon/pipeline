package com.mercari.solution.util.domain.math;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A flat primitive array (row-major) with a shape, so that large numeric values are not boxed value by value.
 * {@link #row} and {@link #reshaped} share the array. A Tensor only lives inside a DoFn: it is not serializable,
 * and {@link #materialize} turns it into the repo's canonical value (a flat list) before it reaches an MElement.
 */
public final class Tensor {

    public enum Type { FLOAT, DOUBLE, INT64, INT32, INT16, INT8, BOOL, STRING }

    private final Type type;
    private final Object data;
    private final int offset;
    private final int size;
    private final long[] shape;

    private Tensor(final Type type, final Object data, final int offset, final long[] shape) {
        this.type = type;
        this.data = data;
        this.offset = offset;
        this.size = size(shape);
        this.shape = shape;
    }

    public static Tensor of(final Object data, final long[] shape) {
        final Type type = switch (data) {
            case float[] a -> Type.FLOAT;
            case double[] a -> Type.DOUBLE;
            case long[] a -> Type.INT64;
            case int[] a -> Type.INT32;
            case short[] a -> Type.INT16;
            case byte[] a -> Type.INT8;
            case boolean[] a -> Type.BOOL;
            case String[] a -> Type.STRING;
            default -> throw new IllegalArgumentException("Not supported tensor data: " + data);
        };
        final int length = java.lang.reflect.Array.getLength(data);
        if(length != size(shape)) {
            throw new IllegalArgumentException("tensor data has " + length + " values, but shape: " + Arrays.toString(shape));
        }
        return new Tensor(type, data, 0, shape);
    }

    /** From flat values, converted to {@code type}. */
    public static Tensor of(final Type type, final List<?> values, final long[] shape) {
        if(values.size() != size(shape)) {
            throw new IllegalArgumentException("tensor shape " + Arrays.toString(shape) + " requires " + size(shape) + " values, but got " + values.size());
        }
        final Object data = newArray(type, values.size());
        for(int i=0; i<values.size(); i++) {
            set(type, data, i, values.get(i));
        }
        return new Tensor(type, data, 0, shape);
    }

    /**
     * The shape of flat row-major values of a declared shape, of which one dynamic dimension (-1) at most is restored from
     * their number. {@code name} says what they are in an error.
     */
    public static long[] flatShape(final String name, final List<Integer> declared, final int size) {
        final long[] shape = new long[declared.size()];
        long known = 1;
        int dynamic = -1;
        for(int d=0; d<shape.length; d++) {
            shape[d] = declared.get(d);
            if(shape[d] < 0) {
                if(dynamic >= 0) {
                    throw new IllegalArgumentException(name + " of matrix shape " + declared + " has more than one dynamic dimension");
                }
                dynamic = d;
            } else {
                known *= shape[d];
            }
        }
        if(dynamic >= 0) {
            if(known == 0 || size % known != 0) {
                throw new IllegalArgumentException(name + " has " + size + " values for matrix shape " + declared);
            }
            shape[dynamic] = size / known;
        }
        return shape;
    }

    public static Tensor fill(final Type type, final long[] shape, final Number value) {
        final int size = size(shape);
        final Object data = newArray(type, size);
        for(int i=0; i<size; i++) {
            set(type, data, i, value);
        }
        return new Tensor(type, data, 0, shape);
    }

    /**
     * Stacks the tensors on a new dim0. Tensors of different shapes are padded to the longest of each dimension with
     * {@code padValue}, at the head if {@code padLeft}; without a {@code padValue} they are an error.
     */
    public static Tensor stack(final List<Tensor> tensors, final Number padValue, final boolean padLeft) {
        return stack(tensors, padValue, padLeft, null);
    }

    /** {@link #stack(List, Number, boolean)} with the length of each dimension of a row rounded up to a multiple of {@code multiples}, also for one tensor. */
    public static Tensor stack(final List<Tensor> tensors, final Number padValue, final boolean padLeft, final long[] multiples) {
        final Tensor first = tensors.getFirst();
        final long[] rowShape = first.shape.clone();
        for(final Tensor tensor : tensors) {
            if(tensor.type != first.type || tensor.shape.length != rowShape.length) {
                throw new IllegalArgumentException("tensors to stack must have the same type and rank: " + first + ", " + tensor);
            }
            for(int d=0; d<rowShape.length; d++) {
                rowShape[d] = Math.max(rowShape[d], tensor.shape[d]);
            }
        }
        if(multiples != null) {
            for(int d=0; d<rowShape.length; d++) {
                rowShape[d] = Math.ceilDiv(rowShape[d], multiples[d]) * multiples[d];
            }
        }
        final boolean padding = tensors.stream().anyMatch(tensor -> !Arrays.equals(tensor.shape, rowShape));
        if(padding && padValue == null) {
            throw new IllegalArgumentException("records in a batch have different shapes: " + tensors.stream().map(t -> Arrays.toString(t.shape)).distinct().toList());
        }

        final long[] shape = new long[rowShape.length + 1];
        shape[0] = tensors.size();
        System.arraycopy(rowShape, 0, shape, 1, rowShape.length);
        final int rowSize = size(rowShape);
        final Object data = newArray(first.type, tensors.size() * rowSize);
        if(padding) {
            for(int i=0; i<tensors.size() * rowSize; i++) {
                set(first.type, data, i, padValue);
            }
        }
        for(int i=0; i<tensors.size(); i++) {
            final Tensor tensor = tensors.get(i);
            copyPadded(tensor.data, tensor.offset, tensor.shape, data, i * rowSize, rowShape, padLeft, 0);
        }
        return new Tensor(first.type, data, 0, shape);
    }

    private static void copyPadded(
            final Object src, final int srcPos, final long[] srcShape,
            final Object dst, final int dstPos, final long[] dstShape,
            final boolean padLeft, final int dim) {

        if(dim == srcShape.length) {
            System.arraycopy(src, srcPos, dst, dstPos, 1);
            return;
        }
        final int padding = padLeft ? (int) (dstShape[dim] - srcShape[dim]) : 0;
        if(dim == srcShape.length - 1) {
            System.arraycopy(src, srcPos, dst, dstPos + padding, (int) srcShape[dim]);
            return;
        }
        final int srcStride = size(Arrays.copyOfRange(srcShape, dim + 1, srcShape.length));
        final int dstStride = size(Arrays.copyOfRange(dstShape, dim + 1, dstShape.length));
        for(int i=0; i<srcShape[dim]; i++) {
            copyPadded(src, srcPos + i * srcStride, srcShape, dst, dstPos + (i + padding) * dstStride, dstShape, padLeft, dim + 1);
        }
    }

    public Type type() {
        return type;
    }

    public Object data() {
        return data;
    }

    public int offset() {
        return offset;
    }

    public int size() {
        return size;
    }

    public long[] shape() {
        return shape.clone();
    }

    public int rank() {
        return shape.length;
    }

    /** The row {@code index} of dim0, sharing the array. */
    public Tensor row(final int index) {
        if(shape.length == 0 || index < 0 || index >= shape[0]) {
            throw new IndexOutOfBoundsException("row: " + index + " of tensor shape " + Arrays.toString(shape));
        }
        final long[] rowShape = Arrays.copyOfRange(shape, 1, shape.length);
        return new Tensor(type, data, offset + index * size(rowShape), rowShape);
    }

    /**
     * The values {@code start} (inclusive) to {@code end} (exclusive) of dimension {@code axis}, which stays with the new
     * length. This shares the array when they are contiguous: for dim0, or if the dimensions before {@code axis} are all 1.
     */
    public Tensor slice(final int axis, final long start, final long end) {
        if(axis < 0 || axis >= shape.length || start < 0 || end > shape[axis] || start > end) {
            throw new IndexOutOfBoundsException("slice: " + start + " to " + end + " of axis " + axis + " of tensor shape " + Arrays.toString(shape));
        }
        final long[] sliced = shape.clone();
        sliced[axis] = end - start;
        final int stride = size(Arrays.copyOfRange(shape, axis + 1, shape.length));
        final int outer = size(Arrays.copyOfRange(shape, 0, axis));
        if(outer == 1) {
            return new Tensor(type, data, offset + Math.toIntExact(start) * stride, sliced);
        }
        final int chunk = Math.toIntExact(end - start) * stride;
        final Object copied = newArray(type, outer * chunk);
        for(int i=0; i<outer; i++) {
            System.arraycopy(data, offset + (i * Math.toIntExact(shape[axis]) + Math.toIntExact(start)) * stride, copied, i * chunk, chunk);
        }
        return new Tensor(type, copied, 0, sliced);
    }

    /** The same values with another shape, sharing the array. */
    public Tensor reshaped(final long[] newShape) {
        if(size(newShape) != size) {
            throw new IllegalArgumentException("tensor shape " + Arrays.toString(shape) + " can not be reshaped to " + Arrays.toString(newShape));
        }
        return new Tensor(type, data, offset, newShape);
    }

    public Tensor as(final Type target) {
        if(target == type) {
            return this;
        }
        final Object converted = newArray(target, size);
        for(int i=0; i<size; i++) {
            set(target, converted, i, get(i));
        }
        return new Tensor(target, converted, 0, shape);
    }

    /** The value {@code index} of the flat values: int8 and int16 widen to int32, as a record field holds them. */
    public Object get(final int index) {
        final int i = offset + index;
        return switch (data) {
            case float[] a -> a[i];
            case double[] a -> a[i];
            case long[] a -> a[i];
            case int[] a -> a[i];
            case short[] a -> (int) a[i];
            case byte[] a -> (int) a[i];
            case boolean[] a -> a[i];
            case String[] a -> a[i];
            default -> throw new IllegalStateException();
        };
    }

    public double getDouble(final int index) {
        final int i = offset + index;
        return switch (data) {
            case float[] a -> a[i];
            case double[] a -> a[i];
            case long[] a -> a[i];
            case int[] a -> a[i];
            case short[] a -> a[i];
            case byte[] a -> a[i];
            default -> throw new IllegalArgumentException("tensor of type " + type + " is not numeric");
        };
    }

    public List<Object> toList() {
        final List<Object> values = new ArrayList<>(size);
        for(int i=0; i<size; i++) {
            values.add(get(i));
        }
        return values;
    }

    /** The values as a list that shares the array: a value is only boxed when it is read. */
    public List<Object> asList() {
        return new AbstractList<>() {
            @Override
            public Object get(final int index) {
                if(index < 0 || index >= size) {
                    throw new IndexOutOfBoundsException(index);
                }
                return Tensor.this.get(index);
            }
            @Override
            public int size() {
                return size;
            }
        };
    }

    /** A Tensor as a record field holds it: a flat list, or the value itself for rank 0. Other values are returned as is. */
    public static Object materialize(final Object value) {
        if(value instanceof Tensor tensor) {
            return tensor.shape.length == 0 ? tensor.get(0) : tensor.toList();
        }
        return value;
    }

    @Override
    public String toString() {
        return "Tensor{" + type + Arrays.toString(shape) + "}";
    }

    private static int size(final long[] shape) {
        long size = 1;
        for(final long dim : shape) {
            size *= dim;
        }
        return Math.toIntExact(size);
    }

    private static Object newArray(final Type type, final int size) {
        return switch (type) {
            case FLOAT -> new float[size];
            case DOUBLE -> new double[size];
            case INT64 -> new long[size];
            case INT32 -> new int[size];
            case INT16 -> new short[size];
            case INT8 -> new byte[size];
            case BOOL -> new boolean[size];
            case STRING -> new String[size];
        };
    }

    private static void set(final Type type, final Object data, final int index, final Object value) {
        if(value == null) {
            throw illegal(type, null);
        }
        switch (type) {
            case FLOAT -> ((float[]) data)[index] = switch (value) {
                case Number n -> n.floatValue();
                case Boolean b -> b ? 1f : 0f;
                case String s -> Float.parseFloat(s);
                default -> throw illegal(type, value);
            };
            case DOUBLE -> ((double[]) data)[index] = switch (value) {
                case Number n -> n.doubleValue();
                case Boolean b -> b ? 1d : 0d;
                case String s -> Double.parseDouble(s);
                default -> throw illegal(type, value);
            };
            case INT64 -> ((long[]) data)[index] = switch (value) {
                case Number n -> finite(type, n).longValue();
                case Boolean b -> b ? 1L : 0L;
                case String s -> Long.parseLong(s);
                default -> throw illegal(type, value);
            };
            case INT32 -> ((int[]) data)[index] = switch (value) {
                case Number n -> finite(type, n).intValue();
                case Boolean b -> b ? 1 : 0;
                case String s -> Integer.parseInt(s);
                default -> throw illegal(type, value);
            };
            case INT16 -> ((short[]) data)[index] = switch (value) {
                case Number n -> finite(type, n).shortValue();
                case Boolean b -> (short) (b ? 1 : 0);
                case String s -> Short.parseShort(s);
                default -> throw illegal(type, value);
            };
            case INT8 -> ((byte[]) data)[index] = switch (value) {
                case Number n -> finite(type, n).byteValue();
                case Boolean b -> (byte) (b ? 1 : 0);
                case String s -> Byte.parseByte(s);
                default -> throw illegal(type, value);
            };
            case BOOL -> ((boolean[]) data)[index] = switch (value) {
                case Boolean b -> b;
                case Number n -> finite(type, n).doubleValue() != 0;
                case String s -> Boolean.parseBoolean(s);
                default -> throw illegal(type, value);
            };
            case STRING -> ((String[]) data)[index] = switch (value) {
                case String s -> s;
                case java.nio.ByteBuffer bb -> new String(bb.array(), java.nio.charset.StandardCharsets.UTF_8);
                default -> value.toString();
            };
        }
    }

    // in an integer or a bool tensor a NaN would silently become 0 or true, and an infinity the largest integer
    private static Number finite(final Type type, final Number value) {
        if((value instanceof Double || value instanceof Float) && !Double.isFinite(value.doubleValue())) {
            throw illegal(type, value);
        }
        return value;
    }

    private static IllegalArgumentException illegal(final Type type, final Object value) {
        return new IllegalArgumentException("value: " + value + " can not be a " + type + " tensor value");
    }

}
