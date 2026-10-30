package com.mercari.solution.util.domain.math;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TensorTest {

    private static Tensor ints(final long[] shape, final int... values) {
        return Tensor.of(values, shape);
    }

    @Test
    public void testRowSharesTheArray() {
        final Tensor tensor = ints(new long[]{2, 3}, 1, 2, 3, 4, 5, 6);
        final Tensor row = tensor.row(1);
        Assertions.assertArrayEquals(new long[]{3}, row.shape());
        Assertions.assertEquals(List.of(4, 5, 6), row.toList());
        Assertions.assertSame(tensor.data(), row.data());
        Assertions.assertEquals(List.of(4, 5, 6), Tensor.materialize(row));
        Assertions.assertEquals(5, Tensor.materialize(row.row(1)));
        Assertions.assertEquals("other", Tensor.materialize("other"));
    }

    @Test
    public void testStackSameShape() {
        final Tensor stacked = Tensor.stack(List.of(ints(new long[]{2}, 1, 2), ints(new long[]{2}, 3, 4)), null, false);
        Assertions.assertArrayEquals(new long[]{2, 2}, stacked.shape());
        Assertions.assertEquals(List.of(1, 2, 3, 4), stacked.toList());
    }

    @Test
    public void testStackScalars() {
        final Tensor stacked = Tensor.stack(List.of(ints(new long[0], 1), ints(new long[0], 2)), null, false);
        Assertions.assertArrayEquals(new long[]{2}, stacked.shape());
        Assertions.assertEquals(List.of(1, 2), stacked.toList());
    }

    @Test
    public void testStackDifferentShapesRequiresPad() {
        final List<Tensor> tensors = List.of(ints(new long[]{1}, 1), ints(new long[]{2}, 2, 3));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Tensor.stack(tensors, null, false));
        final Tensor right = Tensor.stack(tensors, 9, false);
        Assertions.assertEquals(List.of(1, 9, 2, 3), right.toList());
        final Tensor left = Tensor.stack(tensors, 9, true);
        Assertions.assertEquals(List.of(9, 1, 2, 3), left.toList());
    }

    @Test
    public void testStackPadsEveryDimension() {
        final List<Tensor> tensors = List.of(
                ints(new long[]{1, 2}, 1, 2),
                ints(new long[]{2, 1}, 3, 4));
        final Tensor right = Tensor.stack(tensors, 0, false);
        Assertions.assertArrayEquals(new long[]{2, 2, 2}, right.shape());
        Assertions.assertEquals(List.of(1, 2, 0, 0, 3, 0, 4, 0), right.toList());
        final Tensor left = Tensor.stack(tensors, 0, true);
        Assertions.assertEquals(List.of(0, 0, 1, 2, 0, 3, 0, 4), left.toList());
    }

    // a view: only its own values
    @Test
    public void testStackOfRows() {
        final Tensor tensor = ints(new long[]{2, 2}, 1, 2, 3, 4);
        final Tensor stacked = Tensor.stack(List.of(tensor.row(1), tensor.row(0)), null, false);
        Assertions.assertEquals(List.of(3, 4, 1, 2), stacked.toList());
    }

    @Test
    public void testConversions() {
        final Tensor values = Tensor.of(Tensor.Type.FLOAT, List.of(1, 2.5, "3", true), new long[]{4});
        Assertions.assertEquals(List.of(1f, 2.5f, 3f, 1f), values.toList());
        Assertions.assertEquals(List.of(1L, 2L, 3L, 1L), values.as(Tensor.Type.INT64).toList());
        Assertions.assertEquals(List.of(true, true, true, true), values.as(Tensor.Type.BOOL).toList());
        Assertions.assertEquals(List.of(1, 2, 3, 1), values.as(Tensor.Type.INT8).toList());
        Assertions.assertThrows(IllegalArgumentException.class, () -> Tensor.of(Tensor.Type.FLOAT, List.of(1), new long[]{2}));
        Assertions.assertArrayEquals(new float[]{7f, 7f}, (float[]) Tensor.fill(Tensor.Type.FLOAT, new long[]{2}, 7).data());
        Assertions.assertEquals(0, Tensor.fill(Tensor.Type.FLOAT, new long[]{2, 0}, 0).size());
    }

}
