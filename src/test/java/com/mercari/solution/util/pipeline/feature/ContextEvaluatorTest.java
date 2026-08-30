package com.mercari.solution.util.pipeline.feature;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** countByValue / ratioByValue keys: `values: [1]` must match int, long and double fields alike. */
public class ContextEvaluatorTest {

    @Test
    public void testValueKeyNormalisesIntegralNumbers() {
        Assertions.assertEquals("1", ContextEvaluator.valueKey(1));
        Assertions.assertEquals("1", ContextEvaluator.valueKey(1L));
        Assertions.assertEquals("1", ContextEvaluator.valueKey(1.0d));
        Assertions.assertEquals("1", ContextEvaluator.valueKey(1.0f));
        Assertions.assertEquals("1", ContextEvaluator.valueKey("1.0"));
        Assertions.assertEquals("1.5", ContextEvaluator.valueKey(1.5d));
        Assertions.assertEquals("good", ContextEvaluator.valueKey("good"));
        Assertions.assertEquals("true", ContextEvaluator.valueKey(true));
        Assertions.assertEquals("007", ContextEvaluator.valueKey("007")); // strings without a fraction are kept verbatim
    }

}
