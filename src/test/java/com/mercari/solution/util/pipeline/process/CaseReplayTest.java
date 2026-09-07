package com.mercari.solution.util.pipeline.process;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CaseReplayTest {

    private static ProcessSpec spec(final String json) {
        final ProcessSpec spec = ProcessSpec.parse(JsonParser.parseString(json).getAsJsonObject());
        final List<String> errors = spec.validate(null);
        Assertions.assertTrue(errors.isEmpty(), errors.toString());
        return spec;
    }

    private static CaseReplay.Event e(final String activity, final long minutes, final String resource) {
        return new CaseReplay.Event(activity, minutes * 60_000L, resource, null);
    }

    private static Map<String, Boolean> verdicts(final CaseReplay.Result result) {
        return result.verdicts.stream().collect(Collectors.toMap(CaseReplay.Verdict::name, CaseReplay.Verdict::violated));
    }

    @Test
    public void testEdgesVariantAndHandovers() {
        final ProcessSpec spec = spec("{caseId: id, activity: a, resource: r}");
        final CaseReplay.Result r = CaseReplay.replay(List.of(e("A", 0, "x"), e("B", 10, "x"), e("A", 25, "y"), e("C", 30, "z")), spec);
        Assertions.assertEquals(4, r.length);
        Assertions.assertEquals("A -> B -> A -> C", r.variant);
        Assertions.assertEquals(List.of("A", "B", "A", "C"), r.activities);
        Assertions.assertEquals(30 * 60_000L, r.durationMillis());
        Assertions.assertEquals("A", r.firstActivity);
        Assertions.assertEquals("C", r.lastActivity);
        Assertions.assertEquals(List.of(
                new CaseReplay.Edge(ProcessSpec.START_NODE, "A", -1L),
                new CaseReplay.Edge("A", "B", 10 * 60_000L),
                new CaseReplay.Edge("B", "A", 15 * 60_000L),
                new CaseReplay.Edge("A", "C", 5 * 60_000L),
                new CaseReplay.Edge("C", ProcessSpec.END_NODE, -1L)), r.edges);
        Assertions.assertEquals(2L, r.activityCounts.get("A").frequency);
        Assertions.assertTrue(r.activityCounts.get("A").first);
        Assertions.assertFalse(r.activityCounts.get("A").last);
        Assertions.assertTrue(r.activityCounts.get("C").last);
        Assertions.assertEquals(List.of("x", "y", "z"), r.resources);
        Assertions.assertEquals(Map.of("x\u0000y", 1L, "y\u0000z", 1L), r.handovers);
        Assertions.assertEquals(1D, r.fitness(), 1e-12);
    }

    @Test
    public void testNoStartEndAndTruncation() {
        final ProcessSpec spec = spec("{caseId: id, activity: a, dfg: {startEnd: false}, engine: {maxTraceLength: 2}}");
        final CaseReplay.Result r = CaseReplay.replay(List.of(e("A", 0, null), e("B", 1, null), e("C", 2, null), e("D", 3, null)), spec);
        Assertions.assertEquals(4, r.length);
        Assertions.assertTrue(r.truncated);
        Assertions.assertEquals(List.of("A", "B"), r.activities);
        Assertions.assertEquals("A -> B -> ...(+2)", r.variant);
        Assertions.assertEquals(3, r.edges.size());
        Assertions.assertEquals("A", r.edges.get(0).source());
        Assertions.assertEquals("D", r.edges.get(2).target());
    }

    @Test
    public void testSequenceBreaksTimestampTies() {
        final ProcessSpec spec = spec("{caseId: id, activity: a, sequence: s, dfg: {startEnd: false}}");
        final List<CaseReplay.Event> events = List.of(
                new CaseReplay.Event("B", 1000L, null, 2D),
                new CaseReplay.Event("A", 1000L, null, 1D),
                new CaseReplay.Event("C", 2000L, null, 1D));
        Assertions.assertEquals("A -> B -> C", CaseReplay.replay(events, spec).variant);
        // without a sequence field the input order of the tie is kept
        final ProcessSpec noSequence = spec("{caseId: id, activity: a, dfg: {startEnd: false}}");
        Assertions.assertEquals("B -> A -> C", CaseReplay.replay(events, noSequence).variant);
        final List<CaseReplay.Event> text = List.of(
                new CaseReplay.Event("B", 1000L, null, "b"),
                new CaseReplay.Event("A", 1000L, null, "a"));
        Assertions.assertEquals("A -> B", CaseReplay.replay(text, spec).variant);
    }

    @Test
    public void testDeclareConstraints() {
        final ProcessSpec spec = spec("""
                {caseId: id, activity: a, constraints: [
                  {type: existence, activity: A},
                  {type: existence, name: twoB, activity: B, min: 2},
                  {type: absence, activity: X},
                  {type: absence, name: atMostOneA, activity: A, max: 1},
                  {type: exactly, activity: C, count: 1},
                  {type: init, activity: A},
                  {type: end, activity: C},
                  {type: response, activity: A, target: B},
                  {type: precedence, activity: A, target: B},
                  {type: succession, activity: B, target: C},
                  {type: chainResponse, activity: B, target: C},
                  {type: chainPrecedence, activity: B, target: C},
                  {type: notCoexistence, activity: A, target: X},
                  {type: notCoexistence, name: noAandC, activity: A, target: C},
                  {type: duration, maxDuration: PT1H},
                  {type: duration, name: aToC, activity: A, target: C, maxDuration: PT10M}
                ]}""");
        // A(0) B(10) A(20) B(30) C(40)
        final CaseReplay.Result r = CaseReplay.replay(List.of(e("A", 0, null), e("B", 10, null), e("A", 20, null), e("B", 30, null), e("C", 40, null)), spec);
        final Map<String, Boolean> v = verdicts(r);
        Assertions.assertEquals(16, v.size());
        Assertions.assertFalse(v.get("existence(A)"));
        Assertions.assertFalse(v.get("twoB"));
        Assertions.assertFalse(v.get("absence(X)"));
        Assertions.assertTrue(v.get("atMostOneA"));
        Assertions.assertFalse(v.get("exactly(C)"));
        Assertions.assertFalse(v.get("init(A)"));
        Assertions.assertFalse(v.get("end(C)"));
        Assertions.assertFalse(v.get("response(A, B)"));
        Assertions.assertFalse(v.get("precedence(A, B)"));
        Assertions.assertFalse(v.get("succession(B, C)"));
        // the first B is followed by A, not C
        Assertions.assertTrue(v.get("chainResponse(B, C)"));
        Assertions.assertFalse(v.get("chainPrecedence(B, C)"));
        Assertions.assertFalse(v.get("notCoexistence(A, X)"));
        Assertions.assertTrue(v.get("noAandC"));
        Assertions.assertFalse(v.get("duration <= PT1H"));
        Assertions.assertTrue(v.get("aToC"));
        Assertions.assertEquals(List.of("atMostOneA", "chainResponse(B, C)", "noAandC", "aToC"), r.violations());
        Assertions.assertEquals(1D - 4D / 16, r.fitness(), 1e-12);

        // B without any A: response not applicable, precedence violated; chainResponse of a trailing B violated
        final ProcessSpec spec2 = spec("{caseId: id, activity: a, constraints: [{type: response, activity: A, target: B}, {type: precedence, activity: A, target: B}, {type: chainResponse, activity: B, target: C}]}");
        final CaseReplay.Result r2 = CaseReplay.replay(List.of(e("B", 0, null)), spec2);
        Assertions.assertFalse(r2.verdicts.get(0).applicable());
        Assertions.assertTrue(r2.verdicts.get(1).violated());
        Assertions.assertTrue(r2.verdicts.get(2).violated());
        Assertions.assertEquals(0D, r2.fitness(), 1e-12);
    }

    @Test
    public void testSpecValidation() {
        final ProcessSpec spec = ProcessSpec.parse(JsonParser.parseString("{activity: a, activities: [{name: __start__}], dfg: {minFrequency: 0}, constraints: [{type: exactly, activity: A}]}").getAsJsonObject());
        final List<String> errors = spec.validate(null);
        Assertions.assertTrue(errors.stream().anyMatch(m -> m.contains("caseId is required")), errors.toString());
        Assertions.assertTrue(errors.stream().anyMatch(m -> m.contains("exclusive")), errors.toString());
        Assertions.assertTrue(errors.stream().anyMatch(m -> m.contains("reserved")), errors.toString());
        Assertions.assertTrue(errors.stream().anyMatch(m -> m.contains("dfg.minFrequency")), errors.toString());
        Assertions.assertTrue(errors.stream().anyMatch(m -> m.contains("requires count")), errors.toString());
        Assertions.assertThrows(IllegalArgumentException.class, () -> ProcessSpec.parse(JsonParser.parseString("{caseId: id, activity: a, constraints: [{type: eventually}]}").getAsJsonObject()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ProcessSpec.parse(JsonParser.parseString("{caseId: id, activity: a, performance: {unit: weeks}}").getAsJsonObject()));
        Assertions.assertEquals(ProcessSpec.Unit.hours, ProcessSpec.parse(JsonParser.parseString("{caseId: id, activity: a, performance: {unit: HOURS}}").getAsJsonObject()).unit);
    }
}
