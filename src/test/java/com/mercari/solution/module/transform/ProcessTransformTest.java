package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Config-driven e2e tests of the process transform on a small order-handling log: four orders, two of which
 * follow the happy path, one cancelled, one shipped before it was paid.
 */
public class ProcessTransformTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final String ORDERS_SOURCE = """
            sources:
              - name: orders
                module: create
                parameters:
                  type: element
                  elements:
                    - {order_id: O1, status: Created,   updated_at: "2025-01-01T10:00:00Z", assignee: alice, country: JP, seq: 1}
                    - {order_id: O1, status: Paid,      updated_at: "2025-01-01T10:30:00Z", assignee: alice, country: JP, seq: 2}
                    - {order_id: O1, status: Shipped,   updated_at: "2025-01-01T12:00:00Z", assignee: bob,   country: JP, seq: 3}
                    - {order_id: O1, status: Delivered, updated_at: "2025-01-02T12:00:00Z", assignee: carol, country: JP, seq: 4}
                    - {order_id: O2, status: Created,   updated_at: "2025-01-01T11:00:00Z", assignee: alice, country: US, seq: 1}
                    - {order_id: O2, status: Paid,      updated_at: "2025-01-01T11:30:00Z", assignee: alice, country: US, seq: 2}
                    - {order_id: O2, status: Cancelled, updated_at: "2025-01-01T13:00:00Z", assignee: alice, country: US, seq: 3}
                    - {order_id: O3, status: Created,   updated_at: "2025-01-03T09:00:00Z", assignee: bob,   country: JP, seq: 1}
                    - {order_id: O3, status: Shipped,   updated_at: "2025-01-03T10:00:00Z", assignee: bob,   country: JP, seq: 2}
                    - {order_id: O3, status: Paid,      updated_at: "2025-01-03T11:00:00Z", assignee: alice, country: JP, seq: 3}
                    - {order_id: O3, status: Delivered, updated_at: "2025-01-04T11:00:00Z", assignee: carol, country: JP, seq: 4}
                    - {order_id: O4, status: Created,   updated_at: "2025-01-05T10:00:00Z", assignee: alice, country: US, seq: 1}
                    - {order_id: O4, status: Paid,      updated_at: "2025-01-05T10:30:00Z", assignee: alice, country: US, seq: 2}
                    - {order_id: O4, status: Shipped,   updated_at: "2025-01-05T12:00:00Z", assignee: bob,   country: US, seq: 3}
                    - {order_id: O4, status: Delivered, updated_at: "2025-01-06T12:00:00Z", assignee: carol, country: US, seq: 4}
                  schema:
                    fields:
                      - {name: order_id, type: string}
                      - {name: status, type: string}
                      - {name: updated_at, type: timestamp}
                      - {name: assignee, type: string}
                      - {name: country, type: string}
                      - {name: seq, type: int64}
            """;

    private static Map<String, MElement> byKey(final Iterable<MElement> rows, final String... fields) {
        final Map<String, MElement> map = new HashMap<>();
        for (final MElement r : rows) {
            final StringBuilder sb = new StringBuilder();
            for (final String f : fields) {
                if (sb.length() > 0) sb.append('/');
                sb.append(r.getAsString(f));
            }
            map.put(sb.toString(), r);
        }
        return map;
    }

    @Test
    public void testDiscoveryPerformanceAndConformance() throws Exception {
        final String config = ORDERS_SOURCE + """
                transforms:
                  - name: mining
                    module: process
                    inputs: [orders]
                    parameters:
                      caseId: order_id
                      activity: status
                      timestamp: updated_at
                      sequence: seq
                      resource: assignee
                      attributes: [country]
                      performance:
                        unit: minutes
                      constraints:
                        - {name: paidBeforeShipped, type: precedence, activity: Paid, target: Shipped}
                        - {name: startsWithCreated, type: init, activity: Created}
                        - {name: within24h, type: duration, maxDuration: PT24H}
                        - {name: shipToDeliver, type: chainResponse, activity: Shipped, target: Delivered}
                """;
        final Config c = Config.load(config);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, c);

        PAssert.that(outputs.get("mining").getCollection()).satisfies(rows -> {
            final Map<String, MElement> edges = byKey(rows, "source", "target");
            Assertions.assertEquals(4L, edges.get("__start__/Created").getAsLong("frequency"));
            Assertions.assertNull(edges.get("__start__/Created").getAsDouble("durationMean"));
            final MElement createdPaid = edges.get("Created/Paid");
            Assertions.assertEquals(3L, createdPaid.getAsLong("frequency"));
            Assertions.assertEquals(3L, createdPaid.getAsLong("caseCount"));
            Assertions.assertEquals(30D, createdPaid.getAsDouble("durationMean"), 1e-9);
            Assertions.assertEquals(30D, createdPaid.getAsDouble("durationMin"), 1e-9);
            Assertions.assertEquals(30D, createdPaid.getAsDouble("durationMedian"), 1e-9);
            final MElement shippedDelivered = edges.get("Shipped/Delivered");
            Assertions.assertEquals(2L, shippedDelivered.getAsLong("frequency"));
            Assertions.assertEquals(1440D, shippedDelivered.getAsDouble("durationMean"), 1e-9);
            Assertions.assertEquals(1L, edges.get("Created/Shipped").getAsLong("frequency"));
            Assertions.assertEquals(1L, edges.get("Cancelled/__end__").getAsLong("frequency"));
            Assertions.assertEquals(3L, edges.get("Delivered/__end__").getAsLong("frequency"));
            Assertions.assertEquals(10, edges.size());
            return null;
        });
        PAssert.that(outputs.get("mining.nodes").getCollection()).satisfies(rows -> {
            final Map<String, MElement> nodes = byKey(rows, "activity");
            Assertions.assertEquals(5, nodes.size());
            Assertions.assertEquals(4L, nodes.get("Created").getAsLong("frequency"));
            Assertions.assertEquals(4L, nodes.get("Created").getAsLong("startCount"));
            Assertions.assertEquals(0L, nodes.get("Created").getAsLong("endCount"));
            Assertions.assertEquals(3L, nodes.get("Delivered").getAsLong("endCount"));
            Assertions.assertEquals(3L, nodes.get("Delivered").getAsLong("caseCount"));
            Assertions.assertEquals(1L, nodes.get("Cancelled").getAsLong("endCount"));
            return null;
        });
        PAssert.that(outputs.get("mining.variants").getCollection()).satisfies(rows -> {
            final Map<String, MElement> variants = byKey(rows, "variant");
            Assertions.assertEquals(3, variants.size());
            final MElement happy = variants.get("Created -> Paid -> Shipped -> Delivered");
            Assertions.assertEquals(2L, happy.getAsLong("caseCount"));
            Assertions.assertEquals(0.5D, happy.getAsDouble("caseShare"), 1e-9);
            Assertions.assertEquals(4L, happy.getAsLong("length"));
            Assertions.assertEquals(List.of("Created", "Paid", "Shipped", "Delivered"), happy.getPrimitiveValue("activities"));
            Assertions.assertEquals(1560D, happy.getAsDouble("durationMean"), 1e-9);
            Assertions.assertEquals(1L, variants.get("Created -> Paid -> Cancelled").getAsLong("caseCount"));
            return null;
        });
        PAssert.that(outputs.get("mining.cases").getCollection()).satisfies(rows -> {
            final Map<String, MElement> cases = byKey(rows, "caseId");
            Assertions.assertEquals(4, cases.size());
            final MElement o3 = cases.get("O3");
            Assertions.assertEquals("Created -> Shipped -> Paid -> Delivered", o3.getAsString("variant"));
            Assertions.assertEquals("JP", o3.getAsString("country"));
            Assertions.assertEquals(4L, o3.getAsLong("length"));
            Assertions.assertEquals(List.of("bob", "alice", "carol"), o3.getPrimitiveValue("resources"));
            Assertions.assertEquals(3L, o3.getAsLong("resourceCount"));
            Assertions.assertEquals(26 * 60D, o3.getAsDouble("duration"), 1e-9);
            Assertions.assertEquals(List.of("paidBeforeShipped", "within24h", "shipToDeliver"), o3.getPrimitiveValue("violations"));
            Assertions.assertEquals(0.25D, o3.getAsDouble("fitness"), 1e-9);
            final MElement o2 = cases.get("O2");
            Assertions.assertEquals(List.of(), o2.getPrimitiveValue("violations"));
            Assertions.assertEquals(1D, o2.getAsDouble("fitness"), 1e-9);
            Assertions.assertEquals("US", o2.getAsString("country"));
            final MElement o1 = cases.get("O1");
            Assertions.assertEquals(List.of("within24h"), o1.getPrimitiveValue("violations"));
            Assertions.assertEquals(0.75D, o1.getAsDouble("fitness"), 1e-9);
            Assertions.assertEquals("2025-01-01T10:00:00.000Z", o1.getAsJodaInstant("startTime").toString());
            Assertions.assertEquals("2025-01-02T12:00:00.000Z", o1.getAsJodaInstant("endTime").toString());
            return null;
        });
        PAssert.that(outputs.get("mining.handovers").getCollection()).satisfies(rows -> {
            final Map<String, MElement> handovers = byKey(rows, "source", "target");
            Assertions.assertEquals(2L, handovers.get("bob/carol").getAsLong("frequency"));
            Assertions.assertEquals(2L, handovers.get("bob/carol").getAsLong("caseCount"));
            Assertions.assertEquals(2L, handovers.get("alice/bob").getAsLong("frequency"));
            Assertions.assertEquals(1L, handovers.get("bob/alice").getAsLong("frequency"));
            Assertions.assertEquals(1L, handovers.get("alice/carol").getAsLong("frequency"));
            Assertions.assertEquals(4, handovers.size());
            return null;
        });
        PAssert.that(outputs.get("mining.conformance").getCollection()).satisfies(rows -> {
            final Map<String, MElement> conformance = byKey(rows, "constraint");
            Assertions.assertEquals(4, conformance.size());
            final MElement precedence = conformance.get("paidBeforeShipped");
            Assertions.assertEquals("precedence", precedence.getAsString("type"));
            Assertions.assertEquals(4L, precedence.getAsLong("cases"));
            Assertions.assertEquals(3L, precedence.getAsLong("applicable"));
            Assertions.assertEquals(1L, precedence.getAsLong("violations"));
            Assertions.assertEquals(1D / 3, precedence.getAsDouble("violationRate"), 1e-9);
            Assertions.assertEquals(0L, conformance.get("startsWithCreated").getAsLong("violations"));
            Assertions.assertEquals(3L, conformance.get("within24h").getAsLong("violations"));
            Assertions.assertEquals(1L, conformance.get("shipToDeliver").getAsLong("violations"));
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testActivityRulesAndMinFrequency() throws Exception {
        final String config = ORDERS_SOURCE + """
                transforms:
                  - name: mining
                    module: process
                    inputs: [orders]
                    parameters:
                      caseId: [country, order_id]
                      activities:
                        - {name: open, filter: "status = 'Created'"}
                        - {name: money, filter: "status IN ('Paid', 'Cancelled')"}
                        - {name: fulfil, filter: {key: status, op: in, value: [Shipped, Delivered]}}
                      timestamp: updated_at
                      dfg:
                        minFrequency: 2
                        startEnd: false
                """;
        final Config c = Config.load(config);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, c);

        PAssert.that(outputs.get("mining").getCollection()).satisfies(rows -> {
            final Map<String, MElement> edges = byKey(rows, "source", "target");
            // open->money (3), money->fulfil (3), fulfil->fulfil (2); money->money (1), open->fulfil (1), fulfil->money (1) are cut
            Assertions.assertEquals(3, edges.size());
            Assertions.assertEquals(3L, edges.get("open/money").getAsLong("frequency"));
            Assertions.assertEquals(3L, edges.get("money/fulfil").getAsLong("frequency"));
            Assertions.assertEquals(2L, edges.get("fulfil/fulfil").getAsLong("frequency"));
            Assertions.assertEquals(1440D, edges.get("fulfil/fulfil").getAsDouble("durationMean") / 60, 1e-9);
            return null;
        });
        PAssert.that(outputs.get("mining.cases").getCollection()).satisfies(rows -> {
            final Map<String, MElement> cases = byKey(rows, "caseId");
            Assertions.assertEquals(4, cases.size());
            Assertions.assertEquals("open -> money -> fulfil -> fulfil", cases.get("JP|O1").getAsString("variant"));
            Assertions.assertEquals("open -> fulfil -> money -> fulfil", cases.get("JP|O3").getAsString("variant"));
            Assertions.assertEquals(0L, cases.get("JP|O1").getAsLong("resourceCount"));
            Assertions.assertNull(cases.get("JP|O1").getPrimitiveValue("fitness"));
            return null;
        });
        PAssert.that(outputs.get("mining.handovers").getCollection()).empty();
        PAssert.that(outputs.get("mining.conformance").getCollection()).empty();
        pipeline.run();
    }

    @Test
    public void testUnmatchedEventsDroppedByDefault() throws Exception {
        final String config = ORDERS_SOURCE + """
                transforms:
                  - name: mining
                    module: process
                    inputs: [orders]
                    parameters:
                      caseId: order_id
                      activities:
                        - {name: Created, filter: "status = 'Created'"}
                        - {name: Delivered, filter: "status = 'Delivered'"}
                      timestamp: updated_at
                """;
        final Config c = Config.load(config);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, c);
        PAssert.that(outputs.get("mining.variants").getCollection()).satisfies(rows -> {
            final Map<String, MElement> variants = byKey(rows, "variant");
            Assertions.assertEquals(2, variants.size());
            Assertions.assertEquals(3L, variants.get("Created -> Delivered").getAsLong("caseCount"));
            Assertions.assertEquals(1L, variants.get("Created").getAsLong("caseCount"));
            return null;
        });
        pipeline.run();
    }

    /** The documented streaming topology (a session window closes a case) must assemble; every aggregate is per window. */
    @Test
    public void testSessionWindowAggregatesPerWindow() throws Exception {
        final String config = """
                sources:
                  - name: tickets
                    module: create
                    timestampAttribute: at
                    parameters:
                      type: element
                      elements:
                        - {id: T1, status: Open,   at: "2025-01-01T10:00:00Z"}
                        - {id: T1, status: Closed, at: "2025-01-01T10:20:00Z"}
                        - {id: T2, status: Open,   at: "2025-01-10T09:00:00Z"}
                        - {id: T2, status: Closed, at: "2025-01-10T09:30:00Z"}
                        - {id: T3, status: Open,   at: "2025-01-10T09:10:00Z"}
                        - {id: T3, status: Wait,   at: "2025-01-10T09:40:00Z"}
                        - {id: T3, status: Closed, at: "2025-01-10T10:00:00Z"}
                      schema:
                        fields:
                          - {name: id, type: string}
                          - {name: status, type: string}
                          - {name: at, type: timestamp}
                transforms:
                  - name: mining
                    module: process
                    inputs: [tickets]
                    strategy:
                      window:
                        type: session
                        unit: hour
                        gap: 1
                    parameters:
                      caseId: id
                      activity: status
                      timestamp: at
                """;
        final Config c = Config.load(config);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, c);
        // T1 is alone in its session; T2 and T3 overlap and form one window (the rows carry the window end); an edge
        // only one of them walks keeps that case's own session
        PAssert.that(outputs.get("mining.variants").getCollection()).satisfies(rows -> {
            final Map<String, Double> shares = new HashMap<>();
            for (final MElement r : rows) shares.put(r.getTimestamp() + " " + r.getAsString("variant"), r.getAsDouble("caseShare"));
            Assertions.assertEquals(3, shares.size(), shares.toString());
            Assertions.assertEquals(1D, shares.get("2025-01-01T11:19:59.999Z Open -> Closed"), 1e-12, shares.toString());
            Assertions.assertEquals(0.5D, shares.get("2025-01-10T10:59:59.999Z Open -> Closed"), 1e-12, shares.toString());
            Assertions.assertEquals(0.5D, shares.get("2025-01-10T10:59:59.999Z Open -> Wait -> Closed"), 1e-12, shares.toString());
            return null;
        });
        PAssert.that(outputs.get("mining").getCollection()).satisfies(rows -> {
            final Map<String, MElement> edges = new HashMap<>();
            for (final MElement r : rows) edges.put(r.getTimestamp() + " " + r.getAsString("source") + "/" + r.getAsString("target"), r);
            Assertions.assertEquals(8, edges.size(), edges.keySet().toString());
            Assertions.assertEquals(1L, edges.get("2025-01-01T11:19:59.999Z Open/Closed").getAsLong("frequency"), edges.keySet().toString());
            Assertions.assertEquals(1L, edges.get("2025-01-10T10:29:59.999Z Open/Closed").getAsLong("frequency"), edges.keySet().toString());
            Assertions.assertEquals(2L, edges.get("2025-01-10T10:59:59.999Z __start__/Open").getAsLong("caseCount"), edges.keySet().toString());
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testInvalidParametersFailAtAssembly() {
        final String config = ORDERS_SOURCE + """
                transforms:
                  - name: mining
                    module: process
                    inputs: [orders]
                    parameters:
                      caseId: order_no
                      activity: status
                      activities:
                        - {name: x}
                        - {name: y, filter: "status = = 'Paid'"}
                      attributes: [variant]
                      constraints:
                        - {type: response, activity: Paid}
                        - {type: duration, maxDuration: tomorrow}
                """;
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, Config.load(config)));
        final String message = e.getMessage();
        Assertions.assertTrue(message.contains("caseId field 'order_no' is not in the input schema"), message);
        Assertions.assertTrue(message.contains("activity and activities are exclusive"), message);
        Assertions.assertTrue(message.contains("attributes field 'variant' is not in the input schema"), message);
        Assertions.assertTrue(message.contains("activities[1].filter could not be parsed"), message);
        Assertions.assertTrue(message.contains("(response) requires activity and target"), message);
        Assertions.assertTrue(message.contains("is not an ISO-8601 duration"), message);
    }
}
