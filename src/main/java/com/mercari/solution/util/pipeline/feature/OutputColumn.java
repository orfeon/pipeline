package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.feature.FeatureSpec.Scope;

import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One expanded output column with its lineage metadata (docs/design/feature-dsl.md §7 命名と系譜).
 * The canonical name (no prefix, no {@code _} lint) is what other blocks reference; the output name is
 * what appears in the schema.
 */
public class OutputColumn implements Serializable {

    public enum Status {
        /** provable at compile time: availableAt ≤ computeAt */ staticSafe,
        /** sequence/encoding: safe after shifting the window near edge by windowShift */ windowShift,
        /** the engine must filter contributions by effectiveAvailableAt ≤ computeAt per row */ runtimeFilter,
        /** availableAt > computeAt; only allowed as a consumed intermediate */ violation
    }

    public enum Placement { child, parent }

    String canonicalName;
    String outputName;
    String block;
    Scope scope;
    String operator;
    Schema.FieldType fieldType;
    final Map<String, String> coordinates = new LinkedHashMap<>();
    /** canonical references consumed by this column (input relation fields or other columns) */
    final Set<String> inputs = new LinkedHashSet<>();
    /** inputs read from past rows (sequence / encoding contributions) rather than from the row itself */
    final Set<String> pastInputs = new LinkedHashSet<>();
    AvailableAt availableAt;
    AvailableAt computeAt;
    Status status;
    Duration windowShift;
    final Set<String> derivedFrom = new LinkedHashSet<>();
    final Set<String> sources = new LinkedHashSet<>();
    boolean declaredEvidence;
    Duration validFor;
    boolean intermediate;
    boolean anonymous;
    boolean fitted;
    Placement placement = Placement.child;
    /** The {@code output.roles} entry that names this column (data contract, never a feature), null for a feature column. */
    String role;

    public String getCanonicalName() { return canonicalName; }
    public String getOutputName() { return outputName; }
    public String getBlock() { return block; }
    public Scope getScope() { return scope; }
    public String getOperator() { return operator; }
    public Schema.FieldType getFieldType() { return fieldType; }
    public Map<String, String> getCoordinates() { return coordinates; }
    public Set<String> getInputs() { return inputs; }
    public Set<String> getPastInputs() { return pastInputs; }
    public AvailableAt getAvailableAt() { return availableAt; }
    public AvailableAt getComputeAt() { return computeAt; }
    public Status getStatus() { return status; }
    public Duration getWindowShift() { return windowShift; }
    public Set<String> getDerivedFrom() { return derivedFrom; }
    public Set<String> getSources() { return sources; }
    public boolean isDeclaredEvidence() { return declaredEvidence; }
    public Duration getValidFor() { return validFor; }
    /** Not emitted: anonymous desugared expressions, baselines, and {@code _}-prefixed offline columns. */
    public boolean isIntermediate() { return intermediate; }
    public boolean isAnonymous() { return anonymous; }
    public boolean isFitted() { return fitted; }
    public Placement getPlacement() { return placement; }
    public String getRole() { return role; }

    /** Lineage metadata as stored in {@code Schema.Field.options}. */
    public Map<String, String> toOptions() {
        final Map<String, String> options = new LinkedHashMap<>();
        options.put("feature.block", block);
        options.put("feature.scope", scope.name());
        options.put("feature.operator", operator);
        options.put("feature.canonical", canonicalName);
        options.put("feature.availableAt", availableAt == null ? "?" : availableAt.describe());
        options.put("feature.status", status == null ? "?" : status.name());
        if (windowShift != null) options.put("feature.windowShift", windowShift.toString());
        options.put("feature.derivedFrom", String.join(",", derivedFrom));
        options.put("feature.sources", String.join(",", sources));
        options.put("feature.evidence", declaredEvidence ? "declared" : "measured");
        options.put("feature.fit", Boolean.toString(fitted));
        options.put("feature.placement", placement.name());
        if (validFor != null) options.put("feature.validFor", validFor.toString());
        if (computeAt != null) options.put("feature.computeAt", computeAt.describe());
        if (role != null) options.put("feature.role", role);
        for (final Map.Entry<String, String> e : coordinates.entrySet()) {
            options.put("feature.coord." + e.getKey(), e.getValue());
        }
        return options;
    }

    public Schema.Field toField() {
        return Schema.Field.of(outputName, fieldType).withOptions(toOptions());
    }

    public String describe() {
        final StringBuilder sb = new StringBuilder();
        sb.append(outputName).append(" : ").append(fieldType == null ? "?" : fieldType.getType())
                .append(" [").append(scope).append('/').append(operator).append("] ")
                .append("availableAt=").append(availableAt == null ? "?" : availableAt.describe())
                .append(" status=").append(status);
        if (windowShift != null) sb.append("(shift=").append(windowShift).append(')');
        if (!derivedFrom.isEmpty()) sb.append(" derivedFrom=").append(derivedFrom);
        if (declaredEvidence) sb.append(" evidence=declared");
        if (intermediate) sb.append(" (intermediate)");
        if (!inputs.isEmpty()) sb.append(" <- ").append(List.copyOf(inputs));
        return sb.toString();
    }

    @Override
    public String toString() {
        return describe();
    }

}
