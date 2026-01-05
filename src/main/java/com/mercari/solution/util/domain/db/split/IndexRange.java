package com.mercari.solution.util.domain.db.split;

import com.google.gson.JsonObject;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


@DefaultCoder(AvroCoder.class)
public class IndexRange {

    @Nullable
    private IndexPosition from;
    @Nullable
    private IndexPosition to;

    @Nullable
    private Double ratio;

    @Nullable
    private IndexPosition prevFrom;

    public IndexPosition getFrom() {
        return from;
    }

    public void setFrom(IndexPosition from) {
        this.from = from;
    }

    public IndexPosition getTo() {
        return to;
    }

    public void setTo(IndexPosition to) {
        this.to = to;
    }

    public Double getRatio() {
        return ratio;
    }

    public void setRatio(Double ratio) {
        this.ratio = ratio;
    }

    public IndexPosition getPrevFrom() {
        return prevFrom;
    }

    public void setPrevFrom(IndexPosition prevFrom) {
        this.prevFrom = prevFrom;
    }

    public static IndexRange of(
            final IndexPosition from,
            final IndexPosition to) {

        return of(from, to, null);
    }

    public static IndexRange of(
            final IndexPosition from,
            final IndexPosition to,
            final IndexPosition prevFrom) {

        if(from == null || to == null) {
            throw new IllegalArgumentException("Both from and to must not be null for IndexRange");
        }
        if(from.getOffsets() == null || to.getOffsets() == null) {
            throw new IllegalArgumentException("Both from and to must not be null for IndexRange");
        }
        final IndexRange indexRange = new IndexRange();
        indexRange.from = from.copy();
        indexRange.to = to.copy();
        indexRange.ratio = 1.0D;
        indexRange.prevFrom = Optional.ofNullable(prevFrom).map(IndexPosition::copy).orElse(null);
        return indexRange;
    }

    public IndexRange copy() {
        return IndexRange.of(from, to, prevFrom);
    }

    public Map<String, Object> toMap() {
        final Map<String, Object> values = new HashMap<>();
        values.put("from", from);
        values.put("to", to);
        return values;
    }

    @Override
    public String toString() {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.add("from", from.toJson());
        jsonObject.add("to", to.toJson());
        jsonObject.addProperty("ratio", ratio);
        if(prevFrom != null) {
            jsonObject.add("prevFrom", prevFrom.toJson());
        }
        return jsonObject.toString();
    }

}
