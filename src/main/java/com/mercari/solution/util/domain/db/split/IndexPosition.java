package com.mercari.solution.util.domain.db.split;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.db.CharCollation;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

import java.util.*;

@DefaultCoder(AvroCoder.class)
public class IndexPosition {

    @Nullable
    private Boolean isOpen;
    @Nullable
    private List<IndexOffset> offsets;

    public Boolean getIsOpen() {
        return isOpen;
    }

    public void setIsOpen(Boolean isOpen) {
        this.isOpen = isOpen;
    }

    public List<IndexOffset> getOffsets() {
        return offsets;
    }

    public void setOffsets(List<IndexOffset> offsets) {
        this.offsets = offsets;
    }

    public boolean isOverTo(final IndexPosition another, CharCollation charCollation) {
        final int size = Math.min(this.getOffsets().size(), another.getOffsets().size());
        for(int i=0; i<size; i++) {
            final IndexOffset bound = another.getOffsets().get(i);
            if(bound.getValue() == null) {
                return false;
            }
            final IndexOffset point = this.getOffsets().get(i);
            if(point.isLesserThan(bound, charCollation)) {
                return false;
            } else if(point.isGreaterThan(bound, charCollation)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        final JsonArray array = toJsonArray();
        final JsonObject result = new JsonObject();
        result.add("offsets", array);
        result.addProperty("open", this.isOpen);
        return result.toString();
    }

    public JsonObject toJson() {
        final JsonObject result = new JsonObject();
        final JsonArray array = toJsonArray();
        result.add("offsets", array);
        result.addProperty("open", this.isOpen);
        return result;
    }

    public JsonArray toJsonArray() {
        final JsonArray array = new JsonArray();
        for(final IndexOffset offset : this.offsets) {
            final JsonObject offsetObject = new JsonObject();
            offsetObject.addProperty(offset.getFieldName(), offset.valueToString());
            offsetObject.addProperty("order", offset.getAscending() ? "ASC" : "DESC");
            array.add(offsetObject);
        }
        return array;
    }

    public Map<String, Object> toMap() {
        final Map<String, Object> values = new HashMap<>();
        values.put("isOpen", isOpen);
        final List<Map<String,Object>> offsetsList = new ArrayList<>();
        for(final IndexOffset offset : offsets) {
            offsetsList.add(offset.toMap());
        }
        values.put("offsets", offsetsList);
        return values;
    }

    public IndexPosition copy() {
        return copy(null);
    }

    public IndexPosition copy(Boolean isOpen) {
        final IndexPosition position = new IndexPosition();
        position.isOpen = Optional.ofNullable(isOpen).orElse(this.isOpen);
        position.offsets = new ArrayList<>();
        for(final IndexOffset offset : offsets) {
            position.offsets.add(offset.copy());
        }
        return position;
    }

    public boolean isSame(final IndexPosition another) {
        if(another == null) {
            return false;
        }
        if(offsets == null && another.offsets == null) {
            return true;
        } else if(offsets == null || another.offsets == null) {
            return false;
        }
        if(offsets.size() != another.offsets.size()) {
            return false;
        }
        for(int i=0; i<offsets.size(); i++) {
            if(!offsets.get(i).isSame(another.getOffsets().get(i))) {
                return false;
            }
        }
        return true;
    }

    public static IndexPosition of(final List<IndexOffset> offsets, final boolean isOpen) {
        if(offsets == null || offsets.isEmpty()) {
            throw new IllegalArgumentException("offsets must not be null or zero size for IndexPosition");
        }
        final IndexPosition indexPosition = new IndexPosition();
        indexPosition.setIsOpen(isOpen);
        indexPosition.setOffsets(offsets);
        return indexPosition;
    }

    public static Schema createAvroSchema(String name) {
        return SchemaBuilder.builder()
                .record(name)
                .fields()
                .name("isOpen").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
                .name("offsets").type(Schema.createArray(IndexOffset.createAvroSchema())).noDefault()
                .endRecord();
    }

}