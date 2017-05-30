package com.skatepark.heelflip;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupByAgg {

    private String columnName;

    private String groupBy;

    private Map<String, FieldAgg> aggregations;

    public GroupByAgg(String columnName, String groupBy) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(groupBy, "groupBy should not be null.");
        if (columnName.equals(groupBy)) {
            throw new IllegalArgumentException("columnName should not be equal to groupBy.");
        }
        this.columnName = columnName;
        this.groupBy = groupBy;
        this.aggregations = new HashMap<>();
    }

    public String getColumnName() {
        return columnName;
    }

    public String getGroupBy() {
        return groupBy;
    }

    void agg(JsonPrimitive columnValue, JsonPrimitive groupByValue) {
        FieldAgg fieldAgg = aggregations.computeIfAbsent(groupByValue.getAsString(), key -> new FieldAgg(columnName));
        fieldAgg.agg(columnValue);
    }

    public Set<String> groupBy(String value) {
        return value == null || !aggregations.containsKey(value) ?
                Collections.emptySet() :
                aggregations.get(value).distinctValues();
    }

    public Set<String> groupByValues() {
        return aggregations.keySet();
    }

    public Set<String> values() {
        return aggregations.values().stream()
                .flatMap(columnAgg -> columnAgg.distinctValues().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(toJSON());
    }

    public JsonObject toJSON() {
        JsonArray values = new JsonArray();
        for (Map.Entry<String, FieldAgg> entry : aggregations.entrySet()) {
            JsonObject obj = new JsonObject();
            obj.add(entry.getKey(), entry.getValue().toJSON());
            values.add(obj);
        }

        JsonObject json = new JsonObject();
        json.addProperty("groupBy", groupBy);
        json.addProperty("columnName", columnName);
        json.add("values", values);

        return json;
    }
}
