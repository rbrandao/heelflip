package com.skatepark.heelflip;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import com.skatepark.heelflip.util.Extractor;
import com.skatepark.heelflip.util.JsonDumper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The aggregation engine that calculate all aggregations from the given JSON data.
 *
 * @author greatjapa
 */
public class JsonAgg {

    private Map<String, FieldAgg> fieldAggMap;

    private Map<String, Map<String, GroupByAgg>> groupByAggMap;

    public JsonAgg() {
        this.fieldAggMap = new HashMap<>();
        this.groupByAggMap = new HashMap<>();
    }

    /**
     * Add {@link JsonObject} to the aggregation engine.
     *
     * @param json JSON data.
     */
    public void add(JsonObject json) {
        Objects.requireNonNull(json, "json should not be null.");
        aggregate(Extractor.extract(json));
    }

    /**
     * @return total of {@link FieldAgg} objects.
     */
    public int numberOfFieldAgg() {
        return fieldAggMap.size();
    }

    /**
     * @return total of {@link GroupByAgg} objects.
     */
    public int numberOfGroupByAgg() {
        return groupByAggMap.values().stream()
                .mapToInt(m -> m.size())
                .sum();
    }

    /***
     * @return set with all JSON field names.
     */
    public Set<String> fieldNames() {
        return fieldAggMap.keySet();
    }

    /**
     * @param fieldName JSON field name.
     * @return true if has aggregation related to the given field name, false otherwise.
     */
    public boolean hasFieldAgg(String fieldName) {
        return fieldName != null && fieldAggMap.containsKey(fieldName);
    }

    /**
     * Get {@link FieldAgg} object related to the given JSON field.
     *
     * @param fieldName JSON field name.
     * @return aggregation object.
     */
    public FieldAgg getFieldAgg(String fieldName) {
        return !hasFieldAgg(fieldName) ? null : fieldAggMap.get(fieldName);
    }

    /**
     * @param fieldName JSON field name.
     * @return true if has aggregation related to the given field name, false otherwise.
     */
    public boolean hasGroupBy(String fieldName) {
        return fieldName != null && groupByAggMap.containsKey(fieldName);
    }

    /**
     * Get {@link GroupByAgg} object related to the given JSON fields.
     *
     * @param fieldName JSON field name.
     * @param groupBy   JSON field name used to group.
     * @return aggregation object.
     */
    public GroupByAgg getGroupBy(String fieldName, String groupBy) {
        return !hasGroupBy(fieldName) ? null : groupByAggMap.get(fieldName).get(groupBy);
    }

    /**
     * Load {@link InputStream} with JSON newline delimited format.
     *
     * @param stream stream.
     * @throws IOException if IO errors occurs.
     */
    public void loadNDJSON(InputStream stream) throws IOException {
        Objects.requireNonNull(stream, "stream should not be null.");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            JsonParser parser = new JsonParser();

            reader.lines()
                    .map(line -> parser.parse(line))
                    .map(elem -> elem.getAsJsonObject())
                    .forEach(this::add);
        }
    }

    /**
     * Load {@link InputStream} with JSON array where each element is a data entry.
     *
     * @param stream stream.
     * @throws IOException if IO errors occurs.
     */
    public void loadJSONArray(InputStream stream) throws IOException {
        Objects.requireNonNull(stream, "stream should not be null.");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            JsonElement result = new JsonParser().parse(reader);
            if (!result.isJsonArray()) {
                throw new IllegalArgumentException("result is not a JSON array.");
            }
            for (JsonElement elem : result.getAsJsonArray()) {
                add(elem.getAsJsonObject());
            }
        }
    }

    /**
     * Dump aggregation objects into files in the given directory path.
     *
     * @param dirPathStr       path for directory destination.
     * @param includeValues true if needed to write the JSON values related to aggregations, false
     *                      otherwise.
     * @throws IOException if IO errors occurs.
     */
    public void dumpReport(String dirPathStr, boolean includeValues) throws IOException {
        JsonDumper.dumpReport(this, dirPathStr, includeValues);
    }

    /**
     * Build {@link FieldAgg} and {@link GroupByAgg} objects based on the given value map.
     *
     * @param valueMap map field name to their {@link JsonPrimitive} values.
     */
    private void aggregate(Map<String, List<JsonPrimitive>> valueMap) {
        for (Map.Entry<String, List<JsonPrimitive>> entry : valueMap.entrySet()) {
            String fieldName = entry.getKey();
            List<JsonPrimitive> valueList = entry.getValue();
            FieldAgg fieldAgg = fieldAggMap.computeIfAbsent(fieldName, key -> new FieldAgg(key));

            valueList.stream().forEach(fieldAgg::agg);
        }

        for (String fieldName : valueMap.keySet()) {
            for (String groupBy : valueMap.keySet()) {
                if (fieldName.equals(groupBy)) {
                    continue;
                }

                Map<String, GroupByAgg> map = groupByAggMap.computeIfAbsent(fieldName, key -> new HashMap<>());
                GroupByAgg groupByAgg = map.computeIfAbsent(groupBy, key -> new GroupByAgg(fieldName, groupBy));

                for (JsonPrimitive fieldValue : valueMap.get(fieldName)) {
                    for (JsonPrimitive groupByValue : valueMap.get(groupBy)) {
                        groupByAgg.agg(fieldValue, groupByValue);
                    }
                }
            }
        }
    }
}
