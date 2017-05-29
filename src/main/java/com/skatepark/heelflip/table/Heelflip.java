package com.skatepark.heelflip.table;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import com.skatepark.heelflip.table.agg.ColumnAgg;
import com.skatepark.heelflip.table.agg.GroupByAgg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Heelflip {

    private Map<String, ColumnAgg> columnAggMap;

    private Map<String, Map<String, GroupByAgg>> groupByAggMap;

    public Heelflip() {
        this.columnAggMap = new HashMap<>();
        this.groupByAggMap = new HashMap<>();
    }

    public void add(JsonObject json) {
        Objects.requireNonNull(json, "json should not be null.");

        Map<String, List<JsonPrimitive>> valueMap = Extractor.extract(json);
        aggregate(valueMap);
    }

    public int size() {
        return columnAggMap.size();
    }

    public Set<String> columnNames() {
        return Collections.unmodifiableSet(columnAggMap.keySet());
    }

    public boolean hasColumnAgg(String columnName) {
        return columnName != null && columnAggMap.containsKey(columnName);
    }

    public ColumnAgg getColumnAgg(String columnName) {
        return !hasColumnAgg(columnName) ? null : columnAggMap.get(columnName);
    }

    /**
     * Load {@link InputStream} with JSON newline delimited format.
     *
     * @param stream stream.
     * @throws IOException if IO errors occurs.
     */
    public void loadNDJSON(InputStream stream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            JsonParser parser = new JsonParser();

            reader.lines()
                    .map(line -> parser.parse(line))
                    .map(elem -> elem.getAsJsonObject())
                    .forEach(this::add);
        }
    }

    /**
     * Load {@link InputStream} with JSON array where each element is the data itself.
     *
     * @param stream stream.
     * @throws IOException if IO errors occurs.
     */
    public void loadJSONArray(InputStream stream) throws IOException {
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
     * Dump a all aggregation values in a single file.
     *
     * @param filePath file path.
     * @throws IOException if IO errors occurs.
     */
    public void dump(String filePath) throws IOException {
        Report.write(this, filePath);
    }

    private void aggregate(Map<String, List<JsonPrimitive>> valueMap) {
        for (Map.Entry<String, List<JsonPrimitive>> entry : valueMap.entrySet()) {
            String columnName = entry.getKey();
            List<JsonPrimitive> valueList = entry.getValue();
            ColumnAgg columnAgg = columnAggMap.computeIfAbsent(columnName, key -> new ColumnAgg(key));

            valueList.stream().forEach(columnAgg::agg);
        }
    }
}
