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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class JsonAgg {

    private Map<String, ColumnAgg> columnAggMap;

    private Map<String, Map<String, GroupByAgg>> groupByAggMap;

    public JsonAgg() {
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

    public boolean hasGroupBy(String columnName) {
        return columnName != null && groupByAggMap.containsKey(columnName);
    }

    public GroupByAgg getGroupBy(String columnName, String groupBy) {
        return !hasGroupBy(columnName) ? null : groupByAggMap.get(columnName).get(groupBy);
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
     * Dump column aggregations in a single JSON file.
     *
     * @param filePath file path.
     * @throws IOException if IO errors occurs.
     */
    public void dumpColumnAgg(String filePath) throws IOException {
        JsonDumper.dumpColumnAgg(this, filePath);
    }

    /**
     * Dump group by aggregations in a single JSON file.
     *
     * @param filePath file path.
     * @throws IOException if IO errors occurs.
     */
    public void dumpGroupByAgg(String filePath) throws IOException {
        JsonDumper.dumpGroupByAgg(this, filePath);
    }

    private void aggregate(Map<String, List<JsonPrimitive>> valueMap) {
        for (Map.Entry<String, List<JsonPrimitive>> entry : valueMap.entrySet()) {
            String columnName = entry.getKey();
            List<JsonPrimitive> valueList = entry.getValue();
            ColumnAgg columnAgg = columnAggMap.computeIfAbsent(columnName, key -> new ColumnAgg(key));

            valueList.stream().forEach(columnAgg::agg);
        }

        for (String columnName : valueMap.keySet()) {
            for (String groupBy : valueMap.keySet()) {
                if (columnName.equals(groupBy)) {
                    continue;
                }

                Map<String, GroupByAgg> map = groupByAggMap.computeIfAbsent(columnName, key -> new HashMap<>());
                GroupByAgg groupByAgg = map.computeIfAbsent(groupBy, key -> new GroupByAgg(columnName, groupBy));

                for (JsonPrimitive columnValue : valueMap.get(columnName)) {
                    for (JsonPrimitive groupByValue : valueMap.get(groupBy)) {
                        groupByAgg.agg(columnValue, groupByValue);
                    }
                }
            }
        }
    }
}