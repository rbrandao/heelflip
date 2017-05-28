package com.skatepark.heelflip.table;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
import java.util.UUID;

public class Heelflip {

    private Map<String, ObjectColumn> columnsMap;

    public Heelflip() {
        this.columnsMap = new HashMap<>();
    }

    public void add(JsonObject json) {
        Objects.requireNonNull(json, "json should not be null.");

        UUID id = UUID.randomUUID();
        List<HFValue> values = Extractor.extract(id, json);
        values.stream().forEach(this::addValue);
    }

    public int size() {
        return columnsMap.size();
    }

    public Set<String> columnNames() {
        return Collections.unmodifiableSet(columnsMap.keySet());
    }

    public boolean contains(String columnName) {
        return columnName != null && columnsMap.containsKey(columnName);
    }

    public long count(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).count();
    }

    public long count(String columnName, int value) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).count(value);
    }

    public long count(String columnName, long value) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).count(value);
    }

    public long count(String columnName, double value) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).count(value);
    }

    public long count(String columnName, String value) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).count(value);
    }

    public Set<Integer> valuesAsIntSet(String columnName) {
        return !contains(columnName) ?
                Collections.emptySet() :
                columnsMap.get(columnName).valuesAsIntSet();
    }

    public Set<Long> valuesAsLongSet(String columnName) {
        return !contains(columnName) ?
                Collections.emptySet() :
                columnsMap.get(columnName).valuesAsLongSet();
    }

    public Set<Double> valuesAsDoubleSet(String columnName) {
        return !contains(columnName) ?
                Collections.emptySet() :
                columnsMap.get(columnName).valuesAsDoubleSet();
    }

    public Set<String> valuesAsStringSet(String columnName) {
        return !contains(columnName) ?
                Collections.emptySet() :
                columnsMap.get(columnName).valuesAsStringSet();
    }

    public ColumnStatistic getStatistics(String columnName) {
        if (!contains(columnName)) {
            return null;
        }
        return columnsMap.get(columnName).getStatistics();
    }

    /**
     * Add value to respective column.
     *
     * @param value value.
     */
    private void addValue(HFValue value) {
        Objects.requireNonNull(value, "value should not be null.");

        String columnName = value.getColumnName();
        if (!contains(columnName)) {
            columnsMap.put(columnName, new ObjectColumn(columnName));
        }
        columnsMap.get(columnName).add(value);
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
}
