package com.skatepark.heelflip.table;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.skatepark.heelflip.column.IColumn;
import com.skatepark.heelflip.column.type.ObjectColumn;
import com.skatepark.heelflip.util.Flatter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Heelflip {

    private String name;

    private Map<String, IColumn> columnsMap;

    public Heelflip(String name) {
        Objects.requireNonNull(name, "name should not be null.");
        this.name = name;
        this.columnsMap = new HashMap<>();
    }

    public void add(JsonObject json) {
        Objects.requireNonNull(json, "json should not be null.");

        for (JsonObject object : Flatter.flatten(json)) {
            object.entrySet().stream()
                    .forEach(entry -> addValue(entry.getKey(), entry.getValue().getAsString()));
        }
    }

    public void add(Map<String, Object> values) {
        Objects.requireNonNull(values, "values should not be null.");

        values.entrySet().stream()
                .forEach(entry -> addValue(entry.getKey(), entry.getValue()));
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

    public int minAsInt(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).minAsInt();
    }

    public int maxAsInt(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).maxAsInt();
    }

    public int sumAsInt(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).sumAsInt();
    }

    public long minAsLong(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).minAsLong();
    }

    public long maxAsLong(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).maxAsLong();
    }

    public long sumAsLong(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).sumAsLong();
    }

    public double minAsDouble(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).minAsDouble();
    }

    public double maxAsDouble(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).maxAsDouble();
    }

    public double sumAsDouble(String columnName) {
        return !contains(columnName) ?
                -1 :
                columnsMap.get(columnName).sumAsDouble();
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

    /**
     * Add value to respective column.
     *
     * @param columnName column name.
     * @param value      value.
     */
    private void addValue(String columnName, Object value) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(value, "value should not be null.");

        if (!contains(columnName)) {
            columnsMap.put(columnName, new ObjectColumn(columnName));
        }
        columnsMap.get(columnName).add(value.toString());
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
            JsonParser parser = new JsonParser();


            JsonElement result = new JsonParser().parse(new InputStreamReader(stream));
            if (!result.isJsonArray()) {
                throw new IllegalArgumentException("result is not a JSON array.");
            }
            for (JsonElement elem : result.getAsJsonArray()) {
                add(elem.getAsJsonObject());
            }
        }
    }
}
