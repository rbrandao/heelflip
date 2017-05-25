package com.skatepark.heelflip.table;

import com.skatepark.heelflip.column.IColumn;
import com.skatepark.heelflip.column.type.ObjectColumn;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Heelflip {

    private String name;

    private Map<String, IColumn> columnsMap;

    public Heelflip(String name) {
        Objects.requireNonNull(name, "name should not be null.");
        this.name = name;
        this.columnsMap = new HashMap<>();
    }

    public void add(Map<String, Object> values) {
        Objects.requireNonNull(values, "values should not be null.");

        values.entrySet().stream()
                .forEach(entry -> addValue(entry.getKey(), entry.getValue()));
    }

    public int size() {
        return columnsMap.size();
    }

    public boolean contains(String columnName) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        return columnsMap.containsKey(columnName);
    }

    public long minAsLong(String columnName) {
        if (!contains(columnName)) {
            return -1;
        }
        return columnsMap.get(columnName).minAsLong();
    }

    public long maxAsLong(String columnName) {
        if (!contains(columnName)) {
            return -1;
        }
        return columnsMap.get(columnName).maxAsLong();
    }

    public double minAsDouble(String columnName) {
        if (!contains(columnName)) {
            return -1;
        }
        return columnsMap.get(columnName).minAsDouble();
    }

    public double maxAsDouble(String columnName) {
        if (!contains(columnName)) {
            return -1;
        }
        return columnsMap.get(columnName).maxAsDouble();
    }

    private void addValue(String columnName, Object value) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(value, "value should not be null.");

        if (!contains(columnName)) {
            columnsMap.put(columnName, new ObjectColumn(columnName));
        }
        columnsMap.get(columnName).add(value.toString());
    }
}
