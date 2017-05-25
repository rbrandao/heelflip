package com.skatepark.heelflip.table;

import com.skatepark.heelflip.column.IColumn;
import com.skatepark.heelflip.column.type.DoubleColumn;
import com.skatepark.heelflip.column.type.LongColumn;
import com.skatepark.heelflip.column.type.StringColumn;

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

    public void add(String columnName, Integer value) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(value, "value should not be null.");

        add(columnName, value.longValue());
    }

    public void add(String columnName, Long value) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(value, "value should not be null.");

        if (!columnsMap.containsKey(columnName)) {
            columnsMap.put(columnName, new LongColumn(columnName));
        }
        IColumn column = columnsMap.get(columnName);
        if (!column.isLongColumn()) {
            return;
        }
        column.getAsLongColumn().add(value);
    }

    public void add(String columnName, Double value) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(value, "value should not be null.");

        if (!columnsMap.containsKey(columnName)) {
            columnsMap.put(columnName, new DoubleColumn(columnName));
        }
        IColumn column = columnsMap.get(columnName);
        if (!column.isDoubleColumn()) {
            return;
        }
        column.getAsDoubleColumn().add(value);
    }

    public void add(String columnName, String value) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(value, "value should not be null.");

        if (!columnsMap.containsKey(columnName)) {
            columnsMap.put(columnName, new StringColumn(columnName));
        }
        IColumn column = columnsMap.get(columnName);
        if (!column.isStringColumn()) {
            return;
        }
        column.getAsStringColumn().add(value);
    }

    public int size() {
        return columnsMap.size();
    }

    public boolean contains(String columnName) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        return columnsMap.containsKey(columnName);
    }

    public long minAsLong(String columnName) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        if (!columnsMap.containsKey(columnName)) {
            return 0;
        }
        IColumn column = columnsMap.get(columnName);
        if (column.isStringColumn()) {
            return 0;
        }
        if (column.isDoubleColumn()) {
            Double min = column.getAsDoubleColumn().min();
            return min.longValue();
        }
        return column.getAsLongColumn().min();
    }

    public long maxAsLong(String columnName) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        if (!columnsMap.containsKey(columnName)) {
            return 0;
        }
        IColumn column = columnsMap.get(columnName);
        if (column.isStringColumn()) {
            return 0;
        }
        if (column.isDoubleColumn()) {
            Double max = column.getAsDoubleColumn().max();
            return max.longValue();
        }
        return column.getAsLongColumn().max();
    }

    public double minAsDouble(String columnName) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        if (!columnsMap.containsKey(columnName)) {
            return 0;
        }
        IColumn column = columnsMap.get(columnName);
        if (column.isStringColumn()) {
            return 0;
        }
        if (column.isLongColumn()) {
            Long min = column.getAsLongColumn().min();
            return min.doubleValue();
        }
        return column.getAsDoubleColumn().min();
    }

    public double maxAsDouble(String columnName) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        if (!columnsMap.containsKey(columnName)) {
            return 0;
        }
        IColumn column = columnsMap.get(columnName);
        if (column.isStringColumn()) {
            return 0;
        }
        if (column.isLongColumn()) {
            Long max = column.getAsLongColumn().max();
            return max.doubleValue();
        }
        return column.getAsDoubleColumn().max();
    }
}
