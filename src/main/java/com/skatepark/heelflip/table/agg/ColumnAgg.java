package com.skatepark.heelflip.table.agg;

import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ColumnAgg {

    private String columnName;
    private Map<JsonPrimitive, Integer> countMap;

    private int stringCount;
    private int booleanCount;
    private int numberCount;

    private BigDecimal min;
    private BigDecimal max;
    private BigDecimal sum;

    public ColumnAgg(String columnName) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        this.columnName = columnName;
        this.countMap = new HashMap<>();
        this.stringCount = 0;
        this.booleanCount = 0;
        this.numberCount = 0;
    }

    public String getColumnName() {
        return columnName;
    }

    public int cardinality() {
        return countMap.keySet().size();
    }

    public int count() {
        return countMap.values().stream()
                .mapToInt(Integer::intValue)
                .sum();
    }

    public int count(Object value) {
        if (value instanceof String) {
            return count(new JsonPrimitive((String) value));
        }
        if (value instanceof Number) {
            return count(new JsonPrimitive((Number) value));
        }
        if (value instanceof Boolean) {
            return count(new JsonPrimitive((Boolean) value));
        }
        return 0;
    }

    public int getStringCount() {
        return stringCount;
    }

    public int getBooleanCount() {
        return booleanCount;
    }

    public int getNumberCount() {
        return numberCount;
    }

    public BigDecimal getMax() {
        return max;
    }

    public BigDecimal getMin() {
        return min;
    }

    public BigDecimal getSum() {
        return sum;
    }

    public void agg(JsonPrimitive value) {
        countMap.computeIfAbsent(value, key -> 0);
        countMap.put(value, countMap.get(value) + 1);

        if (value.isString()) {
            stringCount++;
        }
        if (value.isBoolean()) {
            booleanCount++;
        }

        if (value.isNumber()) {
            numberCount++;

            BigDecimal v = value.getAsBigDecimal();
            min = min == null || min.compareTo(v) > 0 ? v : min;
            max = max == null || max.compareTo(v) < 0 ? v : max;
            sum = sum == null ? v : sum.add(v);
        }
    }

    private int count(JsonPrimitive value) {
        return !countMap.containsKey(value) ? 0 : countMap.get(value);
    }
}
