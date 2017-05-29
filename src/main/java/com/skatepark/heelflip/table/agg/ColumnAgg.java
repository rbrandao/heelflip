package com.skatepark.heelflip.table.agg;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ColumnAgg {

    private String columnName;
    private Map<String, Integer> countMap;

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

    public int count(String value) {
        return value == null || !countMap.containsKey(value) ? 0 : countMap.get(value);
    }

    public Set<String> distinctValues() {
        return Collections.unmodifiableSet(countMap.keySet());
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
        Objects.requireNonNull(value, "value should not be null.");

        countMap.computeIfAbsent(value.getAsString(), key -> 0);
        countMap.put(value.getAsString(), countMap.get(value.getAsString()) + 1);

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

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(toJSON());
    }

    public JsonObject toJSON() {
        JsonObject json = new JsonObject();
        json.addProperty("columnName", columnName);
        json.addProperty("count", count());
        json.addProperty("cardinality", cardinality());
        json.addProperty("string", stringCount);
        json.addProperty("boolean", booleanCount);
        json.addProperty("number", numberCount);
        if (min != null && max != null & sum != null) {
            json.addProperty("min", min.longValue());
            json.addProperty("max", max.longValue());
            json.addProperty("sum", sum.longValue());
        }
        JsonArray valuesArray = new JsonArray();
        distinctValues().stream().forEach(valuesArray::add);
        json.add("values", valuesArray);
        return json;
    }
}
