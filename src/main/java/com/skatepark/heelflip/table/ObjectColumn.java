package com.skatepark.heelflip.table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

class ObjectColumn {

    private String name;

    private List<HFValue> values;

    public ObjectColumn(String name) {
        Objects.requireNonNull(name, "name should not be null.");
        this.name = name;
        this.values = new ArrayList<>();
    }

    public String name() {
        return name;
    }

    public void add(HFValue value) {
        Objects.requireNonNull(value, "value should not be null.");
        values.add(value);
    }

    public long count(String value) {
        if (value == null) {
            return 0;
        }
        return values.stream()
                .map(hfValue -> hfValue.getAsString())
                .filter(v -> v.equals(value))
                .count();
    }

    public long count(boolean value) {
        return values.stream()
                .map(hfValue -> hfValue.getAsBoolean())
                .filter(v -> v != null && v.equals(value))
                .count();
    }

    public long count(int value) {
        return values.stream()
                .map(hfValue -> hfValue.getAsInt())
                .filter(v -> v != null && v.equals(value))
                .count();
    }

    public long count(long value) {
        return values.stream()
                .map(hfValue -> hfValue.getAsLong())
                .filter(v -> v != null && v.equals(value))
                .count();
    }

    public long count(double value) {
        return values.stream()
                .map(hfValue -> hfValue.getAsDouble())
                .filter(v -> v != null && v.equals(value))
                .count();
    }

    public long count() {
        return values.size();
    }

    public Set<Integer> valuesAsIntSet() {
        Set<Integer> result = new HashSet<>();
        for (HFValue value : values) {
            Double d = value.getAsDouble();
            if (d != null) {
                result.add(d.intValue());
            }
        }
        return result;
    }

    public Set<Long> valuesAsLongSet() {
        Set<Long> result = new HashSet<>();
        for (HFValue value : values) {
            Double d = value.getAsDouble();
            if (d != null) {
                result.add(d.longValue());
            }
        }
        return result;
    }

    public Set<Double> valuesAsDoubleSet() {
        Set<Double> result = new HashSet<>();
        for (HFValue value : values) {
            Double d = value.getAsDouble();
            if (d != null) {
                result.add(d);
            }
        }
        return result;
    }

    public Set<String> valuesAsStringSet() {
        return values.stream()
                .map(HFValue::getAsString)
                .collect(Collectors.toSet());
    }

    public ColumnStatistic getStatistics() {
        ColumnStatistic statistic = new ColumnStatistic(name);
        values.forEach(statistic::agg);
        return statistic;
    }
}
