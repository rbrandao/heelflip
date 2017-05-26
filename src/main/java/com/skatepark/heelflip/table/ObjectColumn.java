package com.skatepark.heelflip.table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

class ObjectColumn implements IColumn {

    private String name;

    private List<HFValue> values;

    public ObjectColumn(String name) {
        Objects.requireNonNull(name, "name should not be null.");
        this.name = name;
        this.values = new ArrayList<>();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void add(HFValue value) {
        Objects.requireNonNull(value, "value should not be null.");
        values.add(value);
    }

    @Override
    public long count(String value) {
        if (value == null) {
            return 0;
        }
        return values.stream()
                .map(hfValue -> hfValue.getAsString())
                .filter(v -> v.equals(value))
                .count();
    }

    @Override
    public long count(int value) {
        return count(Integer.toString(value));
    }

    @Override
    public long count(long value) {
        return count(Long.toString(value));
    }

    @Override
    public long count(double value) {
        return count(Double.toString(value));
    }

    @Override
    public long count() {
        return values.size();
    }

    @Override
    public int minAsInt() {
        OptionalInt min = values.stream()
                .map(HFValue::getAsDouble)
                .filter(Objects::nonNull)
                .mapToInt(Double::intValue)
                .min();
        return min.isPresent() ? min.getAsInt() : -1;
    }

    @Override
    public int maxAsInt() {
        OptionalInt max = values.stream()
                .map(HFValue::getAsDouble)
                .filter(Objects::nonNull)
                .mapToInt(Double::intValue)
                .max();
        return max.isPresent() ? max.getAsInt() : -1;
    }

    @Override
    public int sumAsInt() {
        return values.stream()
                .map(HFValue::getAsDouble)
                .filter(Objects::nonNull)
                .mapToInt(Double::intValue)
                .sum();
    }

    @Override
    public long minAsLong() {
        OptionalLong min = values.stream()
                .map(HFValue::getAsDouble)
                .filter(Objects::nonNull)
                .mapToLong(Double::longValue)
                .min();
        return min.isPresent() ? min.getAsLong() : -1;
    }

    @Override
    public long maxAsLong() {
        OptionalLong max = values.stream()
                .map(HFValue::getAsDouble)
                .filter(Objects::nonNull)
                .mapToLong(Double::longValue)
                .max();
        return max.isPresent() ? max.getAsLong() : -1;
    }

    @Override
    public long sumAsLong() {
        return values.stream()
                .map(HFValue::getAsDouble)
                .filter(Objects::nonNull)
                .mapToLong(Double::longValue)
                .sum();
    }

    @Override
    public double minAsDouble() {
        OptionalDouble min = values.stream()
                .map(HFValue::getAsDouble)
                .filter(Objects::nonNull)
                .mapToDouble(Double::doubleValue)
                .min();
        return min.isPresent() ? min.getAsDouble() : -1;
    }

    @Override
    public double maxAsDouble() {
        OptionalDouble max = values.stream()
                .map(HFValue::getAsDouble)
                .filter(Objects::nonNull)
                .mapToDouble(Double::doubleValue)
                .max();
        return max.isPresent() ? max.getAsDouble() : -1;
    }

    @Override
    public double sumAsDouble() {
        return values.stream()
                .map(HFValue::getAsDouble)
                .filter(Objects::nonNull)
                .mapToDouble(Double::doubleValue)
                .sum();
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
    public Set<String> valuesAsStringSet() {
        return values.stream()
                .map(HFValue::getAsString)
                .collect(Collectors.toSet());
    }
}
