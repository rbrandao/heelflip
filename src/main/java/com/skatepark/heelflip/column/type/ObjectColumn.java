package com.skatepark.heelflip.column.type;

import com.skatepark.heelflip.column.IColumn;
import com.skatepark.heelflip.column.MetaValue;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ObjectColumn implements IColumn {

    private String name;

    private Map<String, MetaValue> values;

    private BigDecimal min;
    private BigDecimal max;
    private BigDecimal sum;

    public ObjectColumn(String name) {
        Objects.requireNonNull(name, "name should not be null.");
        this.name = name;
        this.values = new HashMap<>();
        this.sum = new BigDecimal(0);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void add(String value) {
        Objects.requireNonNull(value, "value should not be null.");

        if (!values.containsKey(value)) {
            values.put(value, new MetaValue());
        }
        MetaValue metaValue = values.get(value);
        metaValue.inc();

        try {
            BigDecimal candidate = new BigDecimal(value);

            if (min == null) {
                min = candidate;
            }
            if (max == null) {
                max = candidate;
            }

            if (min.compareTo(candidate) > 0) {
                min = candidate;
            }
            if (max.compareTo(candidate) < 0) {
                max = candidate;
            }
            sum = sum.add(candidate);
        } catch (NumberFormatException e) {
            // do nothing
        }
    }

    @Override
    public void add(int value) {
        add(Integer.toString(value));
    }

    @Override
    public void add(long value) {
        add(Long.toString(value));
    }

    @Override
    public void add(double value) {
        add(Double.toString(value));
    }

    @Override
    public long count(String value) {
        if (value == null || !values.containsKey(value)) {
            return 0;
        }
        MetaValue metaValue = values.get(value);
        return metaValue.count();
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
        return values.values().stream()
                .mapToLong(mv -> mv.count())
                .sum();
    }

    @Override
    public int minAsInt() {
        return min == null ? -1 : min.intValue();
    }

    @Override
    public int maxAsInt() {
        return max == null ? -1 : max.intValue();
    }

    @Override
    public int sumAsInt() {
        return sum == null ? -1 : sum.intValue();
    }

    @Override
    public long minAsLong() {
        return min == null ? -1 : min.longValue();
    }

    @Override
    public long maxAsLong() {
        return max == null ? -1 : max.longValue();
    }

    @Override
    public long sumAsLong() {
        return sum == null ? -1 : sum.longValue();
    }

    @Override
    public double minAsDouble() {
        return min == null ? -1 : min.doubleValue();
    }

    @Override
    public double maxAsDouble() {
        return max == null ? -1 : max.doubleValue();
    }

    @Override
    public double sumAsDouble() {
        return sum == null ? -1 : sum.doubleValue();
    }

    @Override
    public Set<Integer> valuesAsIntSet() {
        Set<Integer> result = new HashSet<>();
        for (String value : values.keySet()) {
            Double d = toDouble(value);
            if (d != null) {
                result.add(d.intValue());
            }
        }
        return result;
    }

    @Override
    public Set<Long> valuesAsLongSet() {
        Set<Long> result = new HashSet<>();
        for (String value : values.keySet()) {
            Double d = toDouble(value);
            if (d != null) {
                result.add(d.longValue());
            }
        }
        return result;
    }

    @Override
    public Set<Double> valuesAsDoubleSet() {
        Set<Double> result = new HashSet<>();
        for (String value : values.keySet()) {
            Double d = toDouble(value);
            if (d != null) {
                result.add(d);
            }
        }
        return result;
    }

    @Override
    public Set<String> valuesAsStringSet() {
        return Collections.unmodifiableSet(values.keySet());
    }

    /**
     * Internal utility that converts {@link String} to {@link Double}.
     *
     * @param value value as {@link String}.
     * @return value as {@link Double}.
     */
    private Double toDouble(String value) {
        try {
            return Double.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
