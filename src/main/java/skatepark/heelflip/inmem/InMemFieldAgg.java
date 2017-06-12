package skatepark.heelflip.inmem;

import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import skatepark.heelflip.IFieldAgg;

public class InMemFieldAgg implements IFieldAgg {

    private String fieldName;
    private Map<String, Long> countMap;

    private long stringCount;
    private long booleanCount;
    private long numberCount;

    private BigDecimal min;
    private BigDecimal max;
    private BigDecimal sum;

    public InMemFieldAgg(String fieldName) {
        Objects.requireNonNull(fieldName, "fieldName should not be null.");
        this.fieldName = fieldName;
        this.countMap = new HashMap<>();
        this.stringCount = 0;
        this.booleanCount = 0;
        this.numberCount = 0;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public int cardinality() {
        return countMap.keySet().size();
    }

    @Override
    public long count() {
        return countMap.values().stream()
                .mapToLong(Long::longValue)
                .sum();
    }

    @Override
    public long count(String value) {
        return value == null || !countMap.containsKey(value) ? 0 : countMap.get(value);
    }

    @Override
    public Set<String> distinctValues() {
        return countMap.keySet();
    }

    @Override
    public long getStringCount() {
        return stringCount;
    }

    @Override
    public long getBooleanCount() {
        return booleanCount;
    }

    @Override
    public long getNumberCount() {
        return numberCount;
    }

    @Override
    public BigDecimal getMax() {
        return max;
    }

    @Override
    public BigDecimal getMin() {
        return min;
    }

    @Override
    public BigDecimal getSum() {
        return sum;
    }

    @Override
    public void agg(JsonPrimitive value) {
        countMap.computeIfAbsent(value.getAsString(), key -> 0L);
        countMap.put(value.getAsString(), countMap.get(value.getAsString()) + 1);

        if (value.isNumber()) {
            numberCount++;

            BigDecimal v = value.getAsBigDecimal();
            min = min == null || min.compareTo(v) > 0 ? v : min;
            max = max == null || max.compareTo(v) < 0 ? v : max;
            sum = sum == null ? v : sum.add(v);
        } else if (value.isString()) {
            stringCount++;
        } else if (value.isBoolean()) {
            booleanCount++;
        }
    }

    @Override
    public String toString() {
        return toString(false);
    }
}
