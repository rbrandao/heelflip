package skatepark.heelflip.inmem;

import com.google.gson.JsonPrimitive;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import skatepark.heelflip.IGroupByAgg;

public class InMemGroupByAgg implements IGroupByAgg {

    private String fieldName;
    private String groupBy;
    private Map<String, InMemFieldAgg> aggregations;

    public InMemGroupByAgg(String fieldName, String groupBy) {
        Objects.requireNonNull(fieldName, "fieldName should not be null.");
        Objects.requireNonNull(groupBy, "groupBy should not be null.");
        if (fieldName.equals(groupBy)) {
            throw new IllegalArgumentException("fieldName should not be equal to groupBy.");
        }
        this.fieldName = fieldName;
        this.groupBy = groupBy;
        this.aggregations = new HashMap<>();
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getGroupBy() {
        return groupBy;
    }

    @Override
    public void agg(JsonPrimitive fieldNameValue, JsonPrimitive groupByValue) {
        InMemFieldAgg fieldAgg = aggregations.computeIfAbsent(groupByValue.getAsString(), key -> new InMemFieldAgg(fieldName));
        fieldAgg.agg(fieldNameValue);
    }

    @Override
    public InMemFieldAgg groupBy(String value) {
        return value == null || !aggregations.containsKey(value) ?
                null :
                aggregations.get(value);
    }

    @Override
    public Set<String> groupByValues() {
        return aggregations.keySet();
    }

    @Override
    public Set<String> values() {
        return aggregations.values().stream()
                .flatMap(fieldAgg -> fieldAgg.distinctValues().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return toString(false);
    }
}
