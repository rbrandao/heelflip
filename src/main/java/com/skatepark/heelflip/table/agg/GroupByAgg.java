package com.skatepark.heelflip.table.agg;

import com.google.gson.JsonPrimitive;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupByAgg {

    private String columnName;

    private String groupBy;

    private Map<String, Set<String>> relationship;

    public GroupByAgg(String columnName, String groupBy) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(groupBy, "groupBy should not be null.");
        if (columnName.equals(groupBy)) {
            throw new IllegalArgumentException("columnName should not be equal to groupBy.");
        }
        this.columnName = columnName;
        this.groupBy = groupBy;
        this.relationship = new HashMap<>();
    }

    public String getColumnName() {
        return columnName;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public void agg(JsonPrimitive columnValue, JsonPrimitive groupByValue) {
        Objects.requireNonNull(columnValue, "columnValue should not be null.");
        Objects.requireNonNull(groupByValue, "groupByValue should not be null.");

        Set<String> values = relationship.computeIfAbsent(groupByValue.getAsString(), key -> new HashSet<>());
        values.add(columnValue.getAsString());
    }

    public Set<String> groupBy(String value) {
        return value == null || !relationship.containsKey(value) ?
                Collections.emptySet() :
                Collections.unmodifiableSet(relationship.get(value));
    }

    public Set<String> groupByValues() {
        return Collections.unmodifiableSet(relationship.keySet());
    }

    public Set<String> values() {
        return relationship.values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }
}
