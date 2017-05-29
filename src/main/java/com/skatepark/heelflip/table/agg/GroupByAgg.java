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

    private Map<JsonPrimitive, Set<JsonPrimitive>> relationship;

    public GroupByAgg(String columnName, String groupBy) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(groupBy, "groupBy should not be null.");
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

        Set<JsonPrimitive> values = relationship.computeIfAbsent(groupByValue, key -> new HashSet<>());
        values.add(columnValue);
    }

    public Set<JsonPrimitive> groupBy(JsonPrimitive value) {
        return relationship.containsKey(value) ?
                Collections.unmodifiableSet(relationship.get(value)) :
                Collections.emptySet();
    }

    public Set<JsonPrimitive> groupByValues() {
        return Collections.unmodifiableSet(relationship.keySet());
    }

    public Set<JsonPrimitive> values() {
        return relationship.values().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }
}
