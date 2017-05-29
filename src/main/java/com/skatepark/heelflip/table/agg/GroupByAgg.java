package com.skatepark.heelflip.table.agg;

import java.util.Objects;

public class GroupByAgg {

    private String columnName;

    private String groupBy;

    public GroupByAgg(String columnName, String groupBy) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(groupBy, "groupBy should not be null.");
        this.columnName = columnName;
        this.groupBy = groupBy;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getGroupBy() {
        return groupBy;
    }
}
