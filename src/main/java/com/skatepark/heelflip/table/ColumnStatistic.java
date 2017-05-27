package com.skatepark.heelflip.table;

import java.math.BigDecimal;

public class ColumnStatistic {

    private String columnName;
    private int count;
    private int stringCount;
    private int booleanCount;
    private int longCount;
    private int doubleCount;
    private BigDecimal min;
    private BigDecimal max;
    private BigDecimal sum;

    ColumnStatistic(String columnName) {
        this.columnName = columnName;
        this.count = 0;
        this.stringCount = 0;
        this.booleanCount = 0;
        this.longCount = 0;
        this.doubleCount = 0;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getDoubleCount() {
        return doubleCount;
    }

    public int getLongCount() {
        return longCount;
    }

    public int getStringCount() {
        return stringCount;
    }

    public int getBooleanCount() {
        return booleanCount;
    }

    public int getCount() {
        return count;
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

    void agg(HFValue value) {
        if (value.isString()) {
            stringCount++;
        }
        if (value.isBoolean()) {
            booleanCount++;
        }
        if (value.isLong()) {
            longCount++;
        }
        if (value.isDouble()) {
            doubleCount++;
        }

        count++;

        if (value.isNumber()) {
            BigDecimal v = value.getAsBigDecimal();
            min = min == null || min.compareTo(v) > 0 ? v : min;
            max = max == null || max.compareTo(v) < 0 ? v : max;
            sum = sum == null ? v : sum.add(v);
        }
    }
}
