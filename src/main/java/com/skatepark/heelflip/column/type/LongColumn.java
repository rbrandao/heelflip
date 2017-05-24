package com.skatepark.heelflip.column.type;

import com.skatepark.heelflip.column.AbstractNumericColumn;

import java.util.Objects;

public class LongColumn extends AbstractNumericColumn<Long> {

    public LongColumn(String name) {
        super(name);

        sum = 0L;
    }

    @Override
    public void add(Long value) {
        super.add(value);

        if (min == null) {
            min = value;
        }
        if (max == null) {
            max = value;
        }
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
        sum = sum + value;
    }

    public void add(Integer value) {
        Objects.requireNonNull(value, "value should not be null.");
        add(value.longValue());
    }
}
