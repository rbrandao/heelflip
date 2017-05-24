package com.skatepark.heelflip.column.type;


import com.skatepark.heelflip.column.AbstractNumericColumn;

public class DoubleColumn extends AbstractNumericColumn<Double> {

    public DoubleColumn(String name) {
        super(name);

        sum = 0.0;
    }

    @Override
    public void add(Double value) {
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
}
