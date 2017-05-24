package com.skatepark.heelflip.column;

public interface INumericColumn<T extends Number> extends IColumn<T> {

    T min();

    T max();

    T sum();
}
