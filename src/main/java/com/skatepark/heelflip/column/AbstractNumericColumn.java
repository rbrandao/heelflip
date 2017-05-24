package com.skatepark.heelflip.column;

public abstract class AbstractNumericColumn<T extends Number> extends AbstractColumn<T> implements INumericColumn<T> {

    protected T max;

    protected T min;

    protected T sum;

    public AbstractNumericColumn(String name) {
        super(name);
    }

    @Override
    public T max() {
        return max;
    }

    @Override
    public T min() {
        return min;
    }

    @Override
    public T sum() {
        return sum;
    }
}
