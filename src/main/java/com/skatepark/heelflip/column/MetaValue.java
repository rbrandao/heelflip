package com.skatepark.heelflip.column;

import java.util.Objects;

public class MetaValue<T> {

    private T value;

    private long count;

    public MetaValue(T value) {
        Objects.requireNonNull(value, "value should not be null.");
        this.value = value;
        this.count = 0;
    }

    public void inc() {
        count++;
    }

    public long count() {
        return count;
    }
}
