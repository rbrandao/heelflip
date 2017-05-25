package com.skatepark.heelflip.column;

import com.skatepark.heelflip.column.type.DoubleColumn;
import com.skatepark.heelflip.column.type.LongColumn;
import com.skatepark.heelflip.column.type.StringColumn;

import java.util.List;

public interface IColumn<T> {

    String name();

    void add(T value);

    long count();

    long count(T value);

    List<T> values();

    default boolean isLongColumn() {
        return this instanceof LongColumn;
    }

    default boolean isDoubleColumn() {
        return this instanceof DoubleColumn;
    }

    default boolean isStringColumn() {
        return this instanceof StringColumn;
    }

    default LongColumn getAsLongColumn() {
        return (LongColumn) this;
    }

    default DoubleColumn getAsDoubleColumn() {
        return (DoubleColumn) this;
    }

    default StringColumn getAsStringColumn() {
        return (StringColumn) this;
    }
}
