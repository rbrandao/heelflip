package com.skatepark.heelflip.column;

import java.util.List;

public interface IColumn<T> {

    String name();

    void add(T value);

    long count();

    long count(T value);

    List<T> values();
}
