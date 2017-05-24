package com.skatepark.heelflip.column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class AbstractColumn<T> implements IColumn<T> {

    private String name;

    private Map<T, MetaValue<T>> values;

    public AbstractColumn(String name) {
        Objects.requireNonNull(name, "name should not be null.");
        this.name = name;
        this.values = new HashMap<>();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void add(T value) {
        Objects.requireNonNull(value, "value should not be null.");

        if (!values.containsKey(value)) {
            values.put(value, new MetaValue<>(value));
        }
        MetaValue<T> metaValue = values.get(value);
        metaValue.inc();
    }

    @Override
    public long count() {
        return values.values().stream()
                .mapToLong(mv -> mv.count())
                .sum();
    }

    @Override
    public long count(T value) {
        if (value == null || !values.containsKey(value)) {
            return 0;
        }
        MetaValue<T> metaValue = values.get(value);
        return metaValue.count();
    }

    @Override
    public List<T> values() {
        return new ArrayList<>(values.keySet());
    }
}
