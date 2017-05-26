package com.skatepark.heelflip.table;

import java.util.Set;

interface IColumn {

    String name();

    void add(HFValue value);

    long count(String value);

    long count(int value);

    long count(long value);

    long count(double value);

    long count();

    int minAsInt();

    int maxAsInt();

    int sumAsInt();

    long minAsLong();

    long maxAsLong();

    long sumAsLong();

    double minAsDouble();

    double maxAsDouble();

    double sumAsDouble();

    Set<Integer> valuesAsIntSet();

    Set<Long> valuesAsLongSet();

    Set<Double> valuesAsDoubleSet();

    Set<String> valuesAsStringSet();
}
