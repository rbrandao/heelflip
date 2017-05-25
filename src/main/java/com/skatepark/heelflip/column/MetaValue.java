package com.skatepark.heelflip.column;

public class MetaValue {

    private long count;

    public MetaValue() {
        this.count = 0;
    }

    public void inc() {
        count++;
    }

    public long count() {
        return count;
    }
}
