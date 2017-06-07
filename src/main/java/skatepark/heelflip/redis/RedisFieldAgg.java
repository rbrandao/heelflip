package skatepark.heelflip.redis;

import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Set;

import skatepark.heelflip.IFieldAgg;

class RedisFieldAgg implements IFieldAgg {

    private String fieldName;

    public RedisFieldAgg(String fieldName) {
        Objects.requireNonNull(fieldName, "fieldName should not be null.");
        this.fieldName = fieldName;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public int cardinality() {
        return 0;//TODO
    }

    @Override
    public int count() {
        return 0;//TODO
    }

    @Override
    public int count(String value) {
        return 0;//TODO
    }

    @Override
    public Set<String> distinctValues() {
        return null;//TODO
    }

    @Override
    public int getStringCount() {
        return 0;//TODO
    }

    @Override
    public int getBooleanCount() {
        return 0;//TODO
    }

    @Override
    public int getNumberCount() {
        return 0;//TODO
    }

    @Override
    public BigDecimal getMax() {
        return null;//TODO
    }

    @Override
    public BigDecimal getMin() {
        return null;//TODO
    }

    @Override
    public BigDecimal getSum() {
        return null;//TODO
    }

    @Override
    public void agg(JsonPrimitive value) {
        //TODO
    }

    @Override
    public String toString() {
        return toString(false);
    }
}
