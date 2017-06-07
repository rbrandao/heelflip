package skatepark.heelflip.redis;

import com.google.gson.JsonPrimitive;

import java.util.Objects;
import java.util.Set;

import redis.clients.jedis.Jedis;
import skatepark.heelflip.IFieldAgg;
import skatepark.heelflip.IGroupByAgg;

class RedisGroupByAgg implements IGroupByAgg {

    private String fieldName;
    private String groupBy;
    private Jedis jedis;

    public RedisGroupByAgg(String fieldName, String groupBy, Jedis jedis) {
        Objects.requireNonNull(fieldName, "fieldName should not be null.");
        Objects.requireNonNull(groupBy, "groupBy should not be null.");
        Objects.requireNonNull(jedis, "jedis should not be null.");
        if (fieldName.equals(groupBy)) {
            throw new IllegalArgumentException("fieldName should not be equal to groupBy.");
        }
        this.fieldName = fieldName;
        this.groupBy = groupBy;
        this.jedis = jedis;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getGroupBy() {
        return groupBy;
    }

    @Override
    public void agg(JsonPrimitive fieldNameValue, JsonPrimitive groupByValue) {
//TODO
    }

    @Override
    public IFieldAgg groupBy(String value) {
        return null;//TODO
    }

    @Override
    public Set<String> groupByValues() {
        return null;//TODO
    }

    @Override
    public Set<String> values() {
        return null;//TODO
    }

    @Override
    public String toString() {
        return toString(false);
    }
}
