package skatepark.heelflip.redis;

import com.google.gson.JsonPrimitive;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import redis.clients.jedis.Jedis;
import skatepark.heelflip.IFieldAgg;
import skatepark.heelflip.IGroupByAgg;

public class RedisGroupByAgg implements IGroupByAgg {

    private String fieldName;
    private String groupBy;
    private Jedis jedis;

    private final String GROUP_BY_SET;
    private final String NEW_FIELD_NAME_FORMAT;

    private Map<String, RedisFieldAgg> aggregations;

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
        this.aggregations = new HashMap<>();

        this.GROUP_BY_SET = String.format("JSON_AGG:GROUP_BY_SET:%s.GROUP_BY.%s", fieldName, groupBy);
        this.NEW_FIELD_NAME_FORMAT = String.format("%s.GROUP_BY.%s", fieldName, groupBy) + ":%s";
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
        String newFieldName = String.format(NEW_FIELD_NAME_FORMAT, groupByValue.getAsString());
        jedis.sadd(GROUP_BY_SET, newFieldName);
        RedisFieldAgg fieldAgg = aggregations.computeIfAbsent(newFieldName, n -> new RedisFieldAgg(newFieldName, jedis));
        fieldAgg.agg(fieldNameValue);
    }

    @Override
    public IFieldAgg groupBy(String value) {
        String fieldName = String.format(NEW_FIELD_NAME_FORMAT, value);

        Boolean exists = jedis.sismember(GROUP_BY_SET, fieldName);
        if (exists == null || !exists) {
            return null;
        }
        RedisFieldAgg fieldAgg = aggregations.get(fieldName);
        if (fieldAgg == null) {
            fieldAgg = new RedisFieldAgg(fieldName, jedis);
            aggregations.put(fieldName, fieldAgg);
        }
        return fieldAgg;
    }

    @Override
    public Set<String> groupByValues() {
        Set<String> fieldNames = jedis.smembers(GROUP_BY_SET);
        return fieldNames.stream().
                map(m -> m.substring(m.lastIndexOf(":") + 1, m.length())).
                collect(Collectors.toSet());
    }

    @Override
    public Set<String> values() {
        Set<String> result = new HashSet<>();
        for (String fieldName : jedis.smembers(GROUP_BY_SET)) {
            RedisFieldAgg fieldAgg = aggregations.get(fieldName);
            if (fieldAgg == null) {
                fieldAgg = new RedisFieldAgg(fieldName, jedis);
                aggregations.put(fieldName, fieldAgg);
            }
            result.addAll(fieldAgg.distinctValues());
        }
        return result;
    }

    @Override
    public String toString() {
        return toString(false);
    }
}
