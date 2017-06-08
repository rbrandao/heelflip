package skatepark.heelflip.redis;

import com.google.gson.JsonPrimitive;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import redis.clients.jedis.Jedis;
import skatepark.heelflip.IFieldAgg;
import skatepark.heelflip.IGroupByAgg;

class RedisGroupByAgg implements IGroupByAgg {

    private String fieldName;
    private String groupBy;
    private Jedis jedis;

    private final String GROUP_BY_SET;
    private final String NEW_FIELD_NAME_FORMAT;

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
        RedisFieldAgg fieldAgg = new RedisFieldAgg(newFieldName, jedis);//TODO
        fieldAgg.agg(fieldNameValue);
    }

    @Override
    public IFieldAgg groupBy(String value) {
        String fieldName = String.format(NEW_FIELD_NAME_FORMAT, value);

        Boolean exists = jedis.sismember(GROUP_BY_SET, fieldName);
        if (exists == null || !exists) {
            return null;
        }
        return new RedisFieldAgg(fieldName, jedis);
    }

    @Override
    public Set<String> groupByValues() {
        Set<String> members = jedis.smembers(GROUP_BY_SET);
        return members.stream().
                map(member -> member.substring(member.lastIndexOf(":") + 1, member.length())).
                collect(Collectors.toSet());
    }

    @Override
    public Set<String> values() {
        Set<String> members = jedis.smembers(GROUP_BY_SET);
        return members.stream()
                .map(member -> new RedisFieldAgg(member, jedis))
                .flatMap(fieldAgg -> fieldAgg.distinctValues().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return toString(false);
    }
}
