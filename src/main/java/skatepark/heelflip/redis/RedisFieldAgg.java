package skatepark.heelflip.redis;

import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import redis.clients.jedis.Jedis;
import skatepark.heelflip.IFieldAgg;

class RedisFieldAgg implements IFieldAgg {

    private String fieldName;
    private Jedis jedis;

    private final String FIELD_INFO_KEY;
    private final String FIELD_VALUES_KEY;

    public RedisFieldAgg(String fieldName, Jedis jedis) {
        Objects.requireNonNull(fieldName, "fieldName should not be null.");
        Objects.requireNonNull(jedis, "jedis should not be null.");

        this.fieldName = fieldName;
        this.jedis = jedis;

        this.FIELD_INFO_KEY = String.format("FIELD_INFO:%s", fieldName);
        this.FIELD_VALUES_KEY = String.format("FIELD_VALUES:%s", fieldName);
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public int cardinality() {
        Long cardinality = jedis.hlen(FIELD_VALUES_KEY);
        return cardinality == null ? 0 : cardinality.intValue();
    }

    @Override
    public int count() {
        List<String> counters = jedis.hvals(FIELD_VALUES_KEY);
        return counters.stream()
                .mapToInt(count -> Integer.valueOf(count))
                .sum();
    }

    @Override
    public int count(String value) {
        String count = jedis.hget(FIELD_VALUES_KEY, value);
        return count == null ? 0 : Integer.valueOf(count);
    }

    @Override
    public Set<String> distinctValues() {
        return jedis.hkeys(FIELD_VALUES_KEY);
    }

    @Override
    public int getStringCount() {
        String stringCount = jedis.hget(FIELD_INFO_KEY, "stringCount");
        return stringCount == null ? 0 : Integer.valueOf(stringCount);
    }

    @Override
    public int getBooleanCount() {
        String booleanCount = jedis.hget(FIELD_INFO_KEY, "booleanCount");
        return booleanCount == null ? 0 : Integer.valueOf(booleanCount);
    }

    @Override
    public int getNumberCount() {
        String numberCount = jedis.hget(FIELD_INFO_KEY, "numberCount");
        return numberCount == null ? 0 : Integer.valueOf(numberCount);
    }

    @Override
    public BigDecimal getMax() {
        String max = jedis.hget(FIELD_INFO_KEY, "max");
        return max == null ? null : new BigDecimal(max);
    }

    @Override
    public BigDecimal getMin() {
        String min = jedis.hget(FIELD_INFO_KEY, "min");
        return min == null ? null : new BigDecimal(min);
    }

    @Override
    public BigDecimal getSum() {
        String sum = jedis.hget(FIELD_INFO_KEY, "sum");
        return sum == null ? null : new BigDecimal(sum);
    }

    @Override
    public void agg(JsonPrimitive value) {
        jedis.hincrBy(FIELD_VALUES_KEY, value.getAsString(), 1);

        if (value.isNumber()) {
            Double min = value.getAsDouble();
            String minStr = jedis.hget(FIELD_INFO_KEY, "min");//TODO
            if (minStr != null) {
                min = Math.min(min, Double.parseDouble(minStr));
            }
            jedis.hset(FIELD_INFO_KEY, "min", min.toString());

            Double max = value.getAsDouble();
            String maxStr = jedis.hget(FIELD_INFO_KEY, "max");//TODO
            if (maxStr != null) {
                max = Math.max(max, Double.parseDouble(maxStr));
            }
            jedis.hset(FIELD_INFO_KEY, "max", max.toString());

            jedis.hincrByFloat(FIELD_INFO_KEY, "sum", value.getAsDouble());

            jedis.hincrBy(FIELD_INFO_KEY, "numberCount", 1);
        } else if (value.isString()) {
            jedis.hincrBy(FIELD_INFO_KEY, "stringCount", 1);
        } else if (value.isBoolean()) {
            jedis.hincrBy(FIELD_INFO_KEY, "booleanCount", 1);
        }
    }

    @Override
    public String toString() {
        return toString(false);
    }
}
