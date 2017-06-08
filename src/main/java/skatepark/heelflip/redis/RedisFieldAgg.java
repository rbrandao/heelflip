package skatepark.heelflip.redis;

import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import skatepark.heelflip.IFieldAgg;

class RedisFieldAgg implements IFieldAgg {

    private String fieldName;
    private Jedis jedis;

    private final String FIELD_INFO_KEY;
    private final String FIELD_VALUES_KEY;

    private static final String SCRIPT_MIN = "if not redis.call('hget',KEYS[1],'min') or tonumber(redis.call('hget',KEYS[1],'min')) > tonumber(ARGV[1]) then redis.call('hset',KEYS[1],'min', ARGV[1]) end";
    private static final String SCRIPT_MAX = "if not redis.call('hget',KEYS[1],'max') or tonumber(redis.call('hget',KEYS[1],'max')) < tonumber(ARGV[1]) then redis.call('hset',KEYS[1],'max', ARGV[1]) end";

    public RedisFieldAgg(String fieldName, Jedis jedis) {
        Objects.requireNonNull(fieldName, "fieldName should not be null.");
        Objects.requireNonNull(jedis, "jedis should not be null.");

        this.fieldName = fieldName;
        this.jedis = jedis;

        this.FIELD_INFO_KEY = String.format("JSON_AGG:FIELD_INFO:%s", fieldName);
        this.FIELD_VALUES_KEY = String.format("JSON_AGG:FIELD_VALUES:%s", fieldName);
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
    public long count() {
        List<String> counters = jedis.hvals(FIELD_VALUES_KEY);
        return counters.stream()
                .mapToLong(Long::valueOf)
                .sum();
    }

    @Override
    public long count(String value) {
        String count = jedis.hget(FIELD_VALUES_KEY, value);
        return count == null ? 0 : Long.valueOf(count);
    }

    @Override
    public Set<String> distinctValues() {
        return jedis.hkeys(FIELD_VALUES_KEY);
    }

    @Override
    public long getStringCount() {
        String stringCount = jedis.hget(FIELD_INFO_KEY, "stringCount");
        return stringCount == null ? 0 : Long.valueOf(stringCount);
    }

    @Override
    public long getBooleanCount() {
        String booleanCount = jedis.hget(FIELD_INFO_KEY, "booleanCount");
        return booleanCount == null ? 0 : Long.valueOf(booleanCount);
    }

    @Override
    public long getNumberCount() {
        String numberCount = jedis.hget(FIELD_INFO_KEY, "numberCount");
        return numberCount == null ? 0 : Long.valueOf(numberCount);
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
        Pipeline p = jedis.pipelined();
        p.hincrBy(FIELD_VALUES_KEY, value.getAsString(), 1);

        if (value.isNumber()) {
            List<String> keys = Collections.singletonList(FIELD_INFO_KEY);
            List<String> args = Collections.singletonList(value.getAsString());

            p.eval(SCRIPT_MIN, keys, args);
            p.eval(SCRIPT_MAX, keys, args);
            p.hincrByFloat(FIELD_INFO_KEY, "sum", value.getAsDouble());

            p.hincrBy(FIELD_INFO_KEY, "numberCount", 1);
        } else if (value.isString()) {
            p.hincrBy(FIELD_INFO_KEY, "stringCount", 1);
        } else if (value.isBoolean()) {
            p.hincrBy(FIELD_INFO_KEY, "booleanCount", 1);
        }
        p.sync();
    }

    @Override
    public String toString() {
        return toString(false);
    }
}
