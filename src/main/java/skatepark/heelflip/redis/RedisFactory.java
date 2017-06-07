package skatepark.heelflip.redis;

import java.util.Objects;

import redis.clients.jedis.Jedis;
import skatepark.heelflip.IAggFactory;
import skatepark.heelflip.IFieldAgg;
import skatepark.heelflip.IGroupByAgg;

public class RedisFactory implements IAggFactory {

    private Jedis jedis;

    public RedisFactory(Jedis jedis) {
        Objects.requireNonNull(jedis, "jedis should not be null.");
        this.jedis = jedis;
    }

    @Override
    public IFieldAgg newFieldAgg(String fieldName) {
        return new RedisFieldAgg(fieldName, jedis);
    }

    @Override
    public IGroupByAgg newGroupByAgg(String fieldName, String groupBy) {
        return new RedisGroupByAgg(fieldName, groupBy, jedis);
    }
}
