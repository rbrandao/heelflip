package skatepark.heelflip.redis;

import skatepark.heelflip.IAggFactory;
import skatepark.heelflip.IFieldAgg;
import skatepark.heelflip.IGroupByAgg;

public class RedisFactory implements IAggFactory {
    @Override
    public IFieldAgg newFieldAgg(String fieldName) {
        return new RedisFieldAgg(fieldName);
    }

    @Override
    public IGroupByAgg newGroupByAgg(String fieldName, String groupBy) {
        return new RedisGroupByAgg(fieldName, groupBy);
    }
}
