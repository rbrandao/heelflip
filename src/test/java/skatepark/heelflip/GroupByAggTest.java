package skatepark.heelflip;

import com.google.gson.JsonPrimitive;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import skatepark.heelflip.inmem.InMemGroupByAgg;
import skatepark.heelflip.redis.RedisGroupByAgg;

public class GroupByAggTest {

    @Before
    public void cleanRedis() {
        try {
            JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
            pool.getResource().flushAll();
        } catch (JedisConnectionException e) {
            //executing unit tests without redis
        }
    }

    @Test
    public void testGroupByValues() {
        for (IGroupByAgg groupByAgg : getGroupByImplList()) {
            groupByAgg.agg(new JsonPrimitive(10), new JsonPrimitive(true));
            groupByAgg.agg(new JsonPrimitive(20), new JsonPrimitive(true));
            groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));
            groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));

            Assert.assertEquals("a", groupByAgg.getFieldName());
            Assert.assertEquals("b", groupByAgg.getGroupBy());

            Set<String> groupByValues = groupByAgg.groupByValues();
            Assert.assertEquals(2, groupByValues.size());
            Assert.assertTrue(groupByValues.contains("true"));
            Assert.assertTrue(groupByValues.contains("false"));
        }
    }

    @Test
    public void testValues() {
        for (IGroupByAgg groupByAgg : getGroupByImplList()) {
            groupByAgg.agg(new JsonPrimitive(10), new JsonPrimitive(true));
            groupByAgg.agg(new JsonPrimitive(20), new JsonPrimitive(true));
            groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));
            groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));

            Assert.assertEquals("a", groupByAgg.getFieldName());
            Assert.assertEquals("b", groupByAgg.getGroupBy());

            Set<String> values = groupByAgg.values();
            Assert.assertEquals(3, values.size());
            Assert.assertTrue(values.contains("10"));
            Assert.assertTrue(values.contains("20"));
            Assert.assertTrue(values.contains("-1"));
        }
    }

    @Test
    public void testGroupBy() {
        for (IGroupByAgg groupByAgg : getGroupByImplList()) {
            groupByAgg.agg(new JsonPrimitive(10), new JsonPrimitive(true));
            groupByAgg.agg(new JsonPrimitive(20), new JsonPrimitive(true));
            groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));
            groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));

            Assert.assertEquals("a", groupByAgg.getFieldName());
            Assert.assertEquals("b", groupByAgg.getGroupBy());

            IFieldAgg fieldAgg = groupByAgg.groupBy("true");
            Set<String> distinctValues = fieldAgg.distinctValues();
            Assert.assertEquals(2, distinctValues.size());
            Assert.assertTrue(distinctValues.contains("10"));
            Assert.assertTrue(distinctValues.contains("20"));

            Assert.assertEquals(2, fieldAgg.cardinality());
            Assert.assertEquals(2, fieldAgg.count());
            Assert.assertEquals(2, fieldAgg.getNumberCount());
            Assert.assertEquals(10, fieldAgg.getMin().intValue());
            Assert.assertEquals(20, fieldAgg.getMax().intValue());
            Assert.assertEquals(30, fieldAgg.getSum().intValue());

            fieldAgg = groupByAgg.groupBy("false");
            distinctValues = fieldAgg.distinctValues();
            Assert.assertEquals(1, distinctValues.size());
            Assert.assertTrue(distinctValues.contains("-1"));

            Assert.assertEquals(1, fieldAgg.cardinality());
            Assert.assertEquals(2, fieldAgg.count());
            Assert.assertEquals(2, fieldAgg.getNumberCount());
            Assert.assertEquals(-1, fieldAgg.getMin().intValue());
            Assert.assertEquals(-1, fieldAgg.getMax().intValue());
            Assert.assertEquals(-2, fieldAgg.getSum().intValue());
        }
    }

    private List<IGroupByAgg> getGroupByImplList() {
        List<IGroupByAgg> list = new ArrayList<>();
        list.add(new InMemGroupByAgg("a", "b"));

        try {
            JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
            Jedis jedis = pool.getResource();
            list.add(new RedisGroupByAgg("a", "b", jedis));
            System.out.println("WARNING: using redis at localhost for unit tests!");
        } catch (JedisConnectionException e) {
            System.out.println("WARNING: executing unit tests without redis!");
        }
        return list;
    }
}
