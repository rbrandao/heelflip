package com.skatepark.heelflip;

import com.google.gson.JsonPrimitive;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class GroupByAggTest {

    @Test
    public void testGroupByValues() {
        GroupByAgg groupByAgg = new GroupByAgg("a", "b");
        groupByAgg.agg(new JsonPrimitive(10), new JsonPrimitive(true));
        groupByAgg.agg(new JsonPrimitive(20), new JsonPrimitive(true));
        groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));
        groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));

        Set<String> groupByValues = groupByAgg.groupByValues();
        Assert.assertEquals(2, groupByValues.size());
        Assert.assertTrue(groupByValues.contains("true"));
        Assert.assertTrue(groupByValues.contains("false"));
    }

    @Test
    public void testValues() {
        GroupByAgg groupByAgg = new GroupByAgg("a", "b");
        groupByAgg.agg(new JsonPrimitive(10), new JsonPrimitive(true));
        groupByAgg.agg(new JsonPrimitive(20), new JsonPrimitive(true));
        groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));
        groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));

        Set<String> values = groupByAgg.values();
        Assert.assertEquals(3, values.size());
        Assert.assertTrue(values.contains("10"));
        Assert.assertTrue(values.contains("20"));
        Assert.assertTrue(values.contains("-1"));
    }

    @Test
    public void testGroupBy() {
        GroupByAgg groupByAgg = new GroupByAgg("a", "b");
        groupByAgg.agg(new JsonPrimitive(10), new JsonPrimitive(true));
        groupByAgg.agg(new JsonPrimitive(20), new JsonPrimitive(true));
        groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));
        groupByAgg.agg(new JsonPrimitive(-1), new JsonPrimitive(false));

        Set<String> values = groupByAgg.groupBy("true");
        Assert.assertEquals(2, values.size());
        Assert.assertTrue(values.contains("10"));
        Assert.assertTrue(values.contains("20"));

        values = groupByAgg.groupBy("false");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains("-1"));
    }
}
