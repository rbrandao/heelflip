package com.skatepark.heelflip.column.type;

import com.skatepark.heelflip.column.type.DoubleColumn;
import com.skatepark.heelflip.column.type.LongColumn;
import com.skatepark.heelflip.column.type.StringColumn;

import org.junit.Assert;
import org.junit.Test;

public class ColumnTest {

    @Test
    public void testDoubleColumn() {
        DoubleColumn column = new DoubleColumn("a");
        column.add(2.3);
        column.add(2.1);
        column.add(2.0);
        column.add(2.0);

        Assert.assertEquals("a", column.name());
        Assert.assertEquals(8.4, column.sum(), 10E10);
        Assert.assertEquals(2.0, column.min(), 10E10);
        Assert.assertEquals(2.3, column.max(), 10E10);
        Assert.assertEquals(4, column.count());
        Assert.assertEquals(2, column.count(2.0));
        Assert.assertEquals(0, column.count(1.0));
    }

    @Test
    public void testLongColumn() {
        LongColumn column = new LongColumn("a");
        column.add(10);
        column.add(11);
        column.add(12);
        column.add(12);

        Assert.assertEquals("a", column.name());
        Assert.assertEquals(45L, column.sum().longValue());
        Assert.assertEquals(10L, column.min().longValue());
        Assert.assertEquals(12L, column.max().longValue());
        Assert.assertEquals(4, column.count());
        Assert.assertEquals(2, column.count(12L));
        Assert.assertEquals(0, column.count(29L));
    }

    @Test
    public void testStringColumn() {
        StringColumn column = new StringColumn("a");
        column.add("foo");
        column.add("foo");
        column.add("boo");
        column.add("call");

        Assert.assertEquals("a", column.name());
        Assert.assertEquals(4, column.count());
        Assert.assertEquals(2, column.count("foo"));
        Assert.assertEquals(0, column.count("other"));
    }
}
