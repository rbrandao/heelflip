package com.skatepark.heelflip.table.agg;

import com.google.gson.JsonPrimitive;

import org.junit.Assert;
import org.junit.Test;

public class ColumnAggTest {

    @Test
    public void testDoubleValues() {
        ColumnAgg columnAgg = new ColumnAgg("a");
        columnAgg.agg(new JsonPrimitive(2.3));
        columnAgg.agg(new JsonPrimitive(2.1));
        columnAgg.agg(new JsonPrimitive(2.0));
        columnAgg.agg(new JsonPrimitive(2.0));

        Assert.assertEquals("a", columnAgg.getColumnName());

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(3, columnAgg.cardinality());
        Assert.assertEquals(0, columnAgg.getStringCount());
        Assert.assertEquals(0, columnAgg.getBooleanCount());
        Assert.assertEquals(4, columnAgg.getNumberCount());
        Assert.assertEquals(2.0, columnAgg.getMin().doubleValue(), 10E10);
        Assert.assertEquals(2.3, columnAgg.getMax().doubleValue(), 10E10);
        Assert.assertEquals(8.4, columnAgg.getSum().doubleValue(), 10E10);

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(2.3)));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(2.1)));
        Assert.assertEquals(2, columnAgg.count(new JsonPrimitive(2.0)));
        Assert.assertEquals(0, columnAgg.count(new JsonPrimitive(1.0)));
    }

    @Test
    public void testIntValues() {
        ColumnAgg columnAgg = new ColumnAgg("a");
        columnAgg.agg(new JsonPrimitive(10));
        columnAgg.agg(new JsonPrimitive(11));
        columnAgg.agg(new JsonPrimitive(12));
        columnAgg.agg(new JsonPrimitive(12));

        Assert.assertEquals("a", columnAgg.getColumnName());

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(3, columnAgg.cardinality());
        Assert.assertEquals(0, columnAgg.getStringCount());
        Assert.assertEquals(0, columnAgg.getBooleanCount());
        Assert.assertEquals(4, columnAgg.getNumberCount());
        Assert.assertEquals(10, columnAgg.getMin().intValue());
        Assert.assertEquals(12, columnAgg.getMax().intValue());
        Assert.assertEquals(45, columnAgg.getSum().intValue());

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(10)));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(11)));
        Assert.assertEquals(2, columnAgg.count(new JsonPrimitive(12)));
        Assert.assertEquals(0, columnAgg.count(new JsonPrimitive(13)));
    }

    @Test
    public void testLongValues() {
        ColumnAgg columnAgg = new ColumnAgg("a");
        columnAgg.agg(new JsonPrimitive(10L));
        columnAgg.agg(new JsonPrimitive(11L));
        columnAgg.agg(new JsonPrimitive(12L));
        columnAgg.agg(new JsonPrimitive(12L));

        Assert.assertEquals("a", columnAgg.getColumnName());

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(3, columnAgg.cardinality());
        Assert.assertEquals(0, columnAgg.getStringCount());
        Assert.assertEquals(0, columnAgg.getBooleanCount());
        Assert.assertEquals(4, columnAgg.getNumberCount());
        Assert.assertEquals(10, columnAgg.getMin().intValue());
        Assert.assertEquals(12, columnAgg.getMax().intValue());
        Assert.assertEquals(45, columnAgg.getSum().intValue());

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(10L)));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(11L)));
        Assert.assertEquals(2, columnAgg.count(new JsonPrimitive(12L)));
        Assert.assertEquals(0, columnAgg.count(new JsonPrimitive(13L)));
    }

    @Test
    public void testStringColumn() {
        ColumnAgg columnAgg = new ColumnAgg("a");
        columnAgg.agg(new JsonPrimitive("foo"));
        columnAgg.agg(new JsonPrimitive("foo"));
        columnAgg.agg(new JsonPrimitive("boo"));
        columnAgg.agg(new JsonPrimitive("call"));

        Assert.assertEquals("a", columnAgg.getColumnName());

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(3, columnAgg.cardinality());
        Assert.assertEquals(4, columnAgg.getStringCount());
        Assert.assertEquals(0, columnAgg.getBooleanCount());
        Assert.assertEquals(0, columnAgg.getNumberCount());
        Assert.assertNull(columnAgg.getMin());
        Assert.assertNull(columnAgg.getMax());
        Assert.assertNull(columnAgg.getSum());

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(2, columnAgg.count(new JsonPrimitive("foo")));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive("boo")));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive("call")));
        Assert.assertEquals(0, columnAgg.count(new JsonPrimitive("other")));
    }

    @Test
    public void testBooleanColumn() {
        ColumnAgg columnAgg = new ColumnAgg("a");
        columnAgg.agg(new JsonPrimitive(true));
        columnAgg.agg(new JsonPrimitive(true));
        columnAgg.agg(new JsonPrimitive(true));
        columnAgg.agg(new JsonPrimitive(false));

        Assert.assertEquals("a", columnAgg.getColumnName());

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(2, columnAgg.cardinality());
        Assert.assertEquals(0, columnAgg.getStringCount());
        Assert.assertEquals(4, columnAgg.getBooleanCount());
        Assert.assertEquals(0, columnAgg.getNumberCount());
        Assert.assertNull(columnAgg.getMin());
        Assert.assertNull(columnAgg.getMax());
        Assert.assertNull(columnAgg.getSum());

        Assert.assertEquals(4, columnAgg.count());
        Assert.assertEquals(3, columnAgg.count(new JsonPrimitive(true)));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(false)));
        Assert.assertEquals(0, columnAgg.count(new JsonPrimitive("other")));
    }

    @Test
    public void testMixedColumn() {
        ColumnAgg columnAgg = new ColumnAgg("a");
        columnAgg.agg(new JsonPrimitive("foo"));
        columnAgg.agg(new JsonPrimitive("true"));
        columnAgg.agg(new JsonPrimitive("1.2"));
        columnAgg.agg(new JsonPrimitive("10"));
        columnAgg.agg(new JsonPrimitive("20"));
        columnAgg.agg(new JsonPrimitive(1.2));
        columnAgg.agg(new JsonPrimitive(10));
        columnAgg.agg(new JsonPrimitive(15L));

        Assert.assertEquals("a", columnAgg.getColumnName());

        Assert.assertEquals(8, columnAgg.count());
        Assert.assertEquals(8, columnAgg.cardinality());
        Assert.assertEquals(5, columnAgg.getStringCount());
        Assert.assertEquals(0, columnAgg.getBooleanCount());
        Assert.assertEquals(3, columnAgg.getNumberCount());
        Assert.assertEquals(1.2, columnAgg.getMin().doubleValue(), 10E10);
        Assert.assertEquals(15, columnAgg.getMax().longValue());
        Assert.assertEquals(26.2, columnAgg.getSum().doubleValue(), 10E10);

        Assert.assertEquals(8, columnAgg.count());
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive("foo")));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive("true")));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive("1.2")));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive("10")));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive("20")));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(1.2)));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(10)));
        Assert.assertEquals(1, columnAgg.count(new JsonPrimitive(15L)));
        Assert.assertEquals(0, columnAgg.count(new JsonPrimitive(13)));
    }
}
