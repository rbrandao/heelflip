package com.skatepark.heelflip.column.type;

import org.junit.Assert;
import org.junit.Test;

public class ColumnTest {

    @Test
    public void testDoubleValues() {
        ObjectColumn column = new ObjectColumn("a");
        column.add(2.3);
        column.add(2.1);
        column.add(2.0);
        column.add(2.0);

        Assert.assertEquals("a", column.name());
        Assert.assertEquals(2, column.minAsInt());
        Assert.assertEquals(2, column.maxAsInt());
        Assert.assertEquals(8, column.sumAsInt());

        Assert.assertEquals(2, column.minAsLong());
        Assert.assertEquals(2, column.maxAsLong());
        Assert.assertEquals(8, column.sumAsLong());

        Assert.assertEquals(2.0, column.minAsDouble(), 10E10);
        Assert.assertEquals(2.3, column.maxAsDouble(), 10E10);
        Assert.assertEquals(8.4, column.sumAsDouble(), 10E10);

        Assert.assertEquals(4, column.count());
        Assert.assertEquals(2, column.count(2.0));
        Assert.assertEquals(0, column.count(1.0));

        Assert.assertEquals(1, column.valuesAsIntSet().size());
        Assert.assertEquals(1, column.valuesAsLongSet().size());
        Assert.assertEquals(3, column.valuesAsDoubleSet().size());
        Assert.assertEquals(3, column.valuesAsStringSet().size());
    }

    @Test
    public void testLongValues() {
        ObjectColumn column = new ObjectColumn("a");
        column.add(10);
        column.add(11);
        column.add(12);
        column.add(12);

        Assert.assertEquals("a", column.name());
        Assert.assertEquals(10, column.minAsInt());
        Assert.assertEquals(12, column.maxAsInt());
        Assert.assertEquals(45, column.sumAsInt());

        Assert.assertEquals(10, column.minAsLong());
        Assert.assertEquals(12, column.maxAsLong());
        Assert.assertEquals(45, column.sumAsLong());

        Assert.assertEquals(10, column.minAsDouble(), 10E10);
        Assert.assertEquals(12, column.maxAsDouble(), 10E10);
        Assert.assertEquals(45, column.sumAsDouble(), 10E10);

        Assert.assertEquals(4, column.count());
        Assert.assertEquals(2, column.count(12));
        Assert.assertEquals(0, column.count(13));

        Assert.assertEquals(3, column.valuesAsIntSet().size());
        Assert.assertEquals(3, column.valuesAsLongSet().size());
        Assert.assertEquals(3, column.valuesAsDoubleSet().size());
        Assert.assertEquals(3, column.valuesAsStringSet().size());
    }

    @Test
    public void testStringColumn() {
        ObjectColumn column = new ObjectColumn("a");
        column.add("foo");
        column.add("foo");
        column.add("boo");
        column.add("call");

        Assert.assertEquals("a", column.name());
        Assert.assertEquals(4, column.count());
        Assert.assertEquals(2, column.count("foo"));
        Assert.assertEquals(0, column.count("other"));

        Assert.assertEquals(0, column.valuesAsIntSet().size());
        Assert.assertEquals(0, column.valuesAsLongSet().size());
        Assert.assertEquals(0, column.valuesAsDoubleSet().size());
        Assert.assertEquals(3, column.valuesAsStringSet().size());
   }

    @Test
    public void testMixedColumn() {
        ObjectColumn column = new ObjectColumn("a");
        column.add("foo");
        column.add("true");
        column.add("1.2");
        column.add("10");
        column.add("20");
        column.add(1.2);
        column.add(10);
        column.add(15L);

        Assert.assertEquals("a", column.name());
        Assert.assertEquals(1, column.minAsInt());
        Assert.assertEquals(20, column.maxAsInt());
        Assert.assertEquals(57, column.sumAsInt());

        Assert.assertEquals(1, column.minAsLong());
        Assert.assertEquals(20, column.maxAsLong());
        Assert.assertEquals(57, column.sumAsLong());

        Assert.assertEquals(10, column.minAsDouble(), 10E10);
        Assert.assertEquals(12, column.maxAsDouble(), 10E10);
        Assert.assertEquals(45, column.sumAsDouble(), 10E10);

        Assert.assertEquals(8, column.count());
        Assert.assertEquals(2, column.count(10));
        Assert.assertEquals(0, column.count(13));

        Assert.assertEquals(4, column.valuesAsIntSet().size());
        Assert.assertEquals(4, column.valuesAsLongSet().size());
        Assert.assertEquals(4, column.valuesAsDoubleSet().size());
        Assert.assertEquals(6, column.valuesAsStringSet().size());
    }
}
