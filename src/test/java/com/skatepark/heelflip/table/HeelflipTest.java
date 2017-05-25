package com.skatepark.heelflip.table;

import org.junit.Assert;
import org.junit.Test;

public class HeelflipTest {

    @Test
    public void foo() {
        Heelflip heelflip = new Heelflip("table");
        heelflip.add("a", 10);
        heelflip.add("a", 20);
        heelflip.add("a", 30);
        heelflip.add("a", 30);

        heelflip.add("b", 1.2);
        heelflip.add("b", 1.3);
        heelflip.add("b", 1.4);
        heelflip.add("b", 1.5);
        heelflip.add("b", 1.5);

        heelflip.add("c", "foo");
        heelflip.add("c", "foo");
        heelflip.add("c", "boo");
        heelflip.add("c", "boo");

        Assert.assertEquals(3, heelflip.size());
        Assert.assertTrue(heelflip.contains("a"));
        Assert.assertTrue(heelflip.contains("b"));
        Assert.assertTrue(heelflip.contains("c"));
        Assert.assertFalse(heelflip.contains("d"));

        Assert.assertEquals(10, heelflip.minAsLong("a"));
        Assert.assertEquals(30, heelflip.maxAsLong("a"));
        Assert.assertEquals(10, heelflip.minAsDouble("a"), 10E10);
        Assert.assertEquals(30, heelflip.maxAsDouble("a"), 10E10);

        Assert.assertEquals(1, heelflip.minAsLong("b"));
        Assert.assertEquals(1, heelflip.maxAsLong("b"));
        Assert.assertEquals(1.2, heelflip.minAsDouble("b"), 10E10);
        Assert.assertEquals(1.5, heelflip.maxAsDouble("b"), 10E10);

        Assert.assertEquals(-1, heelflip.minAsLong("c"));
        Assert.assertEquals(-1, heelflip.maxAsLong("c"));
        Assert.assertEquals(-1, heelflip.minAsDouble("c"), 10E10);
        Assert.assertEquals(-1, heelflip.maxAsDouble("c"), 10E10);

        Assert.assertEquals(-1, heelflip.minAsLong("d"));
        Assert.assertEquals(-1, heelflip.maxAsLong("d"));
        Assert.assertEquals(-1, heelflip.minAsDouble("d"), 10E10);
        Assert.assertEquals(-1, heelflip.maxAsDouble("d"), 10E10);
    }
}
