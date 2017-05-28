package com.skatepark.heelflip.table;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class HeelflipTest {

    private static final String SAMPLE_00 = "sample00.json";
    private static final String SAMPLE_01 = "sample01.json";

    //http://jsonstudio.com/resources/
    private static final String STOCKS_FILE_PATH = "stocks.json";
    private static final String ZIPS_FILE_PATH = "zips.json";

    @Test
    public void testColumnNames() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_00);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Set<String> names = heelflip.columnNames();
        Assert.assertTrue(names.contains("a"));
        Assert.assertTrue(names.contains("b"));
        Assert.assertTrue(names.contains("c"));
        Assert.assertTrue(names.contains("d"));
        Assert.assertTrue(names.contains("e"));
        Assert.assertTrue(names.contains("f"));
        Assert.assertTrue(names.contains("g"));
        Assert.assertTrue(names.contains("h"));
        Assert.assertTrue(names.contains("i"));
        Assert.assertTrue(names.contains("j"));
        Assert.assertFalse(names.contains("l"));
    }

    @Test
    public void testMin() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_00);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getColumnAgg("a").getMin().intValue());
        Assert.assertEquals(1, heelflip.getColumnAgg("b").getMin().intValue());
        Assert.assertEquals(2, heelflip.getColumnAgg("c").getMin().intValue());
        Assert.assertEquals(3, heelflip.getColumnAgg("d").getMin().intValue());
        Assert.assertEquals(4, heelflip.getColumnAgg("e").getMin().intValue());
        Assert.assertEquals(5, heelflip.getColumnAgg("f").getMin().intValue());
        Assert.assertEquals(6, heelflip.getColumnAgg("g").getMin().intValue());
        Assert.assertEquals(7, heelflip.getColumnAgg("h").getMin().intValue());
        Assert.assertEquals(8, heelflip.getColumnAgg("i").getMin().intValue());
        Assert.assertEquals(9, heelflip.getColumnAgg("j").getMin().intValue());
        Assert.assertNull(heelflip.getColumnAgg("l"));
    }

    @Test
    public void testMax() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_00);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getColumnAgg("a").getMax().intValue());
        Assert.assertEquals(1, heelflip.getColumnAgg("b").getMax().intValue());
        Assert.assertEquals(2, heelflip.getColumnAgg("c").getMax().intValue());
        Assert.assertEquals(3, heelflip.getColumnAgg("d").getMax().intValue());
        Assert.assertEquals(4, heelflip.getColumnAgg("e").getMax().intValue());
        Assert.assertEquals(5, heelflip.getColumnAgg("f").getMax().intValue());
        Assert.assertEquals(6, heelflip.getColumnAgg("g").getMax().intValue());
        Assert.assertEquals(7, heelflip.getColumnAgg("h").getMax().intValue());
        Assert.assertEquals(8, heelflip.getColumnAgg("i").getMax().intValue());
        Assert.assertEquals(9, heelflip.getColumnAgg("j").getMax().intValue());
        Assert.assertNull(heelflip.getColumnAgg("l"));
    }

    @Test
    public void testSum() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_00);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getColumnAgg("a").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("b").getSum().intValue());
        Assert.assertEquals(40, heelflip.getColumnAgg("c").getSum().intValue());
        Assert.assertEquals(60, heelflip.getColumnAgg("d").getSum().intValue());
        Assert.assertEquals(80, heelflip.getColumnAgg("e").getSum().intValue());
        Assert.assertEquals(100, heelflip.getColumnAgg("f").getSum().intValue());
        Assert.assertEquals(120, heelflip.getColumnAgg("g").getSum().intValue());
        Assert.assertEquals(140, heelflip.getColumnAgg("h").getSum().intValue());
        Assert.assertEquals(160, heelflip.getColumnAgg("i").getSum().intValue());
        Assert.assertEquals(180, heelflip.getColumnAgg("j").getSum().intValue());
        Assert.assertNull(heelflip.getColumnAgg("l"));
    }

    @Test
    public void testLoadLargeFiles() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(STOCKS_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);
        Assert.assertEquals(69, heelflip.size());

        stream = getClass().getClassLoader().getResourceAsStream(ZIPS_FILE_PATH);
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(75, heelflip.size());
    }
}
