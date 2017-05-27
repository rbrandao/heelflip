package com.skatepark.heelflip.table;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class HeelflipTest {

    private static final String SAMPLE_FILE_PATH = "sample.json";
    private static final String SAMPLE_ARRAY_FILE_PATH = "sample_array.json";

    //http://jsonstudio.com/resources/
    private static final String STOCKS_FILE_PATH = "stocks.json";
    private static final String ZIPS_FILE_PATH = "zips.json";

    @Test
    public void testColumnNames() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

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
    public void testMinAsInt() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());


        Assert.assertEquals(0, heelflip.getStatistics("a").getMin().intValue());
        Assert.assertEquals(1, heelflip.getStatistics("b").getMin().intValue());
        Assert.assertEquals(2, heelflip.getStatistics("c").getMin().intValue());
        Assert.assertEquals(3, heelflip.getStatistics("d").getMin().intValue());
        Assert.assertEquals(4, heelflip.getStatistics("e").getMin().intValue());
        Assert.assertEquals(5, heelflip.getStatistics("f").getMin().intValue());
        Assert.assertEquals(6, heelflip.getStatistics("g").getMin().intValue());
        Assert.assertEquals(7, heelflip.getStatistics("h").getMin().intValue());
        Assert.assertEquals(8, heelflip.getStatistics("i").getMin().intValue());
        Assert.assertEquals(9, heelflip.getStatistics("j").getMin().intValue());
        Assert.assertNull(heelflip.getStatistics("l"));
    }

    @Test
    public void testMaxAsInt() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getStatistics("a").getMax().intValue());
        Assert.assertEquals(1, heelflip.getStatistics("b").getMax().intValue());
        Assert.assertEquals(2, heelflip.getStatistics("c").getMax().intValue());
        Assert.assertEquals(3, heelflip.getStatistics("d").getMax().intValue());
        Assert.assertEquals(4, heelflip.getStatistics("e").getMax().intValue());
        Assert.assertEquals(5, heelflip.getStatistics("f").getMax().intValue());
        Assert.assertEquals(6, heelflip.getStatistics("g").getMax().intValue());
        Assert.assertEquals(7, heelflip.getStatistics("h").getMax().intValue());
        Assert.assertEquals(8, heelflip.getStatistics("i").getMax().intValue());
        Assert.assertEquals(9, heelflip.getStatistics("j").getMax().intValue());
        Assert.assertNull(heelflip.getStatistics("l"));
    }

    @Test
    public void testSumAsInt() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getStatistics("a").getSum().intValue());
        Assert.assertEquals(20, heelflip.getStatistics("b").getSum().intValue());
        Assert.assertEquals(40, heelflip.getStatistics("c").getSum().intValue());
        Assert.assertEquals(60, heelflip.getStatistics("d").getSum().intValue());
        Assert.assertEquals(80, heelflip.getStatistics("e").getSum().intValue());
        Assert.assertEquals(100, heelflip.getStatistics("f").getSum().intValue());
        Assert.assertEquals(120, heelflip.getStatistics("g").getSum().intValue());
        Assert.assertEquals(140, heelflip.getStatistics("h").getSum().intValue());
        Assert.assertEquals(160, heelflip.getStatistics("i").getSum().intValue());
        Assert.assertEquals(180, heelflip.getStatistics("j").getSum().intValue());
        Assert.assertNull(heelflip.getStatistics("l"));
    }

    @Test
    public void testMinAsLong() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getStatistics("a").getMin().longValue());
        Assert.assertEquals(1, heelflip.getStatistics("b").getMin().longValue());
        Assert.assertEquals(2, heelflip.getStatistics("c").getMin().longValue());
        Assert.assertEquals(3, heelflip.getStatistics("d").getMin().longValue());
        Assert.assertEquals(4, heelflip.getStatistics("e").getMin().longValue());
        Assert.assertEquals(5, heelflip.getStatistics("f").getMin().longValue());
        Assert.assertEquals(6, heelflip.getStatistics("g").getMin().longValue());
        Assert.assertEquals(7, heelflip.getStatistics("h").getMin().longValue());
        Assert.assertEquals(8, heelflip.getStatistics("i").getMin().longValue());
        Assert.assertEquals(9, heelflip.getStatistics("j").getMin().longValue());
        Assert.assertNull(heelflip.getStatistics("l"));
    }

    @Test
    public void testMaxAsLong() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getStatistics("a").getMax().longValue());
        Assert.assertEquals(1, heelflip.getStatistics("b").getMax().longValue());
        Assert.assertEquals(2, heelflip.getStatistics("c").getMax().longValue());
        Assert.assertEquals(3, heelflip.getStatistics("d").getMax().longValue());
        Assert.assertEquals(4, heelflip.getStatistics("e").getMax().longValue());
        Assert.assertEquals(5, heelflip.getStatistics("f").getMax().longValue());
        Assert.assertEquals(6, heelflip.getStatistics("g").getMax().longValue());
        Assert.assertEquals(7, heelflip.getStatistics("h").getMax().longValue());
        Assert.assertEquals(8, heelflip.getStatistics("i").getMax().longValue());
        Assert.assertEquals(9, heelflip.getStatistics("j").getMax().longValue());
        Assert.assertNull(heelflip.getStatistics("l"));
    }

    @Test
    public void testSumAsLong() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getStatistics("a").getSum().longValue());
        Assert.assertEquals(20, heelflip.getStatistics("b").getSum().longValue());
        Assert.assertEquals(40, heelflip.getStatistics("c").getSum().longValue());
        Assert.assertEquals(60, heelflip.getStatistics("d").getSum().longValue());
        Assert.assertEquals(80, heelflip.getStatistics("e").getSum().longValue());
        Assert.assertEquals(100, heelflip.getStatistics("f").getSum().longValue());
        Assert.assertEquals(120, heelflip.getStatistics("g").getSum().longValue());
        Assert.assertEquals(140, heelflip.getStatistics("h").getSum().longValue());
        Assert.assertEquals(160, heelflip.getStatistics("i").getSum().longValue());
        Assert.assertEquals(180, heelflip.getStatistics("j").getSum().longValue());
        Assert.assertNull(heelflip.getStatistics("l"));
    }

    @Test
    public void testMinAsDouble() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getStatistics("a").getMin().doubleValue(), 10E10);
        Assert.assertEquals(1, heelflip.getStatistics("b").getMin().doubleValue(), 10E10);
        Assert.assertEquals(2, heelflip.getStatistics("c").getMin().doubleValue(), 10E10);
        Assert.assertEquals(3, heelflip.getStatistics("d").getMin().doubleValue(), 10E10);
        Assert.assertEquals(4, heelflip.getStatistics("e").getMin().doubleValue(), 10E10);
        Assert.assertEquals(5, heelflip.getStatistics("f").getMin().doubleValue(), 10E10);
        Assert.assertEquals(6, heelflip.getStatistics("g").getMin().doubleValue(), 10E10);
        Assert.assertEquals(7, heelflip.getStatistics("h").getMin().doubleValue(), 10E10);
        Assert.assertEquals(8, heelflip.getStatistics("i").getMin().doubleValue(), 10E10);
        Assert.assertEquals(9, heelflip.getStatistics("j").getMin().doubleValue(), 10E10);
        Assert.assertNull(heelflip.getStatistics("l"));
    }

    @Test
    public void testMaxAsDouble() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getStatistics("a").getMax().doubleValue(), 10E10);
        Assert.assertEquals(1, heelflip.getStatistics("b").getMax().doubleValue(), 10E10);
        Assert.assertEquals(2, heelflip.getStatistics("c").getMax().doubleValue(), 10E10);
        Assert.assertEquals(3, heelflip.getStatistics("d").getMax().doubleValue(), 10E10);
        Assert.assertEquals(4, heelflip.getStatistics("e").getMax().doubleValue(), 10E10);
        Assert.assertEquals(5, heelflip.getStatistics("f").getMax().doubleValue(), 10E10);
        Assert.assertEquals(6, heelflip.getStatistics("g").getMax().doubleValue(), 10E10);
        Assert.assertEquals(7, heelflip.getStatistics("h").getMax().doubleValue(), 10E10);
        Assert.assertEquals(8, heelflip.getStatistics("i").getMax().doubleValue(), 10E10);
        Assert.assertEquals(9, heelflip.getStatistics("j").getMax().doubleValue(), 10E10);
        Assert.assertNull(heelflip.getStatistics("l"));
    }

    @Test
    public void testSumAsDouble() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getStatistics("a").getSum().doubleValue(), 10E10);
        Assert.assertEquals(20, heelflip.getStatistics("b").getSum().doubleValue(), 10E10);
        Assert.assertEquals(40, heelflip.getStatistics("c").getSum().doubleValue(), 10E10);
        Assert.assertEquals(60, heelflip.getStatistics("d").getSum().doubleValue(), 10E10);
        Assert.assertEquals(80, heelflip.getStatistics("e").getSum().doubleValue(), 10E10);
        Assert.assertEquals(100, heelflip.getStatistics("f").getSum().doubleValue(), 10E10);
        Assert.assertEquals(120, heelflip.getStatistics("g").getSum().doubleValue(), 10E10);
        Assert.assertEquals(140, heelflip.getStatistics("h").getSum().doubleValue(), 10E10);
        Assert.assertEquals(160, heelflip.getStatistics("i").getSum().doubleValue(), 10E10);
        Assert.assertEquals(180, heelflip.getStatistics("j").getSum().doubleValue(), 10E10);
        Assert.assertNull(heelflip.getStatistics("l"));
    }

    @Test
    public void testValuesAsIntSet() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(1, heelflip.valuesAsIntSet("a").size());
        Assert.assertEquals(1, heelflip.valuesAsIntSet("b").size());
        Assert.assertEquals(1, heelflip.valuesAsIntSet("c").size());
        Assert.assertEquals(1, heelflip.valuesAsIntSet("d").size());
        Assert.assertEquals(1, heelflip.valuesAsIntSet("e").size());
        Assert.assertEquals(1, heelflip.valuesAsIntSet("f").size());
        Assert.assertEquals(1, heelflip.valuesAsIntSet("g").size());
        Assert.assertEquals(1, heelflip.valuesAsIntSet("h").size());
        Assert.assertEquals(1, heelflip.valuesAsIntSet("i").size());
        Assert.assertEquals(1, heelflip.valuesAsIntSet("j").size());
        Assert.assertEquals(0, heelflip.valuesAsIntSet("l").size());

        Assert.assertTrue(heelflip.valuesAsIntSet("a").contains(0));
        Assert.assertTrue(heelflip.valuesAsIntSet("b").contains(1));
        Assert.assertTrue(heelflip.valuesAsIntSet("c").contains(2));
        Assert.assertTrue(heelflip.valuesAsIntSet("d").contains(3));
        Assert.assertTrue(heelflip.valuesAsIntSet("e").contains(4));
        Assert.assertTrue(heelflip.valuesAsIntSet("f").contains(5));
        Assert.assertTrue(heelflip.valuesAsIntSet("g").contains(6));
        Assert.assertTrue(heelflip.valuesAsIntSet("h").contains(7));
        Assert.assertTrue(heelflip.valuesAsIntSet("i").contains(8));
        Assert.assertTrue(heelflip.valuesAsIntSet("j").contains(9));
    }

    @Test
    public void testValuesAsLongSet() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(1, heelflip.valuesAsLongSet("a").size());
        Assert.assertEquals(1, heelflip.valuesAsLongSet("b").size());
        Assert.assertEquals(1, heelflip.valuesAsLongSet("c").size());
        Assert.assertEquals(1, heelflip.valuesAsLongSet("d").size());
        Assert.assertEquals(1, heelflip.valuesAsLongSet("e").size());
        Assert.assertEquals(1, heelflip.valuesAsLongSet("f").size());
        Assert.assertEquals(1, heelflip.valuesAsLongSet("g").size());
        Assert.assertEquals(1, heelflip.valuesAsLongSet("h").size());
        Assert.assertEquals(1, heelflip.valuesAsLongSet("i").size());
        Assert.assertEquals(1, heelflip.valuesAsLongSet("j").size());
        Assert.assertEquals(0, heelflip.valuesAsLongSet("l").size());

        Assert.assertTrue(heelflip.valuesAsLongSet("a").contains(0L));
        Assert.assertTrue(heelflip.valuesAsLongSet("b").contains(1L));
        Assert.assertTrue(heelflip.valuesAsLongSet("c").contains(2L));
        Assert.assertTrue(heelflip.valuesAsLongSet("d").contains(3L));
        Assert.assertTrue(heelflip.valuesAsLongSet("e").contains(4L));
        Assert.assertTrue(heelflip.valuesAsLongSet("f").contains(5L));
        Assert.assertTrue(heelflip.valuesAsLongSet("g").contains(6L));
        Assert.assertTrue(heelflip.valuesAsLongSet("h").contains(7L));
        Assert.assertTrue(heelflip.valuesAsLongSet("i").contains(8L));
        Assert.assertTrue(heelflip.valuesAsLongSet("j").contains(9L));
    }

    @Test
    public void testValuesAsDoubleSet() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("a").size());
        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("b").size());
        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("c").size());
        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("d").size());
        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("e").size());
        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("f").size());
        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("g").size());
        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("h").size());
        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("i").size());
        Assert.assertEquals(1, heelflip.valuesAsDoubleSet("j").size());
        Assert.assertEquals(0, heelflip.valuesAsDoubleSet("l").size());

        Assert.assertTrue(heelflip.valuesAsDoubleSet("a").contains(0.0));
        Assert.assertTrue(heelflip.valuesAsDoubleSet("b").contains(1.0));
        Assert.assertTrue(heelflip.valuesAsDoubleSet("c").contains(2.0));
        Assert.assertTrue(heelflip.valuesAsDoubleSet("d").contains(3.0));
        Assert.assertTrue(heelflip.valuesAsDoubleSet("e").contains(4.0));
        Assert.assertTrue(heelflip.valuesAsDoubleSet("f").contains(5.0));
        Assert.assertTrue(heelflip.valuesAsDoubleSet("g").contains(6.0));
        Assert.assertTrue(heelflip.valuesAsDoubleSet("h").contains(7.0));
        Assert.assertTrue(heelflip.valuesAsDoubleSet("i").contains(8.0));
        Assert.assertTrue(heelflip.valuesAsDoubleSet("j").contains(9.0));
    }

    @Test
    public void testValuesAsStringSet() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(1, heelflip.valuesAsStringSet("a").size());
        Assert.assertEquals(1, heelflip.valuesAsStringSet("b").size());
        Assert.assertEquals(1, heelflip.valuesAsStringSet("c").size());
        Assert.assertEquals(1, heelflip.valuesAsStringSet("d").size());
        Assert.assertEquals(1, heelflip.valuesAsStringSet("e").size());
        Assert.assertEquals(1, heelflip.valuesAsStringSet("f").size());
        Assert.assertEquals(1, heelflip.valuesAsStringSet("g").size());
        Assert.assertEquals(1, heelflip.valuesAsStringSet("h").size());
        Assert.assertEquals(1, heelflip.valuesAsStringSet("i").size());
        Assert.assertEquals(1, heelflip.valuesAsStringSet("j").size());
        Assert.assertEquals(0, heelflip.valuesAsStringSet("l").size());

        Assert.assertTrue(heelflip.valuesAsStringSet("a").contains("0"));
        Assert.assertTrue(heelflip.valuesAsStringSet("b").contains("1"));
        Assert.assertTrue(heelflip.valuesAsStringSet("c").contains("2"));
        Assert.assertTrue(heelflip.valuesAsStringSet("d").contains("3"));
        Assert.assertTrue(heelflip.valuesAsStringSet("e").contains("4"));
        Assert.assertTrue(heelflip.valuesAsStringSet("f").contains("5"));
        Assert.assertTrue(heelflip.valuesAsStringSet("g").contains("6"));
        Assert.assertTrue(heelflip.valuesAsStringSet("h").contains("7"));
        Assert.assertTrue(heelflip.valuesAsStringSet("i").contains("8"));
        Assert.assertTrue(heelflip.valuesAsStringSet("j").contains("9"));
    }

    @Test
    public void testCountWithArray() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_ARRAY_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(3, heelflip.size());

        Assert.assertEquals(1, heelflip.count("a"));
        Assert.assertEquals(2, heelflip.count("b.x"));
        Assert.assertEquals(2, heelflip.count("b.y"));

        Assert.assertEquals(9, heelflip.getStatistics("b.x").getMin().intValue());
        Assert.assertEquals(10, heelflip.getStatistics("b.y").getMin().intValue());
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
